from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.config import Settings, get_settings
from ml_etl.db import repository, session_scope
from ml_etl.db.models import PUBLISHABLE_STATUSES
from ml_etl.domain.buckets import bucket_key_for
from ml_etl.domain.rejection import RejectionCollector
from ml_etl.logging_config import get_logger
from ml_etl.pipeline import enrich, extract, filters, load, price_index, publish, quality, raw, score, transform
from ml_etl.pipeline import offers as offers_module

logger = get_logger(__name__)


@dataclass
class PipelineResult:
    run_id: str
    bucket_key: str
    status: str
    stats: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None
    quality_report: dict[str, Any] | None = None
    published_snapshot_id: int | None = None


def _previous_ranks(session, bucket_key: str) -> dict[tuple[str, str, str], int]:
    snapshots = repository.fetch_previous_snapshots(session, bucket_key)
    return {
        (snapshot.external_type, snapshot.external_id, snapshot.category_id): snapshot.rank
        for snapshot in snapshots
    }


@dataclass
class CollectedData:
    payloads: list[extract.RawPayload] = field(default_factory=list)
    entities: dict[tuple[str, str], enrich.EnrichedEntity] = field(default_factory=dict)
    categories: dict[str, dict[str, Any]] = field(default_factory=dict)
    offers: dict[tuple[str, str], offers_module.ProductOffers] = field(default_factory=dict)


def _persist_stage(
    settings: Settings,
    run_id: str,
    bucket_key: str,
    payloads: Sequence[extract.RawPayload],
) -> None:
    with session_scope(settings) as session:
        raw.persist_raw(session, run_id, bucket_key, payloads)


def _replay_payloads(settings: Settings, source_run_id: str) -> CollectedData:
    with session_scope(settings) as session:
        payloads = raw.load_raw_for_run(session, source_run_id)
    entities = _entities_from_raw(payloads)
    replayed_offers = offers_module.offers_from_raw(payloads)
    for key, entity in entities.items():
        enrich.apply_offers(entity, replayed_offers.get(key))
    return CollectedData(
        payloads=payloads,
        entities=entities,
        categories=enrich.categories_from_raw(payloads),
        offers=replayed_offers,
    )


def _collect_payloads(
    run_id: str,
    bucket_key: str,
    categories: Sequence[str],
    settings: Settings,
    collector: RejectionCollector,
) -> CollectedData:
    with MercadoLivreApi(settings=settings) as api:
        extracted = extract.extract_all(api, categories, settings)
        collector.extend(extracted.rejections)
        _persist_stage(settings, run_id, bucket_key, extracted.payloads)

        keys = transform.highlight_keys(extracted.payloads)
        enriched = enrich.enrich_entities(api, keys, settings)
        collector.extend(enriched.rejections)
        _persist_stage(settings, run_id, bucket_key, enriched.payloads)

        catalog_keys = [key for key in enriched.entities if key[0] == enrich.TYPE_PRODUCT]
        collected_offers = offers_module.collect_offers(api, catalog_keys, settings)
        collector.extend(collected_offers.rejections)
        _persist_stage(settings, run_id, bucket_key, collected_offers.payloads)

        for key, entity in enriched.entities.items():
            enrich.apply_offers(entity, collected_offers.offers.get(key))

        with session_scope(settings) as session:
            known = repository.fetch_cached_categories(session)
        entity_categories = [
            entity.category_id for entity in enriched.entities.values() if entity.category_id
        ]
        resolved = enrich.resolve_categories(
            api,
            [*categories, *entity_categories],
            known=known,
            settings=settings,
        )
        collector.extend(resolved.rejections)
        _persist_stage(settings, run_id, bucket_key, resolved.payloads)

    return CollectedData(
        payloads=[*extracted.payloads, *enriched.payloads, *collected_offers.payloads, *resolved.payloads],
        entities=enriched.entities,
        categories=resolved.categories,
        offers=collected_offers.offers,
    )


def _observed_at(payloads: Sequence[extract.RawPayload], fallback: datetime) -> datetime:
    moments = [
        moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
        for moment in (payload.collected_at for payload in payloads)
        if moment is not None
    ]
    return max(moments) if moments else fallback


def _entities_from_raw(
    payloads: Sequence[extract.RawPayload],
) -> dict[tuple[str, str], enrich.EnrichedEntity]:
    entities: dict[tuple[str, str], enrich.EnrichedEntity] = {}

    for payload in payloads:
        if payload.stage != extract.STAGE_ENRICH:
            continue
        endpoint = payload.endpoint
        body = payload.payload

        if offers_module.product_id_from_endpoint(endpoint):
            continue

        if endpoint == "/items" and isinstance(body, list):
            for entry in body:
                if not isinstance(entry, dict):
                    continue
                item_body = entry.get("body") or {}
                external_id = item_body.get("id")
                if not external_id:
                    continue
                entity = enrich.entity_from_body(
                    enrich.TYPE_ITEM, str(external_id), item_body, partial=entry.get("code") == 206
                )
                entities[entity.key] = entity
            continue

        if not isinstance(body, dict):
            continue

        external_id = body.get("id")
        if not external_id:
            continue

        if endpoint.startswith("/items/"):
            external_type = enrich.TYPE_ITEM
        elif endpoint.startswith("/products/"):
            external_type = enrich.TYPE_PRODUCT
        elif endpoint.startswith("/user-products/"):
            external_type = enrich.TYPE_USER_PRODUCT
        else:
            continue

        entity = enrich.entity_from_body(
            external_type, str(external_id), body, partial=payload.status_code == 206
        )
        entities[entity.key] = entity

    logger.info("RAW replay: %s entidades reconstruidas", len(entities))
    return entities


def _category_names(session, resolved: dict[str, dict[str, Any]]) -> dict[str, str]:
    names = {
        category_id: row.name for category_id, row in repository.fetch_cached_categories(session).items() if row.name
    }
    for category_id, payload in resolved.items():
        name = payload.get("name")
        if name:
            names[category_id] = name
    return names


def run_pipeline(
    settings: Settings | None = None,
    categories: Sequence[str] | None = None,
    bucket: str | None = None,
    run_id: str | None = None,
    skip_api: bool = False,
) -> PipelineResult:
    active = settings or get_settings()
    granularity = bucket or active.etl.snapshot_bucket
    target_categories = list(categories or active.etl.target_categories)
    started_at = datetime.now(timezone.utc)
    bucket_key = bucket_key_for(started_at, granularity)
    collector = RejectionCollector()

    if skip_api and not run_id:
        raise ValueError("--run-id é obrigatório quando o reprocessamento ignora a API")

    if not skip_api:
        active.mercado_livre.require_credentials()

    params: dict[str, Any] = {
        "categories": target_categories,
        "granularity": granularity,
        "skip_api": skip_api,
    }

    if skip_api:
        with session_scope(active) as session:
            source = repository.get_run(session, run_id)
            if source is None:
                raise repository.RunNotFoundError(run_id)
            params["replay_of"] = (source.params or {}).get("replay_of") or source.run_id
            bucket_key = source.bucket_key
            if categories is None:
                target_categories = list((source.params or {}).get("categories") or target_categories)
                params["categories"] = target_categories
        effective_run_id = str(uuid.uuid4())
    else:
        effective_run_id = run_id or str(uuid.uuid4())

    with session_scope(active) as session:
        repository.create_run(session, effective_run_id, bucket_key, params)

    stats: dict[str, Any] = dict(params)

    logger.info(
        "Pipeline iniciado run_id=%s bucket=%s categorias=%s skip_api=%s",
        effective_run_id,
        bucket_key,
        target_categories,
        skip_api,
    )

    try:
        if skip_api:
            collected = _replay_payloads(active, params["replay_of"])
            observed_at = _observed_at(collected.payloads, started_at)
        else:
            collected = _collect_payloads(effective_run_id, bucket_key, target_categories, active, collector)
            observed_at = started_at
        stats["raw_payloads"] = len(collected.payloads)

        with session_scope(active) as session:
            previous = _previous_ranks(session, bucket_key)
            previous_medians = repository.fetch_previous_price_medians(session, bucket_key)
            transformed = transform.build_transform_result(
                collected.payloads,
                collected.entities,
                previous,
                collected.offers,
                previous_medians,
            )
            collector.extend(transformed.rejections)

            filtered, filter_rejections = filters.filter_small_appliances(transformed.highlights)
            collector.extend(filter_rejections)

            prior_buckets = repository.count_prior_buckets(session, bucket_key)
            scored, score_rejections = score.score_highlights(filtered, transformed.trends, prior_buckets, active)
            collector.extend(score_rejections)

            category_names = _category_names(session, collected.categories)
            current_prices = price_index.points_from_offers(collected.offers, collected.entities)
            price_history = price_index.history_from_observations(
                repository.fetch_previous_domain_prices(
                    session, bucket_key, {point.domain_id for point in current_prices}
                )
            )
            movements = price_index.compute_movements(bucket_key, current_prices, price_history)
            report = quality.evaluate(
                target_categories, collected.payloads, scored, collector.items, active.quality
            )

            stats.update(
                {
                    "entities": load.load_entities(session, collected.entities),
                    "categories_loaded": load.load_categories(session, collected.categories),
                    "snapshots": load.load_snapshots(session, transformed.highlights, effective_run_id, bucket_key),
                    "trends": load.load_trends(session, transformed.trends, effective_run_id, bucket_key),
                    "price_observations": load.load_price_observations(
                        session, collected.offers, effective_run_id, bucket_key, observed_at, collected.entities
                    ),
                    "price_movements": load.load_price_movements(session, movements, effective_run_id),
                    "scores": load.load_scores(session, scored, effective_run_id, bucket_key),
                    "curated": load.load_curated(session, scored, effective_run_id, bucket_key, category_names),
                    "rejections": len(collector),
                    "rejections_by_rule": collector.summary(),
                    "bucket_key": bucket_key,
                    "quality": str(report.verdict),
                }
            )

            status = report.run_status
            error_message = None
            if status == "FAILED":
                error_message = "quality gate reprovou: " + ", ".join(report.failures())
            load.load_rejections(session, collector.rows(effective_run_id))
            repository.finish_run(
                session, effective_run_id, status, stats, error_message, quality_report=report.as_dict()
            )

        logger.info("Pipeline concluido run_id=%s status=%s stats=%s", effective_run_id, status, stats)
        result = PipelineResult(
            run_id=effective_run_id,
            bucket_key=bucket_key,
            status=status,
            stats=stats,
            error_message=error_message,
            quality_report=report.as_dict(),
        )

    except Exception as exc:
        logger.exception("Pipeline falhou run_id=%s", effective_run_id)
        stats["rejections"] = len(collector)
        stats["rejections_by_rule"] = collector.summary()
        with session_scope(active) as session:
            load.load_rejections(session, collector.rows(effective_run_id))
            repository.finish_run(session, effective_run_id, "FAILED", stats, str(exc)[:2000])
        return PipelineResult(
            run_id=effective_run_id,
            bucket_key=bucket_key,
            status="FAILED",
            stats=stats,
            error_message=str(exc),
        )

    if active.auto_publish and status in PUBLISHABLE_STATUSES:
        result.published_snapshot_id = _auto_publish(active, effective_run_id)
    return result


def _auto_publish(settings: Settings, run_id: str) -> int | None:
    try:
        with session_scope(settings) as session:
            return publish.publish_run(session, run_id).snapshot_id
    except Exception:
        logger.exception("Publicação automática falhou run_id=%s", run_id)
        return None
