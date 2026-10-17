from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from ml_etl.db.models import (
    EtlRejection,
    EtlRun,
    MarketCategory,
    MarketCuratedProduct,
    MarketEntity,
    MarketPriceObservation,
    MarketRawResponse,
    MarketScore,
    MarketSnapshot,
    MarketTrend,
)
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 500


class RunNotFoundError(LookupError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"run_id {run_id} não encontrado")
        self.run_id = run_id

UPSERT_SPECS: dict[Any, tuple[tuple[str, ...], tuple[str, ...]]] = {
    MarketCategory: (
        ("category_id",),
        ("name", "parent_id", "path_from_root", "total_items", "permalink", "updated_at"),
    ),
    MarketEntity: (
        ("external_type", "external_id"),
        (
            "title",
            "canonical_title",
            "category_id",
            "domain_id",
            "brand",
            "brand_slug",
            "model",
            "product_key",
            "color",
            "voltage",
            "power_watts",
            "capacity_value",
            "capacity_unit",
            "energy_efficiency",
            "price",
            "available_quantity",
            "condition",
            "permalink",
            "attributes",
            "enrich_status",
            "updated_at",
        ),
    ),
    MarketSnapshot: (
        ("source", "external_type", "external_id", "category_id", "bucket_key"),
        ("rank", "rank_band", "collected_at", "run_id", "updated_at"),
    ),
    MarketTrend: (
        ("category_id", "keyword", "bucket_key"),
        ("canonical_keyword", "url", "position", "collected_at", "run_id", "updated_at"),
    ),
    MarketScore: (
        ("external_type", "external_id", "bucket_key"),
        ("score", "priority_label", "confidence", "components", "computed_at", "run_id", "updated_at"),
    ),
    MarketPriceObservation: (
        ("external_type", "external_id", "bucket_key"),
        (
            "offer_count",
            "seller_count",
            "official_store_count",
            "price_min",
            "price_p25",
            "price_median",
            "price_p75",
            "price_max",
            "original_price_median",
            "discount_pct_median",
            "winner_item_id",
            "winner_price",
            "currency_id",
            "category_id",
            "collected_at",
            "run_id",
            "updated_at",
        ),
    ),
    MarketCuratedProduct: (
        ("source", "external_type", "external_id", "bucket_key"),
        (
            "collected_at",
            "rank",
            "title",
            "score",
            "canonical_title",
            "rank_band",
            "rank_delta",
            "trajectory_label",
            "category_id",
            "category_name",
            "domain_id",
            "brand",
            "brand_slug",
            "model",
            "product_key",
            "voltage",
            "power_watts",
            "capacity_value",
            "capacity_unit",
            "energy_efficiency",
            "price",
            "price_min",
            "price_median",
            "price_max",
            "price_delta_pct",
            "discount_pct",
            "offer_count",
            "seller_count",
            "relevance",
            "domain_reason",
            "permalink",
            "priority_label",
            "confidence",
            "run_id",
            "updated_at",
        ),
    ),
    MarketRawResponse: (
        ("run_id", "payload_hash"),
        ("status_code", "payload", "collected_at"),
    ),
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _chunked(rows: Sequence[dict[str, Any]], size: int) -> Iterable[Sequence[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _deduplicate(rows: Sequence[dict[str, Any]], conflict_cols: Sequence[str]) -> list[dict[str, Any]]:
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(col) for col in conflict_cols)
        merged[key] = row
    return list(merged.values())


def upsert(session: Session, model: Any, rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if model not in UPSERT_SPECS:
        raise ValueError(f"Nenhuma especificação de upsert registrada para {model.__name__}")

    conflict_cols, update_cols = UPSERT_SPECS[model]
    now = _utcnow()
    prepared = []
    for row in rows:
        enriched = dict(row)
        if "updated_at" in update_cols:
            enriched.setdefault("created_at", now)
            enriched["updated_at"] = now
        prepared.append(enriched)

    prepared = _deduplicate(prepared, conflict_cols)
    dialect = session.get_bind().dialect.name
    insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert

    affected = 0
    for chunk in _chunked(prepared, CHUNK_SIZE):
        stmt = insert_fn(model).values(list(chunk))
        columns_present = set(chunk[0].keys())
        assignments = {col: stmt.excluded[col] for col in update_cols if col in columns_present}
        if assignments:
            stmt = stmt.on_conflict_do_update(index_elements=list(conflict_cols), set_=assignments)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=list(conflict_cols))
        session.execute(stmt)
        affected += len(chunk)

    logger.debug("Upsert em %s: %s linhas", model.__tablename__, affected)
    return affected


def create_run(session: Session, run_id: str, bucket_key: str, params: dict[str, Any]) -> EtlRun:
    run = EtlRun(run_id=run_id, bucket_key=bucket_key, params=params, status="RUNNING", started_at=_utcnow())
    session.add(run)
    session.flush()
    return run


def finish_run(
    session: Session,
    run_id: str,
    status: str,
    stats: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    run = session.get(EtlRun, run_id)
    if run is None:
        raise RunNotFoundError(run_id)
    run.status = status
    run.stats = stats
    run.error_message = error_message
    run.finished_at = _utcnow()
    session.flush()


def record_rejections(session: Session, rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0
    session.execute(EtlRejection.__table__.insert(), list(rows))
    return len(rows)


def fetch_raw_payloads(session: Session, run_id: str, stage: str | None = None) -> list[MarketRawResponse]:
    stmt = select(MarketRawResponse).where(MarketRawResponse.run_id == run_id)
    if stage:
        stmt = stmt.where(MarketRawResponse.stage == stage)
    return list(session.execute(stmt.order_by(MarketRawResponse.id)).scalars())


def fetch_previous_snapshots(session: Session, bucket_key: str) -> list[MarketSnapshot]:
    previous_bucket = select(func.max(MarketSnapshot.bucket_key)).where(MarketSnapshot.bucket_key < bucket_key)
    stmt = select(MarketSnapshot).where(MarketSnapshot.bucket_key == previous_bucket.scalar_subquery())
    return list(session.execute(stmt).scalars())


def count_prior_buckets(session: Session, bucket_key: str) -> dict[tuple[str, str], int]:
    stmt = (
        select(
            MarketSnapshot.external_type,
            MarketSnapshot.external_id,
            func.count(func.distinct(MarketSnapshot.bucket_key)),
        )
        .where(MarketSnapshot.bucket_key < bucket_key)
        .group_by(MarketSnapshot.external_type, MarketSnapshot.external_id)
    )
    return {(row[0], row[1]): row[2] for row in session.execute(stmt)}


def fetch_previous_price_medians(session: Session, bucket_key: str) -> dict[tuple[str, str], float]:
    previous_bucket = select(func.max(MarketPriceObservation.bucket_key)).where(
        MarketPriceObservation.bucket_key < bucket_key
    )
    stmt = select(
        MarketPriceObservation.external_type,
        MarketPriceObservation.external_id,
        MarketPriceObservation.price_median,
    ).where(MarketPriceObservation.bucket_key == previous_bucket.scalar_subquery())
    return {(row[0], row[1]): float(row[2]) for row in session.execute(stmt) if row[2] is not None}


def fetch_cached_categories(session: Session) -> dict[str, MarketCategory]:
    stmt = select(MarketCategory)
    return {row.category_id: row for row in session.execute(stmt).scalars()}


def get_run(session: Session, run_id: str) -> EtlRun | None:
    return session.get(EtlRun, run_id)


def recent_runs(session: Session, limit: int = 10) -> list[EtlRun]:
    stmt = select(EtlRun).order_by(EtlRun.started_at.desc()).limit(limit)
    return list(session.execute(stmt).scalars())


def table_counts(session: Session) -> dict[str, int]:
    models = [
        EtlRun,
        MarketRawResponse,
        MarketCategory,
        MarketEntity,
        MarketSnapshot,
        MarketTrend,
        MarketPriceObservation,
        MarketScore,
        MarketCuratedProduct,
        EtlRejection,
    ]
    counts = {}
    for model in models:
        counts[model.__tablename__] = int(session.execute(select(func.count()).select_from(model)).scalar() or 0)
    return counts
