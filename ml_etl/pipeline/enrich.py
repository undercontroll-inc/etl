from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

from ml_etl.api.http_client import ApiError
from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.config import Settings, get_settings
from ml_etl.domain.rejection import MISSING_ENRICHMENT, Layer, Rejection, Severity, Stage
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.extract import STAGE_ENRICH, RawPayload

logger = get_logger(__name__)

TYPE_ITEM = "ITEM"
TYPE_PRODUCT = "PRODUCT"
TYPE_USER_PRODUCT = "USER_PRODUCT"

BRAND_ATTRIBUTE_IDS = ("BRAND", "MARCA")


@dataclass
class EnrichedEntity:
    external_type: str
    external_id: str
    title: str
    category_id: str | None = None
    brand: str | None = None
    price: float | None = None
    available_quantity: int | None = None
    condition: str | None = None
    permalink: str | None = None
    attributes: list[dict[str, Any]] = field(default_factory=list)
    enrich_status: str = "OK"

    @property
    def key(self) -> tuple[str, str]:
        return (self.external_type, self.external_id)


@dataclass
class EnrichResult:
    entities: dict[tuple[str, str], EnrichedEntity] = field(default_factory=dict)
    payloads: list[RawPayload] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)
    categories: dict[str, dict[str, Any]] = field(default_factory=dict)

    def merge(self, other: EnrichResult) -> None:
        self.entities.update(other.entities)
        self.payloads.extend(other.payloads)
        self.rejections.extend(other.rejections)
        self.categories.update(other.categories)


def _extract_brand(attributes: Sequence[dict[str, Any]] | None) -> str | None:
    for attribute in attributes or []:
        identifier = (attribute.get("id") or "").upper()
        if identifier in BRAND_ATTRIBUTE_IDS:
            value = attribute.get("value_name") or attribute.get("values", [{}])[0].get("name")
            if value:
                return str(value)[:120]
    return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def entity_from_body(external_type: str, external_id: str, body: dict[str, Any], partial: bool) -> EnrichedEntity:
    title = body.get("title") or body.get("name") or ""
    attributes = body.get("attributes") or []
    brand = _extract_brand(attributes) or body.get("brand")
    price = _as_float(body.get("price"))
    if price is None:
        buy_box = body.get("buy_box_winner") or {}
        price = _as_float(buy_box.get("price"))

    return EnrichedEntity(
        external_type=external_type,
        external_id=external_id,
        title=str(title),
        category_id=body.get("category_id") or body.get("domain_id"),
        brand=brand,
        price=price,
        available_quantity=_as_int(body.get("available_quantity")),
        condition=body.get("condition") or body.get("status"),
        permalink=body.get("permalink"),
        attributes=attributes if isinstance(attributes, list) else [],
        enrich_status="PARTIAL" if partial else "OK",
    )


def _missing_rejection(external_type: str, external_id: str, detail: str) -> Rejection:
    return Rejection(
        stage=Stage.ENRICH,
        layer=Layer.INTEGRIDADE,
        rule=MISSING_ENRICHMENT,
        severity=Severity.AVISO,
        external_type=external_type,
        external_id=external_id,
        detail=detail,
    )


def _enrich_items(api: MercadoLivreApi, item_ids: Sequence[str]) -> EnrichResult:
    result = EnrichResult()
    if not item_ids:
        return result

    pending = set(item_ids)
    try:
        responses = list(api.iter_item_batches(list(item_ids)))
    except ApiError as exc:
        for item_id in item_ids:
            result.rejections.append(_missing_rejection(TYPE_ITEM, item_id, f"multiget falhou: {exc}"))
        return result

    for response in responses:
        result.payloads.append(
            RawPayload(
                stage=STAGE_ENRICH,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params=response.params,
            )
        )
        entries = response.payload if isinstance(response.payload, list) else []
        for entry in entries:
            code = entry.get("code")
            body = entry.get("body") or {}
            external_id = body.get("id") or entry.get("id")
            if not external_id:
                continue
            pending.discard(external_id)
            if code not in (200, 206):
                result.rejections.append(
                    _missing_rejection(TYPE_ITEM, str(external_id), f"multiget retornou code {code}")
                )
                continue
            entity = entity_from_body(TYPE_ITEM, str(external_id), body, partial=code == 206)
            result.entities[entity.key] = entity

    for item_id in pending:
        result.rejections.append(_missing_rejection(TYPE_ITEM, item_id, "id ausente na resposta do multiget"))

    return result


def _enrich_single(api: MercadoLivreApi, external_type: str, external_id: str) -> EnrichResult:
    result = EnrichResult()
    fetcher = {
        TYPE_PRODUCT: api.get_product,
        TYPE_USER_PRODUCT: api.get_user_product,
        TYPE_ITEM: api.get_item,
    }[external_type]

    try:
        response = fetcher(external_id)
    except ApiError as exc:
        result.rejections.append(_missing_rejection(external_type, external_id, str(exc)))
        return result

    if not response.payload or not isinstance(response.payload, dict):
        result.rejections.append(
            _missing_rejection(external_type, external_id, f"resposta vazia (HTTP {response.status_code})")
        )
        return result

    result.payloads.append(
        RawPayload(
            stage=STAGE_ENRICH,
            endpoint=response.endpoint,
            status_code=response.status_code,
            payload=response.payload,
            request_params=response.params,
            missing_fields=response.missing_fields,
        )
    )
    entity = entity_from_body(external_type, external_id, response.payload, partial=response.is_partial)
    result.entities[entity.key] = entity
    return result


def enrich_entities(
    api: MercadoLivreApi,
    keys: Sequence[tuple[str, str]],
    settings: Settings | None = None,
) -> EnrichResult:
    active = settings or get_settings()
    result = EnrichResult()
    unique_keys = list(dict.fromkeys(keys))
    if not unique_keys:
        return result

    item_ids = [external_id for external_type, external_id in unique_keys if external_type == TYPE_ITEM]
    singles = [(t, i) for t, i in unique_keys if t != TYPE_ITEM]

    result.merge(_enrich_items(api, item_ids))

    if singles:
        workers = min(active.etl.max_workers, len(singles))
        if workers <= 1:
            for external_type, external_id in singles:
                result.merge(_enrich_single(api, external_type, external_id))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for partial in pool.map(lambda key: _enrich_single(api, key[0], key[1]), singles):
                    result.merge(partial)

    logger.info(
        "ENRICH: %s entidades enriquecidas, %s rejeicoes, %s payloads",
        len(result.entities),
        len(result.rejections),
        len(result.payloads),
    )
    return result


def resolve_categories(
    api: MercadoLivreApi,
    category_ids: Sequence[str],
    known: dict[str, Any] | None = None,
    settings: Settings | None = None,
) -> EnrichResult:
    active = settings or get_settings()
    result = EnrichResult()
    cached = known or {}
    pending = [cid for cid in dict.fromkeys(category_ids) if cid and cid not in cached]
    if not pending:
        return result

    def worker(category_id: str) -> tuple[str, Any]:
        try:
            return category_id, api.get_category(category_id)
        except ApiError as exc:
            logger.warning("Falha ao resolver categoria %s: %s", category_id, exc)
            return category_id, None

    workers = min(active.etl.max_workers, len(pending))
    if workers <= 1:
        responses = [worker(cid) for cid in pending]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            responses = list(pool.map(worker, pending))

    for category_id, response in responses:
        if response is None or not response.payload or not isinstance(response.payload, dict):
            continue
        body = response.payload
        result.categories[category_id] = {
            "category_id": category_id,
            "name": str(body.get("name") or category_id)[:255],
            "parent_id": (body.get("path_from_root") or [{}])[-2].get("id")
            if len(body.get("path_from_root") or []) > 1
            else None,
            "path_from_root": body.get("path_from_root"),
            "total_items": _as_int(body.get("total_items_in_this_category")),
            "permalink": body.get("permalink"),
        }
        result.payloads.append(
            RawPayload(
                stage=STAGE_ENRICH,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params=response.params,
            )
        )

    logger.info("ENRICH: %s categorias resolvidas", len(result.categories))
    return result
