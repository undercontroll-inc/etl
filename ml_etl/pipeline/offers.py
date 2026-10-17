from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from ml_etl.api.http_client import ApiError
from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.config import Settings, get_settings
from ml_etl.domain.numbers import as_positive_float
from ml_etl.domain.rejection import Layer, Rejection, Stage
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.extract import STAGE_ENRICH, RawPayload, run_parallel

logger = get_logger(__name__)

OFFERS_ENDPOINT_SUFFIX = "/items"
MISSING_OFFERS = "ofertas de catalogo indisponiveis para o produto"
INCOMPLETE_PAGINATION = "paginacao de ofertas interrompida"


@dataclass
class ProductOffers:
    external_type: str
    external_id: str
    available_offer_count: int | None = None
    observed_offer_count: int = 0
    valid_price_count: int = 0
    sample_complete: bool = False
    observed_seller_count: int = 0
    official_store_count: int = 0
    price_min: float | None = None
    price_p25: float | None = None
    price_median: float | None = None
    price_p75: float | None = None
    price_max: float | None = None
    original_price_median: float | None = None
    discount_pct_median: float | None = None
    lowest_price_item_id: str | None = None
    lowest_price: float | None = None
    currency_id: str | None = None
    category_id: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.external_type, self.external_id)


@dataclass
class OffersResult:
    offers: dict[tuple[str, str], ProductOffers] = field(default_factory=dict)
    payloads: list[RawPayload] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)

    def merge(self, other: OffersResult) -> None:
        self.offers.update(other.offers)
        self.payloads.extend(other.payloads)
        self.rejections.extend(other.rejections)


def percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 2)
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 2)


def _most_common(values: Sequence[Any]) -> Any:
    present = [value for value in values if value]
    if not present:
        return None
    return Counter(present).most_common(1)[0][0]


def product_id_from_endpoint(endpoint: str) -> str | None:
    if not endpoint.startswith("/products/") or not endpoint.endswith(OFFERS_ENDPOINT_SUFFIX):
        return None
    middle = endpoint[len("/products/") : -len(OFFERS_ENDPOINT_SUFFIX)]
    return middle or None


def _paging_total(payload: dict[str, Any]) -> int | None:
    paging = payload.get("paging")
    if not isinstance(paging, dict):
        return None
    try:
        total = int(paging.get("total"))
    except (TypeError, ValueError):
        return None
    return total if total >= 0 else None


def merge_pages(pages: Sequence[Any]) -> dict[str, Any] | None:
    bodies = [page for page in pages if isinstance(page, dict)]
    if not bodies:
        return None
    seen: set[Any] = set()
    merged: list[dict[str, Any]] = []
    for body in bodies:
        results = body.get("results")
        if not isinstance(results, list):
            continue
        for entry in results:
            if not isinstance(entry, dict):
                continue
            item_id = entry.get("item_id")
            if item_id is not None:
                if item_id in seen:
                    continue
                seen.add(item_id)
            merged.append(entry)
    return {"paging": {"total": _paging_total(bodies[0])}, "results": merged}


def aggregate_offers(external_type: str, external_id: str, payload: Any) -> ProductOffers | None:
    if not isinstance(payload, dict):
        return None

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return None

    entries = [entry for entry in results if isinstance(entry, dict)]
    if not entries:
        return None

    priced = [(entry, price) for entry in entries if (price := as_positive_float(entry.get("price"))) is not None]
    prices = [price for _, price in priced]

    total = _paging_total(payload)
    available = None if total is None else max(total, len(entries))

    discounts: list[float] = []
    originals: list[float] = []
    for entry, price in priced:
        original = as_positive_float(entry.get("original_price"))
        if original is None or original <= price:
            continue
        originals.append(original)
        discounts.append(round((original - price) / original * 100, 2))

    lowest_entry, lowest_price = min(priced, key=lambda pair: pair[1]) if priced else (None, None)

    return ProductOffers(
        external_type=external_type,
        external_id=external_id,
        available_offer_count=available,
        observed_offer_count=len(entries),
        valid_price_count=len(prices),
        sample_complete=available is not None and len(entries) >= available,
        observed_seller_count=len(
            {entry.get("seller_id") for entry in entries if entry.get("seller_id") is not None}
        ),
        official_store_count=len(
            {entry.get("official_store_id") for entry in entries if entry.get("official_store_id") is not None}
        ),
        price_min=percentile(prices, 0.0),
        price_p25=percentile(prices, 0.25),
        price_median=percentile(prices, 0.5),
        price_p75=percentile(prices, 0.75),
        price_max=percentile(prices, 1.0),
        original_price_median=percentile(originals, 0.5),
        discount_pct_median=percentile(discounts, 0.5),
        lowest_price_item_id=str(lowest_entry.get("item_id")) if lowest_entry and lowest_entry.get("item_id") else None,
        lowest_price=lowest_price,
        currency_id=_most_common([entry.get("currency_id") for entry in entries]),
        category_id=_most_common([entry.get("category_id") for entry in entries]),
    )


def _next_offset(payload: Any, offset: int) -> int | None:
    if not isinstance(payload, dict):
        return None
    results = payload.get("results")
    total = _paging_total(payload)
    if not isinstance(results, list) or not results or total is None:
        return None
    paging = payload.get("paging") or {}
    try:
        step = int(paging.get("limit") or len(results))
    except (TypeError, ValueError):
        step = len(results)
    following = offset + max(step, len(results))
    return following if following < total else None


def collect_offers(
    api: MercadoLivreApi,
    keys: Sequence[tuple[str, str]],
    settings: Settings | None = None,
) -> OffersResult:
    active = settings or get_settings()
    result = OffersResult()
    unique_keys = list(dict.fromkeys(keys))
    if not unique_keys:
        return result

    def worker(key: tuple[str, str]) -> OffersResult:
        external_type, external_id = key
        local = OffersResult()
        try:
            responses = [api.get_product_items(external_id)]
        except ApiError as exc:
            local.rejections.append(
                Rejection.warning(
                    stage=Stage.ENRICH,
                    layer=Layer.ESTRUTURAL,
                    rule=MISSING_OFFERS,
                    external_type=external_type,
                    external_id=external_id,
                    detail=str(exc),
                )
            )
            return local

        offset = _next_offset(responses[0].payload, 0)
        while offset is not None and len(responses) < active.etl.offers_max_pages:
            try:
                responses.append(api.get_product_items(external_id, offset=offset))
            except ApiError as exc:
                local.rejections.append(
                    Rejection.warning(
                        stage=Stage.ENRICH,
                        layer=Layer.API_LIMITE if exc.status_code == 429 else Layer.ESTRUTURAL,
                        rule=INCOMPLETE_PAGINATION,
                        external_type=external_type,
                        external_id=external_id,
                        detail=f"offset {offset}: {exc}",
                    )
                )
                break
            offset = _next_offset(responses[-1].payload, offset)

        aggregated = aggregate_offers(external_type, external_id, merge_pages([item.payload for item in responses]))
        if aggregated is None:
            local.rejections.append(
                Rejection.warning(
                    stage=Stage.ENRICH,
                    layer=Layer.ESTRUTURAL,
                    rule=MISSING_OFFERS,
                    external_type=external_type,
                    external_id=external_id,
                    detail=f"sem ofertas utilizaveis (HTTP {responses[0].status_code})",
                )
            )
            return local

        local.offers[aggregated.key] = aggregated
        local.payloads.extend(
            RawPayload(
                stage=STAGE_ENRICH,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params={"external_type": external_type, **response.params},
            )
            for response in responses
        )
        return local

    for partial in run_parallel(unique_keys, worker, active.etl.max_workers):
        result.merge(partial)

    logger.info(
        "ENRICH ofertas: %s produtos com preco, %s sem ofertas",
        len(result.offers),
        len(result.rejections),
    )
    return result


def offers_from_raw(payloads: Sequence[RawPayload]) -> dict[tuple[str, str], ProductOffers]:
    pages: dict[tuple[str, str], list[tuple[int, Any]]] = {}
    for payload in payloads:
        product_id = product_id_from_endpoint(payload.endpoint)
        if not product_id:
            continue
        external_type = str(payload.request_params.get("external_type") or "PRODUCT")
        offset = int(payload.request_params.get("offset") or 0)
        pages.setdefault((external_type, product_id), []).append((offset, payload.payload))

    offers: dict[tuple[str, str], ProductOffers] = {}
    for (external_type, product_id), entries in pages.items():
        ordered = [body for _, body in sorted(entries, key=lambda pair: pair[0])]
        aggregated = aggregate_offers(external_type, product_id, merge_pages(ordered))
        if aggregated is not None:
            offers[aggregated.key] = aggregated
    logger.info("RAW replay: %s agregados de ofertas reconstruidos", len(offers))
    return offers
