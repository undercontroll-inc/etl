from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from ml_etl.api.http_client import ApiError
from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.config import Settings, get_settings
from ml_etl.domain.rejection import Layer, Rejection, Severity, Stage
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.extract import STAGE_ENRICH, RawPayload, run_parallel

logger = get_logger(__name__)

OFFERS_ENDPOINT_SUFFIX = "/items"
MISSING_OFFERS = "ofertas de catalogo indisponiveis para o produto"


@dataclass
class ProductOffers:
    external_type: str
    external_id: str
    offer_count: int = 0
    seller_count: int = 0
    official_store_count: int = 0
    price_min: float | None = None
    price_p25: float | None = None
    price_median: float | None = None
    price_p75: float | None = None
    price_max: float | None = None
    original_price_median: float | None = None
    discount_pct_median: float | None = None
    winner_item_id: str | None = None
    winner_price: float | None = None
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


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


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


def aggregate_offers(external_type: str, external_id: str, payload: Any) -> ProductOffers | None:
    if not isinstance(payload, dict):
        return None

    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return None

    entries = [entry for entry in results if isinstance(entry, dict)]
    if not entries:
        return None

    priced = [(entry, price) for entry in entries if (price := _as_float(entry.get("price"))) is not None]
    prices = [price for _, price in priced]

    paging_total = payload.get("paging") or {}
    offer_count = paging_total.get("total") if isinstance(paging_total, dict) else None
    try:
        offer_count = int(offer_count)
    except (TypeError, ValueError):
        offer_count = len(entries)

    discounts: list[float] = []
    originals: list[float] = []
    for entry, price in priced:
        original = _as_float(entry.get("original_price"))
        if original is None or original <= price:
            continue
        originals.append(original)
        discounts.append(round((original - price) / original * 100, 2))

    winner_entry, winner_price = min(priced, key=lambda pair: pair[1]) if priced else (None, None)

    return ProductOffers(
        external_type=external_type,
        external_id=external_id,
        offer_count=max(offer_count, len(entries)),
        seller_count=len({entry.get("seller_id") for entry in entries if entry.get("seller_id") is not None}),
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
        winner_item_id=str(winner_entry.get("item_id")) if winner_entry and winner_entry.get("item_id") else None,
        winner_price=winner_price,
        currency_id=_most_common([entry.get("currency_id") for entry in entries]),
        category_id=_most_common([entry.get("category_id") for entry in entries]),
    )


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
            response = api.get_product_items(external_id)
        except ApiError as exc:
            local.rejections.append(
                Rejection(
                    stage=Stage.ENRICH,
                    layer=Layer.ESTRUTURAL,
                    rule=MISSING_OFFERS,
                    severity=Severity.AVISO,
                    external_type=external_type,
                    external_id=external_id,
                    detail=str(exc),
                )
            )
            return local

        aggregated = aggregate_offers(external_type, external_id, response.payload)
        if aggregated is None:
            local.rejections.append(
                Rejection(
                    stage=Stage.ENRICH,
                    layer=Layer.ESTRUTURAL,
                    rule=MISSING_OFFERS,
                    severity=Severity.AVISO,
                    external_type=external_type,
                    external_id=external_id,
                    detail=f"sem ofertas utilizaveis (HTTP {response.status_code})",
                )
            )
            return local

        local.offers[aggregated.key] = aggregated
        local.payloads.append(
            RawPayload(
                stage=STAGE_ENRICH,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params={"external_type": external_type, **response.params},
            )
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
    offers: dict[tuple[str, str], ProductOffers] = {}
    for payload in payloads:
        product_id = product_id_from_endpoint(payload.endpoint)
        if not product_id:
            continue
        external_type = payload.request_params.get("external_type") or "PRODUCT"
        aggregated = aggregate_offers(str(external_type), product_id, payload.payload)
        if aggregated is not None:
            offers[aggregated.key] = aggregated
    logger.info("RAW replay: %s agregados de ofertas reconstruidos", len(offers))
    return offers
