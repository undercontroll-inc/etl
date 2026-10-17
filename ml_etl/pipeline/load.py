from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from ml_etl.db import repository
from ml_etl.db.models import (
    MarketCategory,
    MarketCuratedProduct,
    MarketEntity,
    MarketPriceObservation,
    MarketScore,
    MarketSnapshot,
    MarketTrend,
)
from ml_etl.domain.canonical import canonicalize
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.enrich import EnrichedEntity
from ml_etl.pipeline.offers import ProductOffers
from ml_etl.pipeline.transform import normalize_collected_at

logger = get_logger(__name__)


def _optional(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def load_categories(session: Session, categories: dict[str, dict[str, Any]]) -> int:
    if not categories:
        return 0
    return repository.upsert(session, MarketCategory, list(categories.values()))


def load_entities(session: Session, entities: dict[tuple[str, str], EnrichedEntity]) -> int:
    if not entities:
        return 0

    rows = [
        {
            "external_type": entity.external_type,
            "external_id": entity.external_id,
            "title": entity.title,
            "canonical_title": canonicalize(entity.title),
            "category_id": entity.category_id,
            "domain_id": entity.domain_id,
            "brand": entity.brand,
            "brand_slug": entity.brand_slug,
            "model": entity.model,
            "product_key": entity.product_key,
            "color": entity.color,
            "voltage": entity.voltage,
            "power_watts": entity.power_watts,
            "capacity_value": entity.capacity_value,
            "capacity_unit": entity.capacity_unit,
            "energy_efficiency": entity.energy_efficiency,
            "price": entity.price,
            "available_quantity": entity.available_quantity,
            "condition": entity.condition,
            "permalink": entity.permalink,
            "attributes": entity.attributes,
            "enrich_status": entity.enrich_status,
        }
        for entity in entities.values()
    ]
    return repository.upsert(session, MarketEntity, rows)


def load_price_observations(
    session: Session,
    offers: dict[tuple[str, str], ProductOffers],
    run_id: str,
    bucket_key: str,
    collected_at: datetime,
) -> int:
    if not offers:
        return 0

    rows = [
        {
            "run_id": run_id,
            "external_type": offer.external_type,
            "external_id": offer.external_id,
            "bucket_key": bucket_key,
            "offer_count": offer.offer_count,
            "seller_count": offer.seller_count,
            "official_store_count": offer.official_store_count,
            "price_min": offer.price_min,
            "price_p25": offer.price_p25,
            "price_median": offer.price_median,
            "price_p75": offer.price_p75,
            "price_max": offer.price_max,
            "original_price_median": offer.original_price_median,
            "discount_pct_median": offer.discount_pct_median,
            "winner_item_id": offer.winner_item_id,
            "winner_price": offer.winner_price,
            "currency_id": offer.currency_id,
            "category_id": offer.category_id,
            "collected_at": collected_at,
        }
        for offer in offers.values()
    ]
    return repository.upsert(session, MarketPriceObservation, rows)


def load_snapshots(session: Session, frame: pd.DataFrame, run_id: str, bucket_key: str) -> int:
    if frame.empty:
        return 0

    rows = [
        {
            "run_id": run_id,
            "source": row["source"],
            "external_type": row["external_type"],
            "external_id": row["external_id"],
            "category_id": row["category_id"],
            "rank": int(row["rank"]),
            "rank_band": row["rank_band"],
            "collected_at": normalize_collected_at(row["collected_at"]),
            "bucket_key": bucket_key,
        }
        for _, row in frame.iterrows()
    ]
    return repository.upsert(session, MarketSnapshot, rows)


def load_trends(session: Session, frame: pd.DataFrame, run_id: str, bucket_key: str) -> int:
    if frame.empty:
        return 0

    rows = [
        {
            "run_id": run_id,
            "category_id": row["category_id"],
            "keyword": row["keyword"],
            "canonical_keyword": _optional(row["canonical_keyword"]),
            "url": _optional(row["url"]),
            "position": int(row["position"]),
            "collected_at": normalize_collected_at(row["collected_at"]),
            "bucket_key": bucket_key,
        }
        for _, row in frame.iterrows()
    ]
    return repository.upsert(session, MarketTrend, rows)


def load_scores(session: Session, frame: pd.DataFrame, run_id: str, bucket_key: str) -> int:
    if frame.empty:
        return 0

    rows = [
        {
            "run_id": run_id,
            "external_type": row["external_type"],
            "external_id": row["external_id"],
            "bucket_key": bucket_key,
            "score": float(row["score"]),
            "priority_label": row["priority_label"],
            "confidence": row["confidence"],
            "components": row["score_components"],
        }
        for _, row in frame.iterrows()
    ]
    return repository.upsert(session, MarketScore, rows)


def load_curated(
    session: Session,
    frame: pd.DataFrame,
    run_id: str,
    bucket_key: str,
    category_names: dict[str, str] | None = None,
) -> int:
    if frame.empty:
        return 0

    names = category_names or {}
    rows = []
    for _, row in frame.iterrows():
        rank_delta = _optional(row.get("rank_delta"))
        offer_count = _optional(row.get("offer_count"))
        seller_count = _optional(row.get("seller_count"))
        power_watts = _optional(row.get("power_watts"))
        category_id = _optional(row.get("enriched_category_id")) or _optional(row.get("category_id"))
        rows.append(
            {
                "collected_at": normalize_collected_at(row["collected_at"]),
                "external_type": row["external_type"],
                "rank": int(row["rank"]),
                "title": row["title"],
                "external_id": row["external_id"],
                "score": float(row["score"]),
                "source": row["source"],
                "canonical_title": row["canonical_title"],
                "rank_band": row["rank_band"],
                "rank_delta": None if rank_delta is None else int(rank_delta),
                "trajectory_label": _optional(row.get("trajectory_label")),
                "category_id": category_id,
                "category_name": names.get(category_id) if category_id else None,
                "domain_id": _optional(row.get("domain_id")),
                "brand": _optional(row.get("brand")),
                "brand_slug": _optional(row.get("brand_slug")),
                "model": _optional(row.get("model")),
                "product_key": _optional(row.get("product_key")),
                "voltage": _optional(row.get("voltage")),
                "power_watts": None if power_watts is None else int(power_watts),
                "capacity_value": _optional(row.get("capacity_value")),
                "capacity_unit": _optional(row.get("capacity_unit")),
                "energy_efficiency": _optional(row.get("energy_efficiency")),
                "price": _optional(row.get("price")),
                "price_min": _optional(row.get("price_min")),
                "price_median": _optional(row.get("price_median")),
                "price_max": _optional(row.get("price_max")),
                "price_delta_pct": _optional(row.get("price_delta_pct")),
                "discount_pct": _optional(row.get("discount_pct_median")),
                "offer_count": None if offer_count is None else int(offer_count),
                "seller_count": None if seller_count is None else int(seller_count),
                "relevance": _optional(row.get("relevance")),
                "domain_reason": _optional(row.get("domain_reason")),
                "permalink": _optional(row.get("permalink")),
                "priority_label": row["priority_label"],
                "confidence": row["confidence"],
                "bucket_key": bucket_key,
                "run_id": run_id,
            }
        )
    return repository.upsert(session, MarketCuratedProduct, rows)


def load_rejections(session: Session, rows: Sequence[dict[str, Any]]) -> int:
    return repository.record_rejections(session, rows)
