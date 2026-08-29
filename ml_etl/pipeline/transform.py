from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from ml_etl.db.models import EXTERNAL_TYPES
from ml_etl.domain.buckets import rank_band, trajectory_label
from ml_etl.domain.canonical import canonicalize
from ml_etl.domain.rejection import (
    EMPTY_CANONICAL_TITLE,
    INVALID_EXTERNAL_TYPE,
    MISSING_ENRICHMENT,
    NULL_REQUIRED_FIELD,
    RANK_OUT_OF_RANGE,
    Layer,
    Rejection,
    Severity,
    Stage,
)
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.enrich import EnrichedEntity
from ml_etl.pipeline.extract import SOURCE_HIGHLIGHTS, SOURCE_TRENDS, RawPayload

logger = get_logger(__name__)

HIGHLIGHT_COLUMNS = [
    "source",
    "external_type",
    "external_id",
    "category_id",
    "rank",
    "collected_at",
]
TREND_COLUMNS = ["category_id", "keyword", "canonical_keyword", "url", "position", "collected_at"]


@dataclass
class TransformResult:
    highlights: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=HIGHLIGHT_COLUMNS))
    trends: pd.DataFrame = field(default_factory=lambda: pd.DataFrame(columns=TREND_COLUMNS))
    rejections: list[Rejection] = field(default_factory=list)


def parse_highlight_payloads(payloads: Sequence[RawPayload]) -> tuple[pd.DataFrame, list[Rejection]]:
    records: list[dict[str, Any]] = []
    rejections: list[Rejection] = []

    for item in payloads:
        if item.source != SOURCE_HIGHLIGHTS or not isinstance(item.payload, dict):
            continue
        query_data = item.payload.get("query_data") or {}
        category_id = item.request_params.get("category_id") or query_data.get("id")
        for entry in item.payload.get("content") or []:
            records.append(
                {
                    "source": SOURCE_HIGHLIGHTS,
                    "external_type": entry.get("type"),
                    "external_id": entry.get("id"),
                    "category_id": category_id,
                    "rank": entry.get("position"),
                    "collected_at": item.collected_at,
                }
            )

    frame = pd.DataFrame(records, columns=HIGHLIGHT_COLUMNS)
    return frame, rejections


def parse_trend_payloads(payloads: Sequence[RawPayload]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for item in payloads:
        if item.source != SOURCE_TRENDS or not isinstance(item.payload, list):
            continue
        category_id = item.request_params.get("category_id") or "GERAL"
        for position, entry in enumerate(item.payload, start=1):
            if not isinstance(entry, dict):
                continue
            keyword = entry.get("keyword")
            if not keyword:
                continue
            records.append(
                {
                    "category_id": category_id or "GERAL",
                    "keyword": str(keyword)[:255],
                    "canonical_keyword": canonicalize(str(keyword))[:255],
                    "url": entry.get("url"),
                    "position": position,
                    "collected_at": item.collected_at,
                }
            )

    frame = pd.DataFrame(records, columns=TREND_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(subset=["category_id", "keyword"], keep="first").reset_index(drop=True)
    return frame


def validate_highlights(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[Rejection]]:
    rejections: list[Rejection] = []
    if frame.empty:
        return frame, rejections

    working = frame.copy()
    working["rank"] = pd.to_numeric(working["rank"], errors="coerce")

    null_required = working["external_id"].isna() | working["source"].isna() | working["collected_at"].isna()
    for _, row in working[null_required].iterrows():
        rejections.append(
            Rejection(
                stage=Stage.TRANSFORM,
                layer=Layer.INTEGRIDADE,
                rule=NULL_REQUIRED_FIELD,
                severity=Severity.BLOQUEANTE,
                external_type=row.get("external_type"),
                external_id=row.get("external_id"),
                detail="campo obrigatorio nulo no snapshot",
            )
        )
    working = working[~null_required]

    invalid_type = ~working["external_type"].isin(EXTERNAL_TYPES)
    for _, row in working[invalid_type].iterrows():
        rejections.append(
            Rejection(
                stage=Stage.TRANSFORM,
                layer=Layer.DOMINIO,
                rule=INVALID_EXTERNAL_TYPE,
                severity=Severity.BLOQUEANTE,
                external_type=row.get("external_type"),
                external_id=row.get("external_id"),
                detail=f"external_type recebido: {row.get('external_type')!r}",
            )
        )
    working = working[~invalid_type]

    invalid_rank = working["rank"].isna() | (working["rank"] < 1) | (working["rank"] > 20)
    for _, row in working[invalid_rank].iterrows():
        rejections.append(
            Rejection(
                stage=Stage.TRANSFORM,
                layer=Layer.DOMINIO,
                rule=RANK_OUT_OF_RANGE,
                severity=Severity.BLOQUEANTE,
                external_type=row.get("external_type"),
                external_id=row.get("external_id"),
                detail=f"rank recebido: {row.get('rank')!r}",
            )
        )
    working = working[~invalid_rank]

    missing_category = working["category_id"].isna()
    for _, row in working[missing_category].iterrows():
        rejections.append(
            Rejection(
                stage=Stage.TRANSFORM,
                layer=Layer.INTEGRIDADE,
                rule="category_id ausente no snapshot",
                severity=Severity.BLOQUEANTE,
                external_type=row.get("external_type"),
                external_id=row.get("external_id"),
            )
        )
    working = working[~missing_category]

    if working.empty:
        return working.reset_index(drop=True), rejections

    working["rank"] = working["rank"].astype(int)
    working = working.sort_values("rank").drop_duplicates(
        subset=["source", "external_type", "external_id", "category_id"], keep="first"
    )
    return working.reset_index(drop=True), rejections


def attach_enrichment(
    frame: pd.DataFrame,
    entities: dict[tuple[str, str], EnrichedEntity],
) -> tuple[pd.DataFrame, list[Rejection]]:
    rejections: list[Rejection] = []
    if frame.empty:
        return frame, rejections

    working = frame.copy()
    columns = {
        "title": [],
        "enriched_category_id": [],
        "brand": [],
        "price": [],
        "permalink": [],
        "enrich_status": [],
    }
    keep_mask = []

    for _, row in working.iterrows():
        entity = entities.get((row["external_type"], row["external_id"]))
        if entity is None or not entity.title:
            rejections.append(
                Rejection(
                    stage=Stage.TRANSFORM,
                    layer=Layer.INTEGRIDADE,
                    rule=MISSING_ENRICHMENT,
                    severity=Severity.AVISO,
                    external_type=row["external_type"],
                    external_id=row["external_id"],
                    detail="entidade sem titulo apos o enriquecimento",
                )
            )
            keep_mask.append(False)
            columns["title"].append(None)
            columns["enriched_category_id"].append(None)
            columns["brand"].append(None)
            columns["price"].append(None)
            columns["permalink"].append(None)
            columns["enrich_status"].append("MISSING")
            continue

        keep_mask.append(True)
        columns["title"].append(entity.title)
        columns["enriched_category_id"].append(entity.category_id)
        columns["brand"].append(entity.brand)
        columns["price"].append(entity.price)
        columns["permalink"].append(entity.permalink)
        columns["enrich_status"].append(entity.enrich_status)

    for name, values in columns.items():
        working[name] = values

    working = working[pd.Series(keep_mask, index=working.index)]
    return working.reset_index(drop=True), rejections


def canonicalize_titles(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[Rejection]]:
    rejections: list[Rejection] = []
    if frame.empty:
        return frame, rejections

    working = frame.copy()
    working["canonical_title"] = working["title"].map(canonicalize)

    empty_canonical = working["canonical_title"].str.len() == 0
    for _, row in working[empty_canonical].iterrows():
        rejections.append(
            Rejection(
                stage=Stage.TRANSFORM,
                layer=Layer.TRANSFORM,
                rule=EMPTY_CANONICAL_TITLE,
                severity=Severity.AVISO,
                external_type=row["external_type"],
                external_id=row["external_id"],
                detail=f"titulo original: {row['title']!r}",
            )
        )
    working = working[~empty_canonical]
    return working.reset_index(drop=True), rejections


def attach_trajectory(
    frame: pd.DataFrame,
    previous_ranks: dict[tuple[str, str, str], int],
) -> pd.DataFrame:
    if frame.empty:
        return frame

    working = frame.copy()
    working["rank_band"] = working["rank"].map(rank_band)

    deltas: list[int | None] = []
    for _, row in working.iterrows():
        key = (row["external_type"], row["external_id"], row["category_id"])
        previous = previous_ranks.get(key)
        deltas.append(None if previous is None else int(previous) - int(row["rank"]))

    working["rank_delta"] = deltas
    working["trajectory_label"] = working["rank_delta"].map(trajectory_label)
    return working


def build_transform_result(
    payloads: Sequence[RawPayload],
    entities: dict[tuple[str, str], EnrichedEntity],
    previous_ranks: dict[tuple[str, str, str], int],
) -> TransformResult:
    result = TransformResult()

    raw_highlights, parse_rejections = parse_highlight_payloads(payloads)
    result.rejections.extend(parse_rejections)

    validated, validation_rejections = validate_highlights(raw_highlights)
    result.rejections.extend(validation_rejections)

    enriched, enrich_rejections = attach_enrichment(validated, entities)
    result.rejections.extend(enrich_rejections)

    canonical, canonical_rejections = canonicalize_titles(enriched)
    result.rejections.extend(canonical_rejections)

    result.highlights = attach_trajectory(canonical, previous_ranks)
    result.trends = parse_trend_payloads(payloads)

    logger.info(
        "TRANSFORM: %s snapshots validos, %s tendencias, %s rejeicoes",
        len(result.highlights),
        len(result.trends),
        len(result.rejections),
    )
    return result


def highlight_keys(payloads: Sequence[RawPayload]) -> list[tuple[str, str]]:
    frame, _ = parse_highlight_payloads(payloads)
    if frame.empty:
        return []
    valid, _ = validate_highlights(frame)
    if valid.empty:
        return []
    return list(dict.fromkeys(zip(valid["external_type"], valid["external_id"], strict=True)))


def normalize_collected_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return pd.to_datetime(value, utc=True).to_pydatetime()
