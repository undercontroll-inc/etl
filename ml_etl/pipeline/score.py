from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from ml_etl.config import Settings, get_settings
from ml_etl.domain.canonical import canonical_tokens
from ml_etl.domain.rejection import INSUFFICIENT_HISTORY, Layer, Rejection, Severity, Stage
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

MAX_RANK = 20
NEUTRAL_TRAJECTORY = 50.0
PERSISTENCE_TARGET_BUCKETS = 3
CONFIDENCE_MIN_OBSERVATIONS = 2
TREND_MIN_OVERLAP = 0.34


@dataclass
class ScoreOutcome:
    score: float
    components: dict[str, Any]
    confidence: str
    priority_label: str


def rank_component(rank: int) -> float:
    bounded = max(1, min(MAX_RANK, int(rank)))
    return round((MAX_RANK - bounded + 1) / MAX_RANK * 100, 2)


def trajectory_component(rank_delta: int | None) -> float:
    if rank_delta is None:
        return NEUTRAL_TRAJECTORY
    bounded = max(-(MAX_RANK - 1), min(MAX_RANK - 1, int(rank_delta)))
    return round((bounded + (MAX_RANK - 1)) / (2 * (MAX_RANK - 1)) * 100, 2)


def trends_component(canonical_title: str, trend_token_sets: list[set[str]]) -> float:
    tokens = canonical_tokens(canonical_title)
    if not tokens or not trend_token_sets:
        return 0.0
    best = 0.0
    for trend_tokens in trend_token_sets:
        if not trend_tokens:
            continue
        overlap = len(tokens & trend_tokens) / len(trend_tokens)
        best = max(best, overlap)
    if best < TREND_MIN_OVERLAP:
        return 0.0
    return round(min(1.0, best) * 100, 2)


def relevance_component(relevance: float | None) -> float:
    if relevance is None:
        return 0.0
    return round(max(0.0, min(1.0, float(relevance))) * 100, 2)


def persistence_component(observed_buckets: int) -> float:
    ratio = min(1.0, max(0, observed_buckets) / PERSISTENCE_TARGET_BUCKETS)
    return round(ratio * 100, 2)


def compute_score(
    rank: int,
    rank_delta: int | None,
    canonical_title: str,
    relevance: float | None,
    prior_buckets: int,
    trend_token_sets: list[set[str]],
    settings: Settings,
) -> ScoreOutcome:
    observed_buckets = max(0, prior_buckets) + 1
    components = {
        "rank": rank_component(rank),
        "trajectory": trajectory_component(rank_delta),
        "trends": trends_component(canonical_title, trend_token_sets),
        "relevance": relevance_component(relevance),
        "persistence": persistence_component(observed_buckets),
    }
    weights = settings.score.weights
    total = sum(components[name] * weights[name] for name in components)
    score = round(max(0.0, min(100.0, total)), 2)

    confidence = "LOW" if observed_buckets < CONFIDENCE_MIN_OBSERVATIONS or rank_delta is None else "HIGH"
    priority = "PRIORIDADE_ALTA" if score >= settings.score.priority_threshold else "PRIORIDADE_NORMAL"

    explained = {name: {"value": value, "weight": round(weights[name], 4)} for name, value in components.items()}
    explained["prior_buckets"] = prior_buckets
    explained["observed_buckets"] = observed_buckets
    explained["weighted_total"] = score

    return ScoreOutcome(score=score, components=explained, confidence=confidence, priority_label=priority)


def score_highlights(
    frame: pd.DataFrame,
    trends: pd.DataFrame,
    prior_buckets: dict[tuple[str, str], int],
    settings: Settings | None = None,
) -> tuple[pd.DataFrame, list[Rejection]]:
    active = settings or get_settings()
    rejections: list[Rejection] = []

    if frame.empty:
        working = frame.copy()
        for column in ("score", "priority_label", "confidence", "score_components"):
            working[column] = []
        return working, rejections

    trend_token_sets = []
    if not trends.empty:
        trend_token_sets = [canonical_tokens(keyword) for keyword in trends["canonical_keyword"].fillna("")]

    working = frame.copy()
    scores: list[float] = []
    priorities: list[str] = []
    confidences: list[str] = []
    components: list[dict[str, Any]] = []

    for _, row in working.iterrows():
        key = (row["external_type"], row["external_id"])
        prior = int(prior_buckets.get(key, 0))
        rank_delta = row.get("rank_delta")
        rank_delta = None if pd.isna(rank_delta) else int(rank_delta)

        outcome = compute_score(
            rank=int(row["rank"]),
            rank_delta=rank_delta,
            canonical_title=row["canonical_title"],
            relevance=row.get("relevance"),
            prior_buckets=prior,
            trend_token_sets=trend_token_sets,
            settings=active,
        )

        scores.append(outcome.score)
        priorities.append(outcome.priority_label)
        confidences.append(outcome.confidence)
        components.append(outcome.components)

        if outcome.confidence == "LOW":
            rejections.append(
                Rejection(
                    stage=Stage.SCORE,
                    layer=Layer.QUALIDADE_TEMPORAL,
                    rule=INSUFFICIENT_HISTORY,
                    severity=Severity.AVISO,
                    external_type=row["external_type"],
                    external_id=row["external_id"],
                    detail=f"buckets anteriores disponiveis: {prior}",
                )
            )

    working["score"] = scores
    working["priority_label"] = priorities
    working["confidence"] = confidences
    working["score_components"] = components

    logger.info(
        "SCORE: %s registros pontuados, %s com baixa confianca",
        len(working),
        sum(1 for value in confidences if value == "LOW"),
    )
    return working, rejections
