from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import median
from typing import Any

from ml_etl.domain.buckets import previous_bucket_key
from ml_etl.pipeline.enrich import EnrichedEntity
from ml_etl.pipeline.offers import ProductOffers


@dataclass(frozen=True)
class PricePoint:
    external_type: str
    external_id: str
    domain_id: str
    currency_id: str | None
    price: float

    @property
    def cohort_key(self) -> tuple[str, str, str | None]:
        return (self.external_type, self.external_id, self.currency_id)


@dataclass(frozen=True)
class PriceHistory:
    bucket_key: str
    run_id: str
    points: Sequence[PricePoint]


@dataclass(frozen=True)
class PriceMovement:
    domain_id: str
    bucket_key: str
    previous_bucket_key: str | None
    previous_run_id: str | None
    consecutive: bool
    paired_count: int
    current_product_count: int
    previous_product_count: int
    current_cohort_median: float | None
    previous_cohort_median: float | None
    price_index_delta_pct: float | None


def points_from_offers(
    offers: Mapping[tuple[str, str], ProductOffers],
    entities: Mapping[tuple[str, str], EnrichedEntity],
) -> list[PricePoint]:
    points: list[PricePoint] = []
    for key, offer in offers.items():
        entity = entities.get(key)
        if entity is None or not entity.domain_id or offer.price_median is None:
            continue
        points.append(
            PricePoint(
                external_type=offer.external_type,
                external_id=offer.external_id,
                domain_id=entity.domain_id,
                currency_id=offer.currency_id,
                price=float(offer.price_median),
            )
        )
    return points


def history_from_observations(
    references: Mapping[str, tuple[str, str, Sequence[Any]]],
) -> dict[str, PriceHistory]:
    return {
        domain_id: PriceHistory(
            bucket_key=bucket_key,
            run_id=run_id,
            points=[
                PricePoint(
                    external_type=row.external_type,
                    external_id=row.external_id,
                    domain_id=domain_id,
                    currency_id=row.currency_id,
                    price=float(row.price_median),
                )
                for row in rows
            ],
        )
        for domain_id, (bucket_key, run_id, rows) in references.items()
    }


def is_consecutive(bucket_key: str, previous: str | None) -> bool:
    if previous is None:
        return False
    granularity = "day" if len(bucket_key) == 10 else "month"
    return previous_bucket_key(bucket_key, granularity) == previous


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


def movement_for_domain(
    domain_id: str,
    bucket_key: str,
    current: Sequence[PricePoint],
    history: PriceHistory | None,
) -> PriceMovement:
    previous_points = list(history.points) if history else []
    previous_by_key = {point.cohort_key: point.price for point in previous_points}
    pairs = [
        (point.price, previous_by_key[point.cohort_key])
        for point in current
        if point.cohort_key in previous_by_key
    ]
    changes = [(now - before) / before * 100 for now, before in pairs if before > 0]

    return PriceMovement(
        domain_id=domain_id,
        bucket_key=bucket_key,
        previous_bucket_key=history.bucket_key if history else None,
        previous_run_id=history.run_id if history else None,
        consecutive=is_consecutive(bucket_key, history.bucket_key if history else None),
        paired_count=len(pairs),
        current_product_count=len({point.cohort_key for point in current}),
        previous_product_count=len(previous_by_key),
        current_cohort_median=_rounded(median(now for now, _ in pairs)) if pairs else None,
        previous_cohort_median=_rounded(median(before for _, before in pairs)) if pairs else None,
        price_index_delta_pct=_rounded(median(changes)) if changes else None,
    )


def compute_movements(
    bucket_key: str,
    current: Sequence[PricePoint],
    history: Mapping[str, PriceHistory],
) -> list[PriceMovement]:
    by_domain: dict[str, list[PricePoint]] = defaultdict(list)
    for point in current:
        by_domain[point.domain_id].append(point)
    return [
        movement_for_domain(domain_id, bucket_key, points, history.get(domain_id))
        for domain_id, points in sorted(by_domain.items())
    ]
