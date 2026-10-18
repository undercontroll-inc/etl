from __future__ import annotations

import unittest

from sqlalchemy import text, update

from ml_etl.db import session_scope
from ml_etl.db.models import MarketPriceObservation
from ml_etl.domain.buckets import previous_bucket_key
from ml_etl.pipeline.enrich import EnrichedEntity
from ml_etl.pipeline.offers import ProductOffers
from ml_etl.pipeline.price_index import (
    PriceHistory,
    PricePoint,
    compute_movements,
    is_consecutive,
    points_from_offers,
)
from tests.support import PipelineTestCase

DOMAIN = "MLB-MICROWAVES"


def point(external_id: str, price: float, currency: str | None = "BRL", domain: str = DOMAIN) -> PricePoint:
    return PricePoint("PRODUCT", external_id, domain, currency, price)


class CohortMovementTest(unittest.TestCase):
    def test_composition_change_does_not_move_the_index(self) -> None:
        history = {DOMAIN: PriceHistory("2026-01", "r1", [point("A", 100.0), point("B", 1000.0)])}

        [movement] = compute_movements("2026-02", [point("A", 100.0), point("C", 100.0)], history)

        self.assertEqual(1, movement.paired_count)
        self.assertEqual(2, movement.current_product_count)
        self.assertEqual(2, movement.previous_product_count)
        self.assertEqual(0.0, movement.price_index_delta_pct, "media simples cairia 81,8% sem mudanca de preco")
        self.assertEqual(100.0, movement.current_cohort_median)
        self.assertEqual(100.0, movement.previous_cohort_median)
        self.assertTrue(movement.consecutive)
        self.assertEqual("r1", movement.previous_run_id)

    def test_index_is_the_median_of_paired_changes(self) -> None:
        history = {DOMAIN: PriceHistory("2026-01", "r1", [point("A", 100.0), point("B", 200.0), point("C", 400.0)])}
        current = [point("A", 110.0), point("B", 300.0), point("C", 400.0)]

        [movement] = compute_movements("2026-02", current, history)

        self.assertEqual(3, movement.paired_count)
        self.assertEqual(10.0, movement.price_index_delta_pct)
        self.assertEqual(300.0, movement.current_cohort_median)
        self.assertEqual(200.0, movement.previous_cohort_median)

    def test_different_currency_is_not_paired(self) -> None:
        history = {DOMAIN: PriceHistory("2026-01", "r1", [point("A", 100.0, currency="USD")])}

        [movement] = compute_movements("2026-02", [point("A", 500.0)], history)

        self.assertEqual(0, movement.paired_count)
        self.assertIsNone(movement.price_index_delta_pct)
        self.assertIsNone(movement.current_cohort_median)

    def test_gap_between_buckets_is_flagged(self) -> None:
        history = {DOMAIN: PriceHistory("2025-11", "r1", [point("A", 100.0)])}

        [movement] = compute_movements("2026-02", [point("A", 120.0)], history)

        self.assertFalse(movement.consecutive)
        self.assertEqual("2025-11", movement.previous_bucket_key)
        self.assertEqual(20.0, movement.price_index_delta_pct)

    def test_domain_without_history(self) -> None:
        movements = compute_movements("2026-02", [point("A", 100.0), point("Z", 50.0, domain="MLB-BLENDERS")], {})

        self.assertEqual(["MLB-BLENDERS", DOMAIN], [item.domain_id for item in movements])
        for movement in movements:
            self.assertIsNone(movement.previous_bucket_key)
            self.assertFalse(movement.consecutive)
            self.assertEqual(0, movement.previous_product_count)

    def test_consecutive_supports_daily_buckets(self) -> None:
        self.assertTrue(is_consecutive("2026-03-01", "2026-02-28"))
        self.assertFalse(is_consecutive("2026-03-01", "2026-02-27"))
        self.assertTrue(is_consecutive("2026-01", "2025-12"))
        self.assertFalse(is_consecutive("2026-01", None))

    def test_points_need_domain_and_price(self) -> None:
        offers = {
            ("PRODUCT", "A"): ProductOffers("PRODUCT", "A", price_median=10.0, currency_id="BRL"),
            ("PRODUCT", "B"): ProductOffers("PRODUCT", "B", price_median=None),
            ("PRODUCT", "C"): ProductOffers("PRODUCT", "C", price_median=30.0),
        }
        entities = {
            ("PRODUCT", "A"): EnrichedEntity("PRODUCT", "A", "a", domain_id=DOMAIN),
            ("PRODUCT", "B"): EnrichedEntity("PRODUCT", "B", "b", domain_id=DOMAIN),
            ("PRODUCT", "C"): EnrichedEntity("PRODUCT", "C", "c"),
        }

        self.assertEqual([point("A", 10.0)], points_from_offers(offers, entities))


class PriceMovementPipelineTest(PipelineTestCase):
    def test_view_exposes_cohort_movement_of_the_published_run(self) -> None:
        first = self._run()
        previous = previous_bucket_key(first.bucket_key, "month")
        with session_scope(self.settings) as session:
            session.execute(
                update(MarketPriceObservation)
                .where(MarketPriceObservation.run_id == first.run_id)
                .values(bucket_key=previous, price_median=MarketPriceObservation.price_median / 2)
            )

        second = self._run()
        self.assertEqual("SUCCESS", second.status, second.error_message)

        with session_scope(self.settings) as session:
            result = session.execute(text("SELECT * FROM vw_market_price_movement ORDER BY domain_id"))
            columns = list(result.keys())
            rows = {row.domain_id: row for row in result}

        self.assertEqual(
            [
                "domain_id",
                "bucket_key",
                "previous_bucket_key",
                "consecutive",
                "paired_count",
                "current_product_count",
                "previous_product_count",
                "current_cohort_median",
                "previous_cohort_median",
                "price_index_delta_pct",
            ],
            columns,
        )
        microwave = rows[DOMAIN]
        self.assertEqual(second.bucket_key, microwave.bucket_key)
        self.assertEqual(previous, microwave.previous_bucket_key)
        self.assertTrue(microwave.consecutive)
        self.assertEqual(1, microwave.paired_count)
        self.assertEqual(599.0, float(microwave.current_cohort_median))
        self.assertEqual(299.5, float(microwave.previous_cohort_median))
        self.assertEqual(100.0, float(microwave.price_index_delta_pct))
        self.assertEqual({"MLB-AIR_FRYERS", "MLB-BLENDERS", DOMAIN}, set(rows))
        self.assertEqual(3, second.stats["price_movements"])


if __name__ == "__main__":
    unittest.main()
