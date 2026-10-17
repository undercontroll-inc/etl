from __future__ import annotations

import unittest
from unittest import mock

from sqlalchemy import text

from ml_etl.api.http_client import ApiError
from ml_etl.db import session_scope
from ml_etl.db.models import EtlRun
from ml_etl.db.repository import table_counts
from ml_etl.pipeline import orchestrator
from ml_etl.pipeline.offers import aggregate_offers
from tests.fake_api import HIGHLIGHTS_CONTENT, FakeMercadoLivreApi
from tests.support import PipelineTestCase

REMOVED_PRODUCT = "MLB19617678"


class ReportRegressionTest(PipelineTestCase):
    @unittest.expectedFailure
    def test_failure_after_collection_keeps_run_and_raw(self) -> None:
        with mock.patch.object(
            orchestrator.score, "score_highlights", side_effect=RuntimeError("injected failure")
        ):
            result = self._run()

        self.assertEqual("FAILED", result.status)
        with session_scope(self.settings) as session:
            counts = table_counts(session)
            run = session.get(EtlRun, result.run_id)

        self.assertIsNotNone(run, "a execucao com falha precisa continuar auditavel")
        self.assertEqual("FAILED", run.status)
        self.assertIn("injected failure", run.error_message)
        self.assertGreater(counts["market_raw_response"], 0, "o RAW coletado nao pode ser perdido")

    @unittest.expectedFailure
    def test_all_highlights_failing_does_not_succeed(self) -> None:
        with mock.patch.object(
            FakeMercadoLivreApi, "get_highlights_by_category", side_effect=ApiError("HTTP 403", 403)
        ):
            result = self._run()

        self.assertEqual("FAILED", result.status)
        with session_scope(self.settings) as session:
            run = session.get(EtlRun, result.run_id)
        self.assertEqual("FAILED", run.status)

    @unittest.expectedFailure
    def test_product_missing_from_new_run_leaves_current_view(self) -> None:
        first = self._run()
        self.assertIn(first.status, ("SUCCESS", "PARTIAL"), first.error_message)

        filtered = {
            key: [item for item in items if item["id"] != REMOVED_PRODUCT]
            for key, items in HIGHLIGHTS_CONTENT.items()
        }
        with mock.patch.dict(HIGHLIGHTS_CONTENT, filtered, clear=True):
            second = self._run()
        self.assertIn(second.status, ("SUCCESS", "PARTIAL"), second.error_message)

        with session_scope(self.settings) as session:
            current = session.execute(
                text("SELECT external_id FROM vw_market_product_current")
            ).scalars().all()

        self.assertTrue(current)
        self.assertNotIn(REMOVED_PRODUCT, current)
        self.assertEqual(len(current), len(set(current)), "a view atual nao pode misturar execucoes")

    @unittest.expectedFailure
    def test_offer_total_is_not_confused_with_sample(self) -> None:
        offers = aggregate_offers(
            "PRODUCT",
            "MLB1",
            {"paging": {"total": 100}, "results": [{"item_id": "one", "price": 200, "seller_id": 1}]},
        )

        self.assertEqual(100, offers.available_offer_count)
        self.assertEqual(1, offers.observed_offer_count)
        self.assertEqual(1, offers.valid_price_count)
        self.assertFalse(offers.sample_complete)
        self.assertEqual(200.0, offers.price_median)


if __name__ == "__main__":
    unittest.main()
