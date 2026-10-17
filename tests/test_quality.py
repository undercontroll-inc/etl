from __future__ import annotations

import os
import unittest
from unittest import mock

import pandas as pd

from ml_etl import config as config_module
from ml_etl.api.http_client import ApiError
from ml_etl.config import QualitySettings
from ml_etl.db import session_scope
from ml_etl.db.models import EtlRun
from ml_etl.domain.rejection import Layer, Rejection, Stage
from ml_etl.pipeline.extract import SOURCE_HIGHLIGHTS, STAGE_EXTRACT, RawPayload
from ml_etl.pipeline.quality import Verdict, evaluate, grade
from tests.fake_api import FakeMercadoLivreApi
from tests.support import PipelineTestCase


def highlights(category_id: str) -> RawPayload:
    return RawPayload(
        stage=STAGE_EXTRACT,
        source=SOURCE_HIGHLIGHTS,
        endpoint=f"/highlights/MLB/category/{category_id}",
        status_code=200,
        payload={"content": []},
        request_params={"category_id": category_id},
    )


def curated(prices: list[float | None]) -> pd.DataFrame:
    return pd.DataFrame({"price_median": prices})


class GradeTest(unittest.TestCase):
    def test_grade_thresholds(self) -> None:
        self.assertEqual(Verdict.FAIL, grade("x", 0.2, warn_below=1.0, fail_below=0.5).verdict)
        self.assertEqual(Verdict.WARN, grade("x", 0.7, warn_below=1.0, fail_below=0.5).verdict)
        self.assertEqual(Verdict.PASS, grade("x", 1.0, warn_below=1.0, fail_below=0.5).verdict)
        self.assertEqual(Verdict.PASS, grade("x", 0.0).verdict)


class EvaluateTest(unittest.TestCase):
    SETTINGS = QualitySettings(
        min_category_coverage=0.5, min_curated_products=1, warn_curated_products=3, min_price_coverage=0.5
    )

    def test_full_coverage_passes(self) -> None:
        report = evaluate(
            ["A", "B"], [highlights("A"), highlights("B")], curated([10.0, 20.0, None]), [], self.SETTINGS
        )
        self.assertEqual(Verdict.PASS, report.verdict)
        self.assertEqual("SUCCESS", report.run_status)
        self.assertEqual(["A", "B"], report.metrics["covered_categories"])
        self.assertEqual(2, report.metrics["priced_products"])

    def test_missing_category_downgrades_to_partial(self) -> None:
        report = evaluate(["A", "B"], [highlights("A")], curated([10.0, 20.0, 30.0]), [], self.SETTINGS)
        self.assertEqual(Verdict.WARN, report.verdict)
        self.assertEqual("PARTIAL", report.run_status)
        self.assertEqual([], report.failures())

    def test_low_price_coverage_warns(self) -> None:
        report = evaluate(["A"], [highlights("A")], curated([10.0, None, None]), [], self.SETTINGS)
        verdicts = {check.name: check.verdict for check in report.checks}
        self.assertEqual(Verdict.WARN, verdicts["price_coverage"])
        self.assertEqual(Verdict.WARN, report.verdict)

    def test_no_highlights_fails(self) -> None:
        blocking = Rejection.blocking(Stage.EXTRACT, Layer.ESTRUTURAL, "falha")
        report = evaluate(["A", "B"], [], curated([]), [blocking], self.SETTINGS)
        self.assertEqual(Verdict.FAIL, report.verdict)
        self.assertEqual("FAILED", report.run_status)
        self.assertEqual(["category_coverage", "curated_products"], report.failures())
        self.assertEqual(1, report.metrics["blocking_rejections"])
        self.assertEqual("FAIL", report.as_dict()["verdict"])

    def test_highlights_outside_targets_do_not_count(self) -> None:
        report = evaluate(["A"], [highlights("Z")], curated([1.0, 2.0, 3.0]), [], self.SETTINGS)
        self.assertEqual(Verdict.FAIL, report.verdict)


class QualitySettingsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_thresholds_come_from_environment(self) -> None:
        os.environ.clear()
        os.environ.update(
            {
                "ETL_QUALITY_MIN_CATEGORY_COVERAGE": "0.75",
                "ETL_QUALITY_MIN_CURATED": "3",
                "ETL_QUALITY_WARN_CURATED": "10",
                "ETL_QUALITY_MIN_PRICE_COVERAGE": "0.25",
            }
        )
        quality = config_module.load_settings(use_dotenv=False).quality
        self.assertEqual(QualitySettings(0.75, 3, 10, 0.25), quality)


class QualityGatePipelineTest(PipelineTestCase):
    def test_one_failed_category_yields_partial_run(self) -> None:
        original = FakeMercadoLivreApi.get_highlights_by_category

        def flaky(api, category_id, *args, **kwargs):
            if category_id == "MLB432825":
                raise ApiError("HTTP 500", 500)
            return original(api, category_id, *args, **kwargs)

        with mock.patch.object(FakeMercadoLivreApi, "get_highlights_by_category", flaky):
            result = self._run()

        self.assertEqual("PARTIAL", result.status, result.error_message)
        self.assertIsNone(result.error_message)
        with session_scope(self.settings) as session:
            run = session.get(EtlRun, result.run_id)
        self.assertEqual("PARTIAL", run.status)
        self.assertEqual(["MLB5726"], run.quality_report["metrics"]["covered_categories"])

    def test_successful_run_stores_quality_report(self) -> None:
        result = self._run()
        self.assertEqual("SUCCESS", result.status)
        with session_scope(self.settings) as session:
            run = session.get(EtlRun, result.run_id)
        self.assertEqual("PASS", run.quality_report["verdict"])
        self.assertEqual("PASS", result.stats["quality"])


if __name__ == "__main__":
    unittest.main()
