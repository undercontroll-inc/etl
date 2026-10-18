from __future__ import annotations

import io
from dataclasses import replace
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from sqlalchemy import text

import main
from ml_etl import config as config_module
from ml_etl.api.http_client import ApiError
from ml_etl.config import ConfigError
from ml_etl.db import session_scope
from ml_etl.db.repository import RunNotFoundError, latest_published_snapshot
from ml_etl.pipeline import orchestrator
from ml_etl.pipeline.publish import PublishError, publish_run
from tests.fake_api import HIGHLIGHTS_CONTENT, FakeMercadoLivreApi
from tests.support import PipelineTestCase

MICROWAVE = "MLB19617678"


def without_microwave() -> dict:
    return {key: [item for item in items if item["id"] != MICROWAVE] for key, items in HIGHLIGHTS_CONTENT.items()}


class PublishTest(PipelineTestCase):
    def _run(self, settings_override=None, **kwargs):
        with mock.patch.object(orchestrator, "MercadoLivreApi", FakeMercadoLivreApi):
            return orchestrator.run_pipeline(settings=settings_override or self.settings, **kwargs)

    def _published(self) -> tuple | None:
        with session_scope(self.settings) as session:
            row = session.execute(
                text("SELECT snapshot_id, run_id, bucket_key, status, published_at FROM vw_market_published_snapshot")
            ).first()
        return None if row is None else tuple(row)

    def _current_ids(self) -> list[str]:
        with session_scope(self.settings) as session:
            return session.execute(text("SELECT external_id FROM vw_market_product_current")).scalars().all()

    def test_successful_run_is_published_automatically(self) -> None:
        result = self._run()

        self.assertIsNotNone(result.published_snapshot_id)
        snapshot_id, run_id, bucket_key, status, published_at = self._published()
        self.assertEqual(result.published_snapshot_id, snapshot_id)
        self.assertEqual((result.run_id, result.bucket_key, "SUCCESS"), (run_id, bucket_key, status))
        self.assertIsNotNone(published_at)

    def test_failed_run_is_not_published_and_views_stay_empty(self) -> None:
        with mock.patch.object(
            FakeMercadoLivreApi, "get_highlights_by_category", side_effect=ApiError("HTTP 403", 403)
        ):
            result = self._run()

        self.assertEqual("FAILED", result.status)
        self.assertIsNone(result.published_snapshot_id)
        self.assertIsNone(self._published())
        self.assertEqual([], self._current_ids())

    def test_failed_run_keeps_previous_publication(self) -> None:
        first = self._run()
        with mock.patch.object(
            FakeMercadoLivreApi, "get_highlights_by_category", side_effect=ApiError("HTTP 403", 403)
        ):
            self._run()

        self.assertEqual(first.run_id, self._published()[1])
        self.assertIn(MICROWAVE, self._current_ids())

    def test_views_follow_only_the_published_run(self) -> None:
        first = self._run()
        with mock.patch.dict(HIGHLIGHTS_CONTENT, without_microwave(), clear=True):
            second = self._run(settings_override=replace(self.settings, auto_publish=False))

        self.assertIsNone(second.published_snapshot_id)
        self.assertEqual(first.run_id, self._published()[1])
        self.assertIn(MICROWAVE, self._current_ids())

        with session_scope(self.settings) as session:
            snapshot = publish_run(session, second.run_id)
            again = publish_run(session, second.run_id)
        self.assertEqual(snapshot.snapshot_id, again.snapshot_id, "publicar de novo e idempotente")
        self.assertEqual(second.run_id, self._published()[1])
        self.assertNotIn(MICROWAVE, self._current_ids())

        with session_scope(self.settings) as session:
            summarized = session.execute(text("SELECT SUM(product_count) FROM vw_market_category_summary")).scalar()
            with_domain = session.execute(
                text("SELECT COUNT(*) FROM vw_market_product_current WHERE domain_id IS NOT NULL")
            ).scalar()
        self.assertEqual(with_domain, summarized, "os resumos agregam somente o run publicado")

    def test_publish_rejects_failed_and_unknown_runs(self) -> None:
        with mock.patch.object(
            FakeMercadoLivreApi, "get_highlights_by_category", side_effect=ApiError("HTTP 403", 403)
        ):
            failed = self._run()

        with self.assertRaises(PublishError), session_scope(self.settings) as session:
            publish_run(session, failed.run_id)
        with self.assertRaises(RunNotFoundError), session_scope(self.settings) as session:
            publish_run(session, "missing")

    def test_auto_publish_failure_does_not_fail_the_run(self) -> None:
        with mock.patch.object(orchestrator.publish, "publish_run", side_effect=RuntimeError("db down")):
            result = self._run()

        self.assertEqual("SUCCESS", result.status)
        self.assertIsNone(result.published_snapshot_id)
        self.assertIsNone(self._published())


class CliTest(PipelineTestCase):
    def _main(self, *argv: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(out),
            redirect_stderr(err),
            mock.patch.object(orchestrator, "MercadoLivreApi", FakeMercadoLivreApi),
        ):
            code = main.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_run_publish_and_status_commands(self) -> None:
        code, out, _ = self._main("run")
        self.assertEqual(0, code)
        self.assertIn("status: SUCCESS", out)
        self.assertIn("snapshot publicado:", out)

        with session_scope(self.settings) as session:
            run_id = latest_published_snapshot(session).run_id

        code, out, _ = self._main("publish", "--run-id", run_id)
        self.assertEqual(0, code)
        self.assertIn(f"run_id: {run_id}", out)

        code, out, _ = self._main("status")
        self.assertEqual(0, code)
        self.assertIn(f"run_id={run_id}", out)
        self.assertIn("market_published_snapshot", out)

    def test_publish_unknown_run_fails(self) -> None:
        code, _, err = self._main("publish", "--run-id", "missing")
        self.assertEqual(1, code)
        self.assertIn("missing", err)

    def test_status_without_publication(self) -> None:
        code, out, _ = self._main("status")
        self.assertEqual(0, code)
        self.assertIn("Nenhum snapshot publicado.", out)


class AutoPublishSettingTest(PipelineTestCase):
    def test_flag_is_parsed_from_environment(self) -> None:
        for raw, expected in (("false", False), ("0", False), ("TRUE", True), ("", True)):
            with mock.patch.dict("os.environ", {"ETL_AUTO_PUBLISH": raw}):
                self.assertEqual(expected, config_module.load_settings(use_dotenv=False).auto_publish, raw)

    def test_invalid_flag_raises(self) -> None:
        with mock.patch.dict("os.environ", {"ETL_AUTO_PUBLISH": "maybe"}), self.assertRaises(ConfigError):
            config_module.load_settings(use_dotenv=False)
