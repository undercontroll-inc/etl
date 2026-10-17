from __future__ import annotations

import unittest
from unittest import mock

from sqlalchemy import func, select

from ml_etl.db import session_scope
from ml_etl.db.models import EtlRejection, EtlRun, MarketPriceObservation, MarketRawResponse
from ml_etl.db.repository import RunNotFoundError, finish_run
from ml_etl.pipeline import orchestrator
from tests.support import PipelineTestCase


class DurableRunTest(PipelineTestCase):
    def _raw_stages(self, run_id: str) -> dict[str, int]:
        with session_scope(self.settings) as session:
            rows = session.execute(
                select(MarketRawResponse.stage, func.count())
                .where(MarketRawResponse.run_id == run_id)
                .group_by(MarketRawResponse.stage)
            ).all()
        return {stage: total for stage, total in rows}

    def test_raw_of_finished_stages_survives_a_later_crash(self) -> None:
        with mock.patch.object(
            orchestrator.offers_module, "collect_offers", side_effect=RuntimeError("offers down")
        ):
            result = self._run()

        self.assertEqual("FAILED", result.status)
        stages = self._raw_stages(result.run_id)
        self.assertGreater(stages.get("EXTRACT", 0), 0)
        self.assertGreater(stages.get("ENRICH", 0), 0)

        with session_scope(self.settings) as session:
            run = session.get(EtlRun, result.run_id)
        self.assertEqual("FAILED", run.status)
        self.assertIsNotNone(run.finished_at)

    def test_failed_run_can_be_replayed_without_collecting_again(self) -> None:
        with mock.patch.object(
            orchestrator.score, "score_highlights", side_effect=RuntimeError("injected failure")
        ):
            failed = self._run()
        self.assertEqual("FAILED", failed.status)

        with mock.patch.object(orchestrator, "MercadoLivreApi", side_effect=AssertionError("sem API")):
            replay = orchestrator.run_pipeline(settings=self.settings, run_id=failed.run_id, skip_api=True)

        self.assertEqual("SUCCESS", replay.status, replay.error_message)
        self.assertGreater(replay.stats["curated"], 0)

    def test_replay_creates_new_run_pointing_to_the_original_raw(self) -> None:
        first = self._run()
        replay = self._run(run_id=first.run_id, skip_api=True)
        replay_of_replay = self._run(run_id=replay.run_id, skip_api=True)

        with session_scope(self.settings) as session:
            runs = {run.run_id: run for run in session.execute(select(EtlRun)).scalars()}
            replay_rejections = session.execute(
                select(func.count()).select_from(EtlRejection).where(EtlRejection.run_id == replay.run_id)
            ).scalar()
            first_rejections = session.execute(
                select(func.count()).select_from(EtlRejection).where(EtlRejection.run_id == first.run_id)
            ).scalar()
            observation_moment = session.execute(
                select(MarketPriceObservation.collected_at).limit(1)
            ).scalar()
            raw_moment = session.execute(
                select(func.max(MarketRawResponse.collected_at)).where(MarketRawResponse.run_id == first.run_id)
            ).scalar()

        self.assertEqual(3, len(runs))
        self.assertEqual(first.run_id, runs[replay.run_id].params["replay_of"])
        self.assertEqual(first.run_id, runs[replay_of_replay.run_id].params["replay_of"])
        self.assertEqual(first.bucket_key, replay.bucket_key)
        self.assertEqual(0, self._raw_stages(replay.run_id).get("EXTRACT", 0), "replay nao duplica RAW")
        self.assertLess(replay_rejections, first_rejections, "replay registra apenas as proprias rejeicoes")
        self.assertEqual(raw_moment.replace(tzinfo=None), observation_moment.replace(tzinfo=None))

    def test_replay_of_unknown_run_raises(self) -> None:
        with self.assertRaises(RunNotFoundError):
            self._run(run_id="missing", skip_api=True)

    def test_finish_run_requires_existing_run(self) -> None:
        with self.assertRaises(RunNotFoundError), session_scope(self.settings) as session:
            finish_run(session, "missing", "FAILED")


if __name__ == "__main__":
    unittest.main()
