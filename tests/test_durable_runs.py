from __future__ import annotations

import unittest
from unittest import mock

from sqlalchemy import func, select, update

from ml_etl.db import session_scope
from ml_etl.db.models import EtlRejection, EtlRun, MarketPriceObservation, MarketRawResponse, MarketSnapshot
from ml_etl.db.repository import RunNotFoundError, count_prior_buckets, fetch_previous_snapshots, finish_run
from ml_etl.domain.buckets import previous_bucket_key
from ml_etl.pipeline import orchestrator
from tests.support import PipelineTestCase


def first_collected_at(session, run_id: str):
    return session.execute(
        select(MarketSnapshot.collected_at).where(MarketSnapshot.run_id == run_id).limit(1)
    ).scalar()


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
                select(MarketPriceObservation.collected_at)
                .where(MarketPriceObservation.run_id == replay.run_id)
                .limit(1)
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

    def test_previous_bucket_ignores_failed_runs(self) -> None:
        first = self._run()
        previous = previous_bucket_key(first.bucket_key, "month")
        with session_scope(self.settings) as session:
            session.execute(
                update(MarketSnapshot).where(MarketSnapshot.run_id == first.run_id).values(bucket_key=previous)
            )
            session.add(EtlRun(run_id="failed", bucket_key=previous, status="FAILED"))
            session.flush()
            session.add(
                MarketSnapshot(
                    run_id="failed",
                    source="HIGHLIGHTS_CATEGORY",
                    external_type="PRODUCT",
                    external_id="MLB19617678",
                    category_id="MLB5726",
                    rank=20,
                    rank_band="MODERADO",
                    collected_at=first_collected_at(session, first.run_id),
                    bucket_key=previous,
                )
            )

        with session_scope(self.settings) as session:
            ranks = {row.external_id: row.rank for row in fetch_previous_snapshots(session, first.bucket_key)}
            prior = count_prior_buckets(session, first.bucket_key)

        self.assertEqual(2, ranks["MLB19617678"], "a execucao FAILED nao serve de referencia")
        self.assertEqual(1, prior[("PRODUCT", "MLB19617678")])

    def test_replay_of_unknown_run_raises(self) -> None:
        with self.assertRaises(RunNotFoundError):
            self._run(run_id="missing", skip_api=True)

    def test_finish_run_requires_existing_run(self) -> None:
        with self.assertRaises(RunNotFoundError), session_scope(self.settings) as session:
            finish_run(session, "missing", "FAILED")


if __name__ == "__main__":
    unittest.main()
