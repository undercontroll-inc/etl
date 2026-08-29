from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

from sqlalchemy import select, update

from ml_etl import config as config_module
from ml_etl.db import engine as engine_module
from ml_etl.db import session_scope
from ml_etl.db.models import MarketCuratedProduct, MarketSnapshot
from ml_etl.db.repository import table_counts
from ml_etl.domain.buckets import bucket_key_for, previous_bucket_key, rank_band, trajectory_label
from ml_etl.domain.canonical import canonicalize
from ml_etl.domain.whitelist import classify_small_appliance
from ml_etl.pipeline import orchestrator, run_pipeline
from tests.fake_api import FakeMercadoLivreApi

TARGET_CATEGORIES = ["MLB5726", "MLB432825"]


class PipelineSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)
        os.environ.clear()
        os.environ.update(
            {
                "DB_BACKEND": "memory",
                "ML_CLIENT_ID": "fake-client-id",
                "ML_CLIENT_SECRET": "fake-client-secret",
                "ETL_TARGET_CATEGORIES": ",".join(TARGET_CATEGORIES),
                "ETL_SNAPSHOT_BUCKET": "month",
                "ETL_MAX_WORKERS": "2",
                "ETL_LOG_LEVEL": "CRITICAL",
            }
        )
        config_module.reset_settings_cache()
        engine_module.dispose_engine()
        self.settings = config_module.load_settings(use_dotenv=False)
        self.assertTrue(self.settings.database.is_memory, "a suite deve rodar apenas em memoria")

    def tearDown(self) -> None:
        engine_module.dispose_engine()
        config_module.reset_settings_cache()
        os.environ.clear()
        os.environ.update(self._env)

    def _run(self, **kwargs):
        with mock.patch.object(orchestrator, "MercadoLivreApi", FakeMercadoLivreApi):
            return run_pipeline(settings=self.settings, **kwargs)

    @staticmethod
    def _curated_scores(session, bucket_key: str) -> dict[str, float]:
        rows = session.execute(
            select(MarketCuratedProduct).where(MarketCuratedProduct.bucket_key == bucket_key)
        ).scalars()
        return {f"{row.external_type}:{row.external_id}": float(row.score) for row in rows}

    def test_pipeline_runs_end_to_end(self) -> None:
        result = self._run()

        self.assertEqual("SUCCESS", result.status, result.error_message)
        self.assertEqual(bucket_key_for(datetime.now(timezone.utc), "month"), result.bucket_key)
        self.assertGreater(result.stats["snapshots"], 0)
        self.assertGreater(result.stats["curated"], 0)
        self.assertGreater(result.stats["trends"], 0)
        self.assertGreater(result.stats["rejections"], 0)

        with session_scope(self.settings) as session:
            counts = table_counts(session)

        self.assertEqual(1, counts["etl_run"])
        self.assertGreater(counts["market_entity"], 0)
        self.assertGreater(counts["market_snapshot"], 0)
        self.assertGreater(counts["market_curated_product"], 0)
        self.assertGreater(counts["market_score"], 0)
        self.assertGreater(counts["market_category"], 0)
        self.assertGreater(counts["etl_rejection"], 0)

    def test_reruns_are_idempotent(self) -> None:
        first = self._run()
        self.assertEqual("SUCCESS", first.status, first.error_message)

        with session_scope(self.settings) as session:
            baseline = table_counts(session)
            baseline_scores = self._curated_scores(session, first.bucket_key)

        second = self._run()
        third = self._run()
        self.assertEqual("SUCCESS", second.status, second.error_message)
        self.assertEqual("SUCCESS", third.status, third.error_message)

        with session_scope(self.settings) as session:
            after = table_counts(session)
            after_scores = self._curated_scores(session, first.bucket_key)

        self.assertEqual(baseline_scores, after_scores, "os scores mudaram entre reexecucoes do mesmo bucket")

        for table in (
            "market_entity",
            "market_snapshot",
            "market_trend",
            "market_score",
            "market_curated_product",
            "market_category",
        ):
            self.assertEqual(baseline[table], after[table], f"tabela {table} duplicou linhas")

        self.assertEqual(3, after["etl_run"])

    def test_invalid_domain_rows_are_rejected(self) -> None:
        result = self._run()
        rules = result.stats["rejections_by_rule"]

        self.assertTrue(
            any("external_type fora do dominio" in key for key in rules),
            f"esperava rejeicao de external_type invalido, recebido {rules}",
        )
        self.assertTrue(
            any("rank fora do intervalo" in key for key in rules),
            f"esperava rejeicao de rank invalido, recebido {rules}",
        )
        self.assertTrue(
            any("pequenos eletrodomesticos" in key for key in rules),
            f"esperava rejeicao de negocio, recebido {rules}",
        )

    def test_day_bucket_creates_new_snapshot(self) -> None:
        monthly = self._run()
        with session_scope(self.settings) as session:
            before = table_counts(session)["market_snapshot"]

        daily = self._run(bucket="day")
        self.assertEqual("SUCCESS", daily.status, daily.error_message)
        self.assertNotEqual(monthly.bucket_key, daily.bucket_key)

        with session_scope(self.settings) as session:
            after = table_counts(session)["market_snapshot"]

        self.assertGreater(after, before)

    def test_skip_api_reprocesses_from_raw(self) -> None:
        first = self._run()
        self.assertEqual("SUCCESS", first.status, first.error_message)

        with session_scope(self.settings) as session:
            baseline = table_counts(session)

        replay = self._run(run_id=first.run_id, skip_api=True)
        self.assertEqual("SUCCESS", replay.status, replay.error_message)
        self.assertEqual(first.run_id, replay.run_id)
        self.assertEqual(first.stats["curated"], replay.stats["curated"])

        with session_scope(self.settings) as session:
            after = table_counts(session)

        self.assertEqual(baseline["market_curated_product"], after["market_curated_product"])
        self.assertEqual(baseline["market_snapshot"], after["market_snapshot"])

    def test_skip_api_requires_run_id(self) -> None:
        with self.assertRaises(ValueError):
            self._run(skip_api=True)

    def test_trajectory_and_confidence_use_previous_bucket(self) -> None:
        first = self._run()
        self.assertEqual("SUCCESS", first.status, first.error_message)

        previous = previous_bucket_key(first.bucket_key, "month")
        with session_scope(self.settings) as session:
            session.execute(
                update(MarketSnapshot)
                .where(MarketSnapshot.bucket_key == first.bucket_key)
                .values(bucket_key=previous, rank=MarketSnapshot.rank + 5)
            )

        second = self._run()
        self.assertEqual("SUCCESS", second.status, second.error_message)

        with session_scope(self.settings) as session:
            curated = session.execute(
                select(MarketCuratedProduct).where(MarketCuratedProduct.bucket_key == second.bucket_key)
            ).scalars().all()

        self.assertTrue(curated)
        for row in curated:
            self.assertEqual(5, row.rank_delta, f"{row.external_id} deveria ter ganho 5 posicoes")
            self.assertEqual("CRESCIMENTO", row.trajectory_label)
            self.assertEqual("HIGH", row.confidence)


class DomainRulesTest(unittest.TestCase):
    def test_canonicalize_normalizes_accents_and_units(self) -> None:
        canonical = canonicalize("Air Fryer Mondial 4 Litros Preta 1500 Watts Com Frete Grátis")
        self.assertNotIn("grátis", canonical)
        self.assertIn("4l", canonical)
        self.assertIn("1500w", canonical)
        self.assertEqual(canonical, canonical.lower())

    def test_canonicalize_preserves_decimal_volumes(self) -> None:
        canonical = canonicalize("Batedeira Planetária Arno 600W Deluxe 4,5L")
        self.assertIn("4.5l", canonical)
        self.assertIn("600w", canonical)

    def test_blacklist_takes_precedence_over_whitelist(self) -> None:
        accessory = classify_small_appliance("Capa Protetora Para Air Fryer Mondial", "MLB432825")
        self.assertFalse(accessory.in_domain)
        self.assertEqual("TERMO_BLACKLIST", accessory.reason)

    def test_canonicalize_handles_empty_input(self) -> None:
        self.assertEqual("", canonicalize(None))
        self.assertEqual("", canonicalize("   "))

    def test_rank_band_ranges(self) -> None:
        self.assertEqual("MUITO_ALTO", rank_band(1))
        self.assertEqual("MUITO_ALTO", rank_band(5))
        self.assertEqual("ALTO", rank_band(6))
        self.assertEqual("ALTO", rank_band(10))
        self.assertEqual("MODERADO", rank_band(11))
        self.assertEqual("MODERADO", rank_band(20))
        self.assertEqual("INDEFINIDO", rank_band(21))
        self.assertEqual("INDEFINIDO", rank_band(None))

    def test_trajectory_label_thresholds(self) -> None:
        self.assertEqual("SEM_HISTORICO", trajectory_label(None))
        self.assertEqual("CRESCIMENTO_FORTE", trajectory_label(10))
        self.assertEqual("CRESCIMENTO", trajectory_label(3))
        self.assertEqual("ESTAVEL", trajectory_label(0))
        self.assertEqual("QUEDA", trajectory_label(-4))

    def test_previous_bucket_key(self) -> None:
        self.assertEqual("2026-07", previous_bucket_key("2026-08", "month"))
        self.assertEqual("2025-12", previous_bucket_key("2026-01", "month"))
        self.assertEqual("2026-08-28", previous_bucket_key("2026-08-29", "day"))
        self.assertEqual("2026-07-31", previous_bucket_key("2026-08-01", "day"))

    def test_classifier_accepts_appliances_and_rejects_accessories(self) -> None:
        appliance = classify_small_appliance("Air Fryer Mondial 4L 1500W", "MLB432825")
        self.assertTrue(appliance.in_domain)
        self.assertGreater(appliance.relevance, 0.5)

        accessory = classify_small_appliance("Capa Protetora Reposição", "MLB432825")
        self.assertFalse(accessory.in_domain)
        self.assertEqual("TERMO_BLACKLIST", accessory.reason)

        bundle = classify_small_appliance("Air Fryer Philips Walita 4,1L Digital", None)
        self.assertTrue(bundle.in_domain)

        unrelated = classify_small_appliance("Camiseta Estampada Algodão", None)
        self.assertFalse(unrelated.in_domain)


if __name__ == "__main__":
    unittest.main()
