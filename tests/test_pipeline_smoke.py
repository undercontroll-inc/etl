from __future__ import annotations

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

from sqlalchemy import select, text, update

from ml_etl import config as config_module
from ml_etl.db import engine as engine_module
from ml_etl.db import session_scope
from ml_etl.db.models import EtlRejection, MarketCuratedProduct, MarketPriceObservation, MarketSnapshot
from ml_etl.db.repository import table_counts
from ml_etl.db.views import VIEWS
from ml_etl.domain.attributes import build_product_key, extract_attributes, parse_capacity, parse_power_watts
from ml_etl.domain.buckets import bucket_key_for, previous_bucket_key, rank_band, trajectory_label
from ml_etl.domain.canonical import canonicalize
from ml_etl.domain.whitelist import classify_small_appliance
from ml_etl.pipeline import orchestrator, run_pipeline
from ml_etl.pipeline.offers import aggregate_offers, percentile
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
            "market_price_observation",
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

    def test_catalog_offers_populate_price_and_real_category(self) -> None:
        result = self._run()
        self.assertEqual("SUCCESS", result.status, result.error_message)
        self.assertGreater(result.stats["price_observations"], 0)

        with session_scope(self.settings) as session:
            microwave = session.execute(
                select(MarketCuratedProduct).where(MarketCuratedProduct.external_id == "MLB19617678")
            ).scalar_one()
            observation = session.execute(
                select(MarketPriceObservation).where(MarketPriceObservation.external_id == "MLB19617678")
            ).scalar_one()

        self.assertEqual("MLB-MICROWAVES", microwave.domain_id)
        self.assertEqual("MLB73057", microwave.category_id, "category_id deve ser o ID real, nao o domain_id")
        self.assertEqual("Micro-ondas", microwave.category_name)
        self.assertEqual("electrolux", microwave.brand_slug)
        self.assertEqual("electrolux:me23s", microwave.product_key)
        self.assertEqual(1400, microwave.power_watts)
        self.assertEqual("220V", microwave.voltage)
        self.assertEqual("L", microwave.capacity_unit)
        self.assertEqual(18, microwave.offer_count)
        self.assertIsNotNone(microwave.price_median)
        self.assertIsNotNone(microwave.relevance)
        self.assertEqual("DOMINIO_WHITELIST", microwave.domain_reason)

        self.assertEqual(526.16, float(observation.price_min))
        self.assertEqual(641.90, float(observation.price_max))
        self.assertEqual("MLB3003724516", observation.winner_item_id)
        self.assertEqual("MLB73057", observation.category_id)
        self.assertEqual(3, observation.seller_count)

    def test_blacklisted_domain_is_rejected_even_with_valid_rank(self) -> None:
        result = self._run()
        self.assertEqual("SUCCESS", result.status, result.error_message)

        with session_scope(self.settings) as session:
            refill = session.execute(
                select(MarketCuratedProduct).where(MarketCuratedProduct.external_id == "MLB27389460")
            ).scalar()
            rejections = session.execute(
                select(EtlRejection).where(EtlRejection.external_id == "MLB27389460")
            ).scalars().all()

        self.assertIsNone(refill, "refil de filtro nao deveria chegar na tabela curada")
        self.assertTrue(
            any("DOMINIO_BLACKLIST" in (item.detail or "") for item in rejections),
            f"esperava rejeicao por dominio, recebido {[item.detail for item in rejections]}",
        )

    def test_price_delta_is_computed_against_previous_bucket(self) -> None:
        first = self._run()
        previous = previous_bucket_key(first.bucket_key, "month")

        with session_scope(self.settings) as session:
            session.execute(
                update(MarketPriceObservation)
                .where(MarketPriceObservation.bucket_key == first.bucket_key)
                .values(bucket_key=previous, price_median=500.0)
            )

        second = self._run()
        self.assertEqual("SUCCESS", second.status, second.error_message)

        with session_scope(self.settings) as session:
            microwave = session.execute(
                select(MarketCuratedProduct).where(
                    MarketCuratedProduct.external_id == "MLB19617678",
                    MarketCuratedProduct.bucket_key == second.bucket_key,
                )
            ).scalar_one()

        self.assertEqual(19.80, float(microwave.price_delta_pct), "599 sobre 500 representa +19,8%")

    def test_consumption_views_expose_grouped_data(self) -> None:
        result = self._run()
        self.assertEqual("SUCCESS", result.status, result.error_message)

        with session_scope(self.settings) as session:
            for view_name, _ in VIEWS:
                total = session.execute(text(f"SELECT COUNT(*) FROM {view_name}")).scalar()
                self.assertIsNotNone(total, f"view {view_name} nao respondeu")

            domains = session.execute(
                text("SELECT domain_id, product_count, brand_count FROM vw_market_category_summary")
            ).all()
            brands = session.execute(text("SELECT brand_slug FROM vw_market_brand_summary")).scalars().all()
            rising = session.execute(text("SELECT title FROM vw_market_rising_products")).scalars().all()

        self.assertIn("MLB-MICROWAVES", [row[0] for row in domains])
        self.assertIn("electrolux", brands)
        self.assertTrue(rising, "a tabela de produtos em alta nao deveria estar vazia")

    def test_skip_api_replays_price_observations(self) -> None:
        first = self._run()
        with session_scope(self.settings) as session:
            baseline = table_counts(session)["market_price_observation"]

        replay = self._run(run_id=first.run_id, skip_api=True)
        self.assertEqual("SUCCESS", replay.status, replay.error_message)
        self.assertEqual(first.stats["price_observations"], replay.stats["price_observations"])

        with session_scope(self.settings) as session:
            after = table_counts(session)["market_price_observation"]

        self.assertEqual(baseline, after)

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


class OfferAggregationTest(unittest.TestCase):
    PAYLOAD = {
        "paging": {"total": 18},
        "results": [
            {"item_id": "A", "price": 100.0, "original_price": 200.0, "seller_id": 1, "category_id": "MLB73057"},
            {"item_id": "B", "price": 200.0, "original_price": None, "seller_id": 2, "category_id": "MLB73057"},
            {"item_id": "C", "price": 300.0, "original_price": None, "seller_id": 2, "category_id": "MLB1234"},
        ],
    }

    def test_aggregates_prices_and_competition(self) -> None:
        offers = aggregate_offers("PRODUCT", "MLB1", self.PAYLOAD)

        self.assertEqual(18, offers.offer_count, "offer_count deve vir de paging.total")
        self.assertEqual(2, offers.seller_count, "vendedores repetidos contam uma vez")
        self.assertEqual(100.0, offers.price_min)
        self.assertEqual(200.0, offers.price_median)
        self.assertEqual(300.0, offers.price_max)
        self.assertEqual("A", offers.winner_item_id)
        self.assertEqual(100.0, offers.winner_price)
        self.assertEqual(50.0, offers.discount_pct_median)
        self.assertEqual("MLB73057", offers.category_id, "deve escolher a categoria mais frequente")

    def test_offer_count_falls_back_to_result_length(self) -> None:
        offers = aggregate_offers("PRODUCT", "MLB1", {"results": self.PAYLOAD["results"]})
        self.assertEqual(3, offers.offer_count)

    def test_ignores_non_positive_and_missing_prices(self) -> None:
        offers = aggregate_offers(
            "PRODUCT",
            "MLB1",
            {"results": [{"item_id": "A", "price": 0}, {"item_id": "B", "price": None}, {"item_id": "C", "price": 50}]},
        )
        self.assertEqual(50.0, offers.price_min)
        self.assertEqual(50.0, offers.price_max)

    def test_returns_none_without_usable_payload(self) -> None:
        self.assertIsNone(aggregate_offers("PRODUCT", "MLB1", None))
        self.assertIsNone(aggregate_offers("PRODUCT", "MLB1", {"results": []}))
        self.assertIsNone(aggregate_offers("PRODUCT", "MLB1", {"paging": {"total": 5}}))

    def test_percentile_interpolates(self) -> None:
        self.assertIsNone(percentile([], 0.5))
        self.assertEqual(10.0, percentile([10.0], 0.5))
        self.assertEqual(15.0, percentile([10.0, 20.0], 0.5))
        self.assertEqual(20.0, percentile([10.0, 20.0, 30.0], 0.5))


class AttributeParsingTest(unittest.TestCase):
    def test_parses_brazilian_thousand_separator_as_power(self) -> None:
        self.assertEqual(1500, parse_power_watts("1.500 W"))
        self.assertEqual(1800, parse_power_watts("1800 W"))
        self.assertIsNone(parse_power_watts("0 W"))

    def test_parses_capacity_with_written_units(self) -> None:
        self.assertEqual((1.8, "L"), parse_capacity("1.8 L"))
        self.assertEqual((12.0, "L"), parse_capacity("12 Litros"))
        self.assertEqual((4.5, "L"), parse_capacity("4,5L"))
        self.assertEqual((500.0, "ML"), parse_capacity("500 ml"))
        self.assertEqual((1.3, "KG"), parse_capacity("1.3 kg"))

    def test_extracts_full_attribute_set(self) -> None:
        parsed = extract_attributes(
            [
                {"id": "BRAND", "value_name": "Electrolux"},
                {"id": "MODEL", "value_name": "ME23S"},
                {"id": "POWER", "value_name": "1400 W"},
                {"id": "VOLTAGE", "value_name": "110/220V"},
                {"id": "VOLUME_CAPACITY", "value_name": "23 L"},
                {"id": "ENERGY_EFFICIENCY", "value_name": "A"},
            ]
        )
        self.assertEqual("Electrolux", parsed.brand)
        self.assertEqual("ME23S", parsed.model)
        self.assertEqual(1400, parsed.power_watts)
        self.assertEqual("BIVOLT", parsed.voltage)
        self.assertEqual(23.0, parsed.capacity_value)
        self.assertEqual("A", parsed.energy_efficiency)

    def test_reads_brand_from_values_list_and_fallback(self) -> None:
        self.assertEqual("Mondial", extract_attributes([{"id": "BRAND", "values": [{"name": "Mondial"}]}]).brand)
        self.assertEqual("Consul", extract_attributes([], fallback_brand="Consul").brand)

    def test_product_key_prefers_model_then_domain(self) -> None:
        self.assertEqual("mondial:af-14", build_product_key("Mondial", "AF-14", "MLB-AIR_FRYERS"))
        self.assertEqual("mondial:mlb-air-fryers", build_product_key("Mondial", None, "MLB-AIR_FRYERS"))
        self.assertIsNone(build_product_key(None, None, None))


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

    def test_term_blacklist_wins_over_whitelisted_domain(self) -> None:
        accessory = classify_small_appliance(
            "Capa Protetora Para Air Fryer Mondial", "MLB456045", "MLB-AIR_FRYERS"
        )
        self.assertFalse(accessory.in_domain)
        self.assertEqual("TERMO_BLACKLIST", accessory.reason)

    def test_domain_whitelist_accepts_real_category_ids(self) -> None:
        microwave = classify_small_appliance(
            "Micro-ondas Electrolux 23L Branco Efficient", "MLB73057", "MLB-MICROWAVES"
        )
        self.assertTrue(microwave.in_domain)
        self.assertEqual("DOMINIO_WHITELIST", microwave.reason)
        self.assertGreaterEqual(microwave.relevance, 0.75)

    def test_incidental_accessory_mention_does_not_reject_appliance(self) -> None:
        vacuum = classify_small_appliance(
            "Aspirador Pó E Água 1400w 10 Litros Filtro Lavável Gtw10 Wap",
            None,
            "MLB-VACUUM_AND_STEAM_CLEANERS",
        )
        self.assertTrue(vacuum.in_domain, "filtro lavavel e caracteristica do aspirador, nao o produto")

        scale = classify_small_appliance("Balança Digital Cozinha Inox 10kg Alimentos Pilha Nixzen")
        self.assertTrue(scale.in_domain, "pilha inclusa nao torna a balanca um acessorio")

        cover = classify_small_appliance("Capa Protetora Para Air Fryer Mondial", None, "MLB-AIR_FRYERS")
        self.assertFalse(cover.in_domain, "acessorio que abre o titulo continua rejeitado")

        replacement = classify_small_appliance("Filtro original Consul w11199342 para purificador de água")
        self.assertFalse(replacement.in_domain, "o filtro vem antes do purificador, logo e peca")

    def test_domain_blacklist_rejects_parts_and_bulbs(self) -> None:
        for title, domain in (
            ("Refil Filtro Acqua Pure Electrolux", "MLB-WATER_PURIFIER_FILTERS_REPLACEMENTS"),
            ("Kit 2 Lampada Led 40w Bulbo 6500k", "MLB-LIGHT_BULBS"),
        ):
            classification = classify_small_appliance(title, None, domain)
            self.assertFalse(classification.in_domain, title)
            self.assertEqual("DOMINIO_BLACKLIST", classification.reason)

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
