from __future__ import annotations

import os
import unittest
from unittest import mock

from ml_etl import config as config_module
from ml_etl.api.http_client import ApiError, ApiResponse
from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.pipeline.offers import INCOMPLETE_PAGINATION, collect_offers, merge_pages, offers_from_raw


def offer(item_id: str, price: float, seller: int) -> dict:
    return {"item_id": item_id, "price": price, "seller_id": seller, "currency_id": "BRL"}


class PagedApi:
    def __init__(self, results: list[dict], total: int, limit: int, fail_at: int | None = None) -> None:
        self.results = results
        self.total = total
        self.limit = limit
        self.fail_at = fail_at
        self.offsets: list[int] = []

    def get_product_items(self, product_id: str, offset: int = 0) -> ApiResponse:
        self.offsets.append(offset)
        if self.fail_at is not None and offset >= self.fail_at:
            raise ApiError("HTTP 429", 429)
        return ApiResponse(
            endpoint=f"/products/{product_id}/items",
            params={"offset": offset} if offset else {},
            status_code=200,
            payload={
                "paging": {"total": self.total, "offset": offset, "limit": self.limit},
                "results": self.results[offset : offset + self.limit],
            },
        )


class OfferPaginationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._env = dict(os.environ)
        os.environ.clear()
        os.environ.update({"DB_BACKEND": "memory", "ETL_MAX_WORKERS": "1", "ETL_OFFERS_MAX_PAGES": "3"})
        self.settings = config_module.load_settings(use_dotenv=False)
        self.offers = [offer(f"I{index}", 100.0 + index, index % 3) for index in range(5)]

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_paginates_until_every_offer_is_observed(self) -> None:
        api = PagedApi(self.offers, total=5, limit=2)

        result = collect_offers(api, [("PRODUCT", "MLB1")], self.settings)
        aggregated = result.offers[("PRODUCT", "MLB1")]

        self.assertEqual([0, 2, 4], api.offsets)
        self.assertEqual(5, aggregated.available_offer_count)
        self.assertEqual(5, aggregated.observed_offer_count)
        self.assertEqual(5, aggregated.valid_price_count)
        self.assertTrue(aggregated.sample_complete)
        self.assertEqual(3, aggregated.observed_seller_count)
        self.assertEqual(3, len(result.payloads), "cada pagina vira um RAW proprio")

    def test_page_cap_keeps_sample_incomplete(self) -> None:
        api = PagedApi(self.offers * 2, total=10, limit=2)

        aggregated = collect_offers(api, [("PRODUCT", "MLB1")], self.settings).offers[("PRODUCT", "MLB1")]

        self.assertEqual(3, len(api.offsets), "ETL_OFFERS_MAX_PAGES limita as chamadas")
        self.assertEqual(10, aggregated.available_offer_count)
        self.assertEqual(5, aggregated.observed_offer_count, "ofertas repetidas entre paginas contam uma vez")
        self.assertFalse(aggregated.sample_complete)

    def test_failed_page_keeps_previous_pages_and_warns(self) -> None:
        api = PagedApi(self.offers, total=5, limit=2, fail_at=2)

        result = collect_offers(api, [("PRODUCT", "MLB1")], self.settings)

        aggregated = result.offers[("PRODUCT", "MLB1")]
        self.assertEqual(2, aggregated.observed_offer_count)
        self.assertFalse(aggregated.sample_complete)
        self.assertEqual([INCOMPLETE_PAGINATION], [item.rule for item in result.rejections])
        self.assertEqual("API_LIMITE", str(result.rejections[0].layer))

    def test_replay_rebuilds_the_same_aggregate_from_paged_raw(self) -> None:
        api = PagedApi(self.offers, total=5, limit=2)
        live = collect_offers(api, [("PRODUCT", "MLB1")], self.settings)

        replayed = offers_from_raw(list(reversed(live.payloads)))

        self.assertEqual(live.offers, replayed)

    def test_merge_pages_ignores_invalid_pages(self) -> None:
        self.assertIsNone(merge_pages([None, "x"]))
        merged = merge_pages([{"paging": {"total": "x"}, "results": [{"price": 1}, {"price": 2}, "bad"]}])
        self.assertIsNone(merged["paging"]["total"])
        self.assertEqual(2, len(merged["results"]), "ofertas sem item_id nao sao deduplicadas")


class ProductItemsRequestTest(unittest.TestCase):
    def test_offset_is_sent_only_after_first_page(self) -> None:
        client = mock.Mock()
        api = MercadoLivreApi(client=client, settings=mock.Mock())

        api.get_product_items("MLB1")
        api.get_product_items("MLB1", offset=50)

        first, second = client.get.call_args_list
        self.assertIsNone(first.kwargs["params"])
        self.assertEqual({"offset": 50}, second.kwargs["params"])


if __name__ == "__main__":
    unittest.main()
