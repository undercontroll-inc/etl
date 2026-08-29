from __future__ import annotations

from collections.abc import Iterator, Sequence

from ml_etl.api.http_client import ApiResponse

CATEGORY_PAYLOADS = {
    "MLB5726": {
        "id": "MLB5726",
        "name": "Eletrodomésticos",
        "permalink": "https://lista.mercadolivre.com.br/eletrodomesticos",
        "total_items_in_this_category": 1250000,
        "path_from_root": [
            {"id": "MLB1574", "name": "Casa, Móveis e Decoração"},
            {"id": "MLB5726", "name": "Eletrodomésticos"},
        ],
    },
    "MLB432825": {
        "id": "MLB432825",
        "name": "Fritadeiras Elétricas",
        "permalink": "https://lista.mercadolivre.com.br/fritadeiras",
        "total_items_in_this_category": 48000,
        "path_from_root": [
            {"id": "MLB5726", "name": "Eletrodomésticos"},
            {"id": "MLB432825", "name": "Fritadeiras Elétricas"},
        ],
    },
}

HIGHLIGHTS_CONTENT = {
    "MLB5726": [
        {"id": "MLB6868664726", "position": 1, "type": "ITEM"},
        {"id": "MLB24162817", "position": 4, "type": "PRODUCT"},
        {"id": "MLBU3013800008", "position": 9, "type": "USER_PRODUCT"},
        {"id": "MLB9999999999", "position": 12, "type": "ITEM"},
        {"id": "MLB5555555555", "position": 45, "type": "ITEM"},
        {"id": "MLB4444444444", "position": 3, "type": "CATALOG"},
    ],
    "MLB432825": [
        {"id": "MLB7777777777", "position": 2, "type": "ITEM"},
        {"id": "MLB24162817", "position": 6, "type": "PRODUCT"},
    ],
}

TRENDS_PAYLOAD = [
    {"keyword": "air fryer", "url": "https://lista.mercadolivre.com.br/air-fryer"},
    {"keyword": "cafeteira expresso", "url": "https://lista.mercadolivre.com.br/cafeteira-expresso"},
    {"keyword": "liquidificador", "url": "https://lista.mercadolivre.com.br/liquidificador"},
]

ITEM_BODIES = {
    "MLB6868664726": {
        "id": "MLB6868664726",
        "title": "Air Fryer Mondial 4L Preta 1500W Fritadeira Elétrica Sem Óleo",
        "category_id": "MLB432825",
        "price": 349.9,
        "available_quantity": 120,
        "condition": "new",
        "permalink": "https://produto.mercadolivre.com.br/MLB6868664726",
        "attributes": [{"id": "BRAND", "value_name": "Mondial"}],
    },
    "MLB9999999999": {
        "id": "MLB9999999999",
        "title": "Capa Protetora Para Air Fryer Kit Reposição",
        "category_id": "MLB432825",
        "price": 29.9,
        "available_quantity": 500,
        "condition": "new",
        "permalink": "https://produto.mercadolivre.com.br/MLB9999999999",
        "attributes": [{"id": "BRAND", "value_name": "Genérico"}],
    },
    "MLB7777777777": {
        "id": "MLB7777777777",
        "title": "Cafeteira Expresso Automática Oster 1300W 220V Inox",
        "category_id": "MLB432825",
        "price": 899.0,
        "available_quantity": 40,
        "condition": "new",
        "permalink": "https://produto.mercadolivre.com.br/MLB7777777777",
        "attributes": [{"id": "BRAND", "value_name": "Oster"}],
    },
}

PRODUCT_BODIES = {
    "MLB24162817": {
        "id": "MLB24162817",
        "name": "Liquidificador Philco PH900 1200W",
        "domain_id": "MLB-BLENDERS",
        "category_id": "MLB5726",
        "attributes": [{"id": "BRAND", "value_name": "Philco"}],
        "buy_box_winner": {"price": 259.9},
    }
}

USER_PRODUCT_BODIES = {
    "MLBU3013800008": {
        "id": "MLBU3013800008",
        "title": "Batedeira Planetária Arno 600W Deluxe 4,5L",
        "category_id": "MLB5726",
        "price": 549.0,
        "permalink": "https://produto.mercadolivre.com.br/MLBU3013800008",
        "attributes": [{"id": "BRAND", "value_name": "Arno"}],
    }
}


class FakeMercadoLivreApi:
    def __init__(self, settings=None, client=None) -> None:
        self.settings = settings
        self.calls: list[str] = []

    def __enter__(self) -> FakeMercadoLivreApi:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def close(self) -> None:
        return None

    def _record(self, endpoint: str) -> None:
        self.calls.append(endpoint)

    def get_category(self, category_id: str) -> ApiResponse:
        endpoint = f"/categories/{category_id}"
        self._record(endpoint)
        payload = CATEGORY_PAYLOADS.get(category_id)
        return ApiResponse(
            endpoint=endpoint,
            params={},
            status_code=200 if payload else 404,
            payload=payload,
        )

    def get_highlights_by_category(self, category_id: str, attribute=None, attribute_value=None) -> ApiResponse:
        endpoint = f"/highlights/MLB/category/{category_id}"
        self._record(endpoint)
        content = HIGHLIGHTS_CONTENT.get(category_id, [])
        return ApiResponse(
            endpoint=endpoint,
            params={},
            status_code=200,
            payload={
                "query_data": {"highlight_type": "BEST_SELLER", "criteria": "CATEGORY", "id": category_id},
                "content": content,
            },
        )

    def get_trends(self, category_id: str | None = None) -> ApiResponse:
        endpoint = "/trends/MLB" if not category_id else f"/trends/MLB/{category_id}"
        self._record(endpoint)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200, payload=TRENDS_PAYLOAD)

    def get_items_multiget(self, item_ids: Sequence[str]) -> ApiResponse:
        self._record("/items")
        entries = []
        for item_id in item_ids:
            body = ITEM_BODIES.get(item_id)
            if body is None:
                entries.append({"code": 404, "body": {"id": item_id}})
            else:
                entries.append({"code": 200, "body": body})
        return ApiResponse(endpoint="/items", params={"ids": ",".join(item_ids)}, status_code=200, payload=entries)

    def iter_item_batches(self, item_ids: Sequence[str]) -> Iterator[ApiResponse]:
        unique = list(dict.fromkeys(item_ids))
        for start in range(0, len(unique), 20):
            yield self.get_items_multiget(unique[start : start + 20])

    def get_item(self, item_id: str) -> ApiResponse:
        endpoint = f"/items/{item_id}"
        self._record(endpoint)
        body = ITEM_BODIES.get(item_id)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200 if body else 404, payload=body)

    def get_product(self, product_id: str) -> ApiResponse:
        endpoint = f"/products/{product_id}"
        self._record(endpoint)
        body = PRODUCT_BODIES.get(product_id)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200 if body else 404, payload=body)

    def get_user_product(self, user_product_id: str) -> ApiResponse:
        endpoint = f"/user-products/{user_product_id}"
        self._record(endpoint)
        body = USER_PRODUCT_BODIES.get(user_product_id)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200 if body else 404, payload=body)

    def get_item_prices(self, item_id: str) -> ApiResponse:
        endpoint = f"/items/{item_id}/prices"
        self._record(endpoint)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200, payload={"prices": []})
