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
    "MLB73057": {
        "id": "MLB73057",
        "name": "Micro-ondas",
        "permalink": "https://lista.mercadolivre.com.br/micro-ondas",
        "total_items_in_this_category": 31000,
        "path_from_root": [
            {"id": "MLB5726", "name": "Eletrodomésticos"},
            {"id": "MLB73057", "name": "Micro-ondas"},
        ],
    },
    "MLB456045": {
        "id": "MLB456045",
        "name": "Fritadeiras Sem Óleo",
        "permalink": "https://lista.mercadolivre.com.br/fritadeiras-sem-oleo",
        "total_items_in_this_category": 22000,
        "path_from_root": [
            {"id": "MLB5726", "name": "Eletrodomésticos"},
            {"id": "MLB456045", "name": "Fritadeiras Sem Óleo"},
        ],
    },
}

HIGHLIGHTS_CONTENT = {
    "MLB5726": [
        {"id": "MLB6868664726", "position": 1, "type": "ITEM"},
        {"id": "MLB19617678", "position": 2, "type": "PRODUCT"},
        {"id": "MLB44117615", "position": 3, "type": "PRODUCT"},
        {"id": "MLB24162817", "position": 4, "type": "PRODUCT"},
        {"id": "MLB27389460", "position": 5, "type": "PRODUCT"},
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
    {"keyword": "micro-ondas", "url": "https://lista.mercadolivre.com.br/micro-ondas"},
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
        "attributes": [
            {"id": "BRAND", "value_name": "Mondial"},
            {"id": "MODEL", "value_name": "AF-14"},
            {"id": "POWER", "value_name": "1500 W"},
            {"id": "VOLUME_CAPACITY", "value_name": "4 L"},
            {"id": "VOLTAGE", "value_name": "220V"},
        ],
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
        "attributes": [
            {"id": "BRAND", "value_name": "Oster"},
            {"id": "MODEL", "value_name": "PrimaLatte"},
            {"id": "POWER", "value_name": "1300 W"},
            {"id": "VOLTAGE", "value_name": "220V"},
        ],
    },
}

PRODUCT_BODIES = {
    "MLB24162817": {
        "id": "MLB24162817",
        "name": "Liquidificador Philco PH900 1200W",
        "domain_id": "MLB-BLENDERS",
        "status": "active",
        "attributes": [
            {"id": "BRAND", "value_name": "Philco"},
            {"id": "MODEL", "value_name": "PH900"},
            {"id": "POWER", "value_name": "1200 W"},
            {"id": "VOLUME_CAPACITY", "value_name": "3,2 L"},
            {"id": "VOLTAGE", "value_name": "Bivolt"},
            {"id": "MAIN_COLOR", "value_name": "Preto"},
        ],
        "buy_box_winner": None,
    },
    "MLB19617678": {
        "id": "MLB19617678",
        "name": "Micro-ondas Electrolux 23L Branco Efficient ME23S",
        "domain_id": "MLB-MICROWAVES",
        "status": "active",
        "attributes": [
            {"id": "BRAND", "value_name": "Electrolux"},
            {"id": "MODEL", "value_name": "ME23S"},
            {"id": "POWER", "value_name": "1400 W"},
            {"id": "VOLUME_CAPACITY", "value_name": "23 L"},
            {"id": "VOLTAGE", "value_name": "220V"},
            {"id": "ENERGY_EFFICIENCY", "value_name": "A"},
            {"id": "MAIN_COLOR", "value_name": "Branco"},
        ],
        "buy_box_winner": None,
    },
    "MLB44117615": {
        "id": "MLB44117615",
        "name": "Fritadeira Elétrica AFON-12L-BG Forno Oven 12L",
        "domain_id": "MLB-AIR_FRYERS",
        "status": "active",
        "attributes": [
            {"id": "BRAND", "value_name": "Mondial"},
            {"id": "ALPHANUMERIC_MODEL", "value_name": "AFON-12L-BG"},
            {"id": "POWER", "value_name": "1.700 W"},
            {"id": "VOLUME_CAPACITY", "value_name": "12 Litros"},
            {"id": "VOLTAGE", "value_name": "110/220V"},
        ],
        "buy_box_winner": None,
    },
    "MLB27389460": {
        "id": "MLB27389460",
        "name": "Refil Filtro Acqua Pure Electrolux Pe12b",
        "domain_id": "MLB-WATER_PURIFIER_FILTERS_REPLACEMENTS",
        "status": "active",
        "attributes": [{"id": "BRAND", "value_name": "Electrolux"}],
        "buy_box_winner": None,
    },
}

USER_PRODUCT_BODIES = {
    "MLBU3013800008": {
        "id": "MLBU3013800008",
        "title": "Batedeira Planetária Arno 600W Deluxe 4,5L",
        "category_id": "MLB5726",
        "price": 549.0,
        "permalink": "https://produto.mercadolivre.com.br/MLBU3013800008",
        "attributes": [
            {"id": "BRAND", "value_name": "Arno"},
            {"id": "MODEL", "value_name": "Deluxe"},
            {"id": "POWER", "value_name": "600 W"},
            {"id": "VOLUME_CAPACITY", "value_name": "4,5 L"},
        ],
    }
}

PRODUCT_OFFERS = {
    "MLB19617678": {
        "paging": {"total": 18, "offset": 0, "limit": 100},
        "results": [
            {
                "item_id": "MLB3003724516",
                "price": 526.16,
                "original_price": 659,
                "seller_id": 480263032,
                "official_store_id": 2707,
                "category_id": "MLB73057",
                "currency_id": "BRL",
            },
            {
                "item_id": "MLB5119142631",
                "price": 599.0,
                "original_price": 599,
                "seller_id": 3466165187,
                "official_store_id": None,
                "category_id": "MLB73057",
                "currency_id": "BRL",
            },
            {
                "item_id": "MLB6899665328",
                "price": 641.9,
                "original_price": 829,
                "seller_id": 508606499,
                "official_store_id": 80564,
                "category_id": "MLB73057",
                "currency_id": "BRL",
            },
        ],
    },
    "MLB44117615": {
        "paging": {"total": 6, "offset": 0, "limit": 100},
        "results": [
            {
                "item_id": "MLB5232182936",
                "price": 519.9,
                "original_price": 899,
                "seller_id": 480263032,
                "official_store_id": 2707,
                "category_id": "MLB456045",
                "currency_id": "BRL",
            },
            {
                "item_id": "MLB7549047582",
                "price": 799.0,
                "original_price": None,
                "seller_id": 3291540344,
                "official_store_id": None,
                "category_id": "MLB456045",
                "currency_id": "BRL",
            },
        ],
    },
    "MLB24162817": {
        "paging": {"total": 4, "offset": 0, "limit": 100},
        "results": [
            {
                "item_id": "MLB1111111111",
                "price": 259.9,
                "original_price": 299.9,
                "seller_id": 111111,
                "official_store_id": None,
                "category_id": "MLB5726",
                "currency_id": "BRL",
            },
            {
                "item_id": "MLB2222222222",
                "price": 289.9,
                "original_price": None,
                "seller_id": 222222,
                "official_store_id": 4242,
                "category_id": "MLB5726",
                "currency_id": "BRL",
            },
        ],
    },
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

    def get_product_items(self, product_id: str) -> ApiResponse:
        endpoint = f"/products/{product_id}/items"
        self._record(endpoint)
        payload = PRODUCT_OFFERS.get(product_id)
        return ApiResponse(
            endpoint=endpoint,
            params={},
            status_code=200 if payload else 404,
            payload=payload,
        )

    def get_user_product(self, user_product_id: str) -> ApiResponse:
        endpoint = f"/user-products/{user_product_id}"
        self._record(endpoint)
        body = USER_PRODUCT_BODIES.get(user_product_id)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200 if body else 404, payload=body)

    def get_item_prices(self, item_id: str) -> ApiResponse:
        endpoint = f"/items/{item_id}/prices"
        self._record(endpoint)
        return ApiResponse(endpoint=endpoint, params={}, status_code=200, payload={"prices": []})
