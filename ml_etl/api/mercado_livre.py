from __future__ import annotations

from collections.abc import Iterator, Sequence

from ml_etl.api.http_client import ApiResponse, HttpClient
from ml_etl.config import Settings, get_settings
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

MULTIGET_LIMIT = 20


class MercadoLivreApi:
    def __init__(self, client: HttpClient | None = None, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = client or HttpClient(self._settings)

    @property
    def site_id(self) -> str:
        return self._settings.mercado_livre.site_id

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> MercadoLivreApi:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def get_trends(self, category_id: str | None = None) -> ApiResponse:
        path = f"/trends/{self.site_id}"
        if category_id:
            path = f"{path}/{category_id}"
        return self._client.get(path, authenticated=True)

    def get_highlights_by_category(
        self,
        category_id: str,
        attribute: str | None = None,
        attribute_value: str | None = None,
    ) -> ApiResponse:
        params: dict[str, str] = {}
        if attribute and attribute_value:
            params = {"attribute": attribute, "attributeValue": attribute_value}
        return self._client.get(
            f"/highlights/{self.site_id}/category/{category_id}",
            params=params,
            authenticated=True,
        )

    def get_site_categories(self) -> ApiResponse:
        return self._client.get(f"/sites/{self.site_id}/categories", authenticated=False)

    def get_category(self, category_id: str) -> ApiResponse:
        return self._client.get(f"/categories/{category_id}", authenticated=False, allow_not_found=True)

    def get_item(self, item_id: str) -> ApiResponse:
        return self._client.get(f"/items/{item_id}", authenticated=True, allow_not_found=True)

    def get_items_multiget(self, item_ids: Sequence[str]) -> ApiResponse:
        return self._client.get("/items", params={"ids": ",".join(item_ids)}, authenticated=True)

    def get_product(self, product_id: str) -> ApiResponse:
        return self._client.get(f"/products/{product_id}", authenticated=True, allow_not_found=True)

    def get_user_product(self, user_product_id: str) -> ApiResponse:
        return self._client.get(f"/user-products/{user_product_id}", authenticated=True, allow_not_found=True)

    def get_item_prices(self, item_id: str) -> ApiResponse:
        return self._client.get(f"/items/{item_id}/prices", authenticated=True, allow_not_found=True)

    def iter_item_batches(self, item_ids: Sequence[str]) -> Iterator[ApiResponse]:
        batch_size = min(self._settings.etl.enrich_batch_size, MULTIGET_LIMIT)
        unique_ids = list(dict.fromkeys(item_ids))
        for start in range(0, len(unique_ids), batch_size):
            batch = unique_ids[start : start + batch_size]
            logger.debug("Multiget de %s itens", len(batch))
            yield self.get_items_multiget(batch)
