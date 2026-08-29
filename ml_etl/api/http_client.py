from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from ml_etl.auth.token_provider import ClientCredentialsTokenProvider
from ml_etl.config import Settings, get_settings
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
BACKOFF_BASE_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
PARTIAL_CONTENT = 206


class ApiError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, endpoint: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint


class RateLimitExhausted(ApiError):
    pass


class NotFoundError(ApiError):
    pass


@dataclass
class ApiResponse:
    endpoint: str
    params: dict[str, Any]
    status_code: int
    payload: Any
    missing_fields: list[str] = field(default_factory=list)

    @property
    def is_partial(self) -> bool:
        return self.status_code == PARTIAL_CONTENT


class HttpClient:
    def __init__(
        self,
        settings: Settings | None = None,
        token_provider: ClientCredentialsTokenProvider | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._token_provider = token_provider or ClientCredentialsTokenProvider(self._settings)
        self._session = session or requests.Session()
        self._session.headers.update({"Accept": "application/json", "User-Agent": "undercontroll-etl/1.0"})

    @property
    def base_url(self) -> str:
        return self._settings.mercado_livre.api_base_url

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        authenticated: bool = True,
        allow_not_found: bool = False,
    ) -> ApiResponse:
        url = f"{self.base_url}{path}"
        query = dict(params or {})
        attempts = self._settings.etl.max_retries + 1
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            headers = self._token_provider.authorization_header() if authenticated else {}
            try:
                response = self._session.get(
                    url,
                    params=query,
                    headers=headers,
                    timeout=self._settings.etl.request_timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                self._sleep_backoff(attempt, path, reason=str(exc))
                continue

            if response.status_code in RETRYABLE_STATUS:
                last_error = ApiError(
                    f"HTTP {response.status_code} em {path}", response.status_code, path
                )
                if attempt == attempts:
                    break
                self._sleep_backoff(
                    attempt,
                    path,
                    reason=f"HTTP {response.status_code}",
                    retry_after=response.headers.get("Retry-After"),
                )
                continue

            if response.status_code == 401 and authenticated:
                logger.warning("HTTP 401 em %s, renovando access token", path)
                self._token_provider.invalidate()
                if attempt == attempts:
                    last_error = ApiError("HTTP 401 persistente", 401, path)
                    break
                continue

            if response.status_code == 404:
                if allow_not_found:
                    return ApiResponse(endpoint=path, params=query, status_code=404, payload=None)
                raise NotFoundError(f"Recurso não encontrado: {path}", 404, path)

            if response.status_code >= 400:
                raise ApiError(
                    f"HTTP {response.status_code} em {path}: {response.text[:300]}",
                    response.status_code,
                    path,
                )

            return ApiResponse(
                endpoint=path,
                params=query,
                status_code=response.status_code,
                payload=self._parse_json(response, path),
                missing_fields=self._missing_fields(response),
            )

        message = f"Tentativas esgotadas para {path} após {attempts} chamadas: {last_error}"
        logger.error(message)
        raise RateLimitExhausted(message, getattr(last_error, "status_code", None), path)

    def _parse_json(self, response: requests.Response, path: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError(f"Payload de {path} não é JSON válido", response.status_code, path) from exc

    @staticmethod
    def _missing_fields(response: requests.Response) -> list[str]:
        raw = response.headers.get("X-Content-Missing")
        if not raw:
            return []
        return [field.strip() for field in raw.split(",") if field.strip()]

    def _sleep_backoff(
        self,
        attempt: int,
        path: str,
        reason: str,
        retry_after: str | None = None,
    ) -> None:
        delay = min(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)), BACKOFF_MAX_SECONDS)
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except ValueError:
                pass
        delay += random.uniform(0, delay * 0.5)
        logger.warning(
            "Retry %s para %s em %.2fs (motivo: %s)",
            attempt,
            path,
            delay,
            reason,
        )
        time.sleep(delay)
