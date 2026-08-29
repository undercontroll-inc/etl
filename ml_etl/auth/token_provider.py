from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests

from ml_etl.config import Settings, get_settings
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

TOKEN_PATH = "/oauth/token"
EXPIRY_MARGIN_SECONDS = 60


class TokenError(RuntimeError):
    pass


@dataclass(frozen=True)
class AccessToken:
    value: str
    expires_at: datetime

    def is_valid(self, margin: int = EXPIRY_MARGIN_SECONDS) -> bool:
        return datetime.now(timezone.utc) + timedelta(seconds=margin) < self.expires_at


class ClientCredentialsTokenProvider:
    def __init__(self, settings: Settings | None = None, session: requests.Session | None = None) -> None:
        self._settings = settings or get_settings()
        self._session = session or requests.Session()
        self._lock = threading.Lock()
        self._token: AccessToken | None = None

    @property
    def token_url(self) -> str:
        return f"{self._settings.mercado_livre.api_base_url}{TOKEN_PATH}"

    def get_token(self) -> str:
        with self._lock:
            if self._token is not None and self._token.is_valid():
                return self._token.value
            self._token = self._request_token()
            return self._token.value

    def invalidate(self) -> None:
        with self._lock:
            self._token = None

    def authorization_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _request_token(self) -> AccessToken:
        credentials = self._settings.mercado_livre
        credentials.require_credentials()

        payload = {
            "grant_type": "client_credentials",
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
        }
        headers = {"accept": "application/json", "content-type": "application/x-www-form-urlencoded"}

        try:
            response = self._session.post(
                self.token_url,
                data=payload,
                headers=headers,
                timeout=self._settings.etl.request_timeout,
            )
        except requests.RequestException as exc:
            raise TokenError(f"Falha de rede ao solicitar o access token: {exc}") from exc

        if response.status_code != 200:
            raise TokenError(
                f"Falha ao obter access token (HTTP {response.status_code}): {response.text[:400]}"
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise TokenError("Resposta do endpoint de token não é um JSON válido") from exc

        access_token = body.get("access_token")
        if not access_token:
            raise TokenError(f"Resposta do token sem access_token: {body}")

        expires_in = int(body.get("expires_in", 21600))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        logger.info("Access token obtido via client_credentials, expira em %ss", expires_in)
        return AccessToken(value=access_token, expires_at=expires_at)
