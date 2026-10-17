from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

VALID_DB_BACKENDS = ("postgres", "sqlite", "memory")
VALID_BUCKETS = ("month", "day")
DEFAULT_SCORE_WEIGHTS = {
    "rank": 0.35,
    "trajectory": 0.25,
    "trends": 0.15,
    "relevance": 0.15,
    "persistence": 0.10,
}


class ConfigError(RuntimeError):
    pass


def _get(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _get_int(name: str, default: int) -> int:
    raw = _get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser um inteiro, recebido {raw!r}") from exc


def _get_float(name: str, default: float) -> float:
    raw = _get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser um número, recebido {raw!r}") from exc


def _get_list(name: str, default: str = "") -> list[str]:
    raw = _get(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _parse_weights(raw: str) -> dict[str, float]:
    if not raw:
        return dict(DEFAULT_SCORE_WEIGHTS)
    weights: dict[str, float] = {}
    for pair in raw.split(","):
        if ":" not in pair:
            raise ConfigError(f"SCORE_WEIGHTS mal formatado no trecho {pair!r}")
        key, value = pair.split(":", 1)
        key = key.strip()
        if key not in DEFAULT_SCORE_WEIGHTS:
            raise ConfigError(f"Componente de score desconhecido: {key!r}")
        try:
            weights[key] = float(value)
        except ValueError as exc:
            raise ConfigError(f"Peso inválido para {key!r}: {value!r}") from exc
    merged = dict(DEFAULT_SCORE_WEIGHTS)
    merged.update(weights)
    total = sum(merged.values())
    if total <= 0:
        raise ConfigError("A soma dos pesos de score deve ser maior que zero")
    return {key: value / total for key, value in merged.items()}


@dataclass(frozen=True)
class MercadoLivreSettings:
    client_id: str
    client_secret: str
    site_id: str
    api_base_url: str

    def require_credentials(self) -> None:
        missing = [
            name
            for name, value in (("ML_CLIENT_ID", self.client_id), ("ML_CLIENT_SECRET", self.client_secret))
            if not value
        ]
        if missing:
            raise ConfigError(
                "Credenciais do Mercado Livre ausentes: "
                + ", ".join(missing)
                + ". Preencha o .env antes de executar o pipeline."
            )


@dataclass(frozen=True)
class DatabaseSettings:
    backend: str
    database_url: str
    sqlite_path: str

    @property
    def is_memory(self) -> bool:
        return self.backend == "memory"

    def resolved_url(self) -> str:
        if self.backend == "postgres":
            if not self.database_url:
                raise ConfigError("DATABASE_URL é obrigatório quando DB_BACKEND=postgres")
            return self.database_url
        if self.backend == "sqlite":
            path = Path(self.sqlite_path)
            if not path.is_absolute():
                path = PROJECT_ROOT / path
            path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite+pysqlite:///{path}"
        return "sqlite+pysqlite:///:memory:"


@dataclass(frozen=True)
class EtlSettings:
    target_categories: list[str]
    snapshot_bucket: str
    max_workers: int
    enrich_batch_size: int
    request_timeout: int
    max_retries: int
    log_level: str
    offers_max_pages: int = 3


@dataclass(frozen=True)
class ScoreSettings:
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_SCORE_WEIGHTS))
    priority_threshold: float = 75.0


@dataclass(frozen=True)
class Settings:
    mercado_livre: MercadoLivreSettings
    database: DatabaseSettings
    etl: EtlSettings
    score: ScoreSettings
    schedule_cron: str


def load_settings(
    env_file: str | os.PathLike[str] | None = None,
    override: bool = False,
    use_dotenv: bool = True,
) -> Settings:
    if use_dotenv:
        target = Path(env_file) if env_file else PROJECT_ROOT / ".env"
        if target.exists():
            load_dotenv(target, override=override)

    backend = _get("DB_BACKEND", "postgres").lower()
    if backend not in VALID_DB_BACKENDS:
        raise ConfigError(f"DB_BACKEND inválido: {backend!r}. Use um de {VALID_DB_BACKENDS}")

    bucket = _get("ETL_SNAPSHOT_BUCKET", "month").lower()
    if bucket not in VALID_BUCKETS:
        raise ConfigError(f"ETL_SNAPSHOT_BUCKET inválido: {bucket!r}. Use um de {VALID_BUCKETS}")

    return Settings(
        mercado_livre=MercadoLivreSettings(
            client_id=_get("ML_CLIENT_ID"),
            client_secret=_get("ML_CLIENT_SECRET"),
            site_id=_get("ML_SITE_ID", "MLB"),
            api_base_url=_get("ML_API_BASE_URL", "https://api.mercadolibre.com").rstrip("/"),
        ),
        database=DatabaseSettings(
            backend=backend,
            database_url=_get("DATABASE_URL"),
            sqlite_path=_get("SQLITE_PATH", "./data/etl.db"),
        ),
        etl=EtlSettings(
            target_categories=_get_list("ETL_TARGET_CATEGORIES", "MLB5726"),
            snapshot_bucket=bucket,
            max_workers=max(1, _get_int("ETL_MAX_WORKERS", 4)),
            enrich_batch_size=min(20, max(1, _get_int("ETL_ENRICH_BATCH_SIZE", 20))),
            request_timeout=_get_int("ETL_REQUEST_TIMEOUT", 30),
            max_retries=max(0, _get_int("ETL_MAX_RETRIES", 5)),
            log_level=_get("ETL_LOG_LEVEL", "INFO").upper(),
            offers_max_pages=max(1, _get_int("ETL_OFFERS_MAX_PAGES", 3)),
        ),
        score=ScoreSettings(
            weights=_parse_weights(_get("SCORE_WEIGHTS")),
            priority_threshold=_get_float("SCORE_PRIORITY_THRESHOLD", 75.0),
        ),
        schedule_cron=_get("ETL_SCHEDULE_CRON", "0 3 1 * *"),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()
