from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ml_etl.api.http_client import ApiError
from ml_etl.api.mercado_livre import MercadoLivreApi
from ml_etl.config import Settings, get_settings
from ml_etl.domain.rejection import EMPTY_PAYLOAD, Layer, Rejection, Severity, Stage
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

STAGE_EXTRACT = "EXTRACT"
STAGE_ENRICH = "ENRICH"
SOURCE_HIGHLIGHTS = "HIGHLIGHTS_CATEGORY"
SOURCE_TRENDS = "TRENDS"


@dataclass
class RawPayload:
    stage: str
    endpoint: str
    status_code: int
    payload: Any
    source: str | None = None
    request_params: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    missing_fields: list[str] = field(default_factory=list)


@dataclass
class ExtractResult:
    payloads: list[RawPayload] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)

    def merge(self, other: ExtractResult) -> None:
        self.payloads.extend(other.payloads)
        self.rejections.extend(other.rejections)


def _run_parallel(items: Sequence[Any], worker, max_workers: int) -> list[Any]:
    if not items:
        return []
    if max_workers <= 1 or len(items) == 1:
        return [worker(item) for item in items]
    with ThreadPoolExecutor(max_workers=min(max_workers, len(items))) as pool:
        return list(pool.map(worker, items))


def extract_categories(api: MercadoLivreApi, category_ids: Sequence[str], settings: Settings | None = None) -> ExtractResult:
    active = settings or get_settings()
    result = ExtractResult()

    def worker(category_id: str) -> ExtractResult:
        local = ExtractResult()
        try:
            response = api.get_category(category_id)
        except ApiError as exc:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.ESTRUTURAL,
                    rule=f"falha ao consultar /categories/{category_id}",
                    severity=Severity.AVISO,
                    external_id=category_id,
                    detail=str(exc),
                )
            )
            return local
        if not response.payload:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.ESTRUTURAL,
                    rule=EMPTY_PAYLOAD,
                    severity=Severity.AVISO,
                    external_id=category_id,
                    detail=f"/categories/{category_id} retornou vazio",
                )
            )
            return local
        local.payloads.append(
            RawPayload(
                stage=STAGE_EXTRACT,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params=response.params,
            )
        )
        return local

    for partial in _run_parallel(list(category_ids), worker, active.etl.max_workers):
        result.merge(partial)

    logger.info("EXTRACT categorias: %s payloads", len(result.payloads))
    return result


def extract_highlights(api: MercadoLivreApi, category_ids: Sequence[str], settings: Settings | None = None) -> ExtractResult:
    active = settings or get_settings()
    result = ExtractResult()

    def worker(category_id: str) -> ExtractResult:
        local = ExtractResult()
        try:
            response = api.get_highlights_by_category(category_id)
        except ApiError as exc:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.API_LIMITE if exc.status_code == 429 else Layer.ESTRUTURAL,
                    rule=f"falha ao consultar highlights de {category_id}",
                    severity=Severity.BLOQUEANTE,
                    external_id=category_id,
                    detail=str(exc),
                )
            )
            return local

        content = (response.payload or {}).get("content") if isinstance(response.payload, dict) else None
        if not content:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.ESTRUTURAL,
                    rule=EMPTY_PAYLOAD,
                    severity=Severity.BLOQUEANTE,
                    external_id=category_id,
                    detail=f"highlights de {category_id} sem content",
                )
            )
            return local

        local.payloads.append(
            RawPayload(
                stage=STAGE_EXTRACT,
                source=SOURCE_HIGHLIGHTS,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params={"category_id": category_id, **response.params},
            )
        )
        return local

    for partial in _run_parallel(list(category_ids), worker, active.etl.max_workers):
        result.merge(partial)

    logger.info("EXTRACT highlights: %s payloads, %s rejeicoes", len(result.payloads), len(result.rejections))
    return result


def extract_trends(api: MercadoLivreApi, category_ids: Sequence[str], settings: Settings | None = None) -> ExtractResult:
    active = settings or get_settings()
    result = ExtractResult()
    targets: list[str | None] = [None, *category_ids]

    def worker(category_id: str | None) -> ExtractResult:
        local = ExtractResult()
        label = category_id or "GERAL"
        try:
            response = api.get_trends(category_id)
        except ApiError as exc:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.API_LIMITE if exc.status_code == 429 else Layer.ESTRUTURAL,
                    rule=f"falha ao consultar trends de {label}",
                    severity=Severity.AVISO,
                    external_id=category_id,
                    detail=str(exc),
                )
            )
            return local

        if not response.payload:
            local.rejections.append(
                Rejection(
                    stage=Stage.EXTRACT,
                    layer=Layer.ESTRUTURAL,
                    rule=EMPTY_PAYLOAD,
                    severity=Severity.AVISO,
                    external_id=category_id,
                    detail=f"trends de {label} retornou vazio",
                )
            )
            return local

        local.payloads.append(
            RawPayload(
                stage=STAGE_EXTRACT,
                source=SOURCE_TRENDS,
                endpoint=response.endpoint,
                status_code=response.status_code,
                payload=response.payload,
                request_params={"category_id": category_id or ""},
            )
        )
        return local

    for partial in _run_parallel(targets, worker, active.etl.max_workers):
        result.merge(partial)

    logger.info("EXTRACT trends: %s payloads", len(result.payloads))
    return result


def extract_all(api: MercadoLivreApi, category_ids: Sequence[str], settings: Settings | None = None) -> ExtractResult:
    active = settings or get_settings()
    result = ExtractResult()
    result.merge(extract_categories(api, category_ids, active))
    result.merge(extract_highlights(api, category_ids, active))
    result.merge(extract_trends(api, category_ids, active))
    return result
