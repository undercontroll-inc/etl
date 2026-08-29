from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence

from sqlalchemy.orm import Session

from ml_etl.db import repository
from ml_etl.db.models import MarketRawResponse
from ml_etl.logging_config import get_logger
from ml_etl.pipeline.extract import RawPayload

logger = get_logger(__name__)


def payload_hash(endpoint: str, params: dict, payload: object) -> str:
    canonical = json.dumps(
        {"endpoint": endpoint, "params": params, "payload": payload},
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def persist_raw(
    session: Session,
    run_id: str,
    bucket_key: str,
    payloads: Sequence[RawPayload],
) -> int:
    if not payloads:
        return 0

    rows = [
        {
            "run_id": run_id,
            "stage": item.stage,
            "source": item.source,
            "endpoint": item.endpoint,
            "http_method": "GET",
            "request_params": item.request_params,
            "status_code": item.status_code,
            "payload": item.payload,
            "payload_hash": payload_hash(item.endpoint, item.request_params, item.payload),
            "collected_at": item.collected_at,
            "bucket_key": bucket_key,
        }
        for item in payloads
    ]

    stored = repository.upsert(session, MarketRawResponse, rows)
    logger.info("RAW: %s respostas registradas para run %s", stored, run_id)
    return stored


def load_raw_for_run(session: Session, run_id: str, stage: str | None = None) -> list[RawPayload]:
    records = repository.fetch_raw_payloads(session, run_id, stage)
    payloads = [
        RawPayload(
            stage=record.stage,
            endpoint=record.endpoint,
            status_code=record.status_code,
            payload=record.payload,
            source=record.source,
            request_params=record.request_params or {},
            collected_at=record.collected_at,
        )
        for record in records
    ]
    logger.info("RAW: %s respostas recuperadas do run %s", len(payloads), run_id)
    return payloads
