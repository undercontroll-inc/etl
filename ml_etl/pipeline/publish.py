from __future__ import annotations

from sqlalchemy.orm import Session

from ml_etl.db import repository
from ml_etl.db.models import PUBLISHABLE_STATUSES, MarketPublishedSnapshot, utcnow
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)


class PublishError(RuntimeError):
    pass


def publish_run(session: Session, run_id: str) -> MarketPublishedSnapshot:
    run = repository.get_run(session, run_id)
    if run is None:
        raise repository.RunNotFoundError(run_id)
    if run.status not in PUBLISHABLE_STATUSES:
        raise PublishError(f"run {run_id} com status {run.status} não pode ser publicado")

    existing = repository.get_published_snapshot(session, run_id)
    if existing is not None:
        logger.info("Run %s já publicado como snapshot %s", run_id, existing.snapshot_id)
        return existing

    snapshot = MarketPublishedSnapshot(
        run_id=run.run_id,
        bucket_key=run.bucket_key,
        status=run.status,
        quality_report=run.quality_report,
        published_at=utcnow(),
    )
    session.add(snapshot)
    session.flush()
    logger.info("Snapshot %s publicado para run %s (%s)", snapshot.snapshot_id, run_id, run.status)
    return snapshot
