from __future__ import annotations

import sys
from datetime import timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ml_etl.config import ConfigError, get_settings
from ml_etl.db import init_schema
from ml_etl.logging_config import get_logger, setup_logging
from ml_etl.pipeline import run_pipeline

logger = get_logger("scheduler")

JOB_ID = "etl_mercado_livre"


def build_trigger(expression: str) -> CronTrigger:
    return CronTrigger.from_crontab(expression, timezone=timezone.utc)


def execute_job() -> None:
    result = run_pipeline()
    if result.status == "SUCCESS":
        logger.info("Execução agendada concluída run_id=%s stats=%s", result.run_id, result.stats)
    elif result.status == "PARTIAL":
        logger.warning("Execução agendada parcial run_id=%s qualidade=%s", result.run_id, result.quality_report)
    else:
        logger.error("Execução agendada falhou run_id=%s erro=%s", result.run_id, result.error_message)


def main() -> int:
    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"Erro de configuração: {exc}", file=sys.stderr)
        return 2

    setup_logging(settings.etl.log_level)

    try:
        trigger = build_trigger(settings.schedule_cron)
    except ValueError as exc:
        print(f"ETL_SCHEDULE_CRON inválido ({settings.schedule_cron!r}): {exc}", file=sys.stderr)
        return 2

    init_schema()

    scheduler = BlockingScheduler(timezone=timezone.utc)
    scheduler.add_job(
        execute_job,
        trigger=trigger,
        id=JOB_ID,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    logger.info("Agendador ativo com cron %r (UTC). Ctrl+C para encerrar.", settings.schedule_cron)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Agendador encerrado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
