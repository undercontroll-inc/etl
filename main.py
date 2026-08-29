from __future__ import annotations

import argparse
import json
import sys

from ml_etl.config import ConfigError, get_settings
from ml_etl.db import init_schema, session_scope
from ml_etl.db.repository import recent_runs, table_counts
from ml_etl.logging_config import get_logger, setup_logging
from ml_etl.pipeline import run_pipeline

logger = get_logger("main")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="ETL Mercado Livre — Projeto UnderControll",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Cria o schema no banco configurado")

    run_parser = subparsers.add_parser("run", help="Executa o pipeline ETL")
    run_parser.add_argument("--categories", help="Lista de category_id separada por vírgula")
    run_parser.add_argument("--bucket", choices=("month", "day"), help="Granularidade do snapshot")
    run_parser.add_argument("--run-id", help="Identificador da execução")
    run_parser.add_argument(
        "--skip-api",
        action="store_true",
        help="Reprocessa a partir do RAW sem chamar a API (exige --run-id)",
    )

    status_parser = subparsers.add_parser("status", help="Mostra execuções recentes e contagens por tabela")
    status_parser.add_argument("--limit", type=int, default=10, help="Quantidade de execuções listadas")

    return parser


def command_init_db() -> int:
    init_schema()
    print("Schema criado ou já existente.")
    return 0


def command_run(args: argparse.Namespace) -> int:
    categories = None
    if args.categories:
        categories = [part.strip() for part in args.categories.split(",") if part.strip()]

    result = run_pipeline(
        categories=categories,
        bucket=args.bucket,
        run_id=args.run_id,
        skip_api=args.skip_api,
    )

    print(f"run_id: {result.run_id}")
    print(f"bucket: {result.bucket_key}")
    print(f"status: {result.status}")
    print(json.dumps(result.stats, indent=2, ensure_ascii=False, default=str))
    if result.error_message:
        print(f"erro: {result.error_message}", file=sys.stderr)
    return 0 if result.status == "SUCCESS" else 1


def command_status(args: argparse.Namespace) -> int:
    with session_scope() as session:
        runs = recent_runs(session, args.limit)
        counts = table_counts(session)

    if not runs:
        print("Nenhuma execução registrada.")
    else:
        print(f"{'run_id':38} {'bucket':12} {'status':9} {'inicio':20} {'fim':20}")
        for run in runs:
            started = run.started_at.strftime("%Y-%m-%d %H:%M:%S") if run.started_at else "-"
            finished = run.finished_at.strftime("%Y-%m-%d %H:%M:%S") if run.finished_at else "-"
            print(f"{run.run_id:38} {run.bucket_key:12} {run.status:9} {started:20} {finished:20}")

    print()
    print("Contagem por tabela:")
    for table, total in counts.items():
        print(f"  {table:26} {total}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        settings = get_settings()
    except ConfigError as exc:
        print(f"Erro de configuração: {exc}", file=sys.stderr)
        return 2

    setup_logging(settings.etl.log_level)

    handlers = {
        "init-db": lambda: command_init_db(),
        "run": lambda: command_run(args),
        "status": lambda: command_status(args),
    }

    try:
        return handlers[args.command]()
    except ConfigError as exc:
        print(f"Erro de configuração: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Execução interrompida.", file=sys.stderr)
        return 130
    except Exception as exc:
        logger.exception("Falha na execução do comando %s", args.command)
        print(f"Falha: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
