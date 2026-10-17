from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from sqlalchemy import Connection, Engine, inspect, select, text

from ml_etl.db.models import Base, SchemaPatch, utcnow
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Patch:
    name: str
    postgres: tuple[str, ...] = ()
    rebuild: tuple[str, ...] = ()
    renames: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    portable: tuple[str, ...] = ()

    def apply(self, connection: Connection) -> None:
        if connection.dialect.name == "postgresql":
            for statement in self.postgres:
                connection.execute(text(statement))
        else:
            for table_name in self.rebuild:
                rebuild_table(connection, table_name, self.renames.get(table_name, {}))
        for statement in self.portable:
            connection.execute(text(statement))


SAMPLE_RENAMES = {
    "seller_count": "observed_seller_count",
    "offer_count": "available_offer_count",
}

PATCHES: tuple[Patch, ...] = (
    Patch(
        name="0001_offer_sample_counts",
        postgres=(
            "ALTER TABLE market_price_observation DROP CONSTRAINT IF EXISTS ck_price_offer_count",
            "ALTER TABLE market_price_observation RENAME COLUMN seller_count TO observed_seller_count",
            "ALTER TABLE market_price_observation RENAME COLUMN offer_count TO available_offer_count",
            "ALTER TABLE market_price_observation ALTER COLUMN available_offer_count DROP NOT NULL",
            "ALTER TABLE market_price_observation ALTER COLUMN available_offer_count DROP DEFAULT",
            "ALTER TABLE market_price_observation RENAME COLUMN winner_item_id TO lowest_price_item_id",
            "ALTER TABLE market_price_observation RENAME COLUMN winner_price TO lowest_price",
            "ALTER TABLE market_price_observation ADD COLUMN observed_offer_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE market_price_observation ADD COLUMN valid_price_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE market_price_observation ADD COLUMN sample_complete BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE market_price_observation ADD CONSTRAINT ck_price_sample_counts "
            "CHECK (valid_price_count <= observed_offer_count)",
            "ALTER TABLE market_curated_product RENAME COLUMN seller_count TO observed_seller_count",
            "ALTER TABLE market_curated_product RENAME COLUMN offer_count TO available_offer_count",
            "ALTER TABLE market_curated_product ADD COLUMN observed_offer_count INTEGER",
            "ALTER TABLE market_curated_product ADD COLUMN valid_price_count INTEGER",
            "ALTER TABLE market_curated_product ADD COLUMN sample_complete BOOLEAN",
        ),
        rebuild=("market_price_observation", "market_curated_product"),
        renames={
            "market_price_observation": {
                **SAMPLE_RENAMES,
                "winner_item_id": "lowest_price_item_id",
                "winner_price": "lowest_price",
            },
            "market_curated_product": SAMPLE_RENAMES,
        },
    ),
    Patch(
        name="0002_run_scoped_rows",
        postgres=(
            "ALTER TABLE market_snapshot DROP CONSTRAINT IF EXISTS uq_snapshot_logical",
            "ALTER TABLE market_snapshot ADD CONSTRAINT uq_snapshot_run "
            "UNIQUE (run_id, source, external_type, external_id, category_id)",
            "CREATE INDEX IF NOT EXISTS ix_snapshot_run ON market_snapshot (run_id)",
            "ALTER TABLE market_trend DROP CONSTRAINT IF EXISTS uq_trend_logical",
            "ALTER TABLE market_trend ADD CONSTRAINT uq_trend_run UNIQUE (run_id, category_id, keyword)",
            "CREATE INDEX IF NOT EXISTS ix_trend_run ON market_trend (run_id)",
            "ALTER TABLE market_score DROP CONSTRAINT IF EXISTS uq_score_logical",
            "ALTER TABLE market_score ADD CONSTRAINT uq_score_run UNIQUE (run_id, external_type, external_id)",
            "CREATE INDEX IF NOT EXISTS ix_score_run ON market_score (run_id)",
            "ALTER TABLE market_price_observation DROP CONSTRAINT IF EXISTS uq_price_observation_logical",
            "ALTER TABLE market_price_observation ADD CONSTRAINT uq_price_observation_run "
            "UNIQUE (run_id, external_type, external_id)",
            "CREATE INDEX IF NOT EXISTS ix_price_run ON market_price_observation (run_id)",
            "ALTER TABLE market_price_observation ADD COLUMN IF NOT EXISTS domain_id VARCHAR(64)",
            "CREATE INDEX IF NOT EXISTS ix_price_domain ON market_price_observation (domain_id)",
            "ALTER TABLE market_curated_product DROP CONSTRAINT IF EXISTS uq_curated_logical",
            "ALTER TABLE market_curated_product ADD CONSTRAINT uq_curated_run "
            "UNIQUE (run_id, source, external_type, external_id)",
            "CREATE INDEX IF NOT EXISTS ix_curated_run ON market_curated_product (run_id)",
        ),
        rebuild=(
            "market_snapshot",
            "market_trend",
            "market_score",
            "market_price_observation",
            "market_curated_product",
        ),
        portable=(
            "UPDATE market_price_observation SET domain_id = ("
            "SELECT e.domain_id FROM market_entity e "
            "WHERE e.external_type = market_price_observation.external_type "
            "AND e.external_id = market_price_observation.external_id"
            ") WHERE domain_id IS NULL",
        ),
    ),
)


def rebuild_table(connection: Connection, table_name: str, renames: Mapping[str, str] | None = None) -> None:
    table = Base.metadata.tables[table_name]
    if not inspect(connection).has_table(table_name):
        table.create(connection)
        return

    renames = renames or {}
    legacy = f"{table_name}__legacy"
    old_columns = [column["name"] for column in inspect(connection).get_columns(table_name)]
    for index_name in connection.execute(
        text("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = :table AND sql IS NOT NULL"),
        {"table": table_name},
    ).scalars().all():
        connection.execute(text(f'DROP INDEX "{index_name}"'))

    connection.exec_driver_sql("PRAGMA legacy_alter_table = ON")
    try:
        connection.exec_driver_sql(f'ALTER TABLE "{table_name}" RENAME TO "{legacy}"')
    finally:
        connection.exec_driver_sql("PRAGMA legacy_alter_table = OFF")
    table.create(connection)

    new_columns = {column.name for column in table.columns}
    pairs = [(old, renames.get(old, old)) for old in old_columns if renames.get(old, old) in new_columns]
    if pairs:
        targets = ", ".join(f'"{new}"' for _, new in pairs)
        sources = ", ".join(f'"{old}"' for old, _ in pairs)
        connection.execute(text(f'INSERT INTO "{table_name}" ({targets}) SELECT {sources} FROM "{legacy}"'))
    connection.execute(text(f'DROP TABLE "{legacy}"'))


def pending_patches(connection: Connection, patches: tuple[Patch, ...] = PATCHES) -> list[Patch]:
    applied = set(connection.execute(select(SchemaPatch.name)).scalars())
    return [patch for patch in patches if patch.name not in applied]


def _mark_applied(connection: Connection, patch: Patch) -> None:
    connection.execute(SchemaPatch.__table__.insert().values(name=patch.name, applied_at=utcnow()))


def migrate(
    engine: Engine,
    patches: tuple[Patch, ...] = PATCHES,
    before_patches: Callable[[Connection], None] | None = None,
) -> list[str]:
    fresh = not inspect(engine).has_table("etl_run")
    Base.metadata.create_all(engine)

    applied: list[str] = []
    with engine.connect() as connection:
        sqlite = connection.dialect.name == "sqlite"
        if sqlite:
            connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
            connection.commit()
        try:
            with connection.begin():
                pending = pending_patches(connection, patches)
                if pending and not fresh and before_patches is not None:
                    before_patches(connection)
                for patch in pending:
                    if not fresh:
                        patch.apply(connection)
                        logger.info("Patch de schema aplicado: %s", patch.name)
                    _mark_applied(connection, patch)
                    applied.append(patch.name)
        finally:
            if sqlite:
                connection.exec_driver_sql("PRAGMA foreign_keys = ON")
                connection.commit()
    return applied
