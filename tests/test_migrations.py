from __future__ import annotations

import unittest
from dataclasses import dataclass, field

from sqlalchemy import Connection, create_engine, event, inspect, select, text
from sqlalchemy.pool import StaticPool

from ml_etl.db.migrations import Patch, migrate, rebuild_table
from ml_etl.db.models import SchemaPatch
from ml_etl.db.views import VIEWS, create_views, drop_views_on


def memory_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _foreign_keys(dbapi_connection, _record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    return engine


LEGACY_RUN = """
CREATE TABLE etl_run (
    run_id VARCHAR(36) PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME,
    state VARCHAR(20) NOT NULL,
    bucket_key VARCHAR(10) NOT NULL,
    params JSON,
    stats JSON,
    error_message TEXT,
    obsolete TEXT,
    CONSTRAINT ck_etl_run_status CHECK (state IN ('RUNNING', 'SUCCESS'))
)
"""


@dataclass(frozen=True)
class RecordingPatch(Patch):
    calls: list[str] = field(default_factory=list)

    def apply(self, connection: Connection) -> None:
        self.calls.append(self.name)


class MigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = memory_engine()

    def tearDown(self) -> None:
        self.engine.dispose()

    def _applied(self) -> list[str]:
        with self.engine.connect() as connection:
            return list(connection.execute(select(SchemaPatch.name)).scalars())

    def test_fresh_database_marks_patches_without_running_them(self) -> None:
        patch = RecordingPatch(name="0001_test")

        applied = migrate(self.engine, (patch,))

        self.assertEqual(["0001_test"], applied)
        self.assertEqual([], patch.calls)
        self.assertEqual(["0001_test"], self._applied())

    def test_existing_database_applies_pending_patches_once(self) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(LEGACY_RUN))
        patch = RecordingPatch(name="0001_test")
        dropped: list[bool] = []

        migrate(self.engine, (patch,), before_patches=lambda _connection: dropped.append(True))
        second = migrate(self.engine, (patch,), before_patches=lambda _connection: dropped.append(True))

        self.assertEqual(["0001_test"], patch.calls)
        self.assertEqual([], second)
        self.assertEqual([True], dropped)

    def test_rebuild_table_renames_columns_and_keeps_child_references(self) -> None:
        with self.engine.begin() as connection:
            connection.execute(text(LEGACY_RUN))
            connection.execute(
                text(
                    "INSERT INTO etl_run (run_id, started_at, state, bucket_key, obsolete) "
                    "VALUES ('r1', '2026-01-01 00:00:00', 'SUCCESS', '2026-01', 'x')"
                )
            )
        rebuild = Patch(name="0001_run", rebuild=("etl_run",), renames={"etl_run": {"state": "status"}})

        migrate(self.engine, (rebuild,))

        with self.engine.begin() as connection:
            columns = {column["name"] for column in inspect(connection).get_columns("etl_run")}
            row = connection.execute(text("SELECT run_id, status FROM etl_run")).one()
            references = connection.execute(text("PRAGMA foreign_key_list(market_raw_response)")).all()
            connection.execute(
                text(
                    "INSERT INTO etl_run (run_id, started_at, status, bucket_key) "
                    "VALUES ('r2', '2026-01-01 00:00:00', 'FAILED', '2026-01')"
                )
            )

        self.assertIn("status", columns)
        self.assertNotIn("obsolete", columns)
        self.assertEqual(("r1", "SUCCESS"), tuple(row))
        self.assertEqual({"etl_run"}, {reference[2] for reference in references})

    def test_rebuild_creates_missing_table(self) -> None:
        with self.engine.begin() as connection:
            rebuild_table(connection, "schema_patch")
            self.assertTrue(inspect(connection).has_table("schema_patch"))

    def test_postgres_statements_run_only_on_postgres(self) -> None:
        patch = Patch(name="0001_pg", postgres=("SELECT missing_function()",))
        with self.engine.begin() as connection:
            patch.apply(connection)

    def test_views_can_be_dropped_and_recreated(self) -> None:
        migrate(self.engine)
        create_views(self.engine)
        with self.engine.begin() as connection:
            drop_views_on(connection)
            remaining = set(inspect(connection).get_view_names())
        self.assertFalse(remaining & {name for name, _ in VIEWS})


if __name__ == "__main__":
    unittest.main()
