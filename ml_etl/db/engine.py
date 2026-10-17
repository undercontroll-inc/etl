from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from ml_etl.config import Settings, get_settings
from ml_etl.logging_config import get_logger

logger = get_logger(__name__)

_ENGINE: Engine | None = None
_SESSION_FACTORY: sessionmaker[Session] | None = None


def _build_engine(settings: Settings) -> Engine:
    url = settings.database.resolved_url()
    kwargs: dict[str, object] = {"future": True, "echo": False}

    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
        if settings.database.is_memory:
            kwargs["poolclass"] = StaticPool
    else:
        kwargs["pool_pre_ping"] = True
        kwargs["pool_size"] = 5
        kwargs["max_overflow"] = 5

    engine = create_engine(url, **kwargs)

    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _enable_sqlite_constraints(dbapi_connection, _record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    logger.info("Engine criada para backend=%s", settings.database.backend)
    return engine


def get_engine(settings: Settings | None = None) -> Engine:
    global _ENGINE, _SESSION_FACTORY
    if _ENGINE is None:
        active = settings or get_settings()
        _ENGINE = _build_engine(active)
        _SESSION_FACTORY = sessionmaker(bind=_ENGINE, expire_on_commit=False, future=True)
        if active.database.is_memory:
            init_schema(_ENGINE)
    return _ENGINE


def get_session_factory(settings: Settings | None = None) -> sessionmaker[Session]:
    get_engine(settings)
    assert _SESSION_FACTORY is not None
    return _SESSION_FACTORY


def init_schema(engine: Engine | None = None) -> None:
    from ml_etl.db.models import Base
    from ml_etl.db.views import create_views

    target = engine or get_engine()
    Base.metadata.create_all(target)
    create_views(target)
    logger.info("Schema garantido em %s", target.dialect.name)


def drop_schema(engine: Engine | None = None) -> None:
    from ml_etl.db.models import Base
    from ml_etl.db.views import drop_views

    target = engine or get_engine()
    drop_views(target)
    Base.metadata.drop_all(target)
    logger.warning("Schema removido em %s", target.dialect.name)


@contextmanager
def session_scope(settings: Settings | None = None) -> Iterator[Session]:
    factory = get_session_factory(settings)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_engine() -> None:
    global _ENGINE, _SESSION_FACTORY
    if _ENGINE is not None:
        _ENGINE.dispose()
    _ENGINE = None
    _SESSION_FACTORY = None
