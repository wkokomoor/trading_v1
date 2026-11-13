"""PostgreSQL access: connection, migrations, and backtest-run storage."""

from __future__ import annotations

from functools import cache
from pathlib import Path

from sqlalchemy import Engine, create_engine

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


@cache
def get_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)


def alembic_config(url: str):
    from alembic.config import Config

    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def upgrade(url: str, revision: str = "head") -> None:
    """Apply migrations up to `revision` (default: latest)."""
    from alembic import command

    command.upgrade(alembic_config(url), revision)


def downgrade(url: str, revision: str) -> None:
    from alembic import command

    command.downgrade(alembic_config(url), revision)


def migration_status(url: str) -> tuple[str | None, str]:
    """(revision the database is at, latest revision in the code)."""
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory

    head = ScriptDirectory.from_config(alembic_config(url)).get_current_head()
    with get_engine(url).connect() as conn:
        current = MigrationContext.configure(conn).get_current_revision()
    return current, head


def require_migrated(url: str) -> Engine:
    """Engine for `url`, or exit with a hint if the schema is not up to date."""
    from sqlalchemy.exc import OperationalError

    try:
        current, head = migration_status(url)
    except OperationalError as e:
        raise SystemExit(
            f"cannot connect to the database ({e.orig}).\n"
            "Is it running?  docker compose -f deploy/docker-compose.yml up -d"
        ) from None
    if current != head:
        raise SystemExit(f"database schema is at {current}, code expects {head}: run `trading db upgrade`")
    return get_engine(url)
