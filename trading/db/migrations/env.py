from alembic import context
from sqlalchemy import create_engine

from trading.db.models import metadata

config = context.config


def _url() -> str:
    url = config.get_main_option("sqlalchemy.url")
    if url:
        return url
    from trading.config import get_settings

    url = get_settings().database_url
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    return url


def run_migrations_offline() -> None:
    context.configure(url=_url(), target_metadata=metadata, literal_binds=True, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
