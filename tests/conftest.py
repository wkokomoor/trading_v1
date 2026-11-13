import datetime as dt
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from trading.data import sessions


def make_bars(days, opens, closes=None) -> pd.DataFrame:
    closes = opens if closes is None else closes
    return pd.DataFrame(
        {"open": opens, "high": opens, "low": opens, "close": closes, "volume": 0},
        index=pd.Index(days, name="date"),
    ).astype({"open": float, "close": float, "high": float, "low": float})


@pytest.fixture
def march_2025():
    """Ten NYSE sessions, 2025-03-03 .. 2025-03-14."""
    return sessions(dt.date(2025, 3, 3), dt.date(2025, 3, 14))


class FakeSource:
    """Deterministic random-walk daily bars for any symbol; records every fetch."""

    name = "fake"

    def __init__(self):
        self.calls = []

    def fetch(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        days = sessions(start, end)
        rng = np.random.default_rng(sum(map(ord, symbol)))
        base = 20.0 if symbol == "VIX" else 100.0
        # Price is a function of the date, so overlapping fetches agree.
        all_days = sessions(dt.date(2000, 1, 3), end)
        walk = base * np.exp(np.cumsum(rng.normal(0, 0.01, len(all_days))))
        px = dict(zip(all_days, walk, strict=True))
        opens = [px[d] for d in days]
        df = make_bars(days, opens, [o * 1.001 for o in opens])
        df["volume"] = 1000
        return df


# --------------------------------------------------------------------------- PostgreSQL


def _test_database_url() -> str | None:
    if url := os.environ.get("TEST_DATABASE_URL"):
        return url
    env_file = Path(__file__).parents[1] / ".env"
    if env_file.exists():
        from dotenv import dotenv_values

        return dotenv_values(env_file).get("TEST_DATABASE_URL")
    return None


@pytest.fixture(scope="session")
def pg_url():
    """A migrated, empty test database. Skips when unavailable (fails in CI)."""
    url = _test_database_url()
    required = bool(os.environ.get("REQUIRE_DB_TESTS"))
    if not url:
        if required:
            pytest.fail("REQUIRE_DB_TESTS is set but TEST_DATABASE_URL is not")
        pytest.skip("TEST_DATABASE_URL not set")
    if not (make_url(url).database or "").endswith("_test"):
        pytest.fail(f"refusing to wipe {make_url(url).database!r}: test database name must end in _test")

    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public"))
    except Exception as e:  # noqa: BLE001 - any connection problem means "no database"
        if required:
            raise
        pytest.skip(f"test database unavailable: {e}")
    finally:
        engine.dispose()

    from trading.db import upgrade

    upgrade(url)
    return url


@pytest.fixture
def pg(pg_url):
    """Engine on the test database, with every table emptied first."""
    from trading.db import get_engine
    from trading.db.models import metadata

    engine = get_engine(pg_url)
    tables = ", ".join(t.name for t in metadata.sorted_tables)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    return engine
