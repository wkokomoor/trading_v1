"""Migrations, run storage, and the CLI against a real PostgreSQL test database."""

import datetime as dt

import pandas as pd
import pytest
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from conftest import FakeSource

from trading import cli
from trading.config import get_settings
from trading.data import PostgresStore, load_bars
from trading.db import downgrade, migration_status, upgrade
from trading.db.models import metadata
from trading.db.runs import jsonable, list_runs, load_run, save_run
from trading.engine import run_backtest
from trading.strategy import load_strategy

START, END = dt.date(2024, 1, 2), dt.date(2024, 6, 28)


def test_migrations_match_table_definitions(pg):
    """Fails if models.py was changed without generating a migration."""
    with pg.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn, opts={"compare_type": True}), metadata)
    assert diff == []


def test_migrations_downgrade_and_upgrade(pg_url, pg):
    downgrade(pg_url, "base")
    assert migration_status(pg_url)[0] is None
    upgrade(pg_url)
    current, head = migration_status(pg_url)
    assert current == head


def test_jsonable_handles_nan_numpy_and_dates():
    import numpy as np

    out = jsonable({"a": float("nan"), "b": np.float64(1.5), "c": np.int64(2), "d": dt.date(2025, 1, 2)})
    assert out == {"a": None, "b": 1.5, "c": 2, "d": "2025-01-02"}


def _backtest(engine):
    strategy = load_strategy("spy_vix")(vix_high=21.0)
    benchmark = load_strategy("buy_and_hold")()
    store = PostgresStore(FakeSource(), engine)
    bars = load_bars(store, ("SPY", "VIX", "UPRO", "SPXU"), START - dt.timedelta(days=10), END)
    result = run_backtest(strategy, bars, START, END, cash=10_000)
    bench = run_backtest(benchmark, bars, result.daily.index[0], result.daily.index[-1], cash=10_000)
    return strategy, benchmark, result, bench


def test_save_and_load_run_round_trip(pg):
    strategy, benchmark, result, bench = _backtest(pg)
    run_id = save_run(
        pg,
        strategy=strategy,
        benchmark=benchmark,
        result=result,
        bench=bench,
        source="fake",
        slippage_bps=5,
        label="test run",
    )
    meta, loaded = load_run(pg, run_id)

    assert meta["strategy"] == "spy_vix"
    assert meta["params"]["vix_high"] == 21.0
    assert meta["label"] == "test run"
    assert meta["git_sha"]  # tests run inside the git checkout

    got = loaded["strategy"]
    pd.testing.assert_series_equal(
        got.daily["equity"], result.daily["equity"], check_names=False, check_index_type=False
    )
    assert len(got.trades) == len(result.trades)
    assert got.summary()["total_return"] == pytest.approx(result.summary()["total_return"])
    assert loaded["benchmark"].summary()["trades"] == 1


def test_list_runs_newest_first_with_filter(pg):
    strategy, benchmark, result, bench = _backtest(pg)
    kwargs = dict(strategy=strategy, benchmark=benchmark, result=result, bench=bench, source="fake")
    first = save_run(pg, slippage_bps=0, **kwargs)
    second = save_run(pg, slippage_bps=5, **kwargs)
    runs = list_runs(pg)
    assert list(runs["id"]) == [second, first]
    assert list_runs(pg, strategy="nope").empty


@pytest.fixture
def cli_env(pg_url, pg, monkeypatch):
    """Point the CLI at the test database and at fake market data."""
    monkeypatch.setenv("DATABASE_URL", pg_url)
    get_settings.cache_clear()
    monkeypatch.setattr(cli, "YahooSource", FakeSource)
    yield
    get_settings.cache_clear()


def test_cli_backtest_saves_run_and_runs_commands_show_it(cli_env, capsys):
    cli.main(["backtest", "-s", "spy_vix", "--start", "2024-01-02", "--end", "2024-06-28", "--label", "e2e"])
    out = capsys.readouterr().out
    assert "saved as run #1" in out

    cli.main(
        ["backtest", "-s", "sma_trend", "-p", "window=20", "--start", "2024-01-02", "--end", "2024-06-28"]
    )
    assert "saved as run #2" in capsys.readouterr().out

    cli.main(["runs", "list"])
    listing = capsys.readouterr().out
    assert "spy_vix" in listing and "sma_trend" in listing and "e2e" in listing

    cli.main(["runs", "show", "1", "2"])
    shown = capsys.readouterr().out
    assert "#1 spy_vix" in shown and "#2 sma_trend" in shown


def test_cli_no_save_and_csv_store(cli_env, capsys, tmp_path):
    args = ["backtest", "-s", "buy_and_hold", "--start", "2024-01-02", "--end", "2024-02-28"]
    cli.main([*args, "--store", "csv", "--cache-dir", str(tmp_path)])
    assert "saved as run" not in capsys.readouterr().out
    assert (tmp_path / "fake" / "SPY.csv").exists()


def test_cli_requires_migrated_schema(cli_env, pg_url):
    downgrade(pg_url, "base")
    try:
        with pytest.raises(SystemExit, match="trading db upgrade"):
            cli.main(["runs", "list"])
    finally:
        upgrade(pg_url)
