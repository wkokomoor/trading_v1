"""Table definitions. Change these, then generate a migration:

uv run alembic revision --autogenerate -m "describe the change"
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Double,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData(
    naming_convention={
        "ix": "ix_%(table_name)s_%(column_0_name)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

# --------------------------------------------------------------------------- market data

bars_daily = Table(
    "bars_daily",
    metadata,
    Column("source", Text, primary_key=True),
    Column("symbol", Text, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("open", Double, nullable=False),
    Column("high", Double),
    Column("low", Double),
    Column("close", Double, nullable=False),
    Column("volume", BigInteger),
    Column("fetched_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Date range already requested from a source for a symbol (a symbol may have no
# bars for part of it, e.g. before it was listed), so we don't re-fetch it.
bar_coverage = Table(
    "bar_coverage",
    metadata,
    Column("source", Text, primary_key=True),
    Column("symbol", Text, primary_key=True),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# --------------------------------------------------------------------------- backtests

backtest_runs = Table(
    "backtest_runs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("strategy", Text, nullable=False),
    Column("params", JSONB, nullable=False),
    Column("benchmark", Text, nullable=False),
    Column("benchmark_params", JSONB, nullable=False),
    Column("source", Text, nullable=False),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    Column("start_cash", Double, nullable=False),
    Column("slippage_bps", Double, nullable=False),
    Column("git_sha", Text),
    Column("git_dirty", Boolean, nullable=False, server_default="false"),
    Column("label", Text),
    Column("summary", JSONB, nullable=False),
    Column("benchmark_summary", JSONB, nullable=False),
    Index("ix_backtest_runs_strategy_created_at", "strategy", "created_at"),
)

# `series` is "strategy" or "benchmark", so both equity curves are stored per run.
backtest_daily = Table(
    "backtest_daily",
    metadata,
    Column("run_id", BigInteger, ForeignKey("backtest_runs.id", ondelete="CASCADE"), primary_key=True),
    Column("series", Text, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("equity", Double, nullable=False),
    Column("cash", Double, nullable=False),
    Column("holding", Text, nullable=False),
    Column("target", Text, nullable=False),
    Column("orders", Text, nullable=False),
    Column("info", JSONB, nullable=False),
)

backtest_trades = Table(
    "backtest_trades",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("run_id", BigInteger, ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False),
    Column("series", Text, nullable=False),
    Column("date", Date, nullable=False),
    Column("symbol", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("qty", Integer, nullable=False),
    Column("price", Double, nullable=False),
    Column("value", Double, nullable=False),
    Index("ix_backtest_trades_run_id", "run_id"),
)
