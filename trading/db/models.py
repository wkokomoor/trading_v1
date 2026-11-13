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
    UniqueConstraint,
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
    Column("cash_rate", Text),  # "tbill" or a constant like "0.04"; NULL for runs before it existed
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

# --------------------------------------------------------------------------- live / shadow trading

# A named book of money a strategy trades: "shadow" books are simulated in this database;
# "live" books track a capped slice of the real Schwab account.
portfolios = Table(
    "portfolios",
    metadata,
    Column("name", Text, primary_key=True),
    Column("mode", Text, nullable=False),  # "shadow" | "live"
    Column("strategy", Text, nullable=False),
    Column("params", JSONB, nullable=False),
    Column("allocation", Double, nullable=False),  # max dollars the strategy may hold
    Column("cash", Double, nullable=False),  # shadow: simulated cash; live: unused
    Column("positions", JSONB, nullable=False),  # shadow: {"UPRO": 12}; live: unused
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# One row per portfolio per trading day. The unique constraint makes the trade job idempotent:
# a retried or duplicated run for the same day finds the existing row and does nothing.
live_runs = Table(
    "live_runs",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("run_date", Date, nullable=False),
    Column("portfolio", Text, ForeignKey("portfolios.name"), nullable=False),
    Column("strategy", Text, nullable=False),
    Column("params", JSONB, nullable=False),
    Column("mode", Text, nullable=False),
    Column("status", Text, nullable=False),  # started | done | blocked | failed
    Column("info", JSONB, nullable=False, server_default="{}"),  # strategy diagnostics, quotes, timing
    Column("target", JSONB),  # weights the strategy asked for; NULL = hold
    Column("risk", JSONB, nullable=False, server_default="[]"),  # risk-check messages
    Column("error", Text),
    Column("git_sha", Text),
    Column("started_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("finished_at", DateTime(timezone=True)),
    UniqueConstraint("run_date", "portfolio", name="uq_live_runs_run_date_portfolio"),
)

orders = Table(
    "orders",
    metadata,
    Column("id", BigInteger, Identity(), primary_key=True),
    Column("live_run_id", BigInteger, ForeignKey("live_runs.id"), nullable=False),
    Column("portfolio", Text, nullable=False),
    Column("symbol", Text, nullable=False),
    Column("side", Text, nullable=False),
    Column("qty", Integer, nullable=False),
    Column("expected_price", Double, nullable=False),  # price used for sizing (quote at decision)
    # shadow: simulated_filled | blocked; live: submitted | filled | partially_filled | rejected | cancelled
    Column("status", Text, nullable=False),
    Column("broker_order_id", Text),
    Column("fill_price", Double),
    Column("filled_qty", Integer),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("filled_at", DateTime(timezone=True)),
    Column("error", Text),
    Index("ix_orders_portfolio_created_at", "portfolio", "created_at"),
)

account_snapshots = Table(
    "account_snapshots",
    metadata,
    Column("date", Date, primary_key=True),
    Column("portfolio", Text, ForeignKey("portfolios.name"), primary_key=True),
    Column("kind", Text, primary_key=True),  # "open" (after trading) | "close" (marked at the close)
    Column("cash", Double, nullable=False),
    Column("equity", Double, nullable=False),
    Column("positions", JSONB, nullable=False),
    Column("prices", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)

# Operational switches, e.g. key "kill_switch" -> {"on": true, "reason": "...", "by": "..."}.
controls = Table(
    "controls",
    metadata,
    Column("key", Text, primary_key=True),
    Column("value", JSONB, nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
