"""Save, list and load backtest runs."""

from __future__ import annotations

import datetime as dt
import math

import numpy as np
import pandas as pd
from sqlalchemy import Engine, desc, insert, select

from trading.config import code_version
from trading.db.models import backtest_daily, backtest_runs, backtest_trades
from trading.engine import BacktestResult
from trading.strategy import Strategy

DAILY_FIELDS = ["equity", "cash", "holding", "target", "orders"]


def jsonable(v):
    """Make a value safe for JSONB (Postgres rejects NaN; numpy and dates need converting)."""
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if math.isnan(v) or math.isinf(v) else float(v)
    if isinstance(v, (dt.date, pd.Timestamp)):
        return v.isoformat()
    return v


def _daily_rows(run_id: int, series: str, daily: pd.DataFrame) -> list[dict]:
    info_cols = [c for c in daily.columns if c not in DAILY_FIELDS]
    rows = []
    for date, r in daily.iterrows():
        rows.append(
            {
                "run_id": run_id,
                "series": series,
                "date": date,
                "equity": float(r["equity"]),
                "cash": float(r["cash"]),
                "holding": r["holding"],
                "target": r["target"],
                "orders": r["orders"],
                "info": jsonable({c: r[c] for c in info_cols}),
            }
        )
    return rows


def _trade_rows(run_id: int, series: str, trades: pd.DataFrame) -> list[dict]:
    return [
        {
            "run_id": run_id,
            "series": series,
            "date": t.date,
            "symbol": t.symbol,
            "side": t.side,
            "qty": int(t.qty),
            "price": float(t.price),
            "value": float(t.value),
        }
        for t in trades.itertuples(index=False)
    ]


def save_run(
    engine: Engine,
    *,
    strategy: Strategy,
    benchmark: Strategy,
    result: BacktestResult,
    bench: BacktestResult,
    source: str,
    slippage_bps: float,
    label: str | None = None,
) -> int:
    sha, dirty = code_version()
    with engine.begin() as conn:
        run_id = conn.execute(
            insert(backtest_runs)
            .values(
                strategy=strategy.name,
                params=jsonable(vars(strategy.p)),
                benchmark=benchmark.name,
                benchmark_params=jsonable(vars(benchmark.p)),
                source=source,
                start_date=result.daily.index[0],
                end_date=result.daily.index[-1],
                start_cash=result.start_cash,
                slippage_bps=slippage_bps,
                git_sha=sha,
                git_dirty=dirty,
                label=label,
                summary=jsonable(result.summary()),
                benchmark_summary=jsonable(bench.summary()),
            )
            .returning(backtest_runs.c.id)
        ).scalar_one()
        for series, res in (("strategy", result), ("benchmark", bench)):
            conn.execute(insert(backtest_daily), _daily_rows(run_id, series, res.daily))
            if not res.trades.empty:
                conn.execute(insert(backtest_trades), _trade_rows(run_id, series, res.trades))
    return run_id


def list_runs(engine: Engine, strategy: str | None = None, limit: int = 20) -> pd.DataFrame:
    r = backtest_runs.c
    q = select(backtest_runs).order_by(desc(r.id)).limit(limit)
    if strategy:
        q = q.where(r.strategy == strategy)
    with engine.connect() as conn:
        rows = conn.execute(q).mappings().all()
    out = []
    for row in rows:
        s, b = row["summary"], row["benchmark_summary"]
        out.append(
            {
                "id": row["id"],
                "created": row["created_at"].astimezone().strftime("%Y-%m-%d %H:%M"),
                "strategy": row["strategy"],
                "params": _short_params(row["params"]),
                "period": f"{row['start_date']}..{row['end_date']}",
                "slip": row["slippage_bps"],
                "cagr": s.get("cagr"),
                "sharpe": s.get("sharpe"),
                "max_dd": s.get("max_drawdown"),
                "bench_cagr": b.get("cagr"),
                "bench_sharpe": b.get("sharpe"),
                "commit": (row["git_sha"] or "")[:7] + ("*" if row["git_dirty"] else ""),
                "label": row["label"] or "",
            }
        )
    return pd.DataFrame(out)


def _short_params(params: dict) -> str:
    # JSONB doesn't keep key order, so sort for a stable display.
    return " ".join(f"{k}={v}" for k, v in sorted(params.items()))


def load_run(engine: Engine, run_id: int) -> tuple[dict, dict[str, BacktestResult]]:
    """Run metadata plus the stored strategy and benchmark results."""
    with engine.connect() as conn:
        meta = conn.execute(select(backtest_runs).where(backtest_runs.c.id == run_id)).mappings().first()
        if meta is None:
            raise LookupError(f"no backtest run #{run_id}")
        d = backtest_daily.c
        daily_rows = conn.execute(
            select(backtest_daily).where(d.run_id == run_id).order_by(d.series, d.date)
        ).mappings()
        daily = pd.DataFrame(list(daily_rows))
        trade_rows = conn.execute(
            select(backtest_trades).where(backtest_trades.c.run_id == run_id).order_by(backtest_trades.c.id)
        ).mappings()
        trades = pd.DataFrame(list(trade_rows))

    results = {}
    for series in ("strategy", "benchmark"):
        sd = daily[daily["series"] == series].set_index("date")[DAILY_FIELDS]
        st = (
            trades[trades["series"] == series][["date", "symbol", "side", "qty", "price", "value"]]
            if not trades.empty
            else pd.DataFrame(columns=["date", "symbol", "side", "qty", "price", "value"])
        )
        name = meta["strategy"] if series == "strategy" else meta["benchmark"]
        params = meta["params"] if series == "strategy" else meta["benchmark_params"]
        results[series] = BacktestResult(
            strategy=f"#{run_id} {name}({_short_params(params)})",
            daily=sd,
            trades=st.reset_index(drop=True),
            start_cash=meta["start_cash"],
        )
    return dict(meta), results
