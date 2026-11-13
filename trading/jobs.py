"""Scheduled jobs (Cloud Run Jobs later; run by hand for now).

  trade      ~09:35 ET  decide at the open, run risk checks, execute (shadow: simulated fills)
  ingest     ~16:30 ET  refresh daily bars for every portfolio's symbols
  reconcile  ~16:45 ET  mark shadow portfolios at the close and record a snapshot

Every job is safe to re-run: `trade` is idempotent per portfolio per day (live_runs has a
unique constraint), and `ingest` / `reconcile` overwrite the same rows.
"""

from __future__ import annotations

import datetime as dt

from trading import portfolio as pf
from trading import risk
from trading.brokers import ShadowBroker, schwab_quotes
from trading.config import code_version
from trading.data import PostgresStore, SchwabSource, last_complete_session
from trading.data.calendar import ET
from trading.engine import rebalance_orders
from trading.live import open_view, session_problem
from trading.strategy import load_strategy


def trade(
    engine, client, portfolio_name: str, now: dt.datetime | None = None, slippage_bps: float = 0.0
) -> dict:
    now = now or dt.datetime.now(ET)
    today = now.date()
    p = pf.get_portfolio(engine, portfolio_name)
    if p.mode != "shadow":
        raise SystemExit("live portfolios need Phase D part 2 (real order placement), not built yet")
    if problem := session_problem(now):
        return {"portfolio": p.name, "status": "skipped", "reason": problem}

    sha, _ = code_version()
    run_id, existing = pf.start_run(engine, today, p, sha)
    if existing:
        return {
            "portfolio": p.name,
            "status": "skipped",
            "reason": f"today's run #{run_id} is already {existing}",
        }

    strategy = load_strategy(p.strategy)(**p.params)
    try:
        symbols = strategy.required_symbols()
        quotes = schwab_quotes(client, set(symbols) | set(p.positions))
        view = open_view(PostgresStore(SchwabSource(client), engine), quotes, strategy, today)
        weights = strategy.on_open(view)

        prices = {s: q.last for s, q in quotes.items()}
        equity = p.cash + sum(q * prices[s] for s, q in p.positions.items())
        orders = []
        if weights is not None:
            budget = min(1.0, p.allocation / equity) if equity > 0 else 0.0  # never exceed the allocation
            orders = rebalance_orders(
                {s: w * budget for s, w in weights.items()}, p.positions, p.cash, prices
            )

        info = {
            **strategy.info,
            "decided_at": now.isoformat(),
            "quotes": {
                s: {"open": q.open, "last": q.last, "bid": q.bid, "ask": q.ask} for s, q in quotes.items()
            },
            "equity_before": equity,
        }
        with engine.begin() as conn:
            res = risk.check(
                orders,
                limits=risk.RiskLimits(allocation=p.allocation),
                quotes=quotes,
                required_symbols=symbols,
                positions=p.positions,
                cash=p.cash,
                prev_close_equity=pf.last_close_equity(conn, p.name, today),
                kill_switch=pf.kill_switch(conn),
                today=today,
            )
            fills, cash, positions = ShadowBroker(slippage_bps).execute(
                res.approved, quotes, p.cash, p.positions
            )
            for o, qty, px in fills:
                pf.record_order(
                    conn, run_id, p.name, symbol=o.symbol, side=o.side, qty=o.qty, expected_price=o.price,
                    status="simulated_filled", fill_price=px, filled_qty=qty, filled_at=now,
                )  # fmt: skip
            for o, why in res.blocked:
                pf.record_order(
                    conn, run_id, p.name, symbol=o.symbol, side=o.side, qty=o.qty, expected_price=o.price,
                    status="blocked", error=why,
                )  # fmt: skip
            pf.set_shadow_state(conn, p.name, cash, positions)
            equity_after = pf.write_snapshot(conn, today, p.name, "open", cash, positions, prices)
            status = "blocked" if res.halted else "done"
            pf.finish_run(conn, run_id, status, info=info, target=weights, risk=res.messages)
        return {
            "portfolio": p.name,
            "status": status,
            "run_id": run_id,
            "target": "hold" if weights is None else (weights or "cash"),
            "fills": [f"{o.side} {qty} {o.symbol} @ {px:.2f}" for o, qty, px in fills],
            "blocked": [f"{o.side} {o.qty} {o.symbol}: {why}" for o, why in res.blocked],
            "risk": res.messages,
            "cash": round(cash, 2),
            "positions": positions,
            "equity": round(equity_after, 2),
            "info": {k: v for k, v in strategy.info.items()},
        }
    except Exception as e:
        with engine.begin() as conn:
            pf.finish_run(conn, run_id, "failed", error=f"{type(e).__name__}: {e}")
        raise


def ingest(engine, client, now: dt.datetime | None = None, history_days: int = 400) -> list[str]:
    """Refresh daily bars (through the last completed session) for every portfolio's symbols."""
    end = last_complete_session(now)
    store = PostgresStore(SchwabSource(client), engine)
    symbols = sorted(
        {
            s
            for p in pf.list_portfolios(engine)
            for s in load_strategy(p.strategy)(**p.params).required_symbols()
        }
    )
    lines = []
    for s in symbols:
        bars = store.fetch(s, end - dt.timedelta(days=history_days), end)
        lines.append(f"{s}: {len(bars)} bars through {bars.index.max() if len(bars) else '-'}")
    return lines


def reconcile(engine, client, now: dt.datetime | None = None) -> list[dict]:
    """After the close: mark every shadow portfolio at today's closing prices."""
    now = now or dt.datetime.now(ET)
    today = now.date()
    if last_complete_session(now) != today:
        return [{"status": "skipped", "reason": f"no completed session today ({today})"}]
    store = PostgresStore(SchwabSource(client), engine)
    out = []
    for p in pf.list_portfolios(engine):
        if p.mode != "shadow":
            continue
        closes = {}
        for s in p.positions:
            bars = store.fetch(s, today - dt.timedelta(days=10), today)
            if today not in bars.index:
                raise RuntimeError(f"no {s} bar for {today} yet; run `job ingest` after the close")
            closes[s] = float(bars.at[today, "close"])
        with engine.begin() as conn:
            prev = pf.last_close_equity(conn, p.name, today)
            equity = pf.write_snapshot(conn, today, p.name, "close", p.cash, p.positions, closes)
        out.append(
            {
                "portfolio": p.name,
                "status": "done",
                "equity": round(equity, 2),
                "day_change": None if prev is None else f"{equity / prev - 1:+.2%}",
                "positions": p.positions,
            }
        )
    return out
