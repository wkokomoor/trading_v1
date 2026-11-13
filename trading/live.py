"""Live helpers: build a strategy's view at today's open, and the read-only dry run.

The dry run prints the orders a strategy WOULD place in the real account; nothing is sent.
"""

from __future__ import annotations

import datetime as dt
import math

import numpy as np
import pandas as pd

from trading.data import ET, SchwabSource, sessions
from trading.engine import Order, rebalance_orders
from trading.strategy import MarketView, Strategy


def live_account(client, symbols) -> tuple[float, dict[str, int]]:
    """Cash and long positions (restricted to `symbols`) for the first linked account."""
    account_hash = client.linked_accounts().json()[0]["hashValue"]
    acct = client.account_details(account_hash, fields="positions").json()["securitiesAccount"]
    positions = {
        p["instrument"]["symbol"]: int(p["longQuantity"])
        for p in acct.get("positions", [])
        if p["instrument"]["symbol"] in symbols and p.get("longQuantity", 0) > 0
    }
    return float(acct["currentBalances"]["cashBalance"]), positions


def open_view(source, quotes: dict, strategy: Strategy, today: dt.date) -> MarketView:
    """The strategy's view at today's open: completed daily bars through yesterday from
    `source` (a data source or bar store), plus today's opening price from `quotes`."""
    hist_start = today - dt.timedelta(days=math.ceil(strategy.required_history() * 1.6) + 15)
    bars = {}
    for s in strategy.required_symbols():
        hist = source.fetch(s, hist_start, today - dt.timedelta(days=1))
        hist = hist[hist.index < today]
        today_row = pd.DataFrame(
            {"open": [quotes[s].open], "high": [np.nan], "low": [np.nan], "close": [np.nan], "volume": [0]},
            index=pd.Index([today], name="date"),
        )
        bars[s] = pd.concat([hist, today_row])
    return MarketView(bars, today)


def session_problem(now: dt.datetime) -> str | None:
    """Why trading at `now` isn't possible (holiday, weekend, before the open), or None."""
    today = now.date()
    if today not in sessions(today, today):
        return f"{today} is not an NYSE trading day"
    if now.time() < dt.time(9, 31):
        return "market has not opened yet"
    return None


def run_dry_run(client, strategy: Strategy, now: dt.datetime | None = None) -> list[Order]:
    from trading.brokers import schwab_quotes

    now = now or dt.datetime.now(ET)
    if problem := session_problem(now):
        raise SystemExit(problem)
    today = now.date()
    symbols = strategy.required_symbols()
    quotes = schwab_quotes(client, symbols)
    last = {s: q.last for s, q in quotes.items()}
    weights = strategy.on_open(open_view(SchwabSource(client), quotes, strategy, today))
    cash, positions = live_account(client, symbols)
    orders = [] if weights is None else rebalance_orders(weights, positions, cash, last)

    print(f"strategy:  {strategy!r}")
    print(f"info:      {strategy.info}")
    print(f"target:    {'hold current position' if weights is None else weights or 'all cash'}")
    print(f"account:   cash={cash:.2f} positions={positions}")
    print("orders that WOULD be placed (dry run, nothing sent):")
    for o in orders or ["  (none)"]:
        print(f"  {o}")
    return orders
