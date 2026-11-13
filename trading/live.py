"""Read-only live check: run a strategy on today's open and print the orders it WOULD place.

Nothing is ever sent to the broker. Real order placement is Phase 4.
"""

from __future__ import annotations

import datetime as dt
import math

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


def run_dry_run(client, strategy: Strategy, now: dt.datetime | None = None) -> list[Order]:
    now = now or dt.datetime.now(ET)
    today = now.date()
    if today not in sessions(today, today):
        raise SystemExit(f"{today} is not an NYSE trading day")
    if now.time() < dt.time(9, 31):
        raise SystemExit("market has not opened yet")

    symbols = strategy.required_symbols()
    source = SchwabSource(client)
    hist_start = today - dt.timedelta(days=math.ceil(strategy.required_lookback() * 1.6) + 15)
    bars = {s: source.fetch(s, hist_start, today - dt.timedelta(days=1)) for s in symbols}

    tickers = {s: source.tickers.get(s, s) for s in symbols}
    quotes = client.quotes(list(tickers.values())).json()
    last = {}
    for s, t in tickers.items():
        q = quotes[t]["quote"]
        today_row = pd.DataFrame({"open": [q["openPrice"]], "high": [float("nan")], "low": [float("nan")],
                                  "close": [float("nan")], "volume": [0]}, index=[today])
        bars[s] = pd.concat([bars[s], today_row])
        last[s] = float(q["lastPrice"])

    weights = strategy.on_open(MarketView(bars, today))
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
