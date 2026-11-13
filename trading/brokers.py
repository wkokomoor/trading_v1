"""Quotes and brokers for the daily trade job.

ShadowBroker  fills orders at live quotes and records them in PostgreSQL; nothing is
              sent to Schwab. This is the "paper trading" stage, since Schwab has no
              paper-trading API.
SchwabBroker  real order placement: Phase D part 2, deliberately not implemented yet.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd

from trading.data.calendar import ET
from trading.data.sources import SchwabSource
from trading.engine import Order


@dataclass(frozen=True)
class Quote:
    symbol: str
    open: float | None
    last: float
    bid: float | None
    ask: float | None
    prev_close: float | None
    quote_time: dt.datetime | None


def _ts(ms):
    return pd.Timestamp(ms, unit="ms", tz="UTC").tz_convert(ET).to_pydatetime() if ms else None


def schwab_quotes(client, symbols) -> dict[str, Quote]:
    """Quotes keyed by canonical symbol ("VIX", not "$VIX")."""
    tickers = {s: SchwabSource.tickers.get(s, s) for s in symbols}
    resp = client.quotes(list(tickers.values()))
    resp.raise_for_status()
    data = resp.json()
    out = {}
    for sym, ticker in tickers.items():
        q = data.get(ticker, {}).get("quote")
        if not q:
            continue
        regular = data[ticker].get("regular", {})
        out[sym] = Quote(
            symbol=sym,
            open=q.get("openPrice") or None,
            last=float(regular.get("regularMarketLastPrice") or q.get("lastPrice")),
            bid=q.get("bidPrice") or None,
            ask=q.get("askPrice") or None,
            prev_close=q.get("closePrice") or None,
            quote_time=_ts(q.get("quoteTime") or q.get("tradeTime")),
        )
    return out


class ShadowBroker:
    """Simulated fills against live quotes: buys at the ask, sells at the bid (falling back
    to the last price), plus optional extra slippage. State lives in the portfolios table."""

    def __init__(self, slippage_bps: float = 0.0):
        self.slip = slippage_bps / 10_000

    def fill_price(self, order: Order, quote: Quote) -> float:
        if order.side == "BUY":
            return (quote.ask or quote.last) * (1 + self.slip)
        return (quote.bid or quote.last) * (1 - self.slip)

    def execute(self, orders: list[Order], quotes: dict[str, Quote], cash: float, positions: dict[str, int]):
        """Returns (fills, cash, positions). Sells first, so buys can use their proceeds;
        a buy that no longer fits the cash is cut to what fits."""
        positions = dict(positions)
        fills = []
        for o in sorted(orders, key=lambda o: o.side != "SELL"):
            px = self.fill_price(o, quotes[o.symbol])
            qty = o.qty
            if o.side == "SELL":
                qty = min(qty, positions.get(o.symbol, 0))
                cash += qty * px
                positions[o.symbol] = positions.get(o.symbol, 0) - qty
            else:
                qty = min(qty, int(cash // px))
                cash -= qty * px
                positions[o.symbol] = positions.get(o.symbol, 0) + qty
            fills.append((o, qty, px))
        return fills, cash, {s: q for s, q in positions.items() if q}


class SchwabBroker:
    def execute(self, *args, **kwargs):
        raise NotImplementedError(
            "Live order placement is Phase D part 2 and is not implemented. Use a shadow portfolio."
        )
