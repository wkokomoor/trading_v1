"""Pre-trade risk checks. Pure functions: everything they need is passed in.

Rules, in order:
  1. Kill switch on                       -> block everything.
  2. Data looks wrong (missing quote, no open, stale quote time, or an opening gap
     beyond `max_gap` versus the previous close) -> block everything. Trading on bad
     data is worse than not trading.
  3. Order for a symbol outside the strategy -> block that order.
  4. Daily loss limit breached (equity now vs. last close snapshot) -> block buys;
     sells (de-risking) are still allowed.
  5. Holdings after the orders would exceed the allocation (with a small tolerance
     for price moves) -> block buys.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from trading.brokers import Quote
from trading.engine import Order


@dataclass(frozen=True)
class RiskLimits:
    allocation: float
    max_daily_loss: float = 0.15  # 3x ETF: a 15% one-day drop is extreme but possible
    max_gap: float = 0.25  # |open / previous close - 1| beyond this = suspect data
    allocation_tolerance: float = 0.02


@dataclass
class RiskResult:
    approved: list[Order] = field(default_factory=list)
    blocked: list[tuple[Order, str]] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    halted: bool = False  # True when nothing at all may trade today


def check(
    orders: list[Order],
    *,
    limits: RiskLimits,
    quotes: dict[str, Quote],
    required_symbols: tuple[str, ...],
    positions: dict[str, int],
    cash: float,
    prev_close_equity: float | None,
    kill_switch: dict,
    today: dt.date,
) -> RiskResult:
    res = RiskResult()

    def halt(msg):
        res.halted = True
        res.messages.append(msg)
        res.blocked = [(o, msg) for o in orders]
        res.approved = []
        return res

    if kill_switch.get("on"):
        return halt(f"kill switch is on: {kill_switch.get('reason') or 'no reason given'}")

    for sym in required_symbols:
        q = quotes.get(sym)
        if q is None:
            return halt(f"no quote for {sym}")
        if not q.open or q.open <= 0:
            return halt(f"{sym} has no opening price yet")
        if q.quote_time is not None and q.quote_time.date() != today:
            return halt(f"{sym} quote is stale ({q.quote_time:%Y-%m-%d %H:%M})")
        if q.prev_close and abs(q.open / q.prev_close - 1) > limits.max_gap:
            return halt(f"{sym} opened {q.open / q.prev_close - 1:+.1%} vs. previous close: suspect data")

    prices = {s: quotes[s].last for s in set(positions) | {o.symbol for o in orders}}
    equity = cash + sum(q * prices[s] for s, q in positions.items())
    loss_breached = prev_close_equity is not None and equity < prev_close_equity * (1 - limits.max_daily_loss)
    if loss_breached:
        res.messages.append(
            f"daily loss limit: equity {equity:,.0f} is more than {limits.max_daily_loss:.0%} below "
            f"the last close ({prev_close_equity:,.0f}); buys blocked"
        )

    after = dict(positions)
    for o in orders:
        after[o.symbol] = after.get(o.symbol, 0) + (o.qty if o.side == "BUY" else -o.qty)
    held_value = sum(q * prices[s] for s, q in after.items() if q > 0)
    over_allocation = held_value > limits.allocation * (1 + limits.allocation_tolerance)
    if over_allocation:
        res.messages.append(
            f"orders would hold {held_value:,.0f}, above the "
            f"{limits.allocation:,.0f} allocation; buys blocked"
        )

    for o in orders:
        if o.symbol not in required_symbols:
            res.blocked.append((o, f"{o.symbol} is not one of the strategy's symbols"))
        elif o.side == "BUY" and loss_breached:
            res.blocked.append((o, "daily loss limit"))
        elif o.side == "BUY" and over_allocation:
            res.blocked.append((o, "allocation cap"))
        else:
            res.approved.append(o)
    return res
