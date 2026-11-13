"""Order sizing, simulated broker, backtest loop, and performance metrics."""

from __future__ import annotations

import datetime as dt
import logging
import math
import os
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from trading.strategy import MarketView, Strategy, Weights

log = logging.getLogger(__name__)

TRADING_DAYS = 252


@dataclass(frozen=True)
class Order:
    symbol: str
    side: str  # "BUY" or "SELL"
    qty: int
    price: float  # expected price before slippage


def validate_weights(weights: Weights) -> None:
    if any(w < 0 for w in weights.values()):
        raise ValueError(f"negative weights (shorting) not supported: {weights}")
    if sum(weights.values()) > 1 + 1e-9:
        raise ValueError(f"weights sum to more than 1 (leverage) not supported: {weights}")


def rebalance_orders(
    weights: Weights,
    positions: dict[str, int],
    cash: float,
    prices: dict[str, float],
    slippage_bps: float = 0.0,
) -> list[Order]:
    """Orders that move `positions` to `weights` of total equity, sells first.

    Buys are capped by the cash available after sells, so cash never goes negative.
    """
    validate_weights(weights)
    slip = slippage_bps / 10_000
    equity = cash + sum(q * prices[s] for s, q in positions.items())
    targets = {s: math.floor(equity * w / (prices[s] * (1 + slip))) for s, w in weights.items()}

    orders: list[Order] = []
    available = cash
    for sym, held in positions.items():
        excess = held - targets.get(sym, 0)
        if excess > 0:
            orders.append(Order(sym, "SELL", excess, prices[sym]))
            available += excess * prices[sym] * (1 - slip)
    for sym, target in targets.items():
        want = target - positions.get(sym, 0)
        qty = min(want, math.floor(available / (prices[sym] * (1 + slip))))
        if qty > 0:
            orders.append(Order(sym, "BUY", qty, prices[sym]))
            available -= qty * prices[sym] * (1 + slip)
    return orders


@dataclass
class SimBroker:
    cash: float
    slippage_bps: float = 0.0
    positions: dict[str, int] = field(default_factory=dict)
    trades: list[dict] = field(default_factory=list)

    def execute(self, orders: list[Order], when) -> None:
        slip = self.slippage_bps / 10_000
        for o in sorted(orders, key=lambda o: o.side != "SELL"):  # sells first
            held = self.positions.get(o.symbol, 0)
            if o.side == "SELL":
                if o.qty > held:
                    raise ValueError(f"cannot sell {o.qty} {o.symbol}, only hold {held}")
                fill = o.price * (1 - slip)
                self.cash += o.qty * fill
                signed_qty = -o.qty
                if held == o.qty:
                    self.positions.pop(o.symbol)
                else:
                    self.positions[o.symbol] = held - o.qty
            elif o.side == "BUY":
                fill = o.price * (1 + slip)
                cost = o.qty * fill
                if cost > self.cash + 1e-9:
                    raise ValueError(f"insufficient cash for {o}: have {self.cash:.2f}")
                self.cash -= cost
                signed_qty = o.qty
                self.positions[o.symbol] = held + o.qty
            else:
                raise ValueError(f"unknown side {o.side!r}")
            self.trades.append(
                {
                    "date": when,
                    "symbol": o.symbol,
                    "side": o.side,
                    "qty": signed_qty,
                    "price": fill,
                    "value": o.qty * fill,
                }
            )

    def equity(self, prices: dict[str, float]) -> float:
        return self.cash + sum(q * prices[s] for s, q in self.positions.items())


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


def max_drawdown(equity: pd.Series) -> float:
    return float((equity / equity.cummax() - 1).min())


def performance(equity: pd.Series, start_cash: float) -> dict:
    """Headline stats for a daily equity series (first value = after day one)."""
    curve = pd.concat([pd.Series([start_cash]), equity.reset_index(drop=True)])
    rets = curve.pct_change().dropna()
    years = len(equity) / TRADING_DAYS
    total = curve.iloc[-1] / start_cash - 1
    std = rets.std()
    return {
        "total_return": total,
        "cagr": (1 + total) ** (1 / years) - 1 if years > 0 and total > -1 else float("nan"),
        "volatility": std * np.sqrt(TRADING_DAYS),
        "sharpe": rets.mean() / std * np.sqrt(TRADING_DAYS) if std > 0 else float("nan"),
        "max_drawdown": max_drawdown(curve),
    }


@dataclass
class BacktestResult:
    strategy: str
    daily: pd.DataFrame
    trades: pd.DataFrame
    start_cash: float

    def summary(self) -> dict:
        if self.daily.empty:
            return {"days": 0}
        return {
            "start": self.daily.index[0],
            "end": self.daily.index[-1],
            "days": len(self.daily),
            "end_equity": round(float(self.daily["equity"].iloc[-1]), 2),
            **performance(self.daily["equity"], self.start_cash),
            "trades": len(self.trades),
            "time_in_market": float((self.daily["holding"] != "CASH").mean()),
        }


# --------------------------------------------------------------------------- #
# Backtest loop
# --------------------------------------------------------------------------- #


def run_backtest(
    strategy: Strategy,
    bars: dict[str, pd.DataFrame],
    start: dt.date,
    end: dt.date,
    cash: float = 10_000.0,
    slippage_bps: float = 0.0,
    cash_yield: float | pd.Series = 0.0,
) -> BacktestResult:
    """Simulate `strategy` on daily bars: decide and fill at each open, mark at the close.

    Only dates where every required symbol has a bar are simulated, and only once
    the strategy has `lookback` completed bars of history.

    cash_yield: annual interest rate earned on idle cash (e.g. 0.04), or a Series of
    annual rates by date (e.g. the 13-week T-bill yield); credited daily as rate / 252.
    """
    symbols = strategy.required_symbols()
    lookback = strategy.required_lookback()
    common = sorted(set.intersection(*(set(bars[s].index) for s in symbols)))
    days = [d for d in common if start <= d <= end]
    if not days:
        raise ValueError(f"no dates between {start} and {end} with data for all of {symbols}")
    if days[0] > start + dt.timedelta(days=7):
        log.warning("data for %s only available from %s (requested %s)", symbols, days[0], start)

    broker = SimBroker(cash=cash, slippage_bps=slippage_bps)
    if isinstance(cash_yield, pd.Series):
        # Rate known as of each day (last published value on or before it).
        daily_rate = (
            cash_yield.sort_index().reindex(days, method="ffill").fillna(0.0) / TRADING_DAYS
        ).to_dict()
    else:
        daily_rate = dict.fromkeys(days, cash_yield / TRADING_DAYS)
    last_weights: Weights | None = None
    rows = []
    warmup = 0

    for day in days:
        view = MarketView(bars, day)
        if min(len(view.history(s, lookback)) for s in symbols) < lookback:
            warmup += 1
            continue

        strategy.info = {}
        weights = strategy.on_open(view)
        orders: list[Order] = []
        if weights is not None and weights != last_weights:
            unknown = set(weights) - set(symbols)
            if unknown:
                raise ValueError(f"{strategy.name} traded {unknown} not listed in its symbols")
            opens = {s: view.open(s) for s in set(weights) | set(broker.positions)}
            orders = rebalance_orders(weights, broker.positions, broker.cash, opens, slippage_bps)
            broker.execute(orders, day)
            last_weights = dict(weights)

        if broker.cash > 0 and daily_rate[day]:
            broker.cash += broker.cash * daily_rate[day]

        closes = {s: float(bars[s].at[day, "close"]) for s in broker.positions}
        rows.append(
            {
                "date": day,
                **strategy.info,
                "target": _fmt_weights(weights),
                "orders": "; ".join(f"{o.side} {o.qty} {o.symbol}" for o in orders),
                "holding": ",".join(sorted(broker.positions)) or "CASH",
                "cash": broker.cash,
                "equity": broker.equity(closes),
            }
        )

    if warmup:
        log.info("%s: skipped %d warm-up days (lookback=%d)", strategy.name, warmup, lookback)
    daily = pd.DataFrame(rows).set_index("date") if rows else pd.DataFrame()
    trades = pd.DataFrame(broker.trades, columns=["date", "symbol", "side", "qty", "price", "value"])
    return BacktestResult(strategy=repr(strategy), daily=daily, trades=trades, start_cash=cash)


def _fmt_weights(weights: Weights | None) -> str:
    if weights is None:
        return "hold"
    if not weights:
        return "cash"
    return ",".join(f"{s}:{w:g}" for s, w in weights.items())


# --------------------------------------------------------------------------- #
# Chart
# --------------------------------------------------------------------------- #

# Fixed categorical order (validated palette): blue, orange, aqua, yellow, magenta, green, violet, red.
SERIES_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]


def plot_equity(results: dict[str, BacktestResult], path: str) -> None:
    """Equity curves on one log-scale axis, legend plus end-of-line labels."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    n = len(results)
    if n > len(SERIES_COLORS):
        raise ValueError(f"can plot at most {len(SERIES_COLORS)} series at once, got {n}")
    fig, ax = plt.subplots(figsize=(10, 5.5 + 0.25 * n), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    ends = []
    for (label, res), color in zip(results.items(), SERIES_COLORS, strict=False):
        eq = res.daily["equity"]
        x = pd.to_datetime(eq.index)
        ax.plot(x, eq.values, color=color, linewidth=2, label=label)
        short = label.split("(")[0].strip()  # full params live in the legend
        ends.append((x[-1], eq.iloc[-1], f"{short}  ${eq.iloc[-1]:,.0f}"))
    ax.set_yscale("log")

    # End-of-line labels, nudged apart vertically so close finishes don't overlap.
    # Leave room on the right for the longest label (~0.0075 of the 10in width per 9pt character).
    longest = max(len(text) for _, _, text in ends)
    right = min(0.86, max(0.55, 1 - 0.0075 * longest - 0.03))
    fig.subplots_adjust(left=0.09, right=right, top=0.93, bottom=0.1 + 0.045 * n)
    to_px = ax.transData.transform
    min_gap_pt = 12
    placed = []
    for x_end, y_end, text in sorted(ends, key=lambda e: e[1]):
        y_pt = to_px((mdates.date2num(x_end), y_end))[1] * 72 / fig.dpi
        target = max(y_pt, placed[-1] + min_gap_pt) if placed else y_pt
        placed.append(target)
        ax.annotate(
            text,
            (x_end, y_end),
            xytext=(6, target - y_pt),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color="#0b0b0b",
            annotation_clip=False,
        )
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.grid(True, which="major", color="#e4e3df", linewidth=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c3c2b7")
    ax.tick_params(colors="#52514e", labelsize=9)
    ax.set_title("Equity (log scale)", loc="left", color="#0b0b0b", fontsize=12)
    ax.legend(
        frameon=False,
        loc="upper left",
        bbox_to_anchor=(0, -0.07),
        labelcolor="#52514e",
        fontsize=9,
    )
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fig.savefig(path, facecolor=fig.get_facecolor())
    plt.close(fig)
