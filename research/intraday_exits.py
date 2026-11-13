"""Run: uv run python research/intraday_exits.py

What changes if positions may be exited during the trading day?

Compares, for "always long UPRO" and the "SPY 20-day realized vol < 20%" filter:
  open->next open   the current model (hold from one open to the next)
  stop-loss X%      exit intraday if UPRO trades X% below today's open, cash until next open
  session only      buy at the open, sell at the close (a day trade every day)
  overnight only    buy at the close, sell at the next open (NOT a day trade)

Stops are simulated from daily bars: if the day's low touched the stop, we assume a fill
at the stop price minus extra slippage. Real stops can fill worse in fast markets.
"""

import datetime as dt

import numpy as np
import pandas as pd

from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, YahooSource, load_bars
from trading.db import get_engine

START, SPLIT, END = dt.date(2010, 4, 13), dt.date(2020, 1, 1), dt.date(2026, 11, 12)
STOP_EXTRA_BPS = 10.0

URL = get_settings().database_url
store = PostgresStore(YahooSource(), get_engine(URL)) if URL else CsvStore(YahooSource())
bars = load_bars(store, ["SPY", "UPRO"], dt.date(2009, 6, 1), END)

u = bars["UPRO"]
spy_close = bars["SPY"]["close"]
df = pd.DataFrame({"o": u["open"], "l": u["low"], "c": u["close"]})
df["o_next"] = df["o"].shift(-1)
rvol = np.log(spy_close).diff().rolling(20).std().shift(1) * np.sqrt(252)  # known at today's open
df["rvol"] = rvol.reindex(df.index)
df = df.dropna()
df = df[(df.index >= START) & (df.index <= END)]

SIGNALS = {
    "always long UPRO": np.ones(len(df), dtype=bool),
    "realized vol < 20% filter": (df["rvol"] < 0.20).to_numpy(),
}


def run(sig, mode, slip_bps, stop=None):
    """Daily equity factors for one variant. Returns (equity curve, fills)."""
    o, lo, c, on = (df[k].to_numpy() for k in ("o", "l", "c", "o_next"))
    slip, extra = slip_bps / 1e4, STOP_EXTRA_BPS / 1e4
    eq, fills, held = np.empty(len(df)), 0, False
    e = 1.0
    for t in range(len(df)):
        if mode == "open_to_open":
            if sig[t]:
                if not held:
                    e /= 1 + slip
                    fills += 1
                if stop is not None and lo[t] <= o[t] * (1 - stop):
                    e *= (1 - stop) * (1 - slip - extra)  # stopped out intraday
                    fills += 1
                    held = False
                else:
                    e *= on[t] / o[t]
                    held = True
            elif held:
                e *= 1 - slip
                fills += 1
                held = False
        elif mode == "session" and sig[t]:
            e *= (c[t] / o[t]) * (1 - slip) / (1 + slip)
            fills += 2
        elif mode == "overnight" and sig[t]:
            e *= (on[t] / c[t]) * (1 - slip) / (1 + slip)
            fills += 2
        eq[t] = e
    return eq, fills


def stats(eq, fills, mask):
    e = eq[mask]
    start = eq[np.argmax(mask) - 1] if np.argmax(mask) > 0 else 1.0
    curve = np.concatenate([[start], e]) / start
    r = curve[1:] / curve[:-1] - 1
    years = len(e) / 252
    return {
        "cagr": curve[-1] ** (1 / years) - 1,
        "maxdd": (curve / np.maximum.accumulate(curve) - 1).min(),
        "sharpe": r.mean() / r.std() * np.sqrt(252) if r.std() > 0 else np.nan,
    }


train = df.index < SPLIT
test = ~train
years_total = len(df) / 252

for slip_bps in (5.0, 1.0):
    print(f"\n===== {slip_bps:g} bps slippage per fill (+{STOP_EXTRA_BPS:g} bps extra on stop fills) =====")
    for sig_name, sig in SIGNALS.items():
        variants = {"open -> next open (current)": ("open_to_open", None)}
        for s in (0.02, 0.03, 0.05, 0.07, 0.10):
            variants[f"  + stop-loss {s:.0%} below open"] = ("open_to_open", s)
        variants["session only (day trade daily)"] = ("session", None)
        variants["overnight only (close -> open)"] = ("overnight", None)
        rows = []
        for name, (mode, stop) in variants.items():
            eq, fills = run(sig, mode, slip_bps, stop)
            tr, te = stats(eq, fills, train), stats(eq, fills, test)
            rows.append(
                {
                    sig_name: name,
                    "CAGR train": f"{tr['cagr']:.1%}",
                    "CAGR test": f"{te['cagr']:.1%}",
                    "maxDD train": f"{tr['maxdd']:.0%}",
                    "maxDD test": f"{te['maxdd']:.0%}",
                    "Sharpe train": f"{tr['sharpe']:.2f}",
                    "Sharpe test": f"{te['sharpe']:.2f}",
                    "fills/yr": f"{fills / years_total:.0f}",
                }
            )
        with pd.option_context("display.width", 250, "display.max_colwidth", 40):
            print(pd.DataFrame(rows).to_string(index=False))
            print()
