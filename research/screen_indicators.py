"""Run: uv run python research/screen_indicators.py

Screen traditional long/cash timing indicators with the feasibility gate.

Every indicator uses only information available at the open of day t (closes up
to t-1, plus today's open). Positions: 1 = UPRO, 0 = cash, held open -> next open.
"""

import datetime as dt

import numpy as np
import pandas as pd

from trading import feasibility as F
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, YahooSource, load_bars
from trading.db import get_engine

URL = get_settings().database_url
START, SPLIT, END = dt.date(2010, 4, 13), dt.date(2020, 1, 1), dt.date(2026, 11, 12)
store = PostgresStore(YahooSource(), get_engine(URL)) if URL else CsvStore(YahooSource())
bars = load_bars(store, ["SPY", "UPRO", "SPXU", "VIX"], dt.date(2008, 1, 2), END)
rets = F.daily_returns(bars, START, END)
idx = rets.index

spy = bars["SPY"]
close = spy["close"]
c1 = close.shift(1)  # yesterday's close: known at today's open


def at_open(series):
    return series.reindex(idx).fillna(False).astype(bool).to_numpy()


def sma(n):
    return close.rolling(n).mean().shift(1)


def rsi(n):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return (100 - 100 / (1 + up / dn)).shift(1)


ema12, ema26 = close.ewm(span=12).mean(), close.ewm(span=26).mean()
macd = ema12 - ema26
macd_sig = macd.ewm(span=9).mean()
rvol20 = np.log(close).diff().rolling(20).std().shift(1) * np.sqrt(252)

# turn of month: last trading day of a month + first 3 of the next
dates = pd.Series(spy.index, index=spy.index)
month = pd.Series([d.month for d in spy.index], index=spy.index)
pos_in_month = dates.groupby(month.ne(month.shift()).cumsum()).cumcount()
is_last = month.ne(month.shift(-1))
tom = (pos_in_month < 3) | is_last

indicators = {
    "always long (baseline)": pd.Series(True, index=idx),
    # --- trend following
    "price > 200-day SMA": c1 > sma(200),
    "price > 200-day SMA, 2% band": None,  # filled below (hysteresis)
    "price > 100-day SMA": c1 > sma(100),
    "price > 50-day SMA": c1 > sma(50),
    "50-day SMA > 200-day (golden cross)": sma(50) > sma(200),
    "12-month momentum > 0": c1 > close.shift(253),
    "MACD > signal line": (macd > macd_sig).shift(1),
    # --- volatility regime
    "20-day realized vol < 20%": rvol20 < 0.20,
    # --- short-term mean reversion
    "yesterday was a down day": close.diff().shift(1) < 0,
    "RSI(2) < 30 (oversold)": rsi(2) < 30,
    "RSI(2) < 30 and price > 200-day SMA": (rsi(2) < 30) & (c1 > sma(200)),
    "VIX open < 20": bars["VIX"]["open"] < 20,
    "VIX open < 25": bars["VIX"]["open"] < 25,
    "20-day realized vol < 15%": rvol20 < 0.15,
    "20-day realized vol < 25%": rvol20 < 0.25,
    # --- calendar
    "turn of the month": tom,
}

# 200-SMA with a 2% band: enter above SMA*1.02, exit below SMA*0.98 (fewer whipsaws)
s200 = sma(200)
state, out = False, []
for d in close.index:
    p, m = c1.get(d), s200.get(d)
    if pd.notna(p) and pd.notna(m):
        if not state and p > m * 1.02:
            state = True
        elif state and p < m * 0.98:
            state = False
    out.append(state)
indicators["price > 200-day SMA, 2% band"] = pd.Series(out, index=close.index)

spy_hold = F.metrics(np.cumprod(1 + rets["index"].to_numpy()))


def evaluate(pos, mask):
    r = rets[mask]
    p = pos[mask]
    m = F.metrics(F.simulate(p, r, 5.0)).iloc[0]
    return {
        "acc": F.balanced_accuracy(p, r["index"]),
        "cagr": m["cagr"],
        "maxdd": m["max_drawdown"],
        "sharpe": m["sharpe"],
        "in_mkt": p.mean(),
        "switch/yr": (np.diff(p) != 0).sum() / (len(p) / 252),
    }


train = idx < SPLIT
test = ~train
rows = []
for name, sig in indicators.items():
    pos = at_open(sig).astype(np.int8)
    tr, te = evaluate(pos, train), evaluate(pos, test)
    rows.append(
        {
            "indicator (long UPRO when..., else cash)": name,
            "acc train": f"{tr['acc']:.1%}",
            "acc test": f"{te['acc']:.1%}",
            "CAGR train": f"{tr['cagr']:.1%}",
            "CAGR test": f"{te['cagr']:.1%}",
            "maxDD train": f"{tr['maxdd']:.0%}",
            "maxDD test": f"{te['maxdd']:.0%}",
            "Sharpe test": f"{te['sharpe']:.2f}",
            "in mkt": f"{(tr['in_mkt'] * train.sum() + te['in_mkt'] * test.sum()) / len(idx):.0%}",
            "switch/yr": f"{(tr['switch/yr'] + te['switch/yr']) / 2:.0f}",
        }
    )

for label, mask in (("train", train), ("test", test)):
    m = F.metrics(np.cumprod(1 + rets["index"][mask].to_numpy())).iloc[0]
    print(f"hold SPY {label}: CAGR {m['cagr']:.1%}, maxDD {m['max_drawdown']:.0%}, Sharpe {m['sharpe']:.2f}")
print()
with pd.option_context("display.width", 250, "display.max_colwidth", 45):
    print(pd.DataFrame(rows).to_string(index=False))
