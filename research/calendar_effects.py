"""Run: uv run python research/calendar_effects.py

Track A: calendar and event effects, all known in advance.
  FOMC  scheduled announcement days (research/data/fomc_announcements.csv), the day before
        (pre-FOMC drift, Lucca & Moench 2015) and the day after
  pre-holiday, options expiration (3rd Friday) and its week, quarter-end, day of week

For each flag: SPY's open -> next-open return (the strategy's holding period) and the
close -> close return (comparable with the literature), split train 2010-19 / test 2020-26.
A flag counts only if t >= 2 with the same sign in BOTH periods (about a dozen flags are
tested, so single-period significance is expected by chance alone).
Then: does adding the surviving flags to vol_filter help?
"""

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from trading import feasibility as F
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, YahooSource, load_bars
from trading.db import get_engine

START, SPLIT, END = dt.date(2010, 4, 13), dt.date(2020, 1, 1), dt.date(2026, 11, 12)
FOMC_CSV = Path(__file__).parent / "data" / "fomc_announcements.csv"

URL = get_settings().database_url
store = PostgresStore(YahooSource(), get_engine(URL)) if URL else CsvStore(YahooSource())
bars = load_bars(store, ["SPY", "UPRO", "SPXU"], dt.date(2010, 1, 4), END)
spy = bars["SPY"]

d = pd.DataFrame(index=spy.index)
d["r_oo"] = spy["open"].shift(-1) / spy["open"] - 1  # open -> next open (what a position earns)
d["r_cc"] = spy["close"] / spy["close"].shift(1) - 1  # previous close -> close
d = d[(d.index >= START) & (d.index <= END)].dropna()
days = list(d.index)
pos = {day: i for i, day in enumerate(days)}

fomc = {dt.date.fromisoformat(x) for x in FOMC_CSV.read_text().split()[1:]}
flags = pd.DataFrame(False, index=d.index, columns=[])
flags["FOMC announcement day"] = [day in fomc for day in days]
flags["day BEFORE FOMC (pre-FOMC)"] = [i + 1 < len(days) and days[i + 1] in fomc for i in range(len(days))]
flags["day AFTER FOMC"] = [i > 0 and days[i - 1] in fomc for i in range(len(days))]

# pre-holiday: the next weekday is a market holiday (ordinary weekends don't count)
holidays = set(mcal.get_calendar("NYSE").holidays().holidays)
flags["pre-holiday"] = [
    np.datetime64((day + dt.timedelta(days=1 if day.weekday() < 4 else 3)), "D") in holidays for day in days
]


def third_friday(year, month):
    first = dt.date(year, month, 1)
    return first + dt.timedelta(days=(4 - first.weekday()) % 7 + 14)


opex = {third_friday(y, m) for y in range(2010, 2027) for m in range(1, 13)}
flags["options expiration Friday"] = [day in opex for day in days]
flags["options expiration week"] = [
    any(day <= f <= day + dt.timedelta(days=4 - day.weekday()) for f in opex if abs((f - day).days) < 7)
    for day in days
]
flags["last session of quarter"] = [
    day.month in (3, 6, 9, 12) and (i + 1 == len(days) or days[i + 1].month != day.month)
    for i, day in enumerate(days)
]
for k, name in enumerate(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]):
    flags[name] = [day.weekday() == k for day in days]

train = d.index < SPLIT
periods = {"2010-19": train, "2020-26": ~train}


def tstat_diff(a, b):
    """Welch t-statistic: mean of a minus mean of b."""
    return (a.mean() - b.mean()) / np.sqrt(a.var(ddof=1) / len(a) + b.var(ddof=1) / len(b))


rows = []
for name in flags.columns:
    row = {"flag": name}
    signs, strong = [], []
    for lab, m in periods.items():
        on = d["r_oo"][m & flags[name]]
        off = d["r_oo"][m & ~flags[name]]
        t = tstat_diff(on, off)
        row[f"days {lab}"] = len(on)
        row[f"open->open avg {lab}"] = f"{on.mean() * 1e4:+.1f} bps"
        row[f"vs other days t {lab}"] = f"{t:+.2f}"
        row[f"close->close avg {lab}"] = f"{d['r_cc'][m & flags[name]].mean() * 1e4:+.1f} bps"
        row[f"up share {lab}"] = f"{(on > 0).mean():.0%}"
        signs.append(np.sign(t))
        strong.append(abs(t) >= 2)
    row["passes (t>=2, same sign, both)"] = "YES" if all(strong) and signs[0] == signs[1] else ""
    rows.append(row)

base = {lab: d["r_oo"][m].mean() * 1e4 for lab, m in periods.items()}
print(f"all days: open->open average {base['2010-19']:+.1f} bps (2010-19), ", end="")
print(f"{base['2020-26']:+.1f} bps (2020-26)\n")
table = pd.DataFrame(rows)
cols = (
    ["flag"]
    + [c for lab in periods for c in table.columns if c.endswith(lab)]
    + ["passes (t>=2, same sign, both)"]
)
with pd.option_context("display.width", 300, "display.max_columns", 20):
    print(table[cols].to_string(index=False))

# ---------------------------------------------------------------- overlays on vol_filter
print("\nOVERLAYS: vol_filter (UPRO while SPY 20-day vol < 20%, else cash), forced LONG on flagged days")
rets = F.daily_returns(bars, START, END)
rets = rets[rets.index.isin(d.index)]
rvol = np.log(spy["close"]).diff().rolling(20).std().shift(1).reindex(rets.index) * np.sqrt(252)
vf = (rvol < 0.20).to_numpy()
fl = flags.reindex(rets.index)
variants = {
    "vol_filter (baseline)": vf,
    "+ long on FOMC day": vf | fl["FOMC announcement day"].to_numpy(),
    "+ long on pre-FOMC day": vf | fl["day BEFORE FOMC (pre-FOMC)"].to_numpy(),
    "+ long on pre-FOMC and FOMC days": vf
    | fl["FOMC announcement day"].to_numpy()
    | fl["day BEFORE FOMC (pre-FOMC)"].to_numpy(),
    "+ long on pre-holiday": vf | fl["pre-holiday"].to_numpy(),
}
rows = []
for name, sig in variants.items():
    p = sig.astype(np.int8)
    row = {"variant": name}
    for lab in periods:
        m = (rets.index < SPLIT) if lab == "2010-19" else (rets.index >= SPLIT)
        met = F.metrics(F.simulate(p[m], rets[m], 5.0)).iloc[0]
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    row["extra days long"] = int((p - vf.astype(np.int8)).sum())
    rows.append(row)
with pd.option_context("display.width", 300):
    print(pd.DataFrame(rows).to_string(index=False))
