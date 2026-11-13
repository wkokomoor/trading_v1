"""Run: uv run python research/options_indicators.py

Options-market indicators for the daily UPRO / cash strategy:

  Gamma exposure (GEX): SqueezeMetrics' free daily estimate of S&P 500 options dealer
      gamma (https://squeezemetrics.com/monitor/dix), end-of-day values. The value for
      day t-1 is used at the open of day t.
  Expected move: the 1-day 1-sigma move implied by options,
      EM = SPY open * IV / 100 / sqrt(252), using Cboe VIX1D (1-day implied vol,
      2023+; the closest public proxy for a 0-DTE ATM straddle) or VIX9D (2011+).
      An ATM straddle costs about 0.8 * EM.

Tests the mechanism claims directly, then as long/cash indicators.
Train = May 2011 - 2019, test = 2020 - Nov 2026.
"""

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from trading import feasibility as F
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, YahooSource, load_bars
from trading.db import get_engine

START, SPLIT, END = dt.date(2011, 5, 3), dt.date(2020, 1, 1), dt.date(2026, 11, 12)
GEX_URL = "https://squeezemetrics.com/monitor/static/DIX.csv"
GEX_CACHE = Path(".cache/squeezemetrics/DIX.csv")


def load_gex() -> pd.Series:
    if not GEX_CACHE.exists() or GEX_CACHE.stat().st_mtime < dt.datetime.now().timestamp() - 86400:
        GEX_CACHE.parent.mkdir(parents=True, exist_ok=True)
        resp = requests.get(GEX_URL, headers={"User-Agent": "trading_v1 research script"}, timeout=30)
        resp.raise_for_status()
        GEX_CACHE.write_bytes(resp.content)
    df = pd.read_csv(GEX_CACHE, usecols=["date", "gex"])
    return pd.Series(df["gex"].to_numpy(), index=[dt.date.fromisoformat(d) for d in df["date"]])


URL = get_settings().database_url
store = PostgresStore(YahooSource(), get_engine(URL)) if URL else CsvStore(YahooSource())
bars = load_bars(store, ["SPY", "UPRO", "SPXU", "VIX9D", "VIX1D"], dt.date(2010, 1, 4), END)
spy = bars["SPY"]

d = pd.DataFrame(index=spy.index)
d["o"], d["h"], d["l"], d["c"] = spy["open"], spy["high"], spy["low"], spy["close"]
d["r_next"] = d["o"].shift(-1) / d["o"] - 1  # what a position opened today earns (to next open)
d["r_sess"] = d["c"] / d["o"] - 1  # today's open -> close
d["range"] = (d["h"] - d["l"]) / d["o"]  # today's intraday high-low range
d["r_prev_cc"] = d["c"].shift(1) / d["c"].shift(2) - 1  # yesterday's close-to-close move
d["gex_prev"] = load_gex().reindex(d.index).shift(1)  # yesterday's end-of-day GEX
d["iv9"] = bars["VIX9D"]["open"].reindex(d.index)
d["iv1"] = bars["VIX1D"]["open"].reindex(d.index)
d["rvol"] = np.log(d["c"]).diff().rolling(20).std().shift(1) * np.sqrt(252)
d = d[(d.index >= START) & (d.index <= END)].dropna(subset=["r_next", "gex_prev", "rvol"])
train = d.index < SPLIT
periods = {"train 2011-19": train, "test 2020-26": ~train}


def tstat(x):
    x = np.asarray(x)
    return x.mean() / (x.std(ddof=1) / np.sqrt(len(x))) if len(x) > 2 else np.nan


def regime_table(groups, df):
    rows = {}
    for name, g in df.groupby(groups, observed=True):
        rows[name] = {
            "days": len(g),
            "next-day mean (bps)": g["r_next"].mean() * 1e4,
            "t-stat": tstat(g["r_next"]),
            "next-day |move| (bps)": g["r_next"].abs().mean() * 1e4,
            "intraday range (bps)": g["range"].mean() * 1e4,
            "worst 5% day (bps)": g["r_next"].quantile(0.05) * 1e4,
            "reversal corr": g["r_prev_cc"].corr(g["r_sess"]),
        }
    return pd.DataFrame(rows).T.round(2)


print("=" * 100)
print("PART A: GAMMA EXPOSURE (yesterday's GEX, known at today's open)")
print("=" * 100)
print("reversal corr = correlation of yesterday's move with today's open->close move.")
print("Claim: positive GEX -> smaller moves, NEGATIVE reversal corr (mean reversion);")
print("       negative GEX -> bigger moves, POSITIVE corr (trend days), worse left tail.\n")
for label, mask in periods.items():
    sub = d[mask]
    print(f"--- {label}: GEX negative on {(sub['gex_prev'] < 0).mean():.1%} of days ---")
    sign = np.where(sub["gex_prev"] > 0, "GEX > 0", "GEX < 0")
    print(regime_table(sign, sub).to_string())
    q = pd.qcut(sub["gex_prev"], 5, labels=["Q1 lowest", "Q2", "Q3", "Q4", "Q5 highest"])
    print(regime_table(q, sub).to_string())
    print()

# Does GEX add anything beyond realized volatility? Compare within calm / volatile days.
print("--- GEX within realized-volatility regimes (full period) ---")
vol_reg = np.where(d["rvol"] < 0.20, "rvol<20%", "rvol>=20%")
both = pd.Series(
    [f"{v} & {'GEX>0' if g > 0 else 'GEX<0'}" for v, g in zip(vol_reg, d["gex_prev"], strict=True)]
)
both.index = d.index
print(regime_table(both, d).to_string())

print()
print("=" * 100)
print("PART B: EXPECTED MOVE (options-implied 1-day 1-sigma move)")
print("=" * 100)
for iv_col, label in (("iv9", "VIX9D, 2011-2026"), ("iv1", "VIX1D (0-DTE proxy), 2023-2026")):
    sub = d.dropna(subset=[iv_col]).copy()
    em = sub["o"] * sub[iv_col] / 100 / np.sqrt(252)
    sub["em_pct"] = em / sub["o"]
    in_sess = ((sub["c"] - sub["o"]).abs() <= em).mean()
    in_next = ((sub["r_next"] * sub["o"]).abs() <= em).mean()
    print(f"\n--- {label}: {len(sub)} days, average expected move {sub['em_pct'].mean():.2%} ---")
    print(f"days the open->close move stayed inside +/-EM:      {in_sess:.1%}  (a fair 1-sigma range: 68%)")
    print(f"days the open->next-open move stayed inside +/-EM:  {in_next:.1%}")
    ratio = ((sub["c"] - sub["o"]).abs() / em).median()
    print(f"realized |open->close| / EM, median:                {ratio:.2f}")

    # Breakouts: yesterday's session closed beyond yesterday's expected-move band.
    up = (sub["c"] - sub["o"]) > em
    dn = (sub["o"] - sub["c"]) > em
    sub["breakout"] = np.where(up, "closed ABOVE +EM", np.where(dn, "closed BELOW -EM", "inside band"))
    sub["breakout_prev"] = sub["breakout"].shift(1)
    sub = sub.dropna(subset=["breakout_prev"])
    print("what happened the day AFTER a breakout:")
    for lab, mask in (
        ("all", np.ones(len(sub), bool)),
        *((k, sub.index < SPLIT if k == "train" else sub.index >= SPLIT) for k in ("train", "test")),
    ):
        part = sub[mask]
        if part.empty or len(part["breakout_prev"].unique()) < 2:
            continue
        t = regime_table(part["breakout_prev"], part)[
            ["days", "next-day mean (bps)", "t-stat", "next-day |move| (bps)"]
        ]
        print(f"  [{lab}]")
        print("  " + t.to_string().replace("\n", "\n  "))

print()
print("=" * 100)
print("PART C: AS LONG-UPRO / CASH INDICATORS (5 bps per fill; cash earns 0% here)")
print("=" * 100)
rets = F.daily_returns(bars, START, END)
x = d.reindex(rets.index)
ok = x["gex_prev"].notna().to_numpy()
rets, x = rets[ok], x[ok]
signals = {
    "always long UPRO": np.ones(len(x), bool),
    "GEX > 0": (x["gex_prev"] > 0).to_numpy(),
    "GEX above its trailing 1-year median": (
        x["gex_prev"] > x["gex_prev"].rolling(252, min_periods=126).median()
    ).to_numpy(),
    "realized vol < 20% (current candidate)": (x["rvol"] < 0.20).to_numpy(),
    "realized vol < 20% AND GEX > 0": ((x["rvol"] < 0.20) & (x["gex_prev"] > 0)).to_numpy(),
    "realized vol < 20% OR GEX > 0": ((x["rvol"] < 0.20) | (x["gex_prev"] > 0)).to_numpy(),
}
rows = []
tr = rets.index < SPLIT
for name, sig in signals.items():
    pos = sig.astype(np.int8)
    row = {"long UPRO when..., else cash": name}
    for lab, m in (("train", tr), ("test", ~tr)):
        met = F.metrics(F.simulate(pos[m], rets[m], 5.0)).iloc[0]
        row[f"acc {lab}"] = f"{F.balanced_accuracy(pos[m], rets['index'][m]):.1%}"
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    row["in mkt"] = f"{pos.mean():.0%}"
    rows.append(row)
with pd.option_context("display.width", 250, "display.max_colwidth", 42):
    print(pd.DataFrame(rows).to_string(index=False))
