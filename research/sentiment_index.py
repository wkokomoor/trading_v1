"""Run: uv run python research/sentiment_index.py   (needs a Schwab login for $UVOL/$DVOL)

Our Fear & Greed index (trading/sentiment.py): chart, plus the hypotheses fixed before looking:
  H1  extreme fear (index <= 10) is followed by ABOVE-average SPY returns over 1/5/20/60 days
  H2  extreme greed (index >= 90) is followed by BELOW-average returns
  H3  capitulation re-entry for vol_filter: when vol_filter is in cash, the index is <= 10 and
      20-day realized vol is lower than 5 sessions earlier (the storm is easing), hold
      A) UPRO or B) SPY instead of cash
Signals use the index from the previous close, acted on at the open. Train = Nov 2006-2019,
test = 2020-Nov 2026. Bar: |Newey-West t| >= 2 with the same sign in both periods.
"""

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

from trading import feasibility as F
from trading import sentiment as S
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, SchwabSource, YahooSource, last_complete_session, load_bars
from trading.db import get_engine
from trading.schwab import make_client

SPLIT = dt.date(2020, 1, 1)
END = last_complete_session()
URL = get_settings().database_url


def store(source):
    return PostgresStore(source, get_engine(URL)) if URL else CsvStore(source)


bars = load_bars(
    store(YahooSource()), ["SPY", "VIX", "TLT", "HYG", "IEI", "UPRO", "SPXU"], dt.date(2006, 1, 3), END
)
bars.update(
    load_bars(store(SchwabSource(make_client(interactive=False))), ["UVOL", "DVOL"], dt.date(2006, 1, 3), END)
)
fg = S.index(bars)
signal = fg["fear_greed"].shift(1)  # known at today's open
latest = fg["fear_greed"].iloc[-1]
print(f"index from {fg['fear_greed'].first_valid_index()} to {fg.index[-1]}", end="; ")
print(f"latest {latest:.1f} ({S.label(latest)})")

# ---------------------------------------------------------------- chart
import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

INK, INK_2, GRID, SURFACE, BLUE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb", "#2a78d6"
x = pd.to_datetime(fg.index)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 6.5), dpi=150, sharex=True, height_ratios=[1.2, 1])
fig.patch.set_facecolor(SURFACE)
for ax in (ax1, ax2):
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, lw=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c3c2b7")
    ax.tick_params(colors=INK_2, labelsize=9)
ax1.plot(x, bars["SPY"]["close"].reindex(fg.index), color=INK, lw=1.2)
ax1.set_yscale("log")
ax1.set_yticks([50, 100, 200, 400, 700])
ax1.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"${v:,.0f}"))
ax1.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
ax1.set_title("SPY (log scale)", loc="left", color=INK, fontsize=11)
ax2.axhspan(0, 25, color="#e34948", alpha=0.10, lw=0)
ax2.axhspan(75, 100, color="#1baf7a", alpha=0.12, lw=0)
ax2.plot(x, fg["fear_greed"].rolling(5).mean(), color=BLUE, lw=1.2)
ax2.set_ylim(0, 100)
ax2.set_title("Fear & Greed index (5-day average)", loc="left", color=INK, fontsize=11)
ax2.text(x[5], 6, "Extreme fear (< 25)", color=INK_2, fontsize=8.5)
ax2.text(x[5], 88, "Extreme greed (> 75)", color=INK_2, fontsize=8.5)
fig.tight_layout()
out = Path("docs/img/fear_greed.png")
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, facecolor=SURFACE)
print(f"chart: {out}")


# ---------------------------------------------------------------- H1 / H2
def newey_west_t(y: np.ndarray, d: np.ndarray, lags: int) -> tuple[float, float]:
    """OLS y = a + b*d; returns (b, Newey-West t-stat of b) with `lags` lags."""
    X = np.column_stack([np.ones_like(d), d])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    u = y - X @ beta
    XtX_inv = np.linalg.inv(X.T @ X)
    Xu = X * u[:, None]
    S_ = Xu.T @ Xu
    for lag in range(1, lags + 1):
        w = 1 - lag / (lags + 1)
        G = Xu[lag:].T @ Xu[:-lag]
        S_ += w * (G + G.T)
    cov = XtX_inv @ S_ @ XtX_inv
    return beta[1], beta[1] / np.sqrt(cov[1, 1])


opens = bars["SPY"]["open"]
rows = []
for h in (1, 5, 20, 60):
    fwd = (opens.shift(-h) / opens - 1).reindex(fg.index)
    df = pd.DataFrame({"fwd": fwd, "sig": signal}).dropna()
    for name, mask_fn in (
        ("H1 extreme fear (<=10)", lambda s: s <= 10),
        ("H2 extreme greed (>=90)", lambda s: s >= 90),
    ):
        row = {"hypothesis": name, "horizon (days)": h}
        ok = []
        for lab, m in (("2006-19", df.index < SPLIT), ("2020-26", df.index >= SPLIT)):
            part = df[m]
            d = mask_fn(part["sig"]).astype(float).to_numpy()
            n = int(d.sum())
            if n < 5:
                row[f"days {lab}"], row[f"extra return {lab}"], row[f"NW t {lab}"] = n, "-", "-"
                ok.append((0, 0))
                continue
            b, t = newey_west_t(part["fwd"].to_numpy(), d, lags=max(h - 1, 1))
            row[f"days {lab}"] = n
            row[f"extra return {lab}"] = f"{b * 1e4:+.0f} bps"
            row[f"NW t {lab}"] = f"{t:+.2f}"
            ok.append((t, np.sign(b)))
        expected = 1 if name.startswith("H1") else -1
        row["passes"] = "YES" if all(abs(t) >= 2 and s == expected for t, s in ok) else ""
        rows.append(row)
print("\nH1/H2: SPY return from today's open over the next h days, vs. all other days")
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))

# ---------------------------------------------------------------- H3
rets = F.daily_returns(
    bars, fg["fear_greed"].first_valid_index(), END, index="SPY", long="UPRO", short="SPXU"
)
r_upro, r_spy = rets["long"].to_numpy(), rets["index"].to_numpy()
rvol = (
    (np.log(bars["SPY"]["close"]).diff().rolling(20).std(ddof=1) * np.sqrt(252)).shift(1).reindex(rets.index)
)
sig = signal.reindex(rets.index)
base = (rvol < 0.20).to_numpy()
easing = (rvol < rvol.shift(5)).to_numpy()


def simulate(holding: np.ndarray, m: np.ndarray, slip_bps: float = 5.0) -> np.ndarray:
    """holding: 'UPRO' / 'SPY' / '' per day; open-to-open returns over the days in mask `m`,
    with slippage on every change of holding."""
    holding = holding[m]
    r = np.where(holding == "UPRO", r_upro[m], np.where(holding == "SPY", r_spy[m], 0.0))
    prev = np.concatenate([[""], holding[:-1]])
    legs = (prev != holding) * ((prev != "").astype(int) + (holding != "").astype(int))
    return np.cumprod((1 + r) * (1 - slip_bps / 1e4) ** legs)


variants = {"vol_filter (baseline)": np.where(base, "UPRO", "")}
for thr in (10, 25):
    reentry = ~base & (sig <= thr).to_numpy() & easing
    tag = "" if thr == 10 else "  [sensitivity]"
    variants[f"H3A re-enter UPRO (index <= {thr}){tag}"] = np.where(
        base, "UPRO", np.where(reentry, "UPRO", "")
    )
    variants[f"H3B re-enter SPY (index <= {thr}){tag}"] = np.where(base, "UPRO", np.where(reentry, "SPY", ""))
rows = []
for name, holding in variants.items():
    row = {"variant": name}
    for lab, m in (("2006-19", rets.index < SPLIT), ("2020-26", rets.index >= SPLIT)):
        met = F.metrics(simulate(holding, np.asarray(m))).iloc[0]
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    row["re-entry days"] = int(((holding != "") & ~base).sum())
    rows.append(row)
print("\nH3: vol_filter with capitulation re-entry (5 bps per trade leg; cash earns 0% here)")
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))
