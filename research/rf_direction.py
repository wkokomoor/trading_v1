"""Run: uv run python research/rf_direction.py

Evaluate the random-forest direction model (trading/ml.py) out of sample:
retrained every January on all prior data (from 1996), scored 2010 -> today.

  1. Skill:        AUC and balanced accuracy, vs. the same model trained on shuffled labels
  2. Calibration:  when the model says P(up) = x, how often was SPY actually up?
  3. Thresholds:   long UPRO when P(up) >= threshold, else cash (5 bps per fill, cash 0%)
  4. Importance:   permutation importance on each out-of-sample year (AUC drop)
"""

import datetime as dt

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

from trading import feasibility as F
from trading import ml
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, YahooSource, load_bars
from trading.db import get_engine

DATA_START, FIRST_TEST_YEAR, SPLIT, END = (
    dt.date(1995, 1, 3),
    2010,
    dt.date(2020, 1, 1),
    dt.date(2026, 11, 12),
)

URL = get_settings().database_url
store = PostgresStore(YahooSource(), get_engine(URL)) if URL else CsvStore(YahooSource())
bars = load_bars(store, ["SPY", "VIX", "UPRO", "SPXU"], DATA_START, END)

X = ml.build_features({"SPY": bars["SPY"], "VIX": bars["VIX"]})
y = ml.labels(bars)
print(f"features: {list(X.columns)}")

proba, models = ml.walk_forward_proba(X, y, FIRST_TEST_YEAR, END)
yt = y.reindex(proba.index)
ok = yt.notna()
proba, yt = proba[ok], yt[ok]
print(f"out-of-sample predictions: {len(proba)} days, {proba.index[0]} .. {proba.index[-1]}")
print(
    f"training rows per model: {models[0][2]} (for {models[0][0]}) .. {models[-1][2]} (for {models[-1][0]})"
)

# Null model: identical pipeline, labels shuffled within each training set.
rng = np.random.default_rng(0)
y_shuffled = pd.Series(rng.permutation(y.to_numpy()), index=y.index).where(y.notna())
null_proba, _ = ml.walk_forward_proba(X, y_shuffled, FIRST_TEST_YEAR, END)
null_proba = null_proba.reindex(proba.index)

periods = {"2010-2019": proba.index < SPLIT, "2020-2026": proba.index >= SPLIT}


def boot_auc(yv, pv, n=500):
    idx = np.arange(len(yv))
    aucs = [roc_auc_score(yv[s], pv[s]) for s in (rng.choice(idx, len(idx)) for _ in range(n))]
    return np.percentile(aucs, [5, 95])


print("\n1) SKILL (AUC 0.50 = no skill; balanced accuracy at P(up) >= 0.5 and at the period's median P)")
rows = []
for lab, m in periods.items():
    yv, pv, nv = yt[m].to_numpy(), proba[m].to_numpy(), null_proba[m].to_numpy()
    lo, hi = boot_auc(yv, pv)
    med = np.median(pv)
    rows.append(
        {
            "period": lab,
            "days": len(yv),
            "up-day share": f"{yv.mean():.1%}",
            "AUC": f"{roc_auc_score(yv, pv):.3f}",
            "AUC 90% CI": f"{lo:.3f}-{hi:.3f}",
            "AUC shuffled-label model": f"{roc_auc_score(yv, nv):.3f}",
            "bal.acc @0.5": f"{F.balanced_accuracy((pv >= 0.5).astype(int), pd.Series(yv * 2 - 1)):.1%}",
            "bal.acc @median": f"{F.balanced_accuracy((pv >= med).astype(int), pd.Series(yv * 2 - 1)):.1%}",
            "share of days P>=0.5": f"{(pv >= 0.5).mean():.0%}",
        }
    )
print(pd.DataFrame(rows).to_string(index=False))

print("\n2) CALIBRATION (all out-of-sample days, P(up) in quintiles)")
q = pd.qcut(proba, 5)
cal = (
    pd.DataFrame({"P": proba, "up": yt})
    .groupby(q, observed=True)
    .agg(days=("up", "size"), avg_predicted=("P", "mean"), actual_up_rate=("up", "mean"))
)
print(cal.round(3).to_string())

print("\n3) THRESHOLDS: long UPRO when P(up) >= threshold, else cash")
rets = F.daily_returns(bars, proba.index[0], END)
pr = proba.reindex(rets.index)
keep = pr.notna().to_numpy()
rets, pr = rets[keep], pr[keep]
rvol = np.log(bars["SPY"]["close"]).diff().rolling(20).std().shift(1).reindex(rets.index) * np.sqrt(252)
signals = {"always long UPRO": np.ones(len(rets), bool), "vol_filter (rvol < 20%)": (rvol < 0.20).to_numpy()}
for t in (0.50, 0.52, 0.54, 0.55, 0.56, 0.58, 0.60):
    signals[f"P(up) >= {t:.2f}"] = (pr >= t).to_numpy()
rows = []
for name, sig in signals.items():
    pos = sig.astype(np.int8)
    row = {"rule": name}
    for lab in periods:
        m = (rets.index < SPLIT) if lab == "2010-2019" else (rets.index >= SPLIT)
        met = F.metrics(F.simulate(pos[m], rets[m], 5.0)).iloc[0]
        row[f"in mkt {lab}"] = f"{pos[m].mean():.0%}"
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    rows.append(row)
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))

print("\n4) PERMUTATION IMPORTANCE (mean AUC drop when a feature is shuffled, out-of-sample years)")
imp = []
for year, model, _ in models:
    test = X[(X.index >= dt.date(year, 1, 1)) & (X.index <= dt.date(year, 12, 31))].dropna()
    ty = y.reindex(test.index)
    test, ty = test[ty.notna()], ty[ty.notna()]
    if len(test) < 50 or ty.nunique() < 2:
        continue
    r = permutation_importance(
        model, test.to_numpy(), ty.to_numpy(), scoring="roc_auc", n_repeats=5, random_state=0
    )
    imp.append(pd.Series(r.importances_mean, index=test.columns, name=year))
imp = pd.concat(imp, axis=1)
summary = pd.DataFrame(
    {
        "mean AUC drop": imp.mean(axis=1),
        "years helping": (imp > 0).sum(axis=1).astype(str) + f"/{imp.shape[1]}",
    }
).sort_values("mean AUC drop", ascending=False)
print(summary.round(4).to_string())
