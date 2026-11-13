"""Run: uv run python research/rf_internals.py      (needs a Schwab login: `trading schwab auth`)

Does adding NYSE internals (Schwab daily $ADD and $TICK) give the random-forest direction
model skill that VIX/SPY features alone don't have (RESEARCH.md Part 7)?

Both models use identical days: internals exist from Aug 2012, so training starts there.
Retrained every January on all prior data; scored out of sample from FIRST_TEST_YEAR.
"""

import datetime as dt

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

from trading import feasibility as F
from trading import ml
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, SchwabSource, YahooSource, load_bars
from trading.db import get_engine
from trading.schwab import make_client

FIRST_TEST_YEAR, SPLIT, END = 2016, dt.date(2020, 1, 1), dt.date(2026, 11, 12)

URL = get_settings().database_url


def store(source):
    return PostgresStore(source, get_engine(URL)) if URL else CsvStore(source)


bars = load_bars(store(YahooSource()), ["SPY", "VIX", "UPRO", "SPXU"], dt.date(2011, 1, 3), END)
bars.update(
    load_bars(store(SchwabSource(make_client(interactive=False))), ["ADD", "TICK"], dt.date(2011, 1, 3), END)
)

full = ml.build_features({s: bars[s] for s in ("SPY", "VIX", "ADD", "TICK")})
internal_cols = [c for c in full.columns if c.startswith(("add_", "tick_"))]
full = full[full[internal_cols].notna().all(axis=1)]  # days where internals exist
base = full.drop(columns=internal_cols)
y = ml.labels(bars)
print(f"days with internals: {len(full)} ({full.index[0]} .. {full.index[-1]})")
print(f"internal features: {internal_cols}")

rng = np.random.default_rng(0)
results = {}
for name, X in (("VIX + SPY", base), ("VIX + SPY + ADD + TICK", full)):
    proba, models = ml.walk_forward_proba(X, y, FIRST_TEST_YEAR, END, min_train_rows=700)
    yt = y.reindex(proba.index)
    keep = yt.notna()
    results[name] = (proba[keep], yt[keep], models, X)
    print(
        f"{name}: training rows {models[0][2]} (for {models[0][0]}) .. {models[-1][2]} (for {models[-1][0]})"
    )

# Shuffled-label null for the full model
y_shuf = pd.Series(rng.permutation(y.to_numpy()), index=y.index).where(y.notna())
null_proba, _ = ml.walk_forward_proba(full, y_shuf, FIRST_TEST_YEAR, END, min_train_rows=700)

periods = {f"{FIRST_TEST_YEAR}-2019": lambda i: i < SPLIT, "2020-2026": lambda i: i >= SPLIT}


def boot_ci(yv, pv, n=500):
    idx = np.arange(len(yv))
    s = [roc_auc_score(yv[k], pv[k]) for k in (rng.choice(idx, len(idx)) for _ in range(n))]
    return np.percentile(s, [5, 95])


print("\n1) SKILL: out-of-sample AUC (0.50 = coin flip)")
rows = []
for name, (proba, yt, _, _) in results.items():
    row = {"model": name}
    for lab, f in periods.items():
        m = f(proba.index)
        lo, hi = boot_ci(yt[m].to_numpy(), proba[m].to_numpy())
        row[f"AUC {lab}"] = f"{roc_auc_score(yt[m], proba[m]):.3f} ({lo:.3f}-{hi:.3f})"
        called_up = (proba[m] >= proba[m].median()).astype(int).to_numpy()
        row[f"bal.acc@median {lab}"] = f"{F.balanced_accuracy(called_up, yt[m] * 2 - 1):.1%}"
    rows.append(row)
proba_full, yt_full = results["VIX + SPY + ADD + TICK"][:2]
nrow = {"model": "full model, shuffled labels"}
for lab, f in periods.items():
    m = f(proba_full.index)
    nrow[f"AUC {lab}"] = f"{roc_auc_score(yt_full[m], null_proba.reindex(proba_full.index)[m]):.3f}"
rows.append(nrow)
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).fillna("").to_string(index=False))

print("\n2) CALIBRATION of the internals model (P(up) quintiles, all out-of-sample days)")
q = pd.qcut(proba_full, 5)
print(
    pd.DataFrame({"P": proba_full, "up": yt_full})
    .groupby(q, observed=True)
    .agg(days=("up", "size"), avg_predicted=("P", "mean"), actual_up_rate=("up", "mean"))
    .round(3)
    .to_string()
)

print("\n3) THRESHOLDS: long UPRO when P(up) >= threshold, else cash (5 bps per fill, cash 0%)")
rets = F.daily_returns(bars, proba_full.index[0], END)
rets = rets[rets.index.isin(proba_full.index)]
rvol = np.log(bars["SPY"]["close"]).diff().rolling(20).std().shift(1).reindex(rets.index) * np.sqrt(252)
signals = {"always long UPRO": np.ones(len(rets), bool), "vol_filter (rvol < 20%)": (rvol < 0.20).to_numpy()}
for name, (proba, *_rest) in results.items():
    p = proba.reindex(rets.index)
    for t in (0.50, 0.53, 0.55, 0.57):
        signals[f"{name}: P >= {t:.2f}"] = (p >= t).to_numpy()
rows = []
for name, sig in signals.items():
    pos = sig.astype(np.int8)
    row = {"rule": name}
    for lab, f in periods.items():
        m = f(rets.index)
        met = F.metrics(F.simulate(pos[m], rets[m], 5.0)).iloc[0]
        row[f"in mkt {lab}"] = f"{pos[m].mean():.0%}"
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    rows.append(row)
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))

print("\n4) PERMUTATION IMPORTANCE in the internals model (mean out-of-sample AUC drop)")
_, _, models, X = results["VIX + SPY + ADD + TICK"]
imp = []
for year, model, _ in models:
    test = X[(X.index >= dt.date(year, 1, 1)) & (X.index <= dt.date(year, 12, 31))].dropna()
    ty = y.reindex(test.index)
    test, ty = test[ty.notna()], ty[ty.notna()]
    if len(test) < 50:
        continue
    r = permutation_importance(
        model, test.to_numpy(), ty.to_numpy(), scoring="roc_auc", n_repeats=5, random_state=0
    )
    imp.append(pd.Series(r.importances_mean, index=test.columns, name=year))
imp = pd.concat(imp, axis=1)
print(
    pd.DataFrame(
        {
            "mean AUC drop": imp.mean(axis=1),
            "years helping": (imp > 0).sum(axis=1).astype(str) + f"/{imp.shape[1]}",
        }
    )
    .sort_values("mean AUC drop", ascending=False)
    .round(4)
    .to_string()
)
