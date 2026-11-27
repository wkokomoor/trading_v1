"""Run: uv run python research/rf_sentiment.py   (needs a Schwab login for $UVOL/$DVOL)

Does our Fear & Greed index (trading/sentiment.py, RESEARCH.md Part 9) give the random-forest
direction model (trading/ml.py, Part 7) skill it doesn't have with VIX/SPY features alone?

Added features, all from the PREVIOUS close (known at the open): the composite score, its five
component scores, and the composite's 5-session change. Both models use identical days and are
retrained every January on all prior data. Bar: out-of-sample AUC clearly above 0.52 in BOTH
periods, and above the model without sentiment.
"""

import datetime as dt

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

from trading import feasibility as F
from trading import ml
from trading import sentiment as S
from trading.config import get_settings
from trading.data import CsvStore, PostgresStore, SchwabSource, YahooSource, last_complete_session, load_bars
from trading.db import get_engine
from trading.schwab import make_client

FIRST_TEST_YEAR, SPLIT, END = 2013, dt.date(2020, 1, 1), last_complete_session()
URL = get_settings().database_url


def store(source):
    return PostgresStore(source, get_engine(URL)) if URL else CsvStore(source)


bars = load_bars(
    store(YahooSource()), ["SPY", "VIX", "TLT", "HYG", "IEI", "UPRO", "SPXU"], dt.date(2005, 1, 3), END
)
bars.update(
    load_bars(store(SchwabSource(make_client(interactive=False))), ["UVOL", "DVOL"], dt.date(2005, 1, 3), END)
)

base_all = ml.build_features({"SPY": bars["SPY"], "VIX": bars["VIX"]})
fg = S.index(bars)
senti = fg[["fear_greed", *S.COMPONENTS]].add_prefix("fg_")
senti["fg_change_5d"] = fg["fear_greed"] - fg["fear_greed"].shift(5)
senti = senti.shift(1)  # yesterday's close -> usable at today's open

full = base_all.join(senti)
full = full[full.notna().all(axis=1)]  # identical rows for both models
base = full[base_all.columns]
y = ml.labels(bars)
print(f"rows with all features: {len(full)} ({full.index[0]} .. {full.index[-1]})")
print(f"added: {list(senti.columns)}")

rng = np.random.default_rng(0)
results = {}
for name, X in (("VIX + SPY", base), ("VIX + SPY + sentiment", full)):
    proba, models = ml.walk_forward_proba(X, y, FIRST_TEST_YEAR, END, min_train_rows=700)
    yt = y.reindex(proba.index)
    keep = yt.notna()
    results[name] = (proba[keep], yt[keep], models, X)
    print(
        f"{name}: training rows {models[0][2]} (for {models[0][0]}) .. {models[-1][2]} (for {models[-1][0]})"
    )

y_shuf = pd.Series(rng.permutation(y.to_numpy()), index=y.index).where(y.notna())
null_proba, _ = ml.walk_forward_proba(full, y_shuf, FIRST_TEST_YEAR, END, min_train_rows=700)
periods = {f"{FIRST_TEST_YEAR}-2019": lambda i: i < SPLIT, f"2020-{END.year}": lambda i: i >= SPLIT}


def boot_ci(yv, pv, n=500):
    idx = np.arange(len(yv))
    s = [roc_auc_score(yv[k], pv[k]) for k in (rng.choice(idx, len(idx)) for _ in range(n))]
    return np.percentile(s, [5, 95])


print("\n1) SKILL: out-of-sample AUC (0.50 = coin flip), 90% bootstrap interval")
rows = []
for name, (proba, yt, _, _) in results.items():
    row = {"model": name}
    for lab, f in periods.items():
        m = f(proba.index)
        lo, hi = boot_ci(yt[m].to_numpy(), proba[m].to_numpy())
        row[f"AUC {lab}"] = f"{roc_auc_score(yt[m], proba[m]):.3f} ({lo:.3f}-{hi:.3f})"
    rows.append(row)
p_full, y_full = results["VIX + SPY + sentiment"][:2]
nrow = {"model": "with sentiment, shuffled labels"}
for lab, f in periods.items():
    m = f(p_full.index)
    nrow[f"AUC {lab}"] = f"{roc_auc_score(y_full[m], null_proba.reindex(p_full.index)[m]):.3f}"
rows.append(nrow)
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))

print("\n2) CALIBRATION with sentiment (P(up) quintiles, all out-of-sample days)")
q = pd.qcut(p_full, 5)
print(
    pd.DataFrame({"P": p_full, "up": y_full})
    .groupby(q, observed=True)
    .agg(days=("up", "size"), avg_predicted=("P", "mean"), actual_up_rate=("up", "mean"))
    .round(3)
    .to_string()
)

print("\n3) THRESHOLDS: long UPRO when P(up) >= threshold, else cash (5 bps per fill, cash 0%)")
rets = F.daily_returns(bars, p_full.index[0], END)
rets = rets[rets.index.isin(p_full.index)]
rvol = np.log(bars["SPY"]["close"]).diff().rolling(20).std().shift(1).reindex(rets.index) * np.sqrt(252)
signals = {"always long UPRO": np.ones(len(rets), bool), "vol_filter (rvol < 20%)": (rvol < 0.20).to_numpy()}
for name, (proba, *_rest) in results.items():
    p = proba.reindex(rets.index)
    for t in (0.50, 0.53, 0.55):
        signals[f"{name}: P >= {t:.2f}"] = (p >= t).to_numpy()
rows = []
for name, sig in signals.items():
    pos = sig.astype(np.int8)
    row = {"rule": name}
    for lab, f in periods.items():
        m = f(rets.index)
        met = F.metrics(F.simulate(pos[m], rets[m], 5.0)).iloc[0]
        row[f"CAGR {lab}"] = f"{met['cagr']:.1%}"
        row[f"maxDD {lab}"] = f"{met['max_drawdown']:.0%}"
        row[f"Sharpe {lab}"] = f"{met['sharpe']:.2f}"
    rows.append(row)
with pd.option_context("display.width", 250):
    print(pd.DataFrame(rows).to_string(index=False))

print("\n4) PERMUTATION IMPORTANCE with sentiment (mean out-of-sample AUC drop; top 10)")
_, _, models, X = results["VIX + SPY + sentiment"]
imp = []
for year, model, _ in models:
    test = X[(X.index >= dt.date(year, 1, 1)) & (X.index <= dt.date(year, 12, 31))]
    ty = y.reindex(test.index)
    test, ty = test[ty.notna()], ty[ty.notna()]
    if len(test) < 50:
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
summary["sentiment?"] = ["yes" if c.startswith("fg_") else "" for c in summary.index]
print(summary.head(10).round(4).to_string())
