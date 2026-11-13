"""Features, labels and walk-forward training for machine-learning direction models.

Every feature for day t uses only information available at day t's open:
completed bars through t-1 plus day t's opening prices. `build_features` is the one
implementation used both for research (all days at once) and live/backtest
(`features_at_open`, which sees only a MarketView), and a test checks that the two agree.

Label: 1 if SPY's open-to-next-open return is positive (the strategy's holding period).
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

# Yahoo's ^VIX opens are mostly stale in 2008-2009 (often equal to the prior close;
# median |overnight gap| ~0.2% vs ~2% in other years), so open-based VIX features
# are set to NaN there and those rows are left out of training.
VIX_OPEN_UNRELIABLE = (dt.date(2008, 1, 1), dt.date(2009, 12, 31))

VIX_OPEN_FEATURES = ("vix_open", "vix_gap", "vix_vs_sma20", "vix_gap_divergence")


def build_features(bars: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """One row per SPY session. Needs SPY and VIX; uses ADD / TICK when present.

    For the current day only `open` needs to be known; closes, highs and lows of day t
    are never used for row t.
    """
    spy = bars["SPY"]
    idx = spy.index
    o, h, lo, c = (spy[k] for k in ("open", "high", "low", "close"))
    pc = c.shift(1)
    f = pd.DataFrame(index=idx)

    # --- SPY context (all from yesterday's close or earlier, plus today's open)
    spy_cc = np.log(c).diff()
    f["spy_gap"] = o / pc - 1
    f["spy_ret_1d"] = pc / c.shift(2) - 1
    f["spy_ret_5d"] = pc / c.shift(6) - 1
    f["spy_ret_20d"] = pc / c.shift(21) - 1
    f["spy_vs_sma200"] = pc / c.rolling(200).mean().shift(1) - 1
    f["spy_rvol20"] = spy_cc.rolling(20).std().shift(1) * np.sqrt(252)
    f["spy_range_1d"] = ((h - lo) / o).shift(1)

    # --- VIX: level, the morning spike or drop, recent trend, and divergence from SPY
    vix = bars["VIX"].reindex(idx)
    vo, vc = vix["open"], vix["close"]
    vix_cc = np.log(vc).diff()
    f["vix_open"] = vo
    f["vix_gap"] = vo / vc.shift(1) - 1
    f["vix_chg_1d"] = vc.shift(1) / vc.shift(2) - 1
    f["vix_chg_5d"] = vc.shift(1) / vc.shift(6) - 1
    f["vix_vs_sma20"] = vo / vc.rolling(20).mean().shift(1) - 1
    # Divergence: how much VIX moved beyond what SPY's move implies. VIX normally moves
    # about -4 to -6x SPY; beta is estimated from the prior 120 days of close-to-close moves.
    beta = (spy_cc.rolling(120).cov(vix_cc) / spy_cc.rolling(120).var()).shift(1)
    f["vix_beta"] = beta
    f["vix_gap_divergence"] = f["vix_gap"] - beta * f["spy_gap"]  # e.g. SPY flat but VIX rising
    f["vix_1d_divergence"] = f["vix_chg_1d"] - beta * f["spy_ret_1d"]

    bad = (idx >= VIX_OPEN_UNRELIABLE[0]) & (idx <= VIX_OPEN_UNRELIABLE[1])
    f.loc[bad, list(VIX_OPEN_FEATURES)] = np.nan

    # --- Market internals, when a data source provides them (daily bars; close = end-of-day value)
    if "ADD" in bars:  # NYSE advancers minus decliners
        add = bars["ADD"].reindex(idx)["close"]
        f["add_prev"] = add.shift(1)
        f["add_chg_1d"] = add.shift(1) - add.shift(2)
        f["add_5d_mean"] = add.rolling(5).mean().shift(1)
        # Rally without participation: SPY up over 5 days while breadth is negative.
        f["add_spy_divergence"] = np.sign(f["spy_ret_5d"]) * -np.sign(f["add_5d_mean"])
    if "TICK" in bars:  # NYSE TICK: daily high / low / close of the intraday index
        tick = bars["TICK"].reindex(idx)
        f["tick_close_prev"] = tick["close"].shift(1)
        f["tick_high_prev"] = tick["high"].shift(1)
        f["tick_low_prev"] = tick["low"].shift(1)
        # Exhaustion: SPY at a 5-day high yesterday while TICK's high was below its 5-day max.
        spy_new_high = h.shift(1) >= h.rolling(5).max().shift(1)
        tick_lagging = tick["high"].shift(1) < tick["high"].rolling(5).max().shift(1)
        f["tick_spy_divergence"] = (spy_new_high & tick_lagging).astype(float)
    return f


def labels(bars: dict[str, pd.DataFrame]) -> pd.Series:
    """1.0 if SPY's open-to-next-open return is positive, 0.0 if not, NaN for the last day."""
    o = bars["SPY"]["open"]
    nxt = o.shift(-1) / o - 1
    return (nxt > 0).astype(float).where(nxt.notna())


def features_at_open(view, symbols=("SPY", "VIX", "ADD", "TICK"), history: int = 260) -> pd.Series:
    """Today's feature row, computed from a MarketView: completed history plus today's opens."""
    snap = {}
    for s in symbols:
        if s not in view._bars:
            continue
        hist = view.history(s, history)
        today = pd.DataFrame(
            {"open": [view.open(s)], "high": [np.nan], "low": [np.nan], "close": [np.nan], "volume": [0]},
            index=pd.Index([view.date], name="date"),
        )
        snap[s] = pd.concat([hist, today])
    return build_features(snap).iloc[-1]


def make_model(n_estimators: int = 300, max_depth: int = 4, min_samples_leaf: int = 50, seed: int = 0):
    """A deliberately shallow, heavily regularized forest: daily returns are mostly noise."""
    from sklearn.ensemble import RandomForestClassifier

    return RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        max_features="sqrt",
        random_state=seed,
        n_jobs=-1,
    )


def training_rows(features: pd.DataFrame, y: pd.Series, before: dt.date):
    """Rows usable for training a model applied from `before` on: complete features, known label."""
    X = features[features.index < before]
    yy = y.reindex(X.index)
    ok = X.notna().all(axis=1) & yy.notna()
    return X[ok], yy[ok]


def walk_forward_proba(
    features: pd.DataFrame,
    y: pd.Series,
    first_test_year: int,
    last_test_date: dt.date,
    min_train_rows: int = 750,
    **model_params,
) -> tuple[pd.Series, list]:
    """Out-of-sample P(up) for every day from `first_test_year`: retrain each January on all
    prior data, then predict that year. Returns (probabilities, [(year, model, train_rows)])."""
    probs, models = [], []
    for year in range(first_test_year, last_test_date.year + 1):
        start = dt.date(year, 1, 1)
        X, yy = training_rows(features, y, start)
        if len(X) < min_train_rows:
            raise ValueError(f"only {len(X)} training rows before {start}")
        model = make_model(**model_params).fit(X.to_numpy(), yy.to_numpy())
        test = features[
            (features.index >= start) & (features.index <= min(last_test_date, dt.date(year, 12, 31)))
        ]
        test = test.dropna()
        if test.empty:
            continue
        probs.append(pd.Series(model.predict_proba(test.to_numpy())[:, 1], index=test.index))
        models.append((year, model, len(X)))
    return pd.concat(probs), models
