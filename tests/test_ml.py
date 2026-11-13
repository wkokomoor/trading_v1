import datetime as dt

import numpy as np
import pandas as pd
import pytest
from conftest import FakeSource

from trading import ml
from trading.engine import run_backtest
from trading.strategy import MarketView, load_strategy

START, END = dt.date(2015, 1, 2), dt.date(2018, 12, 31)


@pytest.fixture(scope="module")
def bars():
    src = FakeSource()
    out = {s: src.fetch(s, START, END) for s in ("SPY", "VIX", "UPRO", "ADD", "TICK")}
    # Give ADD / TICK realistic ranges (FakeSource prices hover around 100).
    out["ADD"][["open", "high", "low", "close"]] -= 100
    out["TICK"][["open", "high", "low", "close"]] *= 5
    return out


@pytest.mark.parametrize("symbols", [("SPY", "VIX"), ("SPY", "VIX", "ADD", "TICK")])
def test_no_lookahead_open_snapshot_matches_full_history(bars, symbols):
    """A feature row computed from only what's known at the open must equal the row
    computed from the full dataset. Any use of day t's close/high/low would break this."""
    full = ml.build_features({s: bars[s] for s in symbols})
    for day in (bars["SPY"].index[300], bars["SPY"].index[500], bars["SPY"].index[-2]):
        view = MarketView({s: bars[s] for s in symbols}, day)
        snap = ml.features_at_open(view, symbols)
        pd.testing.assert_series_equal(snap, full.loc[day], check_names=False)


def test_internal_features_switch_on_with_data(bars):
    base = ml.build_features({"SPY": bars["SPY"], "VIX": bars["VIX"]})
    full = ml.build_features({s: bars[s] for s in ("SPY", "VIX", "ADD", "TICK")})
    added = set(full.columns) - set(base.columns)
    assert {"add_prev", "add_spy_divergence", "tick_high_prev", "tick_spy_divergence"} <= added


def test_labels_are_next_open_direction():
    days = pd.Index([dt.date(2025, 3, d) for d in (3, 4, 5, 6)])
    spy = pd.DataFrame({"open": [100.0, 101.0, 100.5, 100.5]}, index=days)
    y = ml.labels({"SPY": spy})
    assert y.iloc[:3].tolist() == [1.0, 0.0, 0.0]  # up, down, flat counts as not up
    assert np.isnan(y.iloc[-1])


def test_unreliable_vix_opens_are_masked():
    days = pd.Index(pd.bdate_range("2007-06-01", "2010-06-30").date)
    rng = np.random.default_rng(0)
    px = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, len(days))))
    frame = pd.DataFrame({"open": px, "high": px, "low": px, "close": px, "volume": 0}, index=days)
    f = ml.build_features({"SPY": frame, "VIX": frame / 5})
    in_bad = (f.index >= dt.date(2008, 1, 1)) & (f.index <= dt.date(2009, 12, 31))
    for col in ml.VIX_OPEN_FEATURES:
        assert f.loc[in_bad, col].isna().all()
    assert f.loc[f.index >= dt.date(2010, 6, 1), "vix_gap"].notna().all()


def test_training_rows_exclude_the_future_and_unlabeled(bars):
    X = ml.build_features({"SPY": bars["SPY"], "VIX": bars["VIX"]})
    y = ml.labels(bars)
    cut = dt.date(2017, 1, 1)
    Xt, yt = ml.training_rows(X, y, cut)
    assert Xt.index.max() < cut
    assert Xt.notna().all().all() and yt.notna().all()


def test_walk_forward_retrains_each_year_on_growing_history(bars):
    X = ml.build_features({"SPY": bars["SPY"], "VIX": bars["VIX"]})
    y = ml.labels(bars)
    proba, models = ml.walk_forward_proba(X, y, 2017, END, min_train_rows=200, n_estimators=20)
    assert [m[0] for m in models] == [2017, 2018]
    assert models[0][2] < models[1][2]
    assert proba.index.min() >= dt.date(2017, 1, 1)
    assert proba.between(0, 1).all()


def test_rf_strategy_trades_on_threshold_and_retrains_yearly(bars):
    RfDirection = load_strategy("rf_direction")
    s = RfDirection(threshold=0.5, min_train_rows=200, n_estimators=20, train_years=2)
    res = run_backtest(s, {k: bars[k] for k in ("SPY", "VIX", "UPRO")}, dt.date(2016, 6, 1), END)
    p = pd.to_numeric(res.daily["p_up"])
    assert p.notna().any()
    held = res.daily["holding"] == "UPRO"
    scored = p.notna()
    assert ((p[scored] >= 0.5) == held[scored]).mean() > 0.95  # holding follows the threshold
    assert s.model_year == END.year


def test_rf_strategy_waits_in_cash_until_enough_training_data(bars):
    RfDirection = load_strategy("rf_direction")
    s = RfDirection(min_train_rows=10_000, n_estimators=10)
    res = run_backtest(s, {k: bars[k] for k in ("SPY", "VIX", "UPRO")}, dt.date(2016, 6, 1), END)
    assert (res.daily["holding"] == "CASH").all()
