import datetime as dt

import numpy as np
import pandas as pd
import pytest
from conftest import FakeSource

from trading import sentiment as S

START, END = dt.date(2017, 1, 3), dt.date(2019, 12, 31)


@pytest.fixture(scope="module")
def bars():
    src = FakeSource()
    out = {s: src.fetch(s, START, END) for s in S.SYMBOLS}
    rng = np.random.default_rng(1)
    for s in ("UVOL", "DVOL"):
        out[s]["close"] = rng.uniform(1e5, 1e6, len(out[s]))
    return out


def test_index_is_between_0_and_100_and_needs_three_components(bars):
    fg = S.index(bars)
    valid = fg["fear_greed"].dropna()
    assert len(valid) > 300
    assert valid.between(0, 100).all()
    assert (fg.loc[valid.index, "n_components"] >= S.MIN_COMPONENTS).all()


def test_no_lookahead_truncated_history_gives_the_same_values(bars):
    full = S.index(bars)
    for cut in (full.index[400], full.index[600]):
        cut_bars = {s: df[df.index <= cut] for s, df in bars.items()}
        pd.testing.assert_series_equal(S.index(cut_bars).loc[cut], full.loc[cut], check_names=False)


def test_works_without_breadth_data(bars):
    no_breadth = {s: df for s, df in bars.items() if s not in ("UVOL", "DVOL")}
    fg = S.index(no_breadth)
    assert fg["breadth"].isna().all()
    assert fg["fear_greed"].notna().sum() > 300  # 4 of 5 components is enough


def test_bad_volume_prints_are_masked():
    days = pd.date_range("2024-01-01", periods=40).date
    up = pd.Series(500_000.0, index=days)
    down = pd.Series(500_000.0, index=days)
    up.iloc[30], down.iloc[30] = 64_300_268.0, 13_841_219.0  # a real Schwab bad print (~60x normal)
    up.iloc[35], down.iloc[35] = 10.0, 5.0  # near-zero day
    share = S.clean_volume(up, down)
    assert np.isnan(share.iloc[30]) and np.isnan(share.iloc[35])
    assert share.iloc[25] == 0.5


def test_extreme_selloff_reads_as_extreme_fear(bars):
    crash = {s: df.copy() for s, df in bars.items()}
    tail = crash["SPY"].index[-15:]
    crash["SPY"].loc[tail, "close"] *= np.linspace(0.97, 0.75, 15)  # a 25% slide
    crash["VIX"].loc[tail, "close"] *= np.linspace(1.5, 3.0, 15)
    crash["HYG"].loc[tail, "close"] *= np.linspace(0.98, 0.85, 15)
    crash["TLT"].loc[tail, "close"] *= np.linspace(1.01, 1.10, 15)
    crash["UVOL"].loc[tail, "close"] = 1e5  # selling on broad volume: most volume in decliners
    crash["DVOL"].loc[tail, "close"] = 9e5
    fg = S.index(crash).iloc[-1]
    assert fg["fear_greed"] < 10
    assert all(fg[c] < 25 for c in S.COMPONENTS)  # every component agrees


@pytest.mark.parametrize(
    "score, name", [(5, "Extreme Fear"), (30, "Fear"), (50, "Neutral"), (60, "Greed"), (90, "Extreme Greed")]
)
def test_labels(score, name):
    assert S.label(score) == name
