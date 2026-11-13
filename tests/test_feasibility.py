import datetime as dt

import numpy as np
import pandas as pd
import pytest
from conftest import FakeSource

from trading import cli
from trading import feasibility as F
from trading.config import get_settings
from trading.engine import run_backtest
from trading.strategy import Strategy

START, END = dt.date(2023, 1, 3), dt.date(2024, 12, 31)


@pytest.fixture(scope="module")
def market():
    src = FakeSource()
    bars = {s: src.fetch(s, START, END) for s in ("SPY", "UPRO", "SPXU")}
    return bars, F.daily_returns(bars, START, END)


def test_daily_returns_are_open_to_next_open(market):
    bars, rets = market
    d0, d1 = rets.index[0], rets.index[1]
    expected = bars["UPRO"].at[d1, "open"] / bars["UPRO"].at[d0, "open"] - 1
    assert rets.at[d0, "long"] == pytest.approx(expected)
    assert rets.index[-1] < bars["SPY"].index[-1]  # last day has no "next open"


class Scripted(Strategy):
    """Follows a precomputed position list (test-only: lets the engine replay an oracle)."""

    name = "scripted"
    symbols = ("SPY", "UPRO", "SPXU")
    lookback = 0

    def __init__(self, plan):
        super().__init__()
        self.plan = plan

    def on_open(self, view):
        p = self.plan.get(view.date, 0)
        return {"UPRO": 1.0} if p == 1 else {"SPXU": 1.0} if p == -1 else {}


@pytest.mark.parametrize("slippage", [0.0, 5.0])
def test_vectorized_simulation_matches_the_backtest_engine(market, slippage):
    """The fast simulator must agree with the real engine on the same positions."""
    bars, rets = market
    rng = np.random.default_rng(1)
    pos = rng.choice([-1, 0, 1], size=len(rets)).astype(np.int8)
    fast = F.simulate(pos, rets, slippage)[-1]

    plan = dict(zip(rets.index, pos, strict=True))
    cash = 1e9  # whole-share rounding becomes negligible
    res = run_backtest(Scripted(plan), bars, rets.index[0], rets.index[-1], cash, slippage)
    # Engine marks at the close of the last day; the simulator ends at the next open.
    # Compare on equal footing: value the engine's final holdings at that next open.
    last, nxt = rets.index[-1], bars["SPY"].index[bars["SPY"].index.get_loc(rets.index[-1]) + 1]
    held = res.daily.at[last, "holding"]
    value = res.daily.at[last, "cash"]
    if held != "CASH":
        shares = (res.daily.at[last, "equity"] - value) / bars[held].at[last, "close"]
        value += shares * bars[held].at[nxt, "open"]
    assert value / cash == pytest.approx(fast, rel=1e-4)


def test_oracle_positions_modes_and_band():
    r = pd.Series([0.01, -0.01, 0.001, -0.001])
    assert list(F.oracle_positions(r, "long_short")) == [1, -1, 1, -1]
    assert list(F.oracle_positions(r, "long_cash")) == [1, 0, 1, 0]
    assert list(F.oracle_positions(r, "long_short", band=0.005)) == [1, -1, 0, 0]


def test_perfect_oracle_beats_everything():
    r = np.random.default_rng(3).normal(0.0004, 0.01, 1000)
    rets = pd.DataFrame({"index": r, "long": 3 * r, "short": -3 * r})  # idealized 3x ETFs
    oracle = F.metrics(F.simulate(F.oracle_positions(rets["index"]), rets, 5)).iloc[0]
    always_long = F.metrics(F.simulate(np.ones(len(rets), np.int8), rets, 5)).iloc[0]
    assert oracle["cagr"] > always_long["cagr"]
    assert oracle["max_drawdown"] > -0.2


def test_noisy_positions_hit_the_requested_accuracy():
    r = pd.Series(np.random.default_rng(0).normal(0, 0.01, 2000))
    pos = F.noisy_positions(r, 0.6, sims=200, mode="long_short")
    truth = np.where(r >= 0, 1, -1)
    assert (pos == truth).mean() == pytest.approx(0.6, abs=0.005)
    cash_mode = F.noisy_positions(r, 0.6, sims=5, mode="long_cash")
    assert set(np.unique(cash_mode)) <= {0, 1}


def test_balanced_accuracy_ignores_market_drift():
    r = pd.Series([0.01] * 70 + [-0.01] * 30)  # mostly up days
    assert F.balanced_accuracy(np.ones(100), r) == 0.5  # always long knows nothing
    assert F.balanced_accuracy(F.oracle_positions(r), r) == 1.0
    assert F.balanced_accuracy(F.oracle_positions(r, "long_cash"), r) == 1.0


def test_metrics_known_values():
    eq = np.array([1.1, 0.99, 1.21])
    m = F.metrics(eq).iloc[0]
    assert m["multiple"] == pytest.approx(1.21)
    assert m["max_drawdown"] == pytest.approx(-0.1)


def test_break_even_interpolates():
    curve = pd.DataFrame({"accuracy": [0.50, 0.52, 0.54], "cagr_median": [-0.1, 0.1, 0.3]})
    assert F.break_even(curve, 0.0) == pytest.approx(0.51)
    assert F.break_even(curve, 0.5) is None


def test_cli_feasibility_smoke(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr(cli, "YahooSource", FakeSource)
    try:
        cli.main(
            [
                "feasibility", "--start", "2023-02-01", "--end", "2024-12-31", "--sims", "20",
                "--measure", "spy_vix", "--store", "csv", "--cache-dir", str(tmp_path),
                "--plot", str(tmp_path / "f.png"),
            ]
        )  # fmt: skip
    finally:
        get_settings.cache_clear()
    out = capsys.readouterr().out
    for section in ("PERFECT FORESIGHT", "IMPERFECT FORESIGHT", "ACCURACY NEEDED", "MEASURED INDICATORS"):
        assert section in out
    assert (tmp_path / "f.png").exists()
