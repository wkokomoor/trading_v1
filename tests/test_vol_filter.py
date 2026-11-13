import datetime as dt

import numpy as np
import pandas as pd
import pytest
from conftest import FakeSource, make_bars

from trading import cli, research
from trading.config import get_settings
from trading.data import sessions
from trading.engine import run_backtest
from trading.strategy import MarketView, Strategy, load_strategy

VolFilter = load_strategy("vol_filter")
DAYS = sessions(dt.date(2025, 1, 2), dt.date(2025, 3, 31))


def spy_with_daily_moves(move: float, trend: float = 0.0):
    """Closes alternating +move / -move (realized vol ~ move*sqrt(252)), plus a drift."""
    closes = [100.0]
    for i in range(1, len(DAYS)):
        closes.append(closes[-1] * (1 + (move if i % 2 else -move) + trend))
    return make_bars(DAYS, closes, closes)


def view(spy):
    return MarketView({"SPY": spy, "UPRO": make_bars(DAYS, [50.0] * len(DAYS))}, DAYS[-1])


def test_calm_market_holds_upro():
    calm = spy_with_daily_moves(0.003)  # ~5% annualized
    s = VolFilter()
    assert s.on_open(view(calm)) == {"UPRO": 1.0}
    # Daily log returns alternate about +/-0.3%, so annualized vol is about 0.003 * sqrt(252).
    assert s.info["vol"] == pytest.approx(0.003 * np.sqrt(252), rel=0.05)


def test_volatile_market_goes_to_cash_or_risk_off_asset():
    wild = spy_with_daily_moves(0.02)  # ~32% annualized
    assert VolFilter().on_open(view(wild)) == {}
    assert VolFilter(risk_off="SPY").on_open(view(wild)) == {"SPY": 1.0}


def test_trend_filter_blocks_calm_downtrend():
    calm_falling = spy_with_daily_moves(0.002, trend=-0.002)
    assert VolFilter(trend_window=20).on_open(view(calm_falling)) == {}
    assert VolFilter(trend_window=0).on_open(view(calm_falling)) == {"UPRO": 1.0}


def test_lookback_covers_window_and_trend():
    assert VolFilter(window=20).required_lookback() == 21
    assert VolFilter(window=20, trend_window=200).required_lookback() == 200


# ---------------------------------------------------------------- cash interest


class AllCash(Strategy):
    name, symbols, lookback = "all_cash", ("SPY",), 0

    def on_open(self, view):
        return {}


def test_constant_cash_rate_compounds_daily():
    bars = {"SPY": make_bars(DAYS, [100.0] * len(DAYS))}
    res = run_backtest(AllCash(), bars, DAYS[0], DAYS[-1], cash=10_000, cash_yield=0.0504)
    assert res.daily["equity"].iloc[-1] == pytest.approx(10_000 * (1 + 0.0504 / 252) ** len(DAYS))


def test_cash_rate_series_uses_latest_known_rate():
    bars = {"SPY": make_bars(DAYS, [100.0] * len(DAYS))}
    rates = pd.Series({DAYS[0]: 0.0, DAYS[10]: 0.0504})  # rate changes on day 10, then carries forward
    res = run_backtest(AllCash(), bars, DAYS[0], DAYS[-1], cash=10_000, cash_yield=rates)
    eq = res.daily["equity"]
    assert eq.iloc[9] == pytest.approx(10_000)
    assert eq.iloc[-1] == pytest.approx(10_000 * (1 + 0.0504 / 252) ** (len(DAYS) - 10))


def test_no_interest_while_fully_invested(march_2025):
    bars = {"SPY": make_bars(march_2025, [10.0] * 10)}
    res = run_backtest(load_strategy("buy_and_hold")(), bars, march_2025[0], march_2025[-1], 100, 0, 0.05)
    assert res.daily["equity"].iloc[-1] == pytest.approx(100.0)  # 10 shares @ 10, zero cash left


# ---------------------------------------------------------------- walk-forward


def test_walk_forward_picks_by_past_only_and_stitches_years():
    src = FakeSource()
    bars = {s: src.fetch(s, dt.date(2019, 1, 2), dt.date(2023, 12, 29)) for s in ("SPY", "UPRO")}
    grid = research.expand_grid(VolFilter, ["threshold=0.05,0.5"])
    years, oos = research.walk_forward(
        VolFilter, {}, grid, bars, dt.date(2019, 3, 1), dt.date(2023, 12, 29), 2021
    )
    assert list(years["year"]) == [2021, 2022, 2023]
    assert oos.index[0].year == 2021 and oos.index[-1] == dt.date(2023, 12, 29)
    assert oos.iloc[-1] == pytest.approx((1 + years["year_return"]).prod())

    # The 2022 choice must equal the best combo on data through 2021 only.
    from trading.engine import performance

    best, best_score = None, -np.inf
    for combo in grid:
        eq = run_backtest(VolFilter(**combo), bars, dt.date(2019, 3, 1), dt.date(2023, 12, 29)).daily[
            "equity"
        ]
        score = performance(eq[eq.index <= dt.date(2021, 12, 31)], 10_000)["sharpe"]
        if score > best_score:
            best, best_score = combo, score
    assert years.loc[years["year"] == 2022, "threshold"].item() == best["threshold"]


def test_cli_walkforward_smoke(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr(cli, "YahooSource", FakeSource)
    try:
        cli.main(
            [
                "walkforward", "-s", "vol_filter", "-g", "threshold=0.1,0.3",
                "--start", "2019-03-01", "--end", "2023-12-29", "--first-test-year", "2021",
                "--reference", "threshold=0.2", "--cash-rate", "0.03",
                "--store", "csv", "--cache-dir", str(tmp_path), "--plot", str(tmp_path / "wf.png"),
            ]
        )  # fmt: skip
    finally:
        get_settings.cache_clear()
    out = capsys.readouterr().out
    assert "fixed threshold=0.2 return" in out
    assert "walk-forward" in out and "buy_and_hold" in out
    assert (tmp_path / "wf.png").exists()
