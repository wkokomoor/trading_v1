import datetime as dt

import pytest
from conftest import FakeSource, make_bars

from trading import cli, research
from trading.config import get_settings
from trading.data import sessions
from trading.strategy import MarketView, load_strategy

VixGap = load_strategy("vix_gap")


# ---------------------------------------------------------------- vix_gap


def view(vix, gap, spy_closes=None):
    days = sessions(dt.date(2025, 3, 3), dt.date(2025, 3, 14))
    closes = spy_closes or [100.0] * 10
    spy = make_bars(days, [100.0] * 9 + [100.0 * (1 + gap)], closes)
    return MarketView(
        {"SPY": spy, "VIX": make_bars(days, [vix] * 10), "UPRO": make_bars(days, [50] * 10)}, days[-1]
    )


@pytest.mark.parametrize("vix, weight", [(12, 1.0), (15, 1.0), (30, 0.5), (40, 0.25), (80, 0.0)])
def test_vix_gap_sizes_by_vix(vix, weight):
    target = VixGap(target_vol=0.45).on_open(view(vix, 0.0))
    assert target == ({"UPRO": weight} if weight else {})


def test_vix_gap_sizing_off_means_fully_invested():
    assert VixGap(target_vol=0).on_open(view(60, 0.0)) == {"UPRO": 1.0}


@pytest.mark.parametrize("mode, expected", [("hold", None), ("cash", {}), ("ignore", {"UPRO": 1.0})])
def test_vix_gap_gap_up_modes(mode, expected):
    assert VixGap(on_gap_up=mode, target_vol=0).on_open(view(15, 0.01)) == expected


def test_vix_gap_trend_filter_goes_risk_off():
    falling = [float(x) for x in range(110, 100, -1)]
    s = VixGap(trend_window=5, risk_off="SPY")
    assert s.on_open(view(15, 0.0, falling)) == {"SPY": 1.0}
    assert "SPY" in s.required_symbols() and s.required_lookback() == 5
    assert VixGap(trend_window=5).on_open(view(15, 0.0, falling)) == {}


# ---------------------------------------------------------------- sweep


def test_expand_grid_coerces_and_multiplies():
    grid = research.expand_grid(VixGap, ["target_vol=0,0.5", "on_gap_up=hold,cash,ignore"])
    assert len(grid) == 6
    assert grid[0] == {"target_vol": 0.0, "on_gap_up": "hold"}
    with pytest.raises(ValueError):
        research.expand_grid(VixGap, ["nope=1"])


def test_split_periods():
    p = research.split_periods(dt.date(2010, 1, 1), dt.date(2025, 1, 1), dt.date(2020, 1, 1))
    assert p == {
        "train": (dt.date(2010, 1, 1), dt.date(2019, 12, 31)),
        "test": (dt.date(2020, 1, 1), dt.date(2025, 1, 1)),
    }
    assert list(research.split_periods(dt.date(2010, 1, 1), dt.date(2025, 1, 1), None)) == ["full"]


def test_sweep_runs_every_combination_in_every_period():
    src = FakeSource()
    bars = {s: src.fetch(s, dt.date(2023, 1, 3), dt.date(2024, 12, 31)) for s in ("SPY", "VIX", "UPRO")}
    grid = research.expand_grid(VixGap, ["target_vol=0,0.6", "on_gap_up=hold,cash"])
    periods = research.split_periods(dt.date(2023, 2, 1), dt.date(2024, 12, 31), dt.date(2024, 1, 1))
    table = research.sweep(VixGap, {}, grid, bars, periods)
    assert len(table) == 4
    assert {"train_cagr", "train_sharpe", "test_cagr", "test_max_drawdown", "test_trades"} <= set(
        table.columns
    )
    assert table["train_trades"].notna().all()


def test_cli_sweep_smoke(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr(cli, "YahooSource", FakeSource)
    try:
        cli.main(
            [
                "sweep", "-s", "vix_gap", "-g", "target_vol=0,0.6",
                "--start", "2023-02-01", "--end", "2024-12-31", "--split", "2024-01-01",
                "--store", "csv", "--cache-dir", str(tmp_path),
            ]
        )  # fmt: skip
    finally:
        get_settings.cache_clear()
    out = capsys.readouterr().out
    assert "2 combinations x 2 periods" in out
    assert "ranked by train_sharpe" in out
    assert "buy_and_hold" in out
