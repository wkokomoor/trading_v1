import pytest
from conftest import make_bars

from trading.engine import run_backtest
from trading.strategy import MarketView, available_strategies, load_strategy, parse_params

SpyVix = load_strategy("spy_vix")


def spy_vix_view(march_2025, vix, spy_change):
    day = march_2025[1]
    bars = {
        "SPY": make_bars(march_2025[:2], [100, 100 * (1 + spy_change)]),
        "VIX": make_bars(march_2025[:2], [vix, vix]),
    }
    return MarketView(bars, day)


@pytest.mark.parametrize(
    "vix, spy_change, expected",
    [
        (25, 0.00, {"SPXU": 1.0}),  # volatile, flat -> short
        (25, 0.01, {"SPXU": 1.0}),  # volatile, up -> short
        (25, -0.01, None),  # volatile, down -> hold
        (18, 0.00, {"UPRO": 1.0}),  # middle band, flat -> long
        (15, -0.01, {"UPRO": 1.0}),  # calm, down -> long
        (15, 0.01, None),  # calm, up -> hold
        (20, 0.00, {"UPRO": 1.0}),  # exactly vix_high is not volatile
    ],
)
def test_spy_vix_signal(march_2025, vix, spy_change, expected):
    assert SpyVix().on_open(spy_vix_view(march_2025, vix, spy_change)) == expected


def test_spy_vix_long_only_variant(march_2025):
    s = SpyVix(short="")
    assert s.on_open(spy_vix_view(march_2025, 25, 0.0)) == {}
    assert "SPXU" not in s.required_symbols()


def test_spy_vix_end_to_end(march_2025):
    spy = [100, 100, 100, 100, 102, 102, 102, 102, 102, 102]
    vix = [15, 15, 15, 25, 25, 25, 15, 15, 15, 15]
    upro = [50, 50, 51, 52, 50, 49, 50, 51, 52, 53]
    spxu = [20, 20, 19, 18, 20, 21, 20, 19, 18, 17]
    bars = {
        s: make_bars(march_2025, px) for s, px in {"SPY": spy, "VIX": vix, "UPRO": upro, "SPXU": spxu}.items()
    }
    res = run_backtest(SpyVix(), bars, march_2025[1], march_2025[-1], cash=1000)
    assert list(res.daily["signal"]) == [1, 1, -1, -1, -1, 1, 1, 1, 1]
    assert list(res.daily["holding"]) == ["UPRO"] * 2 + ["SPXU"] * 3 + ["UPRO"] * 4
    assert len(res.trades) == 5


def test_sma_trend(march_2025):
    SmaTrend = load_strategy("sma_trend")
    s = SmaTrend(window=3, trade="UPRO")
    rising = {"SPY": make_bars(march_2025, list(range(10))), "UPRO": make_bars(march_2025, [1] * 10)}
    assert s.on_open(MarketView(rising, march_2025[5])) == {"UPRO": 1.0}
    falling = {"SPY": make_bars(march_2025, list(range(10, 0, -1))), "UPRO": rising["UPRO"]}
    assert s.on_open(MarketView(falling, march_2025[5])) == {}


def test_all_strategies_load_and_instantiate():
    names = available_strategies()
    assert {"spy_vix", "buy_and_hold", "sma_trend"} <= set(names)
    for name in names:
        cls = load_strategy(name)
        assert cls.name == name
        cls().required_symbols()


def test_load_strategy_from_file(tmp_path):
    f = tmp_path / "mine.py"
    f.write_text(
        "from trading.strategy import Strategy\n"
        "class Mine(Strategy):\n"
        "    name = 'mine'\n"
        "    symbols = ('SPY',)\n"
        "    def on_open(self, view):\n"
        "        return {'SPY': 1.0}\n"
    )
    assert load_strategy(str(f)).name == "mine"


def test_parse_params_coerces_types_and_rejects_unknown():
    assert parse_params(SpyVix, ["vix_high=25", "short="]) == {"vix_high": 25.0, "short": ""}
    with pytest.raises(ValueError):
        parse_params(SpyVix, ["nope=1"])
    with pytest.raises(ValueError):
        SpyVix(nope=1)
