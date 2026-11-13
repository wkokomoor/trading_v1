import datetime as dt

import pandas as pd
import pytest

from conftest import make_bars
from trading.engine import Order, SimBroker, performance, rebalance_orders, run_backtest
from trading.strategy import MarketView, Strategy

PRICES = {"UPRO": 50.0, "SPXU": 20.0}


# ---------------------------------------------------------------- rebalance

def test_rebalance_from_cash():
    assert rebalance_orders({"UPRO": 1.0}, {}, 1000, PRICES) == [Order("UPRO", "BUY", 20, 50.0)]


def test_rebalance_already_on_target():
    assert rebalance_orders({"UPRO": 1.0}, {"UPRO": 20}, 0, PRICES) == []


def test_rebalance_switch_sells_first_and_uses_proceeds():
    orders = rebalance_orders({"UPRO": 1.0}, {"SPXU": 50}, 5, PRICES)
    assert orders == [Order("SPXU", "SELL", 50, 20.0), Order("UPRO", "BUY", 20, 50.0)]


def test_rebalance_to_cash():
    assert rebalance_orders({}, {"UPRO": 20}, 0, PRICES) == [Order("UPRO", "SELL", 20, 50.0)]


def test_rebalance_split_weights():
    orders = rebalance_orders({"UPRO": 0.5, "SPXU": 0.5}, {}, 1000, PRICES)
    assert orders == [Order("UPRO", "BUY", 10, 50.0), Order("SPXU", "BUY", 25, 20.0)]


def test_rebalance_slippage_never_overspends():
    orders = rebalance_orders({"UPRO": 1.0}, {}, 1000, PRICES, slippage_bps=10)
    assert orders[0].qty == 19


@pytest.mark.parametrize("weights", [{"UPRO": -1.0}, {"UPRO": 0.8, "SPXU": 0.5}])
def test_rebalance_rejects_short_or_leverage(weights):
    with pytest.raises(ValueError):
        rebalance_orders(weights, {}, 1000, PRICES)


# ---------------------------------------------------------------- broker

def test_broker_round_trip_and_signed_trade_log():
    b = SimBroker(cash=1000)
    b.execute([Order("UPRO", "BUY", 20, 50.0)], "d1")
    b.execute([Order("UPRO", "SELL", 20, 55.0)], "d2")
    assert b.positions == {} and b.cash == 1100
    assert [t["qty"] for t in b.trades] == [20, -20]


def test_broker_rejects_overspend_and_oversell():
    b = SimBroker(cash=100)
    with pytest.raises(ValueError):
        b.execute([Order("UPRO", "BUY", 3, 50.0)], "d")
    with pytest.raises(ValueError):
        b.execute([Order("UPRO", "SELL", 1, 50.0)], "d")


# ---------------------------------------------------------------- market view

def test_market_view_hides_the_future(march_2025):
    bars = {"X": make_bars(march_2025, list(range(10)))}
    view = MarketView(bars, march_2025[4])
    assert view.open("X") == 4
    assert list(view.history("X")["close"]) == [0, 1, 2, 3]
    assert list(view.history("X", 2)["close"]) == [2, 3]


# ---------------------------------------------------------------- backtest loop

class Alternate(Strategy):
    """Long A on even days, cash on odd days, hold on every third day."""
    name = "alternate"
    symbols = ("A",)

    def on_open(self, view):
        self.n = getattr(self, "n", -1) + 1
        if self.n % 3 == 2:
            return None
        return {"A": 1.0} if self.n % 2 == 0 else {}


def test_backtest_trades_only_on_target_change_and_marks_at_close(march_2025):
    bars = {"A": make_bars(march_2025, opens=[10] * 10, closes=[11] * 10)}
    res = run_backtest(Alternate(), bars, march_2025[1], march_2025[-1], cash=100)
    d = res.daily
    assert list(d["target"])[:4] == ["A:1", "cash", "hold", "cash"]
    assert d["equity"].iloc[0] == 110  # bought 10 @ open 10, marked at close 11
    assert (d["cash"] >= 0).all()
    assert len(d) == 9  # first session only provides history (lookback=1)


def test_backtest_warns_when_strategy_trades_undeclared_symbol(march_2025):
    class Bad(Strategy):
        name, symbols = "bad", ("A",)

        def on_open(self, view):
            return {"B": 1.0}

    bars = {"A": make_bars(march_2025, [10] * 10), "B": make_bars(march_2025, [10] * 10)}
    with pytest.raises(ValueError, match="not listed"):
        run_backtest(Bad(), bars, march_2025[0], march_2025[-1])


def test_backtest_uses_only_dates_all_symbols_share(march_2025):
    class Two(Strategy):
        name, symbols, lookback = "two", ("A", "B"), 0

        def on_open(self, view):
            return {"A": 1.0}

    bars = {"A": make_bars(march_2025, [10] * 10), "B": make_bars(march_2025[3:], [5] * 7)}
    res = run_backtest(Two(), bars, march_2025[0], march_2025[-1])
    assert res.daily.index[0] == march_2025[3]


# ---------------------------------------------------------------- metrics

def test_performance_metrics():
    eq = pd.Series([110.0, 99.0, 121.0])
    p = performance(eq, 100.0)
    assert p["total_return"] == pytest.approx(0.21)
    assert p["max_drawdown"] == pytest.approx(-0.1)
