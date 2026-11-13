"""Phase D: risk checks, the shadow broker, and the trade / reconcile jobs end to end."""

import datetime as dt

import numpy as np
import pytest
from conftest import FakeSource

from trading import jobs, risk
from trading import portfolio as pf
from trading.brokers import Quote, ShadowBroker
from trading.data.calendar import ET
from trading.engine import Order

TODAY = dt.date(2025, 3, 12)  # a Wednesday
OPEN_PLUS_5 = dt.datetime(2025, 3, 12, 9, 35, tzinfo=ET)


def q(sym, open_=100.0, last=100.0, prev_close=100.0, when=OPEN_PLUS_5, bid=None, ask=None):
    return Quote(sym, open_, last, bid, ask, prev_close, when)


# --------------------------------------------------------------------------- risk


def run_check(orders, quotes=None, positions=None, cash=10_000.0, prev=None, kill=None, allocation=10_000):
    quotes = quotes or {"SPY": q("SPY"), "UPRO": q("UPRO", 50, 50, 50)}
    return risk.check(
        orders,
        limits=risk.RiskLimits(allocation=allocation),
        quotes=quotes,
        required_symbols=("SPY", "UPRO"),
        positions=positions or {},
        cash=cash,
        prev_close_equity=prev,
        kill_switch=kill or {"on": False},
        today=TODAY,
    )


BUY = Order("UPRO", "BUY", 100, 50.0)
SELL = Order("UPRO", "SELL", 100, 50.0)


def test_normal_order_is_approved():
    res = run_check([BUY])
    assert res.approved == [BUY] and not res.blocked and not res.halted


def test_kill_switch_halts_everything():
    res = run_check([BUY, SELL], kill={"on": True, "reason": "testing"})
    assert res.halted and not res.approved and len(res.blocked) == 2
    assert "testing" in res.messages[0]


@pytest.mark.parametrize(
    "quotes, why",
    [
        ({"UPRO": q("UPRO", 50, 50, 50)}, "no quote for SPY"),
        ({"SPY": q("SPY", open_=None), "UPRO": q("UPRO")}, "no opening price"),
        ({"SPY": q("SPY", when=OPEN_PLUS_5 - dt.timedelta(days=1)), "UPRO": q("UPRO")}, "stale"),
        ({"SPY": q("SPY", open_=140, prev_close=100), "UPRO": q("UPRO")}, "suspect data"),
    ],
)
def test_bad_data_halts(quotes, why):
    res = run_check([BUY], quotes=quotes)
    assert res.halted and why in res.messages[0]


def test_symbol_outside_strategy_is_blocked():
    res = run_check(
        [Order("TSLA", "BUY", 1, 50.0)], quotes={"SPY": q("SPY"), "UPRO": q("UPRO"), "TSLA": q("TSLA")}
    )
    assert res.blocked[0][1].startswith("TSLA is not")


def test_daily_loss_limit_blocks_buys_but_allows_sells():
    res = run_check([SELL, BUY], positions={"UPRO": 100}, cash=0, prev=10_000)  # equity 5,000 vs 10,000
    assert res.approved == [SELL]
    assert res.blocked == [(BUY, "daily loss limit")]


def test_allocation_cap_blocks_buys():
    res = run_check([Order("UPRO", "BUY", 300, 50.0)], cash=20_000, allocation=10_000)
    assert res.blocked[0][1] == "allocation cap"


# --------------------------------------------------------------------------- shadow broker


def test_shadow_broker_fills_at_bid_ask_and_never_overspends():
    quotes = {"UPRO": q("UPRO", 50, 50, 50, bid=49.9, ask=50.1)}
    fills, cash, pos = ShadowBroker().execute([Order("UPRO", "BUY", 200, 50.0)], quotes, 5_000, {})
    ((_, qty, px),) = fills
    assert px == 50.1 and qty == 99 and cash == pytest.approx(5_000 - 99 * 50.1) and pos == {"UPRO": 99}
    fills, cash2, pos = ShadowBroker().execute([Order("UPRO", "SELL", 99, 50.0)], quotes, cash, pos)
    assert fills[0][2] == 49.9 and pos == {}


# --------------------------------------------------------------------------- jobs (real Postgres)


class Resp:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        pass


class FakeSchwab:
    """Calm daily bars (so vol_filter wants UPRO) and quotes stamped at `now`."""

    def __init__(self, now=OPEN_PLUS_5, fail=False):
        self.now, self.fail, self.src = now, fail, FakeSource()

    def price_history(self, symbol, startDate, endDate, **_):
        sym = symbol.lstrip("$")
        bars = self.src.fetch(sym, startDate.date(), endDate.date())
        calm = 100.0 * 1.0005 ** np.arange(len(bars))  # steady drift: very low realized volatility
        candles = [
            {"datetime": int(dt.datetime.combine(d, dt.time(0), ET).timestamp() * 1000),
             "open": c, "high": c, "low": c, "close": c, "volume": 1}
            for d, c in zip(bars.index, calm, strict=True)
        ]  # fmt: skip
        return Resp({"candles": candles})

    def quotes(self, symbols):
        if self.fail:
            raise ConnectionError("Schwab is down")
        ms = int(self.now.timestamp() * 1000)
        px = {"SPY": 500.0, "UPRO": 80.0}
        return Resp(
            {
                s: {"quote": {"openPrice": px[s], "lastPrice": px[s], "bidPrice": px[s] - 0.02,
                              "askPrice": px[s] + 0.02, "closePrice": px[s], "quoteTime": ms}}
                for s in symbols
            }
        )  # fmt: skip


@pytest.fixture
def shadow(pg):
    pf.create_portfolio(pg, "shadow-vol", "vol_filter", {"window": 20, "threshold": 0.2}, 10_000)
    return pg


def test_trade_job_simulates_fills_and_records_everything(shadow):
    result = jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5)
    assert result["status"] == "done"
    assert result["fills"] == ["BUY 124 UPRO @ 80.02"]  # 10,000 / 80 = 125 at last; 124 fit at the ask
    p = pf.get_portfolio(shadow, "shadow-vol")
    assert p.positions == {"UPRO": 124} and p.cash == pytest.approx(10_000 - 124 * 80.02)
    run = pf.recent_runs(shadow, "shadow-vol")[0]
    assert run["status"] == "done" and run["target"] == {"UPRO": 1.0}
    assert run["orders"][0]["status"] == "simulated_filled"
    assert run["info"]["vol"] < 0.2


def test_trade_job_is_idempotent_per_day(shadow):
    jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5)
    again = jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5 + dt.timedelta(minutes=5))
    assert again["status"] == "skipped" and "already done" in again["reason"]
    assert len(pf.recent_runs(shadow, "shadow-vol")[0]["orders"]) == 1


def test_failed_run_without_orders_can_be_retried(shadow):
    with pytest.raises(ConnectionError):
        jobs.trade(shadow, FakeSchwab(fail=True), "shadow-vol", now=OPEN_PLUS_5)
    assert pf.recent_runs(shadow, "shadow-vol")[0]["status"] == "failed"
    retry = jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5)
    assert retry["status"] == "done"


def test_kill_switch_blocks_the_run(shadow):
    pf.set_kill_switch(shadow, True, "manual test")
    result = jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5)
    assert result["status"] == "blocked" and result["fills"] == []
    assert pf.get_portfolio(shadow, "shadow-vol").positions == {}
    assert pf.recent_runs(shadow, "shadow-vol")[0]["orders"][0]["status"] == "blocked"
    pf.set_kill_switch(shadow, False)


@pytest.mark.parametrize(
    "now, reason",
    [
        (dt.datetime(2025, 3, 15, 10, 0, tzinfo=ET), "not an NYSE trading day"),  # Saturday
        (dt.datetime(2025, 12, 25, 10, 0, tzinfo=ET), "not an NYSE trading day"),  # Christmas
        (dt.datetime(2025, 3, 12, 9, 15, tzinfo=ET), "not opened yet"),
    ],
)
def test_trade_job_skips_cleanly_when_market_is_closed(shadow, now, reason):
    result = jobs.trade(shadow, FakeSchwab(now=now), "shadow-vol", now=now)
    assert result["status"] == "skipped" and reason in result["reason"]
    assert pf.recent_runs(shadow, "shadow-vol") == []


def test_reconcile_marks_shadow_portfolio_at_the_close(shadow):
    jobs.trade(shadow, FakeSchwab(), "shadow-vol", now=OPEN_PLUS_5)
    after_close = dt.datetime(2025, 3, 12, 16, 45, tzinfo=ET)
    jobs.ingest(shadow, FakeSchwab(now=after_close), now=after_close)
    (result,) = jobs.reconcile(shadow, FakeSchwab(now=after_close), now=after_close)
    assert result["status"] == "done" and result["positions"] == {"UPRO": 124}


def test_live_portfolios_are_not_supported_yet(pg):
    with pg.begin() as conn:
        from trading.db.models import portfolios

        conn.execute(portfolios.insert().values(name="real", mode="live", strategy="vol_filter", params={},
                                                allocation=1000, cash=0, positions={}))  # fmt: skip
    with pytest.raises(SystemExit, match="part 2"):
        jobs.trade(pg, FakeSchwab(), "real", now=OPEN_PLUS_5)
