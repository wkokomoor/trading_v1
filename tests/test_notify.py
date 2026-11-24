import datetime as dt

import pytest

from trading import notify
from trading import portfolio as pf
from trading.data.calendar import ET

D1, D2, D3, D4 = (dt.date(2025, 3, d) for d in (10, 11, 12, 13))


def at(day, hh=9, mm=35):
    return dt.datetime.combine(day, dt.time(hh, mm), ET)


def run_day(engine, day, target, fills=(), status="done", risk=(), blocked=(), close=None):
    """Write one day's run, orders and close snapshot the way the jobs do."""
    p = pf.get_portfolio(engine, "book")
    run_id, _ = pf.start_run(engine, day, p, "test")
    with engine.begin() as conn:
        for side, sym, qty, px in fills:
            fill = {"status": "simulated_filled", "fill_price": px, "filled_qty": qty, "filled_at": at(day)}
            pf.record_order(conn, run_id, "book", symbol=sym, side=side, qty=qty, expected_price=px, **fill)
        for side, sym, qty, why in blocked:
            pf.record_order(conn, run_id, "book", symbol=sym, side=side, qty=qty, expected_price=1.0,
                            status="blocked", error=why)  # fmt: skip
        if close:
            cash, positions, prices = close
            pf.write_snapshot(conn, day, "book", "close", cash, positions, prices)
        pf.finish_run(conn, run_id, status, target=target, risk=list(risk))


@pytest.fixture
def book(pg):
    pf.create_portfolio(pg, "book", "vol_filter", {}, 10_000)
    return pg


def test_direction_labels():
    assert notify.direction({"UPRO": 1.0}) == "Bull"
    assert notify.direction({"SPXU": 1.0}) == "Bear"
    assert notify.direction({}) == "Cash (out of market)"
    assert notify.direction(None) == "Hold (no change)"


def test_buy_day_message(book):
    run_day(book, D1, {"UPRO": 1.0}, fills=[("BUY", "UPRO", 100, 50.0)],
            close=(5_000.0, {"UPRO": 100}, {"UPRO": 51.0}))  # fmt: skip
    assert notify.daily_summary(book, "book", D1) == (
        "Daily direction: Bull\n"
        "BOUGHT: UPRO 100 shares. Total cost: $5,000.00\n"
        "HOLDING: UPRO 100 shares ($5,100.00)\n"
        "CASH: $5,000.00\n"
        "TOTAL: $10,100.00"
    )


def test_sell_profit_uses_average_cost_and_day_change(book):
    run_day(book, D1, {"UPRO": 1.0}, fills=[("BUY", "UPRO", 100, 50.0)],
            close=(5_000.0, {"UPRO": 100}, {"UPRO": 50.0}))  # fmt: skip
    run_day(book, D2, {"UPRO": 1.0}, fills=[("BUY", "UPRO", 100, 60.0)],
            close=(0.0, {"UPRO": 200}, {"UPRO": 60.0}))  # avg cost now 55  # fmt: skip
    run_day(book, D3, {}, fills=[("SELL", "UPRO", 200, 70.0)], close=(14_000.0, {}, {}))
    msg = notify.daily_summary(book, "book", D3)
    assert msg.splitlines() == [
        "Daily direction: Cash (out of market)",
        "SOLD: UPRO 200 shares. Profit $3,000.00",  # (70 - 55) * 200
        "CASH: $14,000.00",
        "TOTAL: $14,000.00 (+16.67% today)",  # vs. 12,000 at the previous close
    ]


def test_hold_day_and_blocked_day(book):
    run_day(book, D1, None, close=(10_000.0, {}, {}))
    assert "No trades today." in notify.daily_summary(book, "book", D1)
    run_day(book, D2, {"UPRO": 1.0}, status="blocked", risk=["kill switch is on: test"],
            blocked=[("BUY", "UPRO", 100, "kill switch is on: test")], close=(10_000.0, {}, {}))  # fmt: skip
    msg = notify.daily_summary(book, "book", D2)
    assert "BLOCKED: kill switch is on: test" in msg
    assert "NOT BOUGHT: UPRO 100 shares (kill switch is on: test)" in msg


def test_no_message_when_portfolio_did_not_run(book):
    assert notify.daily_summary(book, "book", D4) is None


def test_send_email_uses_smtp_ssl_and_all_recipients(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout):
            sent["server"] = (host, port)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def login(self, user, password):
            sent["login"] = user

        def send_message(self, msg):
            sent["to"], sent["subject"], sent["body"] = msg["To"], msg["Subject"], msg.get_content()

    settings = type("S", (), {})()
    settings.smtp_host, settings.smtp_user, settings.smtp_password = (
        "smtp.gmail.com",
        "me@gmail.com",
        "app-pw",
    )
    settings.notify_to = "me@gmail.com, 6085550100@msg.fi.google.com"
    monkeypatch.setattr(notify, "get_settings", lambda: settings)
    monkeypatch.setattr(notify.smtplib, "SMTP_SSL", FakeSMTP)
    to = notify.send_email("subject", "Daily direction: Bull")
    assert to == ["me@gmail.com", "6085550100@msg.fi.google.com"]
    assert sent["server"] == ("smtp.gmail.com", 465) and sent["login"] == "me@gmail.com"
    assert sent["body"].strip() == "Daily direction: Bull"


def test_send_email_refuses_without_configuration(monkeypatch):
    settings = type(
        "S", (), {"smtp_host": "x", "smtp_user": None, "smtp_password": None, "notify_to": None}
    )()
    monkeypatch.setattr(notify, "get_settings", lambda: settings)
    with pytest.raises(RuntimeError, match="not configured"):
        notify.send_email("s", "b")


def test_failed_run_says_so_with_a_short_error(book):
    p = pf.get_portfolio(book, "book")
    run_id, _ = pf.start_run(book, D1, p, "test")
    err = (
        "HTTPError: 500 Server Error: Internal Server Error for url: "
        "https://api.schwabapi.com/marketdata/v1/quotes?symbols=SPY%2CUPRO&indicative=False"
    )
    with book.begin() as conn:
        pf.finish_run(conn, run_id, "failed", error=err)
    lines = notify.daily_summary(book, "book", D1).splitlines()
    assert lines[0] == "Daily direction: unknown (trade run failed)"
    assert lines[1] == "FAILED: HTTPError: 500 Server Error: Internal Server Error"
    assert "https://" not in "\n".join(lines)
