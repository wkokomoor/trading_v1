import datetime as dt

import pytest

from trading import schwab
from trading.data.calendar import ET


class Resp:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.ok = payload, status, status == 200

    def json(self):
        return self.payload


class FakeClient:
    def linked_accounts(self):
        return Resp([{"hashValue": "secret-hash", "accountNumber": "123456789"}])

    def quotes(self, symbols):
        return Resp({s: {"quote": {"lastPrice": 1.0, "openPrice": 1.0}} for s in symbols if s != "$TICK"})

    def price_history(self, symbol, **kw):
        if symbol == "$ADD":
            return Resp({}, status=400)
        ms = int(dt.datetime(2020, 1, 2, tzinfo=ET).timestamp() * 1000)
        return Resp({"candles": [{"datetime": ms}, {"datetime": ms + 86_400_000}]})


def test_check_reports_without_exposing_account_details():
    rows = dict(schwab.check(FakeClient()))
    assert rows["account access"] == "HTTP 200, 1 linked account(s)"
    assert "123456789" not in str(rows) and "secret-hash" not in str(rows)
    assert rows["quote $TICK"] == "missing"
    assert rows["daily history SPY"].startswith("2 candles, 2020-01-02")
    assert "HTTP 400" in rows["daily history $ADD"]


def test_non_interactive_client_raises_instead_of_prompting(monkeypatch):
    import schwabdev

    class PromptingClient:
        def __init__(self, *args, call_on_auth=None, **kw):
            call_on_auth("https://api.schwabapi.com/v1/oauth/authorize?client_id=x")

    monkeypatch.setattr(schwabdev, "Client", PromptingClient)
    monkeypatch.setattr(
        schwab, "get_settings", lambda: type("S", (), {"schwab_app_key": "k", "schwab_app_secret": "s",
                                                        "schwab_callback_url": "https://127.0.0.1"})()
    )  # fmt: skip
    with pytest.raises(schwab.LoginRequired, match="trading schwab auth"):
        schwab.make_client(interactive=False)
