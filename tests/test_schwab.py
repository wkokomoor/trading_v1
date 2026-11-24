import datetime as dt
from pathlib import Path

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
    fake = type("S", (), {})()
    fake.schwab_app_key, fake.schwab_app_secret = "k", "s"
    fake.schwab_callback_url, fake.schwab_tokens_file = "https://127.0.0.1", None
    monkeypatch.setattr(schwab, "get_settings", lambda: fake)
    with pytest.raises(schwab.LoginRequired, match="trading schwab auth"):
        schwab.make_client(interactive=False)


def _tokens_db(path, issued):
    import sqlite3

    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE schwabdev (access_token_issued TEXT, refresh_token_issued TEXT)")
        conn.execute("INSERT INTO schwabdev VALUES (?, ?)", (issued, issued))


def test_login_expiry_is_seven_days_after_issue(tmp_path):
    db = tmp_path / "tokens.db"
    _tokens_db(db, "2026-11-23T02:43:44+00:00")
    assert schwab.login_expires_at(str(db)) == dt.datetime(2026, 11, 30, 2, 43, 44, tzinfo=dt.UTC)
    assert schwab.login_expires_at(str(tmp_path / "missing.db")) is None


def test_cloud_tokens_are_copied_to_a_writable_temp_file(tmp_path, monkeypatch):
    mounted = tmp_path / "mounted" / "tokens.db"
    mounted.parent.mkdir()
    _tokens_db(mounted, "2026-11-23T02:43:44+00:00")
    settings = type("S", (), {"schwab_tokens_file": str(mounted)})()
    monkeypatch.setattr(schwab, "get_settings", lambda: settings)
    monkeypatch.setattr("tempfile.gettempdir", lambda: str(tmp_path / "tmp"))
    path = schwab.tokens_path()
    assert path == str(tmp_path / "tmp" / "schwabdev" / "tokens.db")
    assert Path(path).read_bytes() == mounted.read_bytes()


def test_job_warns_in_cloud_log_format_when_login_is_expiring(tmp_path, monkeypatch, capsys):
    import json

    from trading import cli

    db = tmp_path / "tokens.db"
    _tokens_db(db, (dt.datetime.now(dt.UTC) - dt.timedelta(days=6)).isoformat())
    monkeypatch.setattr(schwab, "get_settings", lambda: type("S", (), {"schwab_tokens_file": str(db)})())
    cli.warn_if_login_expiring()
    line = json.loads(capsys.readouterr().out)
    assert line["severity"] == "WARNING" and "schwab login expires" in line["message"]


class _Resp:
    def __init__(self, status):
        self.status_code = status


def test_with_retries_rides_out_server_errors():
    calls, waits = iter([_Resp(500), _Resp(503), _Resp(200)]), []
    resp = schwab.with_retries(lambda: next(calls), delays=(1, 2, 3), sleep=waits.append)
    assert resp.status_code == 200 and waits == [1, 2]


def test_with_retries_gives_up_and_returns_the_last_error():
    waits = []
    resp = schwab.with_retries(lambda: _Resp(500), delays=(1, 2), sleep=waits.append)
    assert resp.status_code == 500 and waits == [1, 2]


def test_with_retries_does_not_retry_client_errors():
    waits = []
    assert schwab.with_retries(lambda: _Resp(401), delays=(1,), sleep=waits.append).status_code == 401
    assert waits == []


def test_with_retries_retries_dropped_connections():
    import requests

    attempts, waits = [], []

    def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise requests.ConnectionError("reset")
        return _Resp(200)

    assert schwab.with_retries(flaky, delays=(5,), sleep=waits.append).status_code == 200
    assert waits == [5]
