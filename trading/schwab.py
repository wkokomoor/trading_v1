"""Schwab API connection: client creation, login, and a read-only connection check.

Login (OAuth) needs a person: Schwab shows a browser login, then redirects to the
callback URL, which is pasted back. Tokens are stored by schwabdev in
~/.schwabdev/tokens.db. The refresh token lasts 7 days, so log in again weekly.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd

from trading.config import get_settings
from trading.data.calendar import ET


class LoginRequired(RuntimeError):
    pass


def make_client(interactive: bool = True):
    """schwabdev client. Non-interactive clients raise LoginRequired instead of prompting."""
    import schwabdev

    s = get_settings()
    if not s.schwab_app_key or not s.schwab_app_secret:
        raise SystemExit("SCHWAB_APP_KEY / SCHWAB_APP_SECRET missing: see .env.example")

    if interactive:
        return schwabdev.Client(s.schwab_app_key, s.schwab_app_secret, s.schwab_callback_url)

    # schwabdev catches exceptions raised by call_on_auth and keeps going, so record the
    # request and check it ourselves; quiet its retry warnings while doing so.
    login_needed = []
    sd_log = logging.getLogger("Schwabdev")
    level = sd_log.level
    sd_log.setLevel(logging.CRITICAL)
    try:
        client = schwabdev.Client(
            s.schwab_app_key,
            s.schwab_app_secret,
            s.schwab_callback_url,
            call_on_auth=lambda auth_url: login_needed.append(auth_url),
        )
    finally:
        sd_log.setLevel(level)
    if login_needed:
        raise LoginRequired("Schwab login needed: run `uv run trading schwab auth` in a terminal")
    return client


def _candle_span(resp) -> tuple[int, str, str]:
    if not resp.ok:
        return 0, f"HTTP {resp.status_code}", ""
    candles = resp.json().get("candles", [])
    if not candles:
        return 0, "-", "-"
    ts = pd.to_datetime([c["datetime"] for c in candles], unit="ms", utc=True).tz_convert(ET)
    return len(candles), f"{ts.min():%Y-%m-%d %H:%M}", f"{ts.max():%Y-%m-%d %H:%M}"


def check(client, symbols=("SPY", "$VIX", "$ADD", "$TICK")) -> list[tuple[str, str]]:
    """Read-only checks. Returns (check, result) rows; never prints balances or positions."""
    rows = []

    resp = client.linked_accounts()
    rows.append(
        ("account access", f"HTTP {resp.status_code}, {len(resp.json()) if resp.ok else 0} linked account(s)")
    )

    resp = client.quotes(list(symbols))
    if resp.ok:
        data = resp.json()
        for sym in symbols:
            q = data.get(sym, {}).get("quote", {})
            rows.append(
                (f"quote {sym}", f"last={q.get('lastPrice')} open={q.get('openPrice')}" if q else "missing")
            )
    else:
        rows.append(("quotes", f"HTTP {resp.status_code}"))

    today = dt.datetime.now(ET)
    for sym in symbols:
        daily = client.price_history(
            sym,
            periodType="year",
            frequencyType="daily",
            frequency=1,
            startDate=dt.datetime(1990, 1, 1, tzinfo=ET),
            endDate=today,
        )
        n, first, last = _candle_span(daily)
        rows.append((f"daily history {sym}", f"{n} candles, {first} .. {last}"))
        minute = client.price_history(
            sym, periodType="day", period=10, frequencyType="minute", frequency=1, endDate=today
        )
        n, first, last = _candle_span(minute)
        rows.append((f"1-min history {sym} (last 10 days requested)", f"{n} candles, {first} .. {last}"))
    return rows
