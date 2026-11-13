"""Daily OHLCV bars from Yahoo Finance or Schwab, cached as CSV files on disk.

Symbols are canonical names ("SPY", "VIX", ...); each source maps them to its
own tickers. Bars are DataFrames indexed by session date (datetime.date) with
columns open, high, low, close, volume. Prices are split- and dividend-adjusted.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

log = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")
COLUMNS = ["open", "high", "low", "close", "volume"]


def sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    """NYSE session dates in [start, end]."""
    sched = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    return [d.date() for d in sched.index]


def last_complete_session(now: dt.datetime | None = None) -> dt.date:
    """Most recent session whose close has passed."""
    now = now or dt.datetime.now(ET)
    sched = mcal.get_calendar("NYSE").schedule(start_date=now.date() - dt.timedelta(days=10), end_date=now.date())
    closed = sched[sched["market_close"] <= pd.Timestamp(now)]
    return closed.index[-1].date()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=str.lower)[COLUMNS]
    df = df[~df.index.duplicated()].sort_index()
    df.index.name = "date"
    return df.dropna(subset=["open", "close"])


class YahooSource:
    name = "yahoo"
    tickers = {"VIX": "^VIX"}

    def fetch(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        import yfinance as yf

        ticker = self.tickers.get(symbol, symbol)
        df = yf.Ticker(ticker).history(start=start, end=end + dt.timedelta(days=1), auto_adjust=True)
        if df.empty:
            return pd.DataFrame(columns=COLUMNS)
        df.index = [ts.date() for ts in df.index]
        return _normalize(df)


class SchwabSource:
    name = "schwab"
    tickers = {"VIX": "$VIX"}

    def __init__(self, client):
        self.client = client

    def fetch(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        resp = self.client.price_history(
            self.tickers.get(symbol, symbol),
            periodType="year",
            frequencyType="daily",
            frequency=1,
            startDate=dt.datetime.combine(start, dt.time(0, 0), ET),
            endDate=dt.datetime.combine(end, dt.time(23, 59), ET),
        )
        resp.raise_for_status()
        candles = resp.json().get("candles", [])
        if not candles:
            return pd.DataFrame(columns=COLUMNS)
        df = pd.DataFrame(candles)
        df.index = [ts.date() for ts in pd.to_datetime(df.pop("datetime"), unit="ms", utc=True).dt.tz_convert(ET)]
        return _normalize(df)


class CachedSource:
    """Wraps a source; stores each symbol as CSV plus the date range it covers."""

    def __init__(self, source, cache_dir: str | Path = ".cache"):
        self.source = source
        self.name = source.name
        self.dir = Path(cache_dir) / source.name

    def _paths(self, symbol: str) -> tuple[Path, Path]:
        safe = symbol.replace("/", "_").replace("$", "_").replace("^", "_")
        return self.dir / f"{safe}.csv", self.dir / f"{safe}.json"

    def fetch(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        csv_path, meta_path = self._paths(symbol)
        if csv_path.exists() and meta_path.exists():
            meta = json.loads(meta_path.read_text())
            lo, hi = dt.date.fromisoformat(meta["start"]), dt.date.fromisoformat(meta["end"])
            df = pd.read_csv(csv_path, index_col="date", parse_dates=False)
            df.index = [dt.date.fromisoformat(d) for d in df.index]
            df.index.name = "date"
            if lo <= start and hi >= end:
                return df.loc[(df.index >= start) & (df.index <= end)]
            start, end = min(start, lo), max(end, hi)

        log.info("fetching %s %s..%s from %s", symbol, start, end, self.source.name)
        df = self.source.fetch(symbol, start, end)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path)
        meta_path.write_text(json.dumps({"start": start.isoformat(), "end": end.isoformat()}))
        return df


def load_bars(source, symbols, start: dt.date, end: dt.date) -> dict[str, pd.DataFrame]:
    bars = {s: source.fetch(s, start, end) for s in symbols}
    for s, df in bars.items():
        if df.empty:
            raise ValueError(f"no data for {s} from {source.name} between {start} and {end}")
    return bars
