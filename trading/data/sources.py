"""Daily OHLCV bars from Yahoo Finance or Schwab.

Symbols are canonical names ("SPY", "VIX", ...); each source maps them to its
own tickers. Bars are DataFrames indexed by session date (datetime.date) with
columns open, high, low, close, volume. Prices are split- and dividend-adjusted.
"""

from __future__ import annotations

import datetime as dt

import pandas as pd

from trading.data.calendar import ET

COLUMNS = ["open", "high", "low", "close", "volume"]


def empty_bars() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNS)
    df.index.name = "date"
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=str.lower)[COLUMNS]
    df = df[~df.index.duplicated()].sort_index()
    df.index.name = "date"
    return df.dropna(subset=["open", "close"])


class YahooSource:
    name = "yahoo"
    tickers = {"VIX": "^VIX", "VIX3M": "^VIX3M", "IRX": "^IRX"}  # IRX: 13-week T-bill yield, %

    def fetch(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        import yfinance as yf

        ticker = self.tickers.get(symbol, symbol)
        df = yf.Ticker(ticker).history(start=start, end=end + dt.timedelta(days=1), auto_adjust=True)
        if df.empty:
            return empty_bars()
        df.index = [ts.date() for ts in df.index]
        return normalize(df)


class SchwabSource:
    name = "schwab"
    tickers = {"VIX": "$VIX", "VIX3M": "$VIX3M"}

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
            return empty_bars()
        df = pd.DataFrame(candles)
        df.index = [
            ts.date() for ts in pd.to_datetime(df.pop("datetime"), unit="ms", utc=True).dt.tz_convert(ET)
        ]
        return normalize(df)
