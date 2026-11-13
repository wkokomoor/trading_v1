"""NYSE trading calendar helpers."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

ET = ZoneInfo("America/New_York")


def sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    """NYSE session dates in [start, end]."""
    sched = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    return [d.date() for d in sched.index]


def last_complete_session(now: dt.datetime | None = None) -> dt.date:
    """Most recent session whose close has passed."""
    now = now or dt.datetime.now(ET)
    sched = mcal.get_calendar("NYSE").schedule(
        start_date=now.date() - dt.timedelta(days=10), end_date=now.date()
    )
    closed = sched[sched["market_close"] <= pd.Timestamp(now)]
    return closed.index[-1].date()
