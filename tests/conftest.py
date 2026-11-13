import datetime as dt

import pandas as pd
import pytest

from trading.data import sessions


def make_bars(days, opens, closes=None) -> pd.DataFrame:
    closes = opens if closes is None else closes
    return pd.DataFrame(
        {"open": opens, "high": opens, "low": opens, "close": closes, "volume": 0},
        index=pd.Index(days, name="date"),
    ).astype({"open": float, "close": float, "high": float, "low": float})


@pytest.fixture
def march_2025():
    """Ten NYSE sessions, 2025-03-03 .. 2025-03-14."""
    return sessions(dt.date(2025, 3, 3), dt.date(2025, 3, 14))
