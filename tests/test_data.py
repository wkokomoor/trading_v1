import datetime as dt

from conftest import make_bars

from trading.data import ET, CachedSource, last_complete_session, sessions


class CountingSource:
    name = "fake"

    def __init__(self):
        self.calls = []

    def fetch(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        days = sessions(start, end)
        return make_bars(days, [100.0] * len(days))


def test_cache_serves_repeat_and_subset_requests(tmp_path):
    src = CountingSource()
    cached = CachedSource(src, tmp_path)
    a = cached.fetch("SPY", dt.date(2025, 3, 3), dt.date(2025, 3, 14))
    b = cached.fetch("SPY", dt.date(2025, 3, 5), dt.date(2025, 3, 7))
    assert len(src.calls) == 1
    assert len(a) == 10 and list(b.index) == sessions(dt.date(2025, 3, 5), dt.date(2025, 3, 7))
    assert isinstance(b.index[0], dt.date)


def test_cache_refetches_union_when_range_grows(tmp_path):
    src = CountingSource()
    cached = CachedSource(src, tmp_path)
    cached.fetch("VIX", dt.date(2025, 3, 3), dt.date(2025, 3, 7))
    cached.fetch("VIX", dt.date(2025, 3, 5), dt.date(2025, 3, 14))
    assert src.calls[-1] == ("VIX", dt.date(2025, 3, 3), dt.date(2025, 3, 14))


def test_last_complete_session_excludes_open_session():
    during = dt.datetime(2025, 3, 12, 11, 0, tzinfo=ET)  # Wednesday, market open
    after = dt.datetime(2025, 3, 12, 16, 30, tzinfo=ET)
    weekend = dt.datetime(2025, 3, 15, 12, 0, tzinfo=ET)  # Saturday
    assert last_complete_session(during) == dt.date(2025, 3, 11)
    assert last_complete_session(after) == dt.date(2025, 3, 12)
    assert last_complete_session(weekend) == dt.date(2025, 3, 14)
