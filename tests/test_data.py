"""Bar stores. Every behavior test runs against both CsvStore and PostgresStore."""

import datetime as dt

import pandas as pd
import pytest
from conftest import FakeSource, make_bars
from sqlalchemy import func, select

from trading.data import ET, CsvStore, PostgresStore, import_csv_cache, last_complete_session, sessions
from trading.db.models import bars_daily

MAR3, MAR5, MAR7, MAR14 = (dt.date(2025, 3, d) for d in (3, 5, 7, 14))


@pytest.fixture(params=["csv", "postgres"])
def make_store(request, tmp_path):
    if request.param == "csv":
        return lambda src: CsvStore(src, tmp_path)
    engine = request.getfixturevalue("pg")
    return lambda src: PostgresStore(src, engine)


def test_store_serves_repeat_and_subset_requests_without_refetching(make_store):
    src = FakeSource()
    store = make_store(src)
    a = store.fetch("SPY", MAR3, MAR14)
    b = store.fetch("SPY", MAR5, MAR7)
    assert len(src.calls) == 1
    assert len(a) == 10
    assert list(b.index) == sessions(MAR5, MAR7)
    assert isinstance(b.index[0], dt.date)


def test_store_round_trips_values_exactly(make_store):
    src = FakeSource()
    store = make_store(src)
    fresh = store.fetch("SPY", MAR3, MAR14)
    cached = store.fetch("SPY", MAR3, MAR14)
    pd.testing.assert_frame_equal(fresh, cached, check_dtype=False, check_index_type=False)


def test_store_refetches_union_when_range_grows(make_store):
    src = FakeSource()
    store = make_store(src)
    store.fetch("VIX", MAR3, MAR7)
    out = store.fetch("VIX", MAR5, MAR14)
    assert src.calls[-1] == ("VIX", MAR3, MAR14)
    assert list(out.index) == sessions(MAR5, MAR14)  # only the requested slice is returned


def test_store_remembers_ranges_with_no_data(make_store):
    """A symbol that didn't exist yet must not trigger a re-fetch every time."""

    class LateListing(FakeSource):
        def fetch(self, symbol, start, end):
            df = super().fetch(symbol, start, end)
            return df[df.index >= MAR7]

    src = LateListing()
    store = make_store(src)
    store.fetch("NEW", MAR3, MAR14)
    out = store.fetch("NEW", MAR3, MAR14)
    assert len(src.calls) == 1
    assert out.index[0] == MAR7


def test_postgres_upsert_is_idempotent_and_handles_missing_values(pg):
    store = PostgresStore(FakeSource(), pg)
    days = sessions(MAR3, MAR7)
    bars = make_bars(days, [10.0] * len(days))
    bars.loc[days[0], ["high", "low", "volume"]] = [float("nan"), float("nan"), float("nan")]
    store.write("X", bars, MAR3, MAR7)
    store.write("X", bars.assign(close=11.0), MAR3, MAR7)
    with pg.connect() as conn:
        assert conn.execute(select(func.count()).select_from(bars_daily)).scalar_one() == len(days)
    out = store.read("X", MAR3, MAR7)
    assert (out["close"] == 11.0).all()
    assert out.iloc[0]["volume"] is None or pd.isna(out.iloc[0]["volume"])


def test_import_csv_cache_into_postgres(tmp_path, pg):
    csv = CsvStore(FakeSource(), tmp_path)
    expected = csv.fetch("VIX", MAR3, MAR14)
    imported = import_csv_cache(tmp_path, pg)
    assert imported == [("fake", "VIX", 10)]

    src = FakeSource()
    got = PostgresStore(src, pg).fetch("VIX", MAR3, MAR14)
    assert src.calls == []  # served from the imported data
    pd.testing.assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


def test_last_complete_session_excludes_open_session():
    during = dt.datetime(2025, 3, 12, 11, 0, tzinfo=ET)  # Wednesday, market open
    after = dt.datetime(2025, 3, 12, 16, 30, tzinfo=ET)
    weekend = dt.datetime(2025, 3, 15, 12, 0, tzinfo=ET)  # Saturday
    assert last_complete_session(during) == dt.date(2025, 3, 11)
    assert last_complete_session(after) == dt.date(2025, 3, 12)
    assert last_complete_session(weekend) == dt.date(2025, 3, 14)
