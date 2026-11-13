import datetime as dt
from pathlib import Path

from research import fomc_calendar as fc


def test_announcement_date_is_the_last_meeting_day():
    assert fc.announcement(2015, "January", "27-28") == dt.date(2015, 1, 28)
    assert fc.announcement(2013, "April/May", "30-1") == dt.date(2013, 5, 1)
    assert fc.announcement(2026, "March", "17-18*") == dt.date(2026, 3, 18)
    assert fc.announcement(2010, "March", "16") == dt.date(2010, 3, 16)


def test_historical_page_keeps_only_scheduled_meetings(tmp_path, monkeypatch):
    html = "".join(
        f'<h5 class="panel-heading">{h}</h5>'
        for h in [
            "January 28-29 Meeting - 2020",
            "March 2 (unscheduled) Meeting - 2020",
            "March 17-18 (cancelled) Meeting - 2020",
            "March 19 (notation vote) - 2020",
            "July 31-August 1  Meeting - 2020",
            "May 9 Conference Call - 2020",
        ]
    )
    monkeypatch.setattr(fc, "get", lambda url, name: html)
    assert fc.historical(2020) == [dt.date(2020, 1, 29), dt.date(2020, 8, 1)]


def test_committed_calendar_is_complete_and_on_trading_days():
    from trading.data import sessions

    path = Path(fc.__file__).parent / "data" / "fomc_announcements.csv"
    dates = [dt.date.fromisoformat(x) for x in path.read_text().split()[1:]]
    per_year = {y: sum(d.year == y for d in dates) for y in range(2010, 2027)}
    assert all(n == 8 for y, n in per_year.items() if y != 2020) and per_year[2020] == 7
    past = [d for d in dates if d <= dt.date(2026, 11, 12)]
    assert all(d in sessions(d, d) for d in past)
