"""Run: uv run python research/fomc_calendar.py

Builds research/data/fomc_announcements.csv: the announcement date (last day of the
meeting) of every SCHEDULED FOMC meeting, 2010 onward, parsed from the Federal Reserve's
calendar pages. Unscheduled meetings, cancelled meetings, notation votes and conference
calls are excluded. The Fed publishes each year's schedule well in advance, so these dates
are known ahead of time and are fair to use in a strategy.
"""

import datetime as dt
import re
import time
from pathlib import Path

import requests

OUT = Path(__file__).parent / "data" / "fomc_announcements.csv"
CACHE = Path(".cache/fomc")
CURRENT = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
HISTORICAL = "https://www.federalreserve.gov/monetarypolicy/fomchistorical{year}.htm"
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}  # fmt: skip
ABBR = {m[:3]: i for m, i in MONTHS.items()}


def get(url: str, name: str) -> str:
    path = CACHE / name
    if not path.exists():
        CACHE.mkdir(parents=True, exist_ok=True)
        resp = requests.get(url, headers={"User-Agent": "trading_v1 research script"}, timeout=30)
        resp.raise_for_status()
        path.write_text(resp.text)
        time.sleep(1)  # be polite to the Fed's server
    return path.read_text()


def month_num(text: str) -> int:
    t = text.strip().lower()
    return MONTHS.get(t) or ABBR[t[:3]]


def announcement(year: int, month_text: str, day_text: str) -> dt.date:
    """'April/May' + '30-1' -> May 1; 'January' + '27-28' -> January 28; 'March' + '16' -> March 16."""
    months = [month_num(m) for m in month_text.split("/")]
    last_day = int(re.findall(r"\d+", day_text)[-1])
    return dt.date(year, months[-1], last_day)


def historical(year: int) -> list[dt.date]:
    html = get(HISTORICAL.format(year=year), f"h{year}.html")
    dates = []
    for heading in re.findall(r"<h5[^>]*>([^<]*)</h5>", html):
        # "January 27-28 Meeting - 2015", "April/May 30-1 Meeting - 2013",
        # "July 31-August 1  Meeting - 2012". Anything marked "(unscheduled)", "(cancelled)",
        # "(notation vote)", or a "Conference Call" doesn't match and is skipped.
        m = re.fullmatch(r"\s*([^()]+?)\s+Meeting\s+-\s+(\d{4})\s*", heading)
        if m:
            words = re.findall(r"[A-Za-z]+", m.group(1))
            dates.append(announcement(int(m.group(2)), words[-1], m.group(1)))
    return dates


def current() -> list[dt.date]:
    html = get(CURRENT, "current.html")
    dates = []
    sections = re.split(r"<h4><a id=\"\d+\">(\d{4}) FOMC Meetings</a></h4>", html)
    for year, body in zip(sections[1::2], sections[2::2], strict=True):
        pairs = re.findall(
            r'fomc-meeting__month[^"]*"><strong>([^<]+)</strong></div>\s*'
            r'<div class="fomc-meeting__date[^"]*">([^<]+)</div>',
            body,
        )
        for month_text, day_text in pairs:
            if "(" in day_text or "notation" in day_text.lower():
                continue
            dates.append(announcement(int(year), month_text, day_text))
    return dates


def main():
    dates = set(current())
    first_current = min(d.year for d in dates)
    for year in range(2010, first_current):
        dates |= set(historical(year))
    dates = sorted(dates)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("announcement_date\n" + "".join(f"{d.isoformat()}\n" for d in dates))
    by_year = {}
    for d in dates:
        by_year[d.year] = by_year.get(d.year, 0) + 1
    print(f"wrote {len(dates)} scheduled announcement dates to {OUT}")
    print("per year:", by_year)


if __name__ == "__main__":
    main()
