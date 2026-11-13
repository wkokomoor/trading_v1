"""Read-through bar stores: serve bars from storage, fetching from the source only
for date ranges not requested before.

    CsvStore       .cache/<source>/<symbol>.csv + .json   (offline / no database)
    PostgresStore  bars_daily + bar_coverage tables       (default when DATABASE_URL is set)
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, select
from sqlalchemy.dialects.postgresql import insert

from trading.data.sources import COLUMNS, empty_bars
from trading.db.models import bar_coverage, bars_daily

log = logging.getLogger(__name__)


class BarStore:
    def __init__(self, source):
        self.source = source
        self.name = source.name

    # subclasses implement storage
    def coverage(self, symbol: str) -> tuple[dt.date, dt.date] | None:
        raise NotImplementedError

    def read(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        raise NotImplementedError

    def write(self, symbol: str, bars: pd.DataFrame, start: dt.date, end: dt.date) -> None:
        raise NotImplementedError

    def fetch(self, symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        req_start, req_end = start, end
        cov = self.coverage(symbol)
        if cov:
            lo, hi = cov
            if lo <= start and hi >= end:
                return self.read(symbol, start, end)
            start, end = min(start, lo), max(end, hi)

        log.info("fetching %s %s..%s from %s", symbol, start, end, self.source.name)
        bars = self.source.fetch(symbol, start, end)
        self.write(symbol, bars, start, end)
        return bars.loc[(bars.index >= req_start) & (bars.index <= req_end)]


class CsvStore(BarStore):
    def __init__(self, source, cache_dir: str | Path = ".cache"):
        super().__init__(source)
        self.dir = Path(cache_dir) / source.name

    def _paths(self, symbol: str) -> tuple[Path, Path]:
        safe = symbol.replace("/", "_").replace("$", "_").replace("^", "_")
        return self.dir / f"{safe}.csv", self.dir / f"{safe}.json"

    def coverage(self, symbol):
        csv_path, meta_path = self._paths(symbol)
        if not (csv_path.exists() and meta_path.exists()):
            return None
        meta = json.loads(meta_path.read_text())
        return dt.date.fromisoformat(meta["start"]), dt.date.fromisoformat(meta["end"])

    def read(self, symbol, start, end):
        df = pd.read_csv(self._paths(symbol)[0], index_col="date")
        df.index = pd.Index([dt.date.fromisoformat(d) for d in df.index], name="date")
        return df.loc[(df.index >= start) & (df.index <= end)]

    def write(self, symbol, bars, start, end):
        csv_path, meta_path = self._paths(symbol)
        self.dir.mkdir(parents=True, exist_ok=True)
        bars.to_csv(csv_path)
        meta_path.write_text(json.dumps({"start": start.isoformat(), "end": end.isoformat()}))


def _num(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else float(v)


class PostgresStore(BarStore):
    UPSERT_CHUNK = 5000

    def __init__(self, source, engine: Engine):
        super().__init__(source)
        self.engine = engine

    def coverage(self, symbol):
        with self.engine.connect() as conn:
            row = conn.execute(
                select(bar_coverage.c.start_date, bar_coverage.c.end_date).where(
                    bar_coverage.c.source == self.name, bar_coverage.c.symbol == symbol
                )
            ).first()
        return (row.start_date, row.end_date) if row else None

    def read(self, symbol, start, end):
        b = bars_daily.c
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(b.date, b.open, b.high, b.low, b.close, b.volume)
                .where(b.source == self.name, b.symbol == symbol, b.date.between(start, end))
                .order_by(b.date)
            ).all()
        if not rows:
            return empty_bars()
        df = pd.DataFrame(rows, columns=["date", *COLUMNS]).set_index("date")
        return df

    def write(self, symbol, bars, start, end):
        rows = [
            {
                "source": self.name,
                "symbol": symbol,
                "date": d,
                "open": float(r.open),
                "high": _num(r.high),
                "low": _num(r.low),
                "close": float(r.close),
                "volume": None if _num(r.volume) is None else int(r.volume),
            }
            for d, r in zip(bars.index, bars.itertuples(index=False), strict=True)
        ]
        with self.engine.begin() as conn:
            for i in range(0, len(rows), self.UPSERT_CHUNK):
                stmt = insert(bars_daily).values(rows[i : i + self.UPSERT_CHUNK])
                stmt = stmt.on_conflict_do_update(
                    index_elements=["source", "symbol", "date"],
                    set_={c: stmt.excluded[c] for c in [*COLUMNS, "fetched_at"]},
                )
                conn.execute(stmt)
            cov = insert(bar_coverage).values(source=self.name, symbol=symbol, start_date=start, end_date=end)
            conn.execute(
                cov.on_conflict_do_update(
                    index_elements=["source", "symbol"],
                    set_={"start_date": start, "end_date": end, "updated_at": cov.excluded.updated_at},
                )
            )


def import_csv_cache(cache_dir: str | Path, engine: Engine) -> list[tuple[str, str, int]]:
    """Copy every symbol in a CsvStore cache directory into Postgres. Returns (source, symbol, rows)."""

    class _Named:
        def __init__(self, name):
            self.name = name

    imported = []
    for source_dir in sorted(p for p in Path(cache_dir).iterdir() if p.is_dir()):
        csv = CsvStore(_Named(source_dir.name), cache_dir)
        pg = PostgresStore(_Named(source_dir.name), engine)
        for meta_path in sorted(source_dir.glob("*.json")):
            symbol = meta_path.stem  # canonical names are stored verbatim (e.g. VIX)
            lo, hi = csv.coverage(symbol)
            bars = csv.read(symbol, lo, hi)
            pg.write(symbol, bars, lo, hi)
            imported.append((source_dir.name, symbol, len(bars)))
    return imported


def load_bars(source, symbols, start: dt.date, end: dt.date) -> dict[str, pd.DataFrame]:
    bars = {s: source.fetch(s, start, end) for s in symbols}
    for s, df in bars.items():
        if df.empty:
            raise ValueError(f"no data for {s} from {source.name} between {start} and {end}")
    return bars
