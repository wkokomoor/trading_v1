"""Parameter sweeps with an in-sample / out-of-sample split.

Choose parameters by their TRAIN results only; the TEST columns show how that
choice would have held up on data it never saw. Picking by TEST results turns
the test period into more training data and defeats the purpose.
"""

from __future__ import annotations

import datetime as dt
import itertools
import logging

import pandas as pd

from trading.engine import run_backtest
from trading.strategy import Strategy, parse_params

log = logging.getLogger(__name__)

METRICS = ("cagr", "sharpe", "max_drawdown", "trades")


def expand_grid(cls: type[Strategy], grid: list[str]) -> list[dict]:
    """["a=1,2", "b=x,y"] -> [{a:1,b:x}, {a:1,b:y}, {a:2,b:x}, {a:2,b:y}], values type-coerced."""
    keys, choices = [], []
    for item in grid:
        key, sep, raw = item.partition("=")
        if not sep:
            raise ValueError(f"grid {item!r} must look like key=v1,v2,...")
        keys.append(key)
        choices.append([parse_params(cls, [f"{key}={v}"])[key] for v in raw.split(",")])
    return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*choices)]


def split_periods(start: dt.date, end: dt.date, split: dt.date | None) -> dict[str, tuple[dt.date, dt.date]]:
    if split is None:
        return {"full": (start, end)}
    return {"train": (start, split - dt.timedelta(days=1)), "test": (split, end)}


def _row(res) -> dict:
    s = res.summary()
    return {m: s.get(m) for m in METRICS}


def sweep(
    cls: type[Strategy],
    base: dict,
    grid: list[dict],
    bars: dict[str, pd.DataFrame],
    periods: dict[str, tuple[dt.date, dt.date]],
    cash: float = 10_000.0,
    slippage_bps: float = 0.0,
) -> pd.DataFrame:
    """One row per param combination, with metrics for each period (fresh strategy per run)."""
    rows = []
    for i, combo in enumerate(grid, 1):
        params = {**base, **combo}
        row = dict(combo)
        for name, (start, end) in periods.items():
            res = run_backtest(cls(**params), bars, start, end, cash, slippage_bps)
            row.update({f"{name}_{k}": v for k, v in _row(res).items()})
        rows.append(row)
        log.info("%d/%d %s", i, len(grid), combo)
    return pd.DataFrame(rows)


def benchmark_rows(
    benchmark: Strategy,
    bars: dict[str, pd.DataFrame],
    periods: dict[str, tuple[dt.date, dt.date]],
    cash: float,
    slippage_bps: float,
) -> dict:
    row = {}
    for name, (start, end) in periods.items():
        res = run_backtest(type(benchmark)(**vars(benchmark.p)), bars, start, end, cash, slippage_bps)
        row.update({f"{name}_{k}": v for k, v in _row(res).items()})
    return row
