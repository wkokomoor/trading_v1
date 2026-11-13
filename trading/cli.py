"""Command line: list strategies, backtest one against a benchmark, or dry-run it live.

    python -m trading list
    python -m trading backtest -s spy_vix --start 2010-01-01 --plot equity.png
    python -m trading backtest -s spy_vix -p vix_high=25 -p spy_low=-0.01 --slippage-bps 5
    python -m trading backtest -s path/to/my_strategy.py
    python -m trading dry-run -s spy_vix
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import math
import os
import sys

import pandas as pd

from trading.data import CachedSource, SchwabSource, YahooSource, last_complete_session, load_bars
from trading.engine import plot_equity, run_backtest
from trading.strategy import available_strategies, load_strategy, parse_params


def make_schwab_client():
    import schwabdev
    from dotenv import load_dotenv

    load_dotenv()
    key, secret = os.getenv("app_key"), os.getenv("app_secret")
    if not key or not secret:
        raise SystemExit("app_key / app_secret missing: copy .env.example to .env and fill it in")
    return schwabdev.Client(key, secret, os.getenv("callback_url", "https://127.0.0.1"))


def build_strategy(name: str, params: list[str]):
    cls = load_strategy(name)
    return cls(**parse_params(cls, params))


def cmd_list(_args) -> None:
    for name in available_strategies():
        cls = load_strategy(name)
        doc = (sys.modules[cls.__module__].__doc__ or "").strip().splitlines()
        print(f"{name}\n    {doc[0] if doc else ''}\n    params: {cls.params}")


def cmd_backtest(args) -> None:
    strategy = build_strategy(args.strategy, args.param)
    benchmark = build_strategy(args.benchmark, args.benchmark_param)

    end = args.end or last_complete_session()
    lookback = max(strategy.required_lookback(), benchmark.required_lookback())
    fetch_start = args.start - dt.timedelta(days=math.ceil(lookback * 1.6) + 15)
    symbols = tuple(dict.fromkeys(strategy.required_symbols() + benchmark.required_symbols()))

    source = YahooSource() if args.source == "yahoo" else SchwabSource(make_schwab_client())
    if not args.no_cache:
        source = CachedSource(source, args.cache_dir)
    bars = load_bars(source, symbols, fetch_start, end)

    result = run_backtest(strategy, bars, args.start, end, args.cash, args.slippage_bps)
    if result.daily.empty:
        raise SystemExit("no days simulated (not enough data after warm-up?)")
    # Benchmark over exactly the days the strategy traded, for a fair comparison.
    bench = run_backtest(benchmark, bars, result.daily.index[0], result.daily.index[-1],
                         args.cash, args.slippage_bps)

    if args.verbose:
        with pd.option_context("display.width", 200, "display.max_columns", 20, "display.max_rows", 60):
            print(result.daily)
            print(result.trades)
    print_summary({result.strategy: result.summary(), bench.strategy: bench.summary()})

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        result.daily.to_csv(os.path.join(args.out, "daily.csv"))
        result.trades.to_csv(os.path.join(args.out, "trades.csv"), index=False)
        bench.daily.to_csv(os.path.join(args.out, "benchmark_daily.csv"))
        print(f"wrote CSVs to {args.out}/")
    if args.plot:
        plot_equity({strategy.name: result, f"{benchmark.name} (benchmark)": bench}, args.plot)
        print(f"wrote chart to {args.plot}")


PCT_KEYS = {"total_return", "cagr", "volatility", "max_drawdown", "time_in_market"}


def print_summary(summaries: dict[str, dict]) -> None:
    table = pd.DataFrame(summaries)
    for key in table.index:
        fmt = (lambda v: f"{v:.2%}") if key in PCT_KEYS else (
            (lambda v: f"{v:.2f}") if key == "sharpe" else str)
        table.loc[key] = [fmt(v) for v in table.loc[key]]
    with pd.option_context("display.width", 200, "display.max_colwidth", 80):
        print(table.to_string())


def cmd_dry_run(args) -> None:
    from trading.live import run_dry_run

    run_dry_run(make_schwab_client(), build_strategy(args.strategy, args.param))


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m trading", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show available strategies and their params")

    def strategy_args(sp):
        sp.add_argument("-s", "--strategy", required=True, help="strategy name or path to a .py file")
        sp.add_argument("-p", "--param", action="append", default=[], metavar="KEY=VALUE",
                        help="override a strategy param (repeatable)")
        sp.add_argument("-v", "--verbose", action="store_true")

    bt = sub.add_parser("backtest", help="simulate a strategy on daily bars")
    strategy_args(bt)
    bt.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2010, 1, 1))
    bt.add_argument("--end", type=dt.date.fromisoformat, help="default: last completed session")
    bt.add_argument("--cash", type=float, default=10_000.0)
    bt.add_argument("--slippage-bps", type=float, default=0.0, help="per-fill slippage in basis points")
    bt.add_argument("--source", choices=["yahoo", "schwab"], default="yahoo")
    bt.add_argument("--benchmark", default="buy_and_hold")
    bt.add_argument("--benchmark-param", action="append", default=[], metavar="KEY=VALUE")
    bt.add_argument("--cache-dir", default=".cache")
    bt.add_argument("--no-cache", action="store_true")
    bt.add_argument("--out", help="directory for daily/trades CSVs")
    bt.add_argument("--plot", help="write an equity-curve PNG here")

    dr = sub.add_parser("dry-run", help="today's target and the orders it WOULD place (sends nothing)")
    strategy_args(dr)
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO if getattr(args, "verbose", False) else logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s")
    {"list": cmd_list, "backtest": cmd_backtest, "dry-run": cmd_dry_run}[args.command](args)
