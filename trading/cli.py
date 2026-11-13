"""Command line: list strategies, run and compare backtests, manage the database, dry-run live.

trading list
trading backtest -s spy_vix --start 2010-01-01 --plot equity.png
trading backtest -s spy_vix -p vix_high=25 -p spy_low=-0.01 --slippage-bps 5 --label "wider vix"
trading backtest -s path/to/my_strategy.py
trading runs list
trading runs show 3 7 --plot compare.png
trading db upgrade
trading db import-csv
trading dry-run -s spy_vix
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import math
import os
import sys

import pandas as pd

from trading.config import get_settings
from trading.data import (
    CsvStore,
    PostgresStore,
    SchwabSource,
    YahooSource,
    import_csv_cache,
    last_complete_session,
    load_bars,
)
from trading.engine import plot_equity, run_backtest
from trading.strategy import available_strategies, load_strategy, parse_params


def make_schwab_client():
    import schwabdev

    s = get_settings()
    if not s.schwab_app_key or not s.schwab_app_secret:
        raise SystemExit("SCHWAB_APP_KEY / SCHWAB_APP_SECRET missing: see .env.example")
    return schwabdev.Client(s.schwab_app_key, s.schwab_app_secret, s.schwab_callback_url)


def database_url() -> str:
    url = get_settings().database_url
    if not url:
        raise SystemExit("DATABASE_URL is not set: see .env.example")
    return url


def db_engine():
    from trading.db import require_migrated

    return require_migrated(database_url())


def build_strategy(name: str, params: list[str]):
    cls = load_strategy(name)
    return cls(**parse_params(cls, params))


# --------------------------------------------------------------------------- commands


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

    store = args.store
    if store == "auto":
        store = "postgres" if get_settings().database_url else "csv"
    engine = db_engine() if store == "postgres" or args.save else None

    source = YahooSource() if args.source == "yahoo" else SchwabSource(make_schwab_client())
    if store == "postgres":
        source = PostgresStore(source, engine)
    elif store == "csv":
        source = CsvStore(source, args.cache_dir)
    bars = load_bars(source, symbols, fetch_start, end)

    result = run_backtest(strategy, bars, args.start, end, args.cash, args.slippage_bps)
    if result.daily.empty:
        raise SystemExit("no days simulated (not enough data after warm-up?)")
    # Benchmark over exactly the days the strategy traded, for a fair comparison.
    bench = run_backtest(
        benchmark, bars, result.daily.index[0], result.daily.index[-1], args.cash, args.slippage_bps
    )

    if args.verbose:
        with pd.option_context("display.width", 200, "display.max_columns", 20, "display.max_rows", 60):
            print(result.daily)
            print(result.trades)
    print_summary({result.strategy: result.summary(), bench.strategy: bench.summary()})

    save = args.save if args.save is not None else store == "postgres"
    if save:
        from trading.db.runs import save_run

        run_id = save_run(
            engine,
            strategy=strategy,
            benchmark=benchmark,
            result=result,
            bench=bench,
            source=args.source,
            slippage_bps=args.slippage_bps,
            label=args.label,
        )
        print(f"saved as run #{run_id}")

    if args.out:
        os.makedirs(args.out, exist_ok=True)
        result.daily.to_csv(os.path.join(args.out, "daily.csv"))
        result.trades.to_csv(os.path.join(args.out, "trades.csv"), index=False)
        bench.daily.to_csv(os.path.join(args.out, "benchmark_daily.csv"))
        print(f"wrote CSVs to {args.out}/")
    if args.plot:
        plot_equity({strategy.name: result, f"{benchmark.name} (benchmark)": bench}, args.plot)
        print(f"wrote chart to {args.plot}")


PCT_KEYS = {"total_return", "cagr", "volatility", "max_drawdown", "time_in_market", "max_dd", "bench_cagr"}
RATIO_KEYS = {"sharpe", "bench_sharpe"}


def _fmt(key, v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "-"
    if key in PCT_KEYS:
        return f"{v:.2%}"
    if key in RATIO_KEYS:
        return f"{v:.2f}"
    return str(v)


def print_summary(summaries: dict[str, dict]) -> None:
    table = pd.DataFrame(summaries)
    for key in table.index:
        table.loc[key] = [_fmt(key, v) for v in table.loc[key]]
    with pd.option_context("display.width", 200, "display.max_colwidth", 80):
        print(table.to_string())


def cmd_runs(args) -> None:
    from trading.db.runs import list_runs, load_run

    engine = db_engine()
    if args.runs_command == "list":
        runs = list_runs(engine, strategy=args.strategy, limit=args.limit)
        if runs.empty:
            print("no saved runs yet: run a backtest with DATABASE_URL set")
            return
        for col in runs.columns:
            runs[col] = [_fmt(col, v) for v in runs[col]]
        with pd.option_context("display.width", 250, "display.max_colwidth", 60):
            print(runs.to_string(index=False))
        return

    # show
    summaries, curves = {}, {}
    for run_id in args.ids:
        try:
            meta, results = load_run(engine, run_id)
        except LookupError as e:
            raise SystemExit(str(e)) from None
        res = results["strategy"]
        summaries[res.strategy] = res.summary()
        curves[res.strategy] = res
        if len(args.ids) == 1 or args.benchmarks:
            b = results["benchmark"]
            summaries[f"{b.strategy} (benchmark)"] = b.summary()
            curves[f"{b.strategy} (benchmark)"] = b
        if len(args.ids) == 1:
            sha = (meta["git_sha"] or "unknown")[:7] + (" (uncommitted changes)" if meta["git_dirty"] else "")
            print(
                f"run #{run_id}  created {meta['created_at']:%Y-%m-%d %H:%M}  source={meta['source']}  "
                f"slippage={meta['slippage_bps']}bps  commit {sha}"
                + (f"  label={meta['label']!r}" if meta["label"] else "")
            )
    print_summary(summaries)
    if args.plot:
        plot_equity(curves, args.plot)
        print(f"wrote chart to {args.plot}")


def cmd_db(args) -> None:
    from trading import db

    url = database_url()
    if args.db_command == "upgrade":
        db.upgrade(url)
        print("database schema is up to date")
    elif args.db_command == "status":
        current, head = db.migration_status(url)
        print(f"database: {current}\nlatest:   {head}")
        print("up to date" if current == head else "NEEDS UPGRADE: run `trading db upgrade`")
    elif args.db_command == "import-csv":
        for source, symbol, n in import_csv_cache(args.cache_dir, db_engine()):
            print(f"imported {n:>6} bars  {source}/{symbol}")


def cmd_dry_run(args) -> None:
    from trading.live import run_dry_run

    run_dry_run(make_schwab_client(), build_strategy(args.strategy, args.param))


# --------------------------------------------------------------------------- arguments


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="trading", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="show available strategies and their params")

    def strategy_args(sp):
        sp.add_argument("-s", "--strategy", required=True, help="strategy name or path to a .py file")
        sp.add_argument(
            "-p",
            "--param",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help="override a strategy param (repeatable)",
        )
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
    bt.add_argument(
        "--store",
        choices=["auto", "postgres", "csv", "none"],
        default="auto",
        help="where price bars are cached (auto: postgres if DATABASE_URL is set, else csv)",
    )
    bt.add_argument("--cache-dir", default=".cache", help="directory for --store csv")
    bt.add_argument(
        "--save",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="save the run to the database (default: yes when using postgres)",
    )
    bt.add_argument("--label", help="short note stored with the run")
    bt.add_argument("--out", help="directory for daily/trades CSVs")
    bt.add_argument("--plot", help="write an equity-curve PNG here")

    runs = sub.add_parser("runs", help="list and compare saved backtest runs")
    runs_sub = runs.add_subparsers(dest="runs_command", required=True)
    rl = runs_sub.add_parser("list", help="most recent runs first")
    rl.add_argument("-s", "--strategy", help="only this strategy")
    rl.add_argument("-n", "--limit", type=int, default=20)
    rs = runs_sub.add_parser("show", help="summary of one run, or several side by side")
    rs.add_argument("ids", type=int, nargs="+")
    rs.add_argument("--benchmarks", action="store_true", help="include each run's benchmark")
    rs.add_argument("--plot", help="write their equity curves to this PNG")

    dbp = sub.add_parser("db", help="database schema and data")
    db_sub = dbp.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("upgrade", help="create or migrate tables to the latest schema")
    db_sub.add_parser("status", help="show the schema version")
    ic = db_sub.add_parser("import-csv", help="copy the .cache/ CSV bars into Postgres")
    ic.add_argument("--cache-dir", default=".cache")

    dr = sub.add_parser("dry-run", help="today's target and the orders it WOULD place (sends nothing)")
    strategy_args(dr)
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if getattr(args, "verbose", False) else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    commands = {
        "list": cmd_list,
        "backtest": cmd_backtest,
        "runs": cmd_runs,
        "db": cmd_db,
        "dry-run": cmd_dry_run,
    }
    commands[args.command](args)
