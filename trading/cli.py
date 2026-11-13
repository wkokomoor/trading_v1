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


def make_schwab_client(interactive: bool = True):
    from trading.schwab import make_client

    return make_client(interactive)


def cmd_schwab(args) -> None:
    from trading import schwab

    if args.schwab_command == "auth":
        print("Opening Schwab login. After approving, your browser lands on the callback URL")
        print("(it may show 'site can't be reached'; that's expected). Copy the FULL address bar URL")
        print("and paste it here.\n")
        client = schwab.make_client(interactive=True)
        client.update_tokens(force_refresh_token=args.force)
        print("\nLogged in. Tokens saved to ~/.schwabdev/tokens.db (valid 7 days).")
        print("Test with: uv run trading schwab check")
        return

    try:
        client = schwab.make_client(interactive=False)
        rows = schwab.check(client)
    except schwab.LoginRequired as e:
        raise SystemExit(str(e)) from None
    width = max(len(k) for k, _ in rows)
    for k, v in rows:
        print(f"{k:<{width}}  {v}")


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


def load_market(args, strategies, end: dt.date, want_db: bool = False, extra_symbols=()):
    """Bars for every symbol the given strategies need, plus the DB engine if one is in use."""
    lookback = max([s.required_history() for s in strategies] or [1])
    fetch_start = args.start - dt.timedelta(days=math.ceil(lookback * 1.6) + 15)
    if getattr(args, "cash_rate", "0") == "tbill":
        extra_symbols = (*extra_symbols, "IRX")
    symbols = tuple(
        dict.fromkeys([*extra_symbols, *(sym for s in strategies for sym in s.required_symbols())])
    )

    store = args.store
    if store == "auto":
        store = "postgres" if get_settings().database_url else "csv"
    engine = db_engine() if store == "postgres" or want_db else None

    source = YahooSource() if args.source == "yahoo" else SchwabSource(make_schwab_client())
    if store == "postgres":
        source = PostgresStore(source, engine)
    elif store == "csv":
        source = CsvStore(source, args.cache_dir)
    return load_bars(source, symbols, fetch_start, end), engine, store


def cash_yield(args, bars):
    """Annual interest on idle cash: a constant, or the 13-week T-bill yield by date."""
    if args.cash_rate == "tbill":
        return bars["IRX"]["close"] / 100
    try:
        return float(args.cash_rate)
    except ValueError:
        raise SystemExit(f"--cash-rate must be a number or 'tbill', got {args.cash_rate!r}") from None


def cmd_backtest(args) -> None:
    strategy = build_strategy(args.strategy, args.param)
    benchmark = build_strategy(args.benchmark, args.benchmark_param)

    end = args.end or last_complete_session()
    bars, engine, store = load_market(args, [strategy, benchmark], end, want_db=bool(args.save))

    cy = cash_yield(args, bars)
    result = run_backtest(strategy, bars, args.start, end, args.cash, args.slippage_bps, cy)
    if result.daily.empty:
        raise SystemExit("no days simulated (not enough data after warm-up?)")
    # Benchmark over exactly the days the strategy traded, for a fair comparison.
    bench = run_backtest(
        benchmark, bars, result.daily.index[0], result.daily.index[-1], args.cash, args.slippage_bps, cy
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
            cash_rate=str(args.cash_rate),
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


def cmd_sweep(args) -> None:
    from trading import research

    cls = load_strategy(args.strategy)
    base = parse_params(cls, args.param)
    grid = research.expand_grid(cls, args.grid)
    benchmark = build_strategy(args.benchmark, args.benchmark_param)

    end = args.end or last_complete_session()
    periods = research.split_periods(args.start, end, args.split)
    variants = [cls(**{**base, **combo}) for combo in grid]
    bars, _, _ = load_market(args, [*variants, benchmark], end)

    print(
        f"{len(grid)} combinations x {len(periods)} periods: "
        + ", ".join(f"{name} {s}..{e}" for name, (s, e) in periods.items())
    )
    cy = cash_yield(args, bars)
    table = research.sweep(cls, base, grid, bars, periods, args.cash, args.slippage_bps, cy)
    first = next(iter(periods))
    rank_col = f"{first}_{args.rank}"
    # Higher is better for all rank metrics (drawdowns are negative numbers).
    table = table.sort_values(rank_col, ascending=False).head(args.top)

    bench = research.benchmark_rows(benchmark, bars, periods, args.cash, args.slippage_bps, cy)
    bench_label = {k: "" for k in grid[0]} | {next(iter(grid[0])): f"[{benchmark!r}]"}
    table = pd.concat([pd.DataFrame([{**bench_label, **bench}]), table], ignore_index=True)

    for col in table.columns:
        table[col] = [_fmt(col.split("_", 1)[-1] if "_" in col else col, v) for v in table[col]]
    print(f"ranked by {rank_col} (choose using {first} columns only)")
    with pd.option_context("display.width", 250, "display.max_columns", 40):
        print(table.to_string(index=False))


def cmd_walkforward(args) -> None:
    from trading import research
    from trading.engine import BacktestResult, performance

    cls = load_strategy(args.strategy)
    base = parse_params(cls, args.param)
    grid = research.expand_grid(cls, args.grid)
    benchmark = build_strategy(args.benchmark, args.benchmark_param)
    references = [
        cls(**{**base, **parse_params(cls, [p for p in ref.split(",") if p])}) for ref in args.reference
    ]

    end = args.end or last_complete_session()
    variants = [cls(**{**base, **combo}) for combo in grid]
    bars, _, _ = load_market(args, [*variants, benchmark, *references], end)
    cy = cash_yield(args, bars)

    print(
        f"{len(grid)} combinations; each year from {args.first_test_year} is traded with the combination "
        f"that had the best {args.rank} from {args.start} through the previous year."
    )
    years, oos = research.walk_forward(
        cls,
        base,
        grid,
        bars,
        args.start,
        end,
        args.first_test_year,
        args.rank,
        args.cash,
        args.slippage_bps,
        cy,
    )
    oos_days = oos.index

    def oos_curve(strategy):
        """Equity (1.0 before the first out-of-sample day) over exactly the OOS days.

        Runs from --start, like the walk-forward combos, so positions on day one are realistic.
        """
        eq = run_backtest(strategy, bars, args.start, oos_days[-1], args.cash, args.slippage_bps, cy)
        rets = eq.daily["equity"].pct_change().reindex(oos_days).fillna(0.0)
        return (1 + rets).cumprod()

    curves = {"walk-forward": oos, f"{benchmark.name}": oos_curve(benchmark)}
    for spec, ref in zip(args.reference, references, strict=True):
        curves[f"fixed {spec}"] = oos_curve(ref)

    def year_returns(eq):
        yearly = eq.groupby([d.year for d in eq.index]).last()
        return yearly / yearly.shift(1).fillna(1.0) - 1

    table = years.rename(columns={"year_return": "walk-forward return"})
    for label, eq in list(curves.items())[1:]:
        table[f"{label} return"] = table["year"].map(year_returns(eq))
    for col in table.columns:
        if col.endswith("return"):
            table[col] = [_fmt("total_return", v) for v in table[col]]
    table[f"train_{args.rank}"] = [f"{v:.2f}" for v in table[f"train_{args.rank}"]]
    with pd.option_context("display.width", 250, "display.max_columns", 30):
        print(table.to_string(index=False))

    summaries = {}
    for label, eq in curves.items():
        perf = performance(eq, 1.0)
        summaries[label] = {
            "period": f"{oos_days[0]}..{oos_days[-1]}",
            **{k: perf[k] for k in ("total_return", "cagr", "volatility", "sharpe", "max_drawdown")},
        }
    print()
    print_summary(summaries)

    if args.plot:
        results = {
            label: BacktestResult(label, pd.DataFrame({"equity": eq * args.cash}), pd.DataFrame(), args.cash)
            for label, eq in curves.items()
        }
        plot_equity(results, args.plot)
        print(f"wrote chart to {args.plot}")


def parse_measure(spec: str):
    """'spy_vix' or 'spy_vix:vix_high=25,short=' -> strategy instance."""
    name, _, params = spec.partition(":")
    return build_strategy(name, [p for p in params.split(",") if p] if params else [])


def cmd_feasibility(args) -> None:
    import numpy as np

    from trading import feasibility as F

    measured_strategies = [parse_measure(m) for m in args.measure]
    labels = dict(zip(map(id, measured_strategies), args.measure, strict=True))
    end = args.end or last_complete_session()
    bars, _, _ = load_market(
        args, measured_strategies, end, extra_symbols=(args.index, args.long, args.short)
    )
    rets = F.daily_returns(bars, args.start, end, args.index, args.long, args.short)
    n, slip = len(rets), args.slippage_bps
    print(
        f"{rets.index[0]}..{rets.index[-1]}: {n} days, decide at each open and hold to the next open, "
        f"{slip:g} bps slippage per fill.\n{args.index} rose on {(rets['index'] >= 0).mean():.1%} of days."
    )

    def run(pos):
        return F.metrics(F.simulate(pos, rets, slip)).iloc[0]

    spy_hold = F.metrics(np.cumprod(1 + rets["index"].to_numpy())).iloc[0]
    long_hold = run(np.ones(n, dtype=np.int8))
    references = {f"hold {args.index}": spy_hold["cagr"], f"hold {args.long}": long_hold["cagr"]}

    print("\n1) PERFECT FORESIGHT (the ceiling)")
    rows = {f"hold {args.index}": spy_hold, f"hold {args.long}": long_hold}
    for mode in F.MODES:
        pos = F.oracle_positions(rets["index"], mode)
        rows[f"oracle {mode}"] = run(pos)
    table = pd.DataFrame(rows).T
    table["multiple"] = [f"{v:,.3g}x" for v in table["multiple"]]
    for col in ("cagr", "max_drawdown"):
        table[col] = [f"{v:.1%}" for v in table[col]]
    table["sharpe"] = [f"{v:.2f}" for v in table["sharpe"]]
    print(table.to_string())

    print(f"\n2) IMPERFECT FORESIGHT ({args.sims} random indicators per accuracy level)")
    accuracies = np.round(np.arange(args.min_accuracy, args.max_accuracy + 1e-9, 0.01), 3)
    curves = {}
    for mode in F.MODES:
        c = F.accuracy_curve(rets, accuracies, args.sims, slip, mode)
        curves[mode] = c
        shown = c[["accuracy", "cagr_p10", "cagr_median", "cagr_p90", "sharpe_median", "max_dd_median"]]
        shown = shown.copy()
        shown["accuracy"] = [f"{v:.0%}" for v in shown["accuracy"]]
        for col in ("cagr_p10", "cagr_median", "cagr_p90", "max_dd_median"):
            shown[col] = [f"{v:.1%}" for v in shown[col]]
        shown["sharpe_median"] = [f"{v:.2f}" for v in shown["sharpe_median"]]
        print(f"\n{mode}:")
        print(shown.to_string(index=False))

    print("\n3) ACCURACY NEEDED (median outcome)")
    targets = {"break even (0%)": 0.0, **{f"beat {k}": v for k, v in references.items()}}
    need = pd.DataFrame(
        {
            mode: {
                t: (f"{a:.1%}" if (a := F.break_even(c, v)) is not None else "> range")
                for t, v in targets.items()
            }
            for mode, c in curves.items()
        }
    )
    print(need.to_string())

    measured = {}
    if measured_strategies:
        print("\n4) MEASURED INDICATORS (balanced accuracy: always-long and coin flips score 50%)")
        for strat in measured_strategies:
            res = run_backtest(strat, bars, args.start, end, args.cash, slip, cash_yield(args, bars))
            holding = res.daily["holding"].reindex(rets.index).fillna("CASH")
            pos = np.where(holding == args.long, 1, np.where(holding == args.short, -1, 0))
            acc = F.balanced_accuracy(pos, rets["index"])
            cagr = res.summary()["cagr"]
            measured[labels[id(strat)]] = (acc, cagr)
            print(f"  {strat!r}\n    balanced accuracy {acc:.2%}, CAGR {cagr:.1%}")

    if args.plot:
        F.plot_accuracy(curves, references, measured, args.plot)
        print(f"\nwrote chart to {args.plot}")


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

    def market_args(sp, slippage_default=0.0):
        sp.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2010, 1, 1))
        sp.add_argument("--end", type=dt.date.fromisoformat, help="default: last completed session")
        sp.add_argument("--cash", type=float, default=10_000.0)
        sp.add_argument(
            "--slippage-bps",
            type=float,
            default=slippage_default,
            help=f"per-fill slippage in basis points (default {slippage_default:g})",
        )
        sp.add_argument("--source", choices=["yahoo", "schwab"], default="yahoo")
        sp.add_argument("--benchmark", default="buy_and_hold")
        sp.add_argument("--benchmark-param", action="append", default=[], metavar="KEY=VALUE")
        sp.add_argument(
            "--store",
            choices=["auto", "postgres", "csv", "none"],
            default="auto",
            help="where price bars are cached (auto: postgres if DATABASE_URL is set, else csv)",
        )
        sp.add_argument("--cache-dir", default=".cache", help="directory for --store csv")
        sp.add_argument(
            "--cash-rate",
            default="tbill",
            help="annual interest on idle cash: a number like 0.04, or 'tbill' for the historical "
            "13-week T-bill yield (default; Yahoo data)",
        )

    bt = sub.add_parser("backtest", help="simulate a strategy on daily bars")
    strategy_args(bt)
    market_args(bt)
    bt.add_argument(
        "--save",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="save the run to the database (default: yes when using postgres)",
    )
    bt.add_argument("--label", help="short note stored with the run")
    bt.add_argument("--out", help="directory for daily/trades CSVs")
    bt.add_argument("--plot", help="write an equity-curve PNG here")

    sw = sub.add_parser(
        "sweep",
        help="backtest a grid of params, tuned on a train period and checked on a test period",
    )
    strategy_args(sw)
    market_args(sw, slippage_default=5.0)
    sw.add_argument(
        "-g",
        "--grid",
        action="append",
        required=True,
        metavar="KEY=V1,V2,...",
        help="values to try for a param (repeatable; all combinations are run)",
    )
    sw.add_argument(
        "--split",
        type=dt.date.fromisoformat,
        default=dt.date(2020, 1, 1),
        help="first day of the test period (default 2020-01-01)",
    )
    sw.add_argument("--rank", choices=["sharpe", "cagr", "max_drawdown"], default="sharpe")
    sw.add_argument("--top", type=int, default=20, help="show the best N combinations")

    wf = sub.add_parser(
        "walkforward",
        help="re-tune every year on all prior years, trade the next year: an honest out-of-sample test",
    )
    strategy_args(wf)
    market_args(wf, slippage_default=5.0)
    wf.add_argument("-g", "--grid", action="append", required=True, metavar="KEY=V1,V2,...")
    wf.add_argument("--first-test-year", type=int, default=2015)
    wf.add_argument("--rank", choices=["sharpe", "cagr", "max_drawdown"], default="sharpe")
    wf.add_argument(
        "--reference",
        action="append",
        default=[],
        metavar="K=V,K=V",
        help="also show a fixed param choice for comparison, e.g. window=20,threshold=0.2 (repeatable)",
    )
    wf.add_argument("--plot", help="write the out-of-sample equity curves to this PNG")

    fe = sub.add_parser(
        "feasibility",
        help="oracle test: what would long/short/cash earn with a perfect or p%%-accurate indicator?",
    )
    market_args(fe, slippage_default=5.0)
    fe.add_argument("--index", default="SPY")
    fe.add_argument("--long", default="UPRO")
    fe.add_argument("--short", default="SPXU")
    fe.add_argument("--sims", type=int, default=500, help="random indicators per accuracy level")
    fe.add_argument("--min-accuracy", type=float, default=0.46)
    fe.add_argument("--max-accuracy", type=float, default=0.60)
    fe.add_argument(
        "--measure",
        action="append",
        default=[],
        metavar="STRATEGY[:k=v,...]",
        help="measure a strategy's balanced accuracy, e.g. spy_vix or spy_vix:vix_high=25 (repeatable)",
    )
    fe.add_argument("--plot", help="write the accuracy chart to this PNG")
    fe.add_argument("-v", "--verbose", action="store_true")

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

    sc = sub.add_parser("schwab", help="Schwab API login and connection check")
    sc_sub = sc.add_subparsers(dest="schwab_command", required=True)
    au = sc_sub.add_parser("auth", help="interactive browser login (run in a terminal; needed weekly)")
    au.add_argument("--force", action="store_true", help="log in again even if the current login is valid")
    sc_sub.add_parser("check", help="read-only connection test and data-depth check (no balances shown)")

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
        "sweep": cmd_sweep,
        "feasibility": cmd_feasibility,
        "walkforward": cmd_walkforward,
        "runs": cmd_runs,
        "db": cmd_db,
        "dry-run": cmd_dry_run,
        "schwab": cmd_schwab,
    }
    commands[args.command](args)
