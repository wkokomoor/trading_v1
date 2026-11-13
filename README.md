# trading_v1

A backtesting framework for daily trading strategies, where each strategy lives in its own file. The framework also has a read-only "dry run" that shows what a strategy would trade today in a Schwab account.

## Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/) and pinned in `uv.lock`, so your machine, CI and the cloud all install identical versions.

```sh
brew install uv        # or: curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync                # creates .venv with exact locked versions (Python 3.13)
cp .env.example .env   # Schwab keys + database URLs
```

To add a dependency, run `uv add <package>` (or `uv add --dev <package>`). This updates both `pyproject.toml` and `uv.lock`; commit both.

### Database (PostgreSQL)

Price bars and backtest runs are stored in PostgreSQL. Locally, it runs in Docker (Docker Desktop must be running):

```sh
docker compose -f deploy/docker-compose.yml up -d    # starts Postgres with databases `trading` and `trading_test`
uv run trading db upgrade                            # create or migrate the tables
uv run trading db import-csv                         # optional: load an existing .cache/ into Postgres
```

`DATABASE_URL` and `TEST_DATABASE_URL` in `.env` point at these databases (see `.env.example`). Without `DATABASE_URL`, everything still works: bars are cached as CSV files under `.cache/` and runs aren't saved.

To stop Postgres, run `docker compose -f deploy/docker-compose.yml down`. Your data stays in a Docker volume; add `-v` to delete it.

## Usage

```sh
uv run trading list                                   # strategies and their params

uv run trading backtest -s spy_vix                    # 2010 to now vs. buy-and-hold SPY
uv run trading backtest -s spy_vix -p short=          # long-only variant
uv run trading backtest -s spy_vix -p vix_high=25 --slippage-bps 5 --start 2018-01-01
uv run trading backtest -s sma_trend --benchmark-param symbol=UPRO
uv run trading backtest -s spy_vix --plot results/equity.png --out results/ -v
uv run trading backtest -s spy_vix -p vix_high=25 --label "wider vix band"   # note stored with the run

uv run trading backtest -s vol_filter                 # current candidate: UPRO when SPY's 20-day vol < 20%, else cash

uv run trading sweep -s vix_gap -g target_vol=0,0.45,0.75 -g trend_window=0,200   # tune on 2010-19, check on 2020+
uv run trading walkforward -s vol_filter -g threshold=0.15,0.2,0.25 --reference threshold=0.2
                                                      # re-tune yearly vs. a fixed choice, out of sample
uv run trading feasibility --measure spy_vix --plot feasibility.png               # would a perfect / p%-accurate
                                                                                  # indicator pay? how accurate is this one?

uv run trading runs list                              # saved runs, newest first (-s spy_vix to filter)
uv run trading runs show 4                            # one run vs. its benchmark
uv run trading runs show 1 3 4 --plot compare.png     # several runs side by side

uv run trading schwab auth                            # Schwab browser login (in a terminal; weekly)
uv run trading schwab check                           # read-only connection test + data-depth report
uv run trading dry-run -s spy_vix                     # today's orders, NOT sent
```

Idle cash earns the historical 13-week T-bill yield by default (`--cash-rate tbill`), or pass a fixed rate such as `--cash-rate 0.04`.

Backtests print a side-by-side summary of the strategy against the benchmark: total return, CAGR, volatility, Sharpe, max drawdown, trade count and time in market. `-v` prints every day and every trade, `--out` saves CSVs, and `--plot` saves an equity-curve chart.

When `DATABASE_URL` is set, every backtest is saved as a numbered run. A run stores its params, its summary, the daily equity of both the strategy and the benchmark, its trades, and the git commit it ran on (`*` marks uncommitted changes). Pass `--no-save` to skip saving one.

### How a backtest works

- Daily bars come from Yahoo Finance by default (`--source schwab` is also available). Prices are adjusted for splits and dividends.
- Bars are fetched once and then served from storage: Postgres when `DATABASE_URL` is set, otherwise CSV files in `.cache/`. Use `--store` to choose explicitly.
- Each session, the strategy decides at the **open** and orders fill at the open price, plus `--slippage-bps` if set.
- Portfolio equity is valued at the **close**.
- Trades only happen when the strategy's target changes. Share counts are whole numbers and cash never goes negative.
- The benchmark is run over exactly the same days as the strategy.

## Shadow trading (paper trading)

A **shadow portfolio** runs a strategy on live Schwab data every morning and records the orders it would place, filled at live bid/ask prices. **No orders are sent to Schwab.** Schwab has no paper-trading API, so this is how a strategy is proven before real money.

```sh
uv run trading schwab auth                                       # weekly: Schwab login (in a terminal)
uv run trading portfolio create shadow-vol -s vol_filter --allocation 10000
uv run trading job trade          # each trading day after 09:31 ET: decide, risk-check, simulate fills
uv run trading job ingest         # after 16:15 ET: refresh daily bars
uv run trading job reconcile      # then: mark shadow portfolios at the close
uv run trading portfolio show shadow-vol                         # state, daily runs, orders
uv run trading kill-switch on --reason "..."                     # block all trading (or TRADING_KILL_SWITCH=1)
```

`job trade` is safe to re-run: each portfolio trades at most once per day, and on weekends, holidays or before the open it exits as "skipped". Every order passes the risk checks in `trading/risk.py` first: the kill switch, data sanity, the strategy's symbol list, a daily loss limit and the allocation cap.

## Writing a strategy

Add a file to `strategies/`. It will appear in `list` and you can run it with `-s <name>`. You can also pass any file path with `-s path/to/file.py`.

```python
from trading.strategy import MarketView, Strategy


class Momentum(Strategy):
    name = "momentum"  # must match the filename for `-s momentum`
    symbols = ("SPY", "UPRO")  # every symbol you read or trade
    params = {"window": 50}  # defaults; override with -p window=20
    lookback = 50  # completed bars needed before the first decision

    def on_open(self, view: MarketView):
        closes = view.history("SPY", self.p.window)["close"]  # completed bars before today
        today = view.open("SPY")  # today's open
        self.info = {"sma": closes.mean()}  # optional: shows up in -v output
        return {"UPRO": 1.0} if today > closes.mean() else {}
```

`on_open` returns **target weights**:

| Return value | Meaning |
|---|---|
| `{"UPRO": 1.0}` | put everything in UPRO |
| `{"UPRO": 0.6, "SPY": 0.4}` | split the portfolio between the two |
| `{}` | sell everything and hold cash |
| `None` | keep the current holdings |

Weights must be ≥ 0 and add up to at most 1, because shorting and margin aren't modeled.

A strategy can only see completed bars before today plus today's open, so it can't accidentally peek at future prices. "VIX" is a canonical symbol, and each data source maps it to its own ticker.

When `symbols` or `lookback` depend on params, override `required_symbols()` or `required_lookback()`; `sma_trend.py` is an example.

### Included strategies

| Name | Idea |
|---|---|
| `vol_filter` | **Current candidate.** Hold UPRO while SPY's 20-day realized volatility is below 20%, otherwise cash. See docs/RESEARCH.md Part 5. |
| `spy_vix` | The original VIX/SPY-gap switch (rejected; see docs/RESEARCH.md) between UPRO and SPXU. `-p short=` makes it go to cash instead of shorting. |
| `vix_gap` | SPY/VIX v2: gap rule plus VIX-based sizing and an optional trend filter. Research only. |
| `rf_direction` | Random forest on VIX/SPY features (plus ADD/TICK when available); long UPRO when P(up) ≥ threshold. **No skill found** with VIX, SPY or Schwab ADD/TICK features (docs/RESEARCH.md Part 7). |
| `sma_trend` | Hold UPRO while SPY is above its 200-day average, otherwise cash. |
| `buy_and_hold` | Hold one symbol. This is the default benchmark. |

## Tests and checks

```sh
uv run pytest              # tests
uv run ruff check .        # lint
uv run ruff format .       # auto-format
```

Database tests use `TEST_DATABASE_URL` and are skipped when it isn't set. The test database is wiped on every run, so its name must end in `_test`. GitHub Actions runs everything, database tests included, on every push and pull request (`.github/workflows/ci.yml`).

### Changing the database schema

1. Edit the tables in `trading/db/models.py`.
2. Run `uv run alembic revision --autogenerate -m "what changed"`, then review the generated file in `trading/db/migrations/versions/`.
3. Run `uv run trading db upgrade`.

A test fails if `models.py` and the migrations ever disagree.

## Roadmap

See [docs/DEVELOPMENT_PLAN.md](docs/DEVELOPMENT_PLAN.md) for the target architecture (GCP Cloud Run Jobs, Cloud SQL for PostgreSQL) and the phased plan.
