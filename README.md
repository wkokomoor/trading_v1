# trading_v1

A backtesting framework for daily trading strategies, where each strategy lives in its own file. The framework also has a read-only "dry run" that shows what a strategy would trade today in a Schwab account.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env   # only needed for Schwab: add your app key/secret
```

## Usage

```sh
.venv/bin/python -m trading list                                # strategies and their params

.venv/bin/python -m trading backtest -s spy_vix                 # 2010 to now vs. buy-and-hold SPY
.venv/bin/python -m trading backtest -s spy_vix -p short=       # long-only variant
.venv/bin/python -m trading backtest -s spy_vix -p vix_high=25 --slippage-bps 5 --start 2018-01-01
.venv/bin/python -m trading backtest -s sma_trend --benchmark-param symbol=UPRO
.venv/bin/python -m trading backtest -s spy_vix --plot results/equity.png --out results/ -v

.venv/bin/python -m trading dry-run -s spy_vix                  # today's orders, NOT sent
```

Backtests print a side-by-side summary of the strategy against the benchmark: total return, CAGR, volatility, Sharpe, max drawdown, trade count and time in market. `-v` prints every day and every trade, `--out` saves CSVs, and `--plot` saves an equity-curve chart.

### How a backtest works

- Daily bars come from Yahoo Finance by default (`--source schwab` is also available). Prices are adjusted for splits and dividends.
- Bars are cached under `.cache/`. Delete that folder to force a fresh download.
- Each session, the strategy decides at the **open** and orders fill at the open price, plus `--slippage-bps` if set.
- Portfolio equity is valued at the **close**.
- Trades only happen when the strategy's target changes. Share counts are whole numbers and cash never goes negative.
- The benchmark is run over exactly the same days as the strategy.

## Writing a strategy

Add a file to `strategies/`. It will appear in `list` and you can run it with `-s <name>`. You can also pass any file path with `-s path/to/file.py`.

```python
from trading.strategy import MarketView, Strategy


class Momentum(Strategy):
    name = "momentum"                      # must match the filename for `-s momentum`
    symbols = ("SPY", "UPRO")              # every symbol you read or trade
    params = {"window": 50}                # defaults; override with -p window=20
    lookback = 50                          # completed bars needed before the first decision

    def on_open(self, view: MarketView):
        closes = view.history("SPY", self.p.window)["close"]   # completed bars before today
        today = view.open("SPY")                                 # today's open
        self.info = {"sma": closes.mean()}                       # optional: shows up in -v output
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
| `spy_vix` | The original VIX/SPY-gap switch between UPRO and SPXU. `-p short=` makes it go to cash instead of shorting. |
| `sma_trend` | Hold UPRO while SPY is above its 200-day average, otherwise cash. |
| `buy_and_hold` | Hold one symbol. This is the default benchmark. |

## Tests

```sh
.venv/bin/python -m pytest
```

## Roadmap

See [docs/DEVELOPMENT_PLAN.md](docs/DEVELOPMENT_PLAN.md) for the target architecture (GCP Cloud Run Jobs, Cloud SQL for PostgreSQL) and the phased plan.
