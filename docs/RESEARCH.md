# Research notes: improving the SPY/VIX strategy

*November 2026. Data: Yahoo daily bars from April 2010 to November 2026, with prices adjusted for splits and dividends. All results include 5 bps slippage per fill unless stated otherwise.*

## TL;DR

- **The original `spy_vix` lost about 99% of its value.** Almost all of the damage came from its short leg: holding SPXU when VIX was above 20.
- **High VIX predicts violent markets, not falling ones.** Average UPRO returns were *highest* on days when VIX was 30 or more. VIX is useful for position **sizing**, not for picking direction.
- **The SPY-gap idea is real but small.** UPRO did poorly after gap-up opens and well after gap-down opens, in both the training and test periods. After trading costs, though, it adds almost nothing.
- **Nothing tested beat buy-and-hold SPY on risk-adjusted return** (Sharpe), in or out of sample.
- **The best trade-off found is `vix_gap` with a 200-day trend filter and VIX-based sizing.** It made about 22% a year against SPY's 14%, with a −50% max drawdown against UPRO's −77%. It held up on data it was not tuned on.

![Equity curves](img/candidate_vs_original.png)

| Run (Apr 2010 – Nov 2026, 5 bps) | CAGR | Max drawdown | Sharpe | Trades |
|---|---|---|---|---|
| #6 original `spy_vix` | −14.6% | −98.8% | −0.11 | 287 |
| #7 buy-and-hold SPY | 13.7% | −33.7% | 0.84 | 1 |
| #8 buy-and-hold UPRO | 27.8% | −76.8% | 0.74 | 1 |
| **#4 `vix_gap` candidate** | **22.3%** | **−50.2%** | **0.74** | 232 |

Candidate params: `trend_window=200 risk_off=SPY target_vol=0.75 on_gap_up=hold`. Everything else is at its default.

## Method

To avoid overfitting, all parameter choices were made on **2010–2019 (train)**, and **2020–2026 (test)** was only used to check them. If you pick parameters by looking at test results, the test period silently becomes training data.

### Step 1: Diagnose before tuning

This step needs no strategy code. For each trading day, UPRO, SPXU and SPY returns were measured from that day's open to the next day's open. The days were then grouped by conditions that are known at the open. Script: an ad-hoc pandas analysis; see "Reproducing" below.

| Condition at the open | UPRO Sharpe, train | UPRO Sharpe, test | SPXU Sharpe, train / test |
|---|---|---|---|
| All days | 0.87 | 0.65 | −0.92 / −0.67 |
| VIX < 15 | 0.88 | 0.85 | −0.92 / −0.60 |
| VIX 20–25 | 1.98 | 1.02 | −1.95 / −0.97 |
| VIX 30+ | 2.04 | 1.46 | −2.24 / −1.87 |
| SPY gapped **down** > 0.5% | 1.45 | 1.26 | −1.47 / −1.29 |
| SPY gap flat | 1.16 | 0.65 | −1.18 / −0.60 |
| SPY gapped **up** > 0.5% | −0.26 | 0.10 | 0.13 / −0.20 |
| SPY below its 200-day average | 1.74 | 0.55 | −1.76 / −0.79 |

What this shows:

1. **SPXU had negative returns in nearly every condition.** There was no setup in which shorting paid reliably.
2. **High VIX came with *higher* average UPRO returns, but also enormous volatility**: UPRO's annualized volatility was about 95% in training and 120% in testing when VIX was 30+. With 3x leverage, volatility itself destroys compounded returns (volatility drag). That argues for **holding less when VIX is high**, not for shorting.
3. **The gap effect showed up in both periods.** It's the part of the original idea worth keeping.

### Step 2: Sweep with a train/test split

```sh
uv run trading sweep -s spy_vix -g short=SPXU, -g vix_high=20,25,30
uv run trading sweep -s vix_gap -g target_vol=0,0.3,0.45,0.6,0.75 -g on_gap_up=hold,cash -g spy_high=0.0025,0.005,0.01
uv run trading sweep -s vix_gap -g trend_window=0,200 -g risk_off=,SPY -g target_vol=0,0.45,0.6,0.75 -g on_gap_up=ignore,hold
```

What the sweeps found:

- **Original `spy_vix`:** every variant with the SPXU leg collapsed in the test period, with Sharpe between −0.61 and 0.11. Removing the short helps but doesn't rescue it.
- **Trading the gap daily (`on_gap_up=cash`)** produced about 1,000 trades per period. Slippage consumed most of the edge: test Sharpe was 0.34–0.63, against SPY's 0.79.
- **VIX sizing alone** cut drawdowns roughly in proportion to returns, so Sharpe stayed about the same.
- **Trend filter + VIX sizing + SPY when risk-off** was the most stable family. The chosen combination went from a training Sharpe of 0.77 to a test Sharpe of 0.71, and from 22.7% to 21.7% CAGR. It degraded the least of all the candidates.

## Caveats (read before trusting any number above)

- **Cash earns 0% in the backtester.** In reality, idle cash in a brokerage account earns interest (about 4–5% a year recently). Strategies that sit in cash are understated, most of all the variants with `risk_off=` (cash).
- **One train/test split is weak evidence.** 2020–2026 is a single path. A walk-forward analysis, which re-tunes and re-tests over several rolling windows, is the next step.
- **Fills at the open are idealized.** Real orders fill after the open, and Phase D will measure real slippage. Leveraged ETFs also carry expense ratios and financing costs, which their prices already reflect.
- **The VIX "open" is its first calculated value**, which can be noisy. A live implementation should read VIX a few minutes after 9:30.
- **The −50% max drawdown is a real number.** It happened in 2020 and 2022. Any live allocation has to be sized so a drawdown like that is survivable.

## What "better" means is still an open decision

On risk-adjusted terms, a plain SPY index fund is hard to beat, and nothing here did. Leveraged strategies are a choice to take on more risk for more return. The candidate strategy makes that choice more carefully than holding UPRO outright.

Which objective should drive the next round of research?

| Objective | Implication |
|---|---|
| Best risk-adjusted return (Sharpe) | Hold SPY, or study blends of SPY with cash or bonds. |
| Maximum growth within a drawdown limit (for example −35%) | Tune `vix_gap` sizing toward that limit, and add a drawdown constraint to the sweep. |
| Maximum growth, drawdowns acceptable | UPRO-heavy variants. The trend filter mostly reduces pain without giving up much return. |

## Next steps

1. Walk-forward analysis across rolling windows (2010–14 → 2015, 2010–15 → 2016, and so on).
2. Credit interest on cash, as a parameter: `--cash-rate`.
3. Measure the strategy against the objective chosen above, for example "highest CAGR subject to max drawdown ≥ −35%".
4. Candidate ideas, each tested the same way:
   - VIX term structure (VIX/VIX3M; the data is already wired in as `VIX3M`);
   - realized-volatility sizing instead of VIX;
   - a bond or T-bill ETF instead of SPY for the risk-off asset.

## Reproducing

```sh
docker compose -f deploy/docker-compose.yml up -d && uv run trading db upgrade
uv run trading backtest -s vix_gap -p trend_window=200 -p risk_off=SPY -p target_vol=0.75 --slippage-bps 5
uv run trading backtest -s buy_and_hold -p symbol=UPRO --start 2010-04-13 --slippage-bps 5
uv run trading backtest -s spy_vix --start 2010-04-13 --slippage-bps 5
uv run trading backtest -s buy_and_hold --start 2010-04-13 --slippage-bps 5
uv run trading runs show <ids> --plot docs/img/candidate_vs_original.png
```
