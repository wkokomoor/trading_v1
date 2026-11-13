"""Feasibility of a daily long / short / cash strategy with 3x ETFs: the oracle test.

Question: if an indicator told us each morning which way SPY would move until the
next morning, how much could the UPRO / SPXU / cash approach earn, and how often
must the indicator be right for it to work at all?

Model (same as the backtester): decide at each open, hold until the next open,
then rebalance. Daily return of a position is its ETF's open-to-next-open return.
Each switch pays slippage on the exit and on the entry, exactly like SimBroker.

This module may look at future returns (that's the point) so it deliberately does
NOT go through Strategy / MarketView, which forbid it.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

TRADING_DAYS = 252
MODES = ("long_short", "long_cash")


def daily_returns(
    bars: dict[str, pd.DataFrame],
    start: dt.date,
    end: dt.date,
    index: str = "SPY",
    long: str = "UPRO",
    short: str = "SPXU",
) -> pd.DataFrame:
    """Open-to-next-open returns of the index and both ETFs, for days all three trade."""
    opens = pd.DataFrame(
        {k: bars[s]["open"] for k, s in (("index", index), ("long", long), ("short", short))}
    )
    opens = opens.dropna()
    rets = (opens.shift(-1) / opens - 1).dropna()
    return rets[(rets.index >= start) & (rets.index <= end)]


def simulate(positions: np.ndarray, rets: pd.DataFrame, slippage_bps: float = 0.0) -> np.ndarray:
    """Equity curves (starting at 1.0) for positions in {1 long, -1 short, 0 cash}.

    positions: shape (days,) or (sims, days). Returns the same shape.
    """
    pos = np.atleast_2d(positions).astype(np.int8)
    r_long = rets["long"].to_numpy()
    r_short = rets["short"].to_numpy()
    daily = np.where(pos == 1, r_long, np.where(pos == -1, r_short, 0.0))

    slip = slippage_bps / 10_000
    prev = np.concatenate([np.zeros((pos.shape[0], 1), dtype=np.int8), pos[:, :-1]], axis=1)
    changed = pos != prev
    exit_factor = np.where(changed & (prev != 0), 1 - slip, 1.0)
    entry_factor = np.where(changed & (pos != 0), 1 / (1 + slip), 1.0)

    equity = np.cumprod(exit_factor * entry_factor * (1 + daily), axis=1)
    return equity if np.ndim(positions) == 2 else equity[0]


def metrics(equity: np.ndarray) -> pd.DataFrame:
    """CAGR, Sharpe, max drawdown and final multiple for each equity curve (row)."""
    eq = np.atleast_2d(equity)
    years = eq.shape[1] / TRADING_DAYS
    curve = np.concatenate([np.ones((eq.shape[0], 1)), eq], axis=1)
    rets = curve[:, 1:] / curve[:, :-1] - 1
    std = rets.std(axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        sharpe = np.where(std > 0, rets.mean(axis=1) / std * np.sqrt(TRADING_DAYS), np.nan)
    final = curve[:, -1]
    return pd.DataFrame(
        {
            "multiple": final,
            "cagr": np.where(final > 0, final ** (1 / years) - 1, -1.0),
            "sharpe": sharpe,
            "max_drawdown": (curve / np.maximum.accumulate(curve, axis=1) - 1).min(axis=1),
        }
    )


def oracle_positions(r_index: pd.Series, mode: str = "long_short", band: float = 0.0) -> np.ndarray:
    """Perfect foresight: long before up moves, short (or cash) before down moves.

    Moves smaller than `band` in either direction are sat out in cash.
    """
    r = r_index.to_numpy()
    pos = np.where(r > band, 1, np.where(r < -band, -1 if mode == "long_short" else 0, 0))
    return pos.astype(np.int8)


def noisy_positions(r_index: pd.Series, accuracy: float, sims: int, mode: str, seed: int = 0) -> np.ndarray:
    """Random indicators that call the next move's direction correctly with probability `accuracy`.

    long_short: always invested, long if it calls up, short if it calls down.
    long_cash:  long if it calls up, cash if it calls down.
    Each day's error is independent, a simplification (real indicators err in streaks).
    """
    rng = np.random.default_rng(seed)
    truth = np.where(r_index.to_numpy() >= 0, 1, -1)
    correct = rng.random((sims, len(truth))) < accuracy
    call = np.where(correct, truth, -truth)
    if mode == "long_cash":
        call = np.where(call == 1, 1, 0)
    return call.astype(np.int8)


def accuracy_curve(
    rets: pd.DataFrame,
    accuracies,
    sims: int = 500,
    slippage_bps: float = 5.0,
    mode: str = "long_short",
    seed: int = 0,
) -> pd.DataFrame:
    """Distribution of outcomes for random indicators at each accuracy level."""
    rows = []
    for i, p in enumerate(accuracies):
        pos = noisy_positions(rets["index"], p, sims, mode, seed=seed + i)
        m = metrics(simulate(pos, rets, slippage_bps))
        rows.append(
            {
                "accuracy": p,
                "cagr_p10": m["cagr"].quantile(0.10),
                "cagr_median": m["cagr"].median(),
                "cagr_p90": m["cagr"].quantile(0.90),
                "sharpe_median": m["sharpe"].median(),
                "max_dd_median": m["max_drawdown"].median(),
                "switches_per_year": float(
                    (np.diff(pos, axis=1) != 0).sum(axis=1).mean() / (pos.shape[1] / TRADING_DAYS)
                ),
            }
        )
    return pd.DataFrame(rows)


def break_even(curve: pd.DataFrame, target_cagr: float) -> float | None:
    """Lowest accuracy whose median CAGR reaches `target_cagr` (linear interpolation)."""
    acc, med = curve["accuracy"].to_numpy(), curve["cagr_median"].to_numpy()
    above = np.nonzero(med >= target_cagr)[0]
    if len(above) == 0:
        return None
    i = above[0]
    if i == 0:
        return float(acc[0])
    a0, a1, m0, m1 = acc[i - 1], acc[i], med[i - 1], med[i]
    return float(a0 + (target_cagr - m0) * (a1 - a0) / (m1 - m0))


def balanced_accuracy(positions: np.ndarray, r_index: pd.Series) -> float:
    """Mean of (share of up days called up) and (share of down days not called up).

    A position of 1 counts as an "up" call; short or cash counts as "not up". Always-long
    and coin flips both score 50%, so this measures information, not market drift.
    """
    called_up = np.asarray(positions) == 1
    up = r_index.to_numpy() >= 0
    return float((called_up[up].mean() + (~called_up[~up]).mean()) / 2)


# --------------------------------------------------------------------------- chart

COLORS = {"long_short": "#2a78d6", "long_cash": "#eb6834"}  # categorical slots 1, 2
LABELS = {"long_short": "long / short (UPRO / SPXU)", "long_cash": "long / cash (UPRO / cash)"}
INK, INK_2, GRID, SURFACE, REF = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb", "#8a8984"


def plot_accuracy(
    curves: dict[str, pd.DataFrame],
    references: dict[str, float],
    measured: dict[str, tuple[float, float]],
    path: str,
) -> None:
    """Median CAGR (10th-90th percentile band) vs indicator accuracy, per mode.

    references: horizontal lines, label -> CAGR.  measured: label -> (balanced accuracy, CAGR).
    """
    import os

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter, PercentFormatter

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    for mode, c in curves.items():
        x = c["accuracy"] * 100
        ax.fill_between(x, c["cagr_p10"] * 100, c["cagr_p90"] * 100, color=COLORS[mode], alpha=0.15, lw=0)
        ax.plot(
            x,
            c["cagr_median"] * 100,
            color=COLORS[mode],
            lw=2,
            label=f"{LABELS[mode]}: median, shaded = middle 80% of random indicators",
        )
        ax.annotate(
            LABELS[mode].split(" (")[0],
            (x.iloc[-1], c["cagr_median"].iloc[-1] * 100),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=INK,
            annotation_clip=False,
        )

    x_min = min(c["accuracy"].min() for c in curves.values()) * 100
    for label, cagr in references.items():
        ax.axhline(cagr * 100, color=REF, lw=1, ls="--", zorder=1)
        ax.annotate(
            f"{label} {cagr:.1%}",
            (x_min, cagr * 100),
            xytext=(4, 3),
            textcoords="offset points",
            fontsize=8.5,
            color=INK_2,
        )
    ax.axhline(0, color="#c3c2b7", lw=1, zorder=1)

    for label, (acc, cagr) in measured.items():
        ax.scatter([acc * 100], [cagr * 100], s=64, color=INK, edgecolor=SURFACE, linewidth=2, zorder=5)
        ax.annotate(
            f"{label}\n{acc:.1%} accurate, {cagr:.1%}/yr",
            (acc * 100, cagr * 100),
            xytext=(8, -4),
            textcoords="offset points",
            va="top",
            fontsize=8.5,
            color=INK,
        )

    ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_xlabel("Indicator accuracy (balanced: average of up-day and down-day hit rates)", color=INK_2)
    ax.set_ylabel("CAGR after 5 bps slippage", color=INK_2)
    ax.grid(True, color=GRID, lw=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c3c2b7")
    ax.tick_params(colors=INK_2, labelsize=9)
    ax.set_title("How accurate must a daily direction indicator be?", loc="left", color=INK, fontsize=12)
    ax.legend(frameon=False, loc="upper left", labelcolor=INK_2, fontsize=9)
    fig.subplots_adjust(left=0.09, right=0.84, top=0.92, bottom=0.11)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
