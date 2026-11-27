"""A daily Fear & Greed index built from public market data (0 = extreme fear, 100 = extreme greed).

Modeled on CNN's Fear & Greed Index, which has no official data feed, from components we can
source ourselves:

  fear_vix    VIX vs. its 50-day average (inverted: a VIX spike is fear)          Yahoo ^VIX
  momentum    SPY vs. its 125-day average                                         Yahoo SPY
  breadth     share of NYSE volume in advancing stocks, 10-day average            Schwab $UVOL / $DVOL
  junk_bond   junk bonds vs. similar-duration Treasuries (HYG / IEI) vs. its      Yahoo HYG, IEI
              50-day average: a credit-spread proxy. (HYG / LQD was tried first, but
              LQD's much longer duration made rate shocks like 2022 read as "greed".)
  safe_haven  SPY minus long Treasuries (TLT), 20-day return                       Yahoo SPY, TLT

Each component is scored 0-100 by where today's value ranks within the previous year (252
sessions), so it adapts to changing regimes and needs no fitted parameters. The index is the
average of the available component scores (at least 3 of 5).

Timing: every value for day t uses closes through day t, so it's known after the close.
A strategy deciding at day t's open must use day t-1's value (MarketView.history does this).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

COMPONENTS = ("fear_vix", "momentum", "breadth", "junk_bond", "safe_haven")
SYMBOLS = ("SPY", "VIX", "TLT", "HYG", "IEI", "UVOL", "DVOL")
RANK_WINDOW = 252
MIN_COMPONENTS = 3


def clean_volume(up: pd.Series, down: pd.Series) -> pd.Series:
    """Share of volume in advancing issues, with bad prints masked.

    Schwab's $UVOL/$DVOL change units around 2013 (both by the same factor, so the share is
    unaffected) and contain days whose total is wildly off (e.g. 60x normal, or near zero).
    A day is masked when its total is >10x or <0.1x the median of the previous 20 days,
    which uses only past data.
    """
    total = up + down
    ref = total.shift(1).rolling(20, min_periods=10).median()
    ok = (total > 0) & (total < 10 * ref) & (total > 0.1 * ref)
    return (up / total).where(ok)


def components(bars: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Raw component values (higher = greedier), indexed by SPY's session dates."""
    idx = bars["SPY"].index

    def close(sym):
        return bars[sym]["close"].reindex(idx) if sym in bars else pd.Series(np.nan, index=idx)

    spy, vix, tlt = close("SPY"), close("VIX"), close("TLT")
    raw = pd.DataFrame(index=idx)
    raw["fear_vix"] = -(vix / vix.rolling(50).mean() - 1)
    raw["momentum"] = spy / spy.rolling(125).mean() - 1
    if "UVOL" in bars and "DVOL" in bars:
        raw["breadth"] = clean_volume(close("UVOL"), close("DVOL")).rolling(10, min_periods=7).mean()
    else:
        raw["breadth"] = np.nan
    ratio = close("HYG") / close("IEI")
    raw["junk_bond"] = ratio / ratio.rolling(50).mean() - 1
    raw["safe_haven"] = spy.pct_change(20) - tlt.pct_change(20)
    return raw


def _trailing_percentile(window: np.ndarray) -> float:
    """Percentile (0-100) of the window's last value among the window's earlier values."""
    *past, last = window
    past = np.asarray(past)
    past = past[~np.isnan(past)]
    if np.isnan(last) or len(past) < RANK_WINDOW // 2:
        return np.nan
    return 100.0 * ((past < last).sum() + 0.5 * (past == last).sum()) / len(past)


def index(bars: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Component scores (0-100) plus the composite `fear_greed` (0-100) and `n_components`."""
    raw = components(bars)
    scores = raw.rolling(RANK_WINDOW + 1, min_periods=RANK_WINDOW // 2 + 1).apply(
        _trailing_percentile, raw=True
    )
    n = scores.notna().sum(axis=1)
    scores["fear_greed"] = scores[list(COMPONENTS)].mean(axis=1).where(n >= MIN_COMPONENTS)
    scores["n_components"] = n
    return scores


def label(score: float) -> str:
    """CNN-style label for a 0-100 score."""
    if np.isnan(score):
        return "n/a"
    for upper, name in ((25, "Extreme Fear"), (45, "Fear"), (55, "Neutral"), (75, "Greed")):
        if score < upper:
            return name
    return "Extreme Greed"
