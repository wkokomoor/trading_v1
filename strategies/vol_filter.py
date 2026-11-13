"""Volatility filter: hold UPRO while SPY's recent realized volatility is low, else step aside.

Why (docs/RESEARCH.md, Part 3): no indicator predicts the next day's direction, but
volatility clusters. Calm markets tend to stay calm, and the violent stretches that
cause most of a 3x fund's losses tend to arrive after volatility has already risen.
Of the textbook filters screened, this one was the most consistent across both periods.

At each open, using only completed bars:
  realized vol = std of SPY's daily log returns over the last `window` closes, annualized
  vol < threshold                   -> all in `long`
  vol >= threshold                  -> all in `risk_off` ("" = cash, earning --cash-rate)
  trend_window > 0 and SPY's last
  close is below its average        -> also risk off
"""

import numpy as np

from trading.strategy import MarketView, Strategy


class VolFilter(Strategy):
    name = "vol_filter"
    params = {
        "window": 20,
        "threshold": 0.20,
        "trend_window": 0,
        "signal": "SPY",
        "long": "UPRO",
        "risk_off": "",
    }

    def required_symbols(self):
        return tuple(s for s in dict.fromkeys((self.p.signal, self.p.long, self.p.risk_off)) if s)

    def required_lookback(self):
        return max(self.p.window + 1, self.p.trend_window)

    def on_open(self, view: MarketView):
        closes = view.history(self.p.signal, max(self.p.window + 1, self.p.trend_window))["close"]
        rets = np.diff(np.log(closes.to_numpy()[-(self.p.window + 1) :]))
        vol = float(rets.std(ddof=1) * np.sqrt(252))
        calm = vol < self.p.threshold
        uptrend = True
        if self.p.trend_window > 0:
            uptrend = bool(closes.iloc[-1] > closes.iloc[-self.p.trend_window :].mean())

        self.info = {"vol": vol, "uptrend": uptrend}
        if calm and uptrend:
            return {self.p.long: 1.0}
        return {self.p.risk_off: 1.0} if self.p.risk_off else {}
