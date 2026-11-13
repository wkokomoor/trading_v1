"""SPY/VIX v2: buy UPRO after flat or down SPY gaps, size the position by VIX, never short.

Evidence behind it (2010-2026, open-to-open returns; see docs/RESEARCH.md):
  * SPXU lost money in almost every VIX / gap / trend condition, so there is no short leg.
  * High VIX did NOT predict falling prices; it predicted *violent* prices. So VIX
    sets position size (volatility targeting), not direction.
  * After SPY gaps up more than ~0.5%, UPRO's next-day return was roughly zero;
    after flat or down gaps it was strong. That part of the original idea is kept.

At each open:
  trend_window > 0 and SPY's last close is below
  its trend_window-day average                -> risk off: all in `risk_off` ("" = cash)
  SPY gap (open vs previous open) > spy_high  -> on_gap_up: "hold" current position, "cash",
                                                 or "ignore" (no gap rule)
  otherwise                                   -> hold `long` at weight
                                                 min(1, target_vol / (lev * VIX/100)),
                                                 rounded down to `step`; the rest in cash.
target_vol=0 turns sizing off (always 100% when invested).
"""

import math

from trading.strategy import MarketView, Strategy


class VixGap(Strategy):
    name = "vix_gap"
    params = {
        "spy_high": 0.005,
        "on_gap_up": "hold",
        "target_vol": 0.45,
        "lev": 3.0,
        "step": 0.25,
        "long": "UPRO",
        "trend_window": 0,
        "risk_off": "",
    }

    def required_symbols(self):
        return tuple(s for s in dict.fromkeys(("SPY", "VIX", self.p.long, self.p.risk_off)) if s)

    def required_lookback(self):
        return max(1, self.p.trend_window)

    def on_open(self, view: MarketView):
        vix = view.open("VIX")
        gap = view.open("SPY") / view.history("SPY", 1)["open"].iloc[-1] - 1

        if self.p.trend_window > 0:
            closes = view.history("SPY", self.p.trend_window)["close"]
            if closes.iloc[-1] < closes.mean():
                self.info = {"vix": vix, "gap": gap, "weight": 0.0, "trend": "off"}
                return {self.p.risk_off: 1.0} if self.p.risk_off else {}

        if self.p.target_vol > 0:
            raw = min(1.0, self.p.target_vol / (self.p.lev * vix / 100))
            weight = math.floor(raw / self.p.step + 1e-9) * self.p.step
        else:
            weight = 1.0
        self.info = {"vix": vix, "gap": gap, "weight": weight}

        if gap > self.p.spy_high and self.p.on_gap_up != "ignore":
            if self.p.on_gap_up == "cash":
                return {}
            return None
        return {self.p.long: weight} if weight > 0 else {}
