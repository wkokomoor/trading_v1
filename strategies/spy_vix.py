"""The original SPY/VIX strategy, using daily opens.

At each open:
  VIX > vix_high and SPY not down more than spy_low  -> all SPXU (3x short)
  VIX > vix_high and SPY down                        -> hold current position
  VIX <= vix_high and SPY not up more than spy_high  -> all UPRO (3x long)
  VIX <= vix_high and SPY up                         -> hold current position
SPY's move is today's open vs. the previous session's open.
Set short= (empty) to go to cash instead of shorting.
"""

from trading.strategy import MarketView, Strategy


class SpyVix(Strategy):
    name = "spy_vix"
    symbols = ("SPY", "VIX", "UPRO", "SPXU")
    params = {
        "vix_high": 20.0,
        "vix_low": 17.0,
        "spy_high": 0.005,
        "spy_low": -0.005,
        "long": "UPRO",
        "short": "SPXU",
    }
    lookback = 1

    def required_symbols(self):
        return tuple(s for s in dict.fromkeys(("SPY", "VIX", self.p.long, self.p.short)) if s)

    def on_open(self, view: MarketView):
        vix = view.open("VIX")
        spy_open = view.open("SPY")
        spy_prev = view.history("SPY", 1)["open"].iloc[-1]
        spy_change = spy_open / spy_prev - 1

        volatile = vix > self.p.vix_high
        if spy_change > self.p.spy_high:
            spy_move = 1
        elif spy_change < self.p.spy_low:
            spy_move = -1
        else:
            spy_move = 0

        if volatile:
            signal = -1 if spy_move != -1 else 0
        else:
            signal = 1 if spy_move != 1 else 0

        self.info = {"vix": vix, "spy_change": spy_change, "signal": signal}
        if signal == 1:
            return {self.p.long: 1.0}
        if signal == -1:
            return {self.p.short: 1.0} if self.p.short else {}
        return None
