"""Trend filter: hold `trade` while `signal`'s last close is above its moving average, else cash."""

from trading.strategy import MarketView, Strategy


class SmaTrend(Strategy):
    name = "sma_trend"
    params = {"signal": "SPY", "trade": "UPRO", "window": 200}

    def required_symbols(self):
        return tuple(dict.fromkeys((self.p.signal, self.p.trade)))

    def required_lookback(self):
        return self.p.window

    def on_open(self, view: MarketView):
        closes = view.history(self.p.signal, self.p.window)["close"]
        sma = closes.mean()
        above = closes.iloc[-1] > sma
        self.info = {"close": closes.iloc[-1], "sma": sma}
        return {self.p.trade: 1.0} if above else {}
