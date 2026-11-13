"""Put everything in one symbol on day one and keep it. The default benchmark."""

from trading.strategy import MarketView, Strategy


class BuyAndHold(Strategy):
    name = "buy_and_hold"
    params = {"symbol": "SPY"}
    lookback = 0

    def required_symbols(self):
        return (self.p.symbol,)

    def on_open(self, view: MarketView):
        return {self.p.symbol: 1.0}
