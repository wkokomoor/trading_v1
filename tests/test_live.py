import datetime as dt

from trading.data import ET, sessions
from trading.engine import Order
from trading.live import run_dry_run
from trading.strategy import load_strategy


class Resp:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        pass


class FakeSchwab:
    """Mimics the schwabdev response shapes the dry run reads."""

    def __init__(self, today_quotes, prev_spy_open):
        self.today_quotes = today_quotes
        self.prev_spy_open = prev_spy_open
        self.placed = []

    def price_history(self, symbol, startDate, endDate, **_):
        days = sessions(startDate.date(), endDate.date())
        px = self.prev_spy_open if symbol == "SPY" else 10.0
        candles = [
            {
                "datetime": int(dt.datetime.combine(d, dt.time(0), ET).timestamp() * 1000),
                "open": px,
                "high": px,
                "low": px,
                "close": px,
                "volume": 1,
            }
            for d in days
        ]
        return Resp({"candles": candles})

    def quotes(self, symbols):
        return Resp({s: {"quote": {"openPrice": o, "lastPrice": o}} for s, o in self.today_quotes.items()})

    def linked_accounts(self):
        return Resp([{"hashValue": "abc"}])

    def account_details(self, account_hash, fields=None):
        return Resp(
            {
                "securitiesAccount": {
                    "currentBalances": {"cashBalance": 5.0},
                    "positions": [
                        {"instrument": {"symbol": "SPXU"}, "longQuantity": 50.0},
                        {
                            "instrument": {"symbol": "AAPL"},
                            "longQuantity": 3.0,
                        },  # ignored: not the strategy's
                    ],
                }
            }
        )

    def place_order(self, *a, **k):  # must never be called
        self.placed.append(a)


def test_dry_run_switches_from_short_to_long_and_sends_nothing(capsys):
    client = FakeSchwab({"SPY": 100.0, "$VIX": 15.0, "UPRO": 50.0, "SPXU": 20.0}, prev_spy_open=100.0)
    now = dt.datetime(2025, 3, 12, 10, 0, tzinfo=ET)
    orders = run_dry_run(client, load_strategy("spy_vix")(), now=now)
    assert orders == [Order("SPXU", "SELL", 50, 20.0), Order("UPRO", "BUY", 20, 50.0)]
    assert client.placed == []
    assert "nothing sent" in capsys.readouterr().out
