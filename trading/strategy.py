"""Strategy base class, the point-in-time market view, and strategy loading.

To write a new strategy, add a file to `strategies/` (or anywhere, and pass its
path) containing one `Strategy` subclass:

    class MyStrategy(Strategy):
        name = "my_strategy"
        symbols = ("SPY", "UPRO")          # every symbol you read or trade
        params = {"window": 50}            # defaults; override with -p window=20
        lookback = 50                      # completed bars needed before day one

        def on_open(self, view):
            closes = view.history("SPY", self.p.window)["close"]
            return {"UPRO": 1.0} if closes.iloc[-1] > closes.mean() else {}

`on_open` runs once per session at the open and returns target portfolio
weights ({"UPRO": 1.0} = all in UPRO, {} = all cash), or None to keep the
current holdings unchanged.
"""

from __future__ import annotations

import datetime as dt
import importlib
import importlib.util
import inspect
import pkgutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

Weights = dict[str, float]


class MarketView:
    """What a strategy is allowed to see at the open of `date`.

    Completed daily bars strictly before `date`, plus today's open. Nothing
    later, so strategies can't accidentally look ahead.
    """

    def __init__(self, bars: dict[str, pd.DataFrame], date: dt.date):
        self.date = date
        self._bars = bars
        self._pos = {s: int(df.index.searchsorted(date)) for s, df in bars.items()}

    def open(self, symbol: str) -> float:
        df, i = self._bars[symbol], self._pos[symbol]
        if i >= len(df) or df.index[i] != self.date:
            raise LookupError(f"no {symbol} bar on {self.date}")
        return float(df["open"].iloc[i])

    def history(self, symbol: str, n: int | None = None) -> pd.DataFrame:
        """Last `n` completed bars (all if None) before today, oldest first."""
        i = self._pos[symbol]
        start = 0 if n is None else max(0, i - n)
        return self._bars[symbol].iloc[start:i]


class Strategy:
    name: str = ""
    symbols: tuple[str, ...] = ()
    params: dict = {}
    lookback: int = 1

    def __init__(self, **overrides):
        unknown = set(overrides) - set(self.params)
        if unknown:
            raise ValueError(f"{self.name}: unknown params {sorted(unknown)}; valid: {sorted(self.params)}")
        self.p = SimpleNamespace(**{**self.params, **overrides})
        self.info: dict = {}  # per-day diagnostics; copied into the backtest's daily table

    def required_symbols(self) -> tuple[str, ...]:
        """Symbols to load. Override when they depend on params."""
        return self.symbols

    def required_lookback(self) -> int:
        """Completed bars needed before the first decision. Override when param-dependent."""
        return self.lookback

    def on_open(self, view: MarketView) -> Weights | None:
        raise NotImplementedError

    def __repr__(self):
        return f"{self.name}({', '.join(f'{k}={v}' for k, v in vars(self.p).items())})"


def parse_params(cls: type[Strategy], pairs: list[str]) -> dict:
    """Turn CLI `key=value` strings into params, coerced to each default's type."""
    out = {}
    for pair in pairs:
        key, sep, raw = pair.partition("=")
        if not sep:
            raise ValueError(f"param {pair!r} must look like key=value")
        if key not in cls.params:
            raise ValueError(f"{cls.name}: unknown param {key!r}; valid: {sorted(cls.params)}")
        default = cls.params[key]
        if isinstance(default, bool):
            out[key] = raw.lower() in ("1", "true", "yes", "y")
        elif isinstance(default, (int, float)):
            out[key] = type(default)(raw)
        else:
            out[key] = raw
    return out


def _strategy_in_module(module) -> type[Strategy]:
    found = [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, Strategy) and obj is not Strategy and obj.__module__ == module.__name__
    ]
    if len(found) != 1:
        raise ValueError(f"{module.__name__} must define exactly one Strategy subclass, found {len(found)}")
    return found[0]


def load_strategy(name_or_path: str) -> type[Strategy]:
    """Load by name from the `strategies` package, or from a .py file path."""
    if name_or_path.endswith(".py"):
        path = Path(name_or_path)
        spec = importlib.util.spec_from_file_location(path.stem, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return _strategy_in_module(module)
    try:
        module = importlib.import_module(f"strategies.{name_or_path}")
    except ModuleNotFoundError:
        raise ValueError(f"no strategy {name_or_path!r}; available: {available_strategies()}") from None
    return _strategy_in_module(module)


def available_strategies() -> list[str]:
    import strategies

    return sorted(m.name for m in pkgutil.iter_modules(strategies.__path__) if not m.name.startswith("_"))
