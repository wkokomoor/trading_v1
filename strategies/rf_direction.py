"""Random-forest direction model: hold UPRO when the model's P(SPY up until tomorrow's open)
is at least `threshold`, otherwise cash.

Features (trading/ml.py): VIX level, morning gap, trend and divergence from SPY, plus SPY
context. NYSE ADD / TICK features switch on automatically when those symbols are listed in
`internals` and a data source provides them.

The model retrains every January on all data before that year (walk-forward), so every
prediction is out of sample. It needs years of history: `train_years` of data is loaded
before --start, and the model only trains once `min_train_rows` labeled rows exist.
"""

import pandas as pd

from trading import ml
from trading.strategy import MarketView, Strategy


class RfDirection(Strategy):
    name = "rf_direction"
    params = {
        "threshold": 0.55,
        "long": "UPRO",
        "train_years": 8,
        "min_train_rows": 750,
        "n_estimators": 300,
        "max_depth": 4,
        "min_samples_leaf": 50,
        "internals": "",  # e.g. "ADD,TICK" once a data source provides them
    }
    lookback = 260  # rows needed to compute one day's features (200-day SMA, 120-day beta)

    def __init__(self, **overrides):
        super().__init__(**overrides)
        self.model = None
        self.model_year = None

    def _feature_symbols(self):
        extra = tuple(s for s in self.p.internals.split(",") if s)
        return ("SPY", "VIX", *extra)

    def required_symbols(self):
        return tuple(dict.fromkeys((*self._feature_symbols(), self.p.long)))

    def required_history(self):
        return self.p.train_years * 252

    def _train(self, view: MarketView):
        bars = {s: view.history(s) for s in self._feature_symbols()}
        # Labels for the training rows need the next open; the last history row's next open
        # is today's open, which is known now.
        spy = pd.concat([bars["SPY"], pd.DataFrame({"open": [view.open("SPY")]}, index=[view.date])])
        y = ml.labels({"SPY": spy}).reindex(bars["SPY"].index)
        X, yy = ml.training_rows(ml.build_features(bars), y, view.date)
        if len(X) < self.p.min_train_rows:
            return None
        return ml.make_model(self.p.n_estimators, self.p.max_depth, self.p.min_samples_leaf).fit(
            X.to_numpy(), yy.to_numpy()
        )

    def on_open(self, view: MarketView):
        if self.model is None or view.date.year != self.model_year:
            self.model = self._train(view)
            self.model_year = view.date.year
        if self.model is None:
            self.info = {"p_up": None}
            return {}
        row = ml.features_at_open(view, self._feature_symbols())
        if row.isna().any():
            self.info = {"p_up": None}
            return None  # can't score today (missing data): keep the current position
        p_up = float(self.model.predict_proba(row.to_numpy().reshape(1, -1))[0, 1])
        self.info = {"p_up": p_up}
        return {self.p.long: 1.0} if p_up >= self.p.threshold else {}
