"""Portfolios, daily runs, orders, snapshots and the kill switch (PostgreSQL)."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from sqlalchemy import Engine, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from trading.db.models import account_snapshots, controls, live_runs, orders, portfolios
from trading.db.runs import jsonable

MODES = ("shadow", "live")


@dataclass
class Portfolio:
    name: str
    mode: str
    strategy: str
    params: dict
    allocation: float
    cash: float
    positions: dict[str, int]


def create_portfolio(
    engine: Engine, name: str, strategy: str, params: dict, allocation: float, mode: str = "shadow"
) -> Portfolio:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if allocation <= 0:
        raise ValueError("allocation must be positive")
    with engine.begin() as conn:
        conn.execute(
            insert(portfolios).values(
                name=name,
                mode=mode,
                strategy=strategy,
                params=jsonable(params),
                allocation=allocation,
                cash=allocation if mode == "shadow" else 0.0,
                positions={},
            )
        )
    return get_portfolio(engine, name)


def get_portfolio(engine_or_conn, name: str) -> Portfolio:
    ctx = engine_or_conn.connect() if isinstance(engine_or_conn, Engine) else _NoClose(engine_or_conn)
    with ctx as conn:
        row = conn.execute(select(portfolios).where(portfolios.c.name == name)).mappings().first()
    if row is None:
        raise LookupError(f"no portfolio named {name!r}")
    return Portfolio(
        name=row["name"],
        mode=row["mode"],
        strategy=row["strategy"],
        params=dict(row["params"]),
        allocation=row["allocation"],
        cash=row["cash"],
        positions={k: int(v) for k, v in row["positions"].items()},
    )


def list_portfolios(engine: Engine) -> list[Portfolio]:
    with engine.connect() as conn:
        names = conn.execute(select(portfolios.c.name).order_by(portfolios.c.name)).scalars().all()
    return [get_portfolio(engine, n) for n in names]


def set_shadow_state(conn, name: str, cash: float, positions: dict[str, int]) -> None:
    conn.execute(
        update(portfolios)
        .where(portfolios.c.name == name)
        .values(cash=cash, positions={k: v for k, v in positions.items() if v}, updated_at=func.now())
    )


class _NoClose:
    """Lets helpers accept either an Engine or an open Connection."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


# --------------------------------------------------------------------------- runs


def start_run(engine: Engine, run_date: dt.date, p: Portfolio, git_sha: str | None) -> tuple[int, str | None]:
    """Claim today's run for a portfolio. Returns (run_id, None) when this call owns it.

    If a run already exists, returns (its id, its status) and the caller must not trade,
    except that a "failed" run with no recorded orders is safe to retry and is reclaimed.
    """
    with engine.begin() as conn:
        stmt = (
            pg_insert(live_runs)
            .values(
                run_date=run_date,
                portfolio=p.name,
                strategy=p.strategy,
                params=jsonable(p.params),
                mode=p.mode,
                status="started",
                git_sha=git_sha,
            )
            .on_conflict_do_nothing(constraint="uq_live_runs_run_date_portfolio")
            .returning(live_runs.c.id)
        )
        run_id = conn.execute(stmt).scalar()
        if run_id is not None:
            return run_id, None

        existing = (
            conn.execute(
                select(live_runs.c.id, live_runs.c.status)
                .where(live_runs.c.run_date == run_date, live_runs.c.portfolio == p.name)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        n_orders = conn.execute(
            select(func.count()).select_from(orders).where(orders.c.live_run_id == existing["id"])
        ).scalar_one()
        if existing["status"] == "failed" and n_orders == 0:
            conn.execute(
                update(live_runs)
                .where(live_runs.c.id == existing["id"])
                .values(status="started", error=None, started_at=func.now(), finished_at=None)
            )
            return existing["id"], None
        return existing["id"], existing["status"]


def finish_run(conn, run_id: int, status: str, **values) -> None:
    conn.execute(
        update(live_runs)
        .where(live_runs.c.id == run_id)
        .values(status=status, finished_at=func.now(), **{k: jsonable(v) for k, v in values.items()})
    )


def record_order(conn, run_id: int, portfolio: str, **values) -> int:
    return conn.execute(
        insert(orders).values(live_run_id=run_id, portfolio=portfolio, **values).returning(orders.c.id)
    ).scalar_one()


def write_snapshot(conn, date: dt.date, portfolio: str, kind: str, cash, positions, prices) -> float:
    equity = cash + sum(q * prices[s] for s, q in positions.items())
    stmt = pg_insert(account_snapshots).values(
        date=date,
        portfolio=portfolio,
        kind=kind,
        cash=cash,
        equity=equity,
        positions=jsonable(positions),
        prices=jsonable(prices),
    )
    conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["date", "portfolio", "kind"],
            set_={c: stmt.excluded[c] for c in ("cash", "equity", "positions", "prices", "created_at")},
        )
    )
    return equity


def last_close_equity(conn, portfolio: str, before: dt.date) -> float | None:
    return conn.execute(
        select(account_snapshots.c.equity)
        .where(
            account_snapshots.c.portfolio == portfolio,
            account_snapshots.c.kind == "close",
            account_snapshots.c.date < before,
        )
        .order_by(account_snapshots.c.date.desc())
        .limit(1)
    ).scalar()


def recent_runs(engine: Engine, portfolio: str, limit: int = 10) -> list[dict]:
    with engine.connect() as conn:
        runs = (
            conn.execute(
                select(live_runs)
                .where(live_runs.c.portfolio == portfolio)
                .order_by(live_runs.c.run_date.desc())
                .limit(limit)
            )
            .mappings()
            .all()
        )
        result = []
        for r in runs:
            ords = conn.execute(select(orders).where(orders.c.live_run_id == r["id"])).mappings().all()
            result.append({**dict(r), "orders": [dict(o) for o in ords]})
    return result


# --------------------------------------------------------------------------- kill switch

KILL_SWITCH = "kill_switch"


def kill_switch(engine_or_conn) -> dict:
    """{"on": bool, "reason": str}. Also on whenever the TRADING_KILL_SWITCH env var is set."""
    import os

    if os.environ.get("TRADING_KILL_SWITCH", "").lower() in ("1", "true", "on", "yes"):
        return {"on": True, "reason": "TRADING_KILL_SWITCH environment variable"}
    ctx = engine_or_conn.connect() if isinstance(engine_or_conn, Engine) else _NoClose(engine_or_conn)
    with ctx as conn:
        value = conn.execute(select(controls.c.value).where(controls.c.key == KILL_SWITCH)).scalar()
    return dict(value) if value else {"on": False, "reason": ""}


def set_kill_switch(engine: Engine, on: bool, reason: str = "") -> None:
    stmt = pg_insert(controls).values(key=KILL_SWITCH, value={"on": on, "reason": reason})
    with engine.begin() as conn:
        conn.execute(
            stmt.on_conflict_do_update(
                index_elements=["key"], set_={"value": stmt.excluded.value, "updated_at": func.now()}
            )
        )
