"""Daily summary message for each portfolio, sent by email after the close (`trading job notify`).

Example:
    Daily direction: Bull
    SOLD: SPXU 10 shares. Profit $100.00
    BOUGHT: UPRO 124 shares. Total cost: $9,922.48
    HOLDING: UPRO 124 shares ($9,950.00)
    CASH: $77.52
    TOTAL: $10,027.52 (+0.27% today)

Email goes out through SMTP (Gmail by default) using an app password. Any address in
NOTIFY_TO works, including a carrier's email-to-text gateway (e.g. Google Fi:
<10-digit number>@msg.fi.google.com), which turns the email into an SMS.
"""

from __future__ import annotations

import datetime as dt
import smtplib
from email.message import EmailMessage

from sqlalchemy import select

from trading.config import get_settings
from trading.db.models import account_snapshots, live_runs, orders

BULL = {"UPRO", "SPY", "SSO", "TQQQ", "QQQ"}
BEAR = {"SPXU", "SH", "SDS", "SQQQ", "PSQ"}


def direction(target: dict | None) -> str:
    if target is None:
        return "Hold (no change)"
    held = {s for s, w in target.items() if w > 0}
    if not held:
        return "Cash (out of market)"
    if held <= BULL:
        return "Bull"
    if held <= BEAR:
        return "Bear"
    return ", ".join(sorted(held))


def _money(x: float) -> str:
    return f"-${-x:,.2f}" if x < 0 else f"${x:,.2f}"


def realized_profits(conn, portfolio: str, day: dt.date) -> dict[int, float]:
    """Profit of each SELL fill on `day` (order id -> dollars), against the average cost of
    the shares held at the time, replaying the portfolio's fills in order."""
    fills = conn.execute(
        select(
            orders.c.id,
            orders.c.symbol,
            orders.c.side,
            orders.c.filled_qty,
            orders.c.fill_price,
            orders.c.filled_at,
        )
        .where(orders.c.portfolio == portfolio, orders.c.fill_price.is_not(None), orders.c.filled_qty > 0)
        .order_by(orders.c.filled_at, orders.c.id)
    ).all()
    qty: dict[str, int] = {}
    cost: dict[str, float] = {}
    profits = {}
    for f in fills:
        held, basis = qty.get(f.symbol, 0), cost.get(f.symbol, 0.0)
        if f.side == "BUY":
            qty[f.symbol], cost[f.symbol] = held + f.filled_qty, basis + f.filled_qty * f.fill_price
        else:
            avg = basis / held if held else f.fill_price
            if f.filled_at.date() == day:
                profits[f.id] = (f.fill_price - avg) * f.filled_qty
            qty[f.symbol] = held - f.filled_qty
            cost[f.symbol] = basis - avg * f.filled_qty if qty[f.symbol] > 0 else 0.0
    return profits


def daily_summary(engine, portfolio: str, day: dt.date) -> str | None:
    """The day's message for one portfolio, or None if it didn't run that day."""
    with engine.connect() as conn:
        run = (
            conn.execute(
                select(live_runs).where(live_runs.c.portfolio == portfolio, live_runs.c.run_date == day)
            )
            .mappings()
            .first()
        )
        if run is None:
            return None
        ords = (
            conn.execute(select(orders).where(orders.c.live_run_id == run["id"]).order_by(orders.c.id))
            .mappings()
            .all()
        )
        snaps = {
            s["kind"]: s
            for s in conn.execute(
                select(account_snapshots).where(
                    account_snapshots.c.portfolio == portfolio, account_snapshots.c.date == day
                )
            ).mappings()
        }
        prev = conn.execute(
            select(account_snapshots.c.equity)
            .where(
                account_snapshots.c.portfolio == portfolio,
                account_snapshots.c.kind == "close",
                account_snapshots.c.date < day,
            )
            .order_by(account_snapshots.c.date.desc())
            .limit(1)
        ).scalar()
        profits = realized_profits(conn, portfolio, day)

    lines = [f"Daily direction: {direction(run['target'])}"]
    if run["status"] == "failed":
        lines.append(f"FAILED: {run['error']}")
    elif run["status"] == "blocked":
        lines.append(f"BLOCKED: {'; '.join(run['risk']) or 'risk checks'}")

    traded = False
    for o in ords:
        if o["status"] == "blocked":
            verb = "SOLD" if o["side"] == "SELL" else "BOUGHT"
            lines.append(f"NOT {verb}: {o['symbol']} {o['qty']} shares ({o['error']})")
        elif o["fill_price"] and o["filled_qty"]:
            traded = True
            if o["side"] == "SELL":
                profit = profits.get(o["id"])
                tail = f" Profit {_money(profit)}" if profit is not None else ""
                lines.append(f"SOLD: {o['symbol']} {o['filled_qty']} shares.{tail}")
            else:
                lines.append(
                    f"BOUGHT: {o['symbol']} {o['filled_qty']} shares. "
                    f"Total cost: {_money(o['filled_qty'] * o['fill_price'])}"
                )
    if not traded and run["status"] == "done":
        lines.append("No trades today.")

    snap = snaps.get("close") or snaps.get("open")
    if snap:
        for sym, q in snap["positions"].items():
            lines.append(f"HOLDING: {sym} {q} shares ({_money(q * snap['prices'][sym])})")
        lines.append(f"CASH: {_money(snap['cash'])}")
        change = f" ({snap['equity'] / prev - 1:+.2%} today)" if prev else ""
        lines.append(f"TOTAL: {_money(snap['equity'])}{change}")
        if "close" not in snaps:
            lines.append("(values at the open; the close wasn't recorded)")
    return "\n".join(lines)


def send_email(subject: str, body: str) -> list[str]:
    """Send through SMTP. Returns the recipients, or raises if email isn't configured."""
    s = get_settings()
    if not (s.smtp_user and s.smtp_password and s.notify_to):
        raise RuntimeError("email not configured: set SMTP_USER, SMTP_PASSWORD and NOTIFY_TO")
    to = [a.strip() for a in s.notify_to.split(",") if a.strip()]
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = s.smtp_user, ", ".join(to), subject
    msg.set_content(body)
    with smtplib.SMTP_SSL(s.smtp_host, 465, timeout=30) as smtp:
        smtp.login(s.smtp_user, s.smtp_password)
        smtp.send_message(msg)
    return to
