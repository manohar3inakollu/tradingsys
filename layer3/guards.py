"""
Daily guards — enforce hard limits before allowing new entries.
  • Max 3 trades per day
  • Max $240 loss per day
"""

from db.queries_layer3 import get_daily_session
from logger import setup_logger

log = setup_logger('layer3')

MAX_TRADES = 3
MAX_LOSS   = 240.0   # absolute loss in dollars


def check_daily_guards(conn) -> tuple[bool, str | None]:
    """
    Returns (ok, reason).
    ok=False means no new trades should be entered.
    """
    session = get_daily_session(conn)

    if session.get('session_halted'):
        reason = session.get('halt_reason', 'session halted')
        log.info(f"guards: session already halted — {reason}")
        return False, reason

    trades_today = session.get('trades_count', 0)
    if trades_today >= MAX_TRADES:
        reason = f"max trades reached ({trades_today}/{MAX_TRADES})"
        log.info(f"guards: {reason}")
        return False, reason

    pnl_today = float(session.get('total_pnl', 0))
    if pnl_today <= -MAX_LOSS:
        reason = f"daily loss limit hit (${pnl_today:.2f} <= -${MAX_LOSS:.0f})"
        log.info(f"guards: {reason}")
        return False, reason

    return True, None


def risk_budget(conn) -> float:
    """Return per-trade risk budget from today's session."""
    session = get_daily_session(conn)
    return float(session.get('risk_budget', 80.0))
