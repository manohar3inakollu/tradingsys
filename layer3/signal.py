"""
Live trade plan — recalculated at the moment of breakout.

  stop   = entry candle low (below the bar that closed above ORB high)
  entry  ≈ current close (proxy for "next 1-min open")
  risk   = entry - stop  (per share)
  shares = floor(risk_budget / risk)
  T1     = entry + 1R
  T2     = entry + 2R
"""

import math
import os
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()

log = setup_logger('layer3')

_MIN_STOP_DIST    = 0.05   # minimum stop distance to avoid division by tiny numbers
_MAX_STOP_PCT     = 0.05   # reject if ORB-based stop > 5% of entry (low-efficiency setup)
_CAPITAL          = float(os.getenv('TRADING_CAPITAL',
                                    os.getenv('ACCOUNT_START_BALANCE', 8000)))
_MAX_POSITION_PCT = float(os.getenv('MAX_POSITION_PCT', 95.0))


def live_trade_plan(
    entry_candle_low: float,
    current_close:    float,
    risk_budget:      float = 80.0,
) -> dict | None:
    """
    Calculate live trade plan at breakout.

    Returns plan dict or None if the numbers don't work out
    (e.g. stop is above entry, or shares would be 0).
    """
    entry = round(current_close    * 1.0005, 4)   # +0.05% slippage buffer
    stop  = round(entry_candle_low * 0.9990, 4)   # -0.10% stop buffer

    risk_per_share = round(entry - stop, 4)

    if risk_per_share < _MIN_STOP_DIST:
        log.warning(
            f"signal: stop too close — entry={entry} stop={stop} "
            f"risk_per_share={risk_per_share} (min={_MIN_STOP_DIST}) — skip"
        )
        return None

    stop_pct = risk_per_share / entry
    if stop_pct > _MAX_STOP_PCT:
        log.warning(
            f"signal: stop too wide — entry={entry} stop={stop} "
            f"({stop_pct*100:.1f}% > {_MAX_STOP_PCT*100:.0f}% max) — skip"
        )
        return None

    shares_from_risk = math.floor(risk_budget / risk_per_share)
    if shares_from_risk <= 0:
        log.warning(
            f"signal: shares=0 — entry={entry} stop={stop} "
            f"risk_per_share={risk_per_share} budget={risk_budget} — skip"
        )
        return None

    max_by_capital = math.floor(_CAPITAL * _MAX_POSITION_PCT / 100 / entry)
    shares         = min(shares_from_risk, max(1, max_by_capital))
    if shares < shares_from_risk:
        log.info(
            f"signal: position capped by capital limit "
            f"({shares_from_risk} -> {shares} shares, "
            f"max=${_CAPITAL:.0f}*{_MAX_POSITION_PCT:.0f}%/${entry})"
        )

    actual_risk = round(shares * risk_per_share, 2)
    t1 = round(entry + risk_per_share, 4)       # +1R
    t2 = round(entry + 2 * risk_per_share, 4)   # +2R

    plan = {
        'entry_price': entry,
        'stop_price':  stop,
        't1_price':    t1,
        't2_price':    t2,
        'shares':      shares,
        'risk_amount': actual_risk,
    }

    log.info(
        f"signal: plan — entry={entry} stop={stop} "
        f"T1={t1} T2={t2} shares={shares} risk=${actual_risk}"
    )
    return plan
