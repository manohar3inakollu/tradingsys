"""
Session gates checked once before any per-ticker scoring.
Any failure -> skip all trades today.
Also determines position size (risk_per_trade) from capital and RISK_PCT_PER_TRADE env var.
"""
import os
from datetime import datetime
from dotenv import load_dotenv
from timing import ET, MARKET_HOLIDAYS_2026
from logger import setup_logger
from layer3 import tradier_client

load_dotenv()

log = setup_logger('layer1')


def get_spy_change() -> float:
    """
    Returns SPY % change (today's open vs prev_close) from Tradier SIP quote.
    Tradier quote includes real-time open and prevclose; Alpaca IEX daily bars
    only return closed bars so they show stale data during market hours.
    """
    try:
        q = tradier_client.get_quote('SPY')
        if not q:
            return 0.0
        prev_close = float(q.get('prevclose') or 0)
        today_open = float(q.get('open') or 0)
        if prev_close <= 0 or today_open <= 0:
            return 0.0
        return round((today_open - prev_close) / prev_close * 100, 4)
    except Exception as e:
        log.warning(f"session_gates: SPY check failed: {e}")
        return 0.0


def check_session_gates(vix: float) -> tuple:
    """
    Returns (go: bool, reason: str, risk_per_trade: float, spy_change: float).
    Gates: VIX>40, SPY<-1%, market holiday.
    """
    today = datetime.now(ET).strftime('%Y-%m-%d')

    if today in MARKET_HOLIDAYS_2026:
        return False, f'market holiday ({today})', 0.0, 0.0

    if vix > 40:
        return False, f'VIX={vix} > 40 (extreme volatility)', 0.0, 0.0

    spy_chg = get_spy_change()
    if spy_chg < -1.0:
        return False, f'SPY={spy_chg}% < -1% (broad market sell-off)', 0.0, spy_chg

    capital   = float(os.getenv('TRADING_CAPITAL',
                                os.getenv('ACCOUNT_START_BALANCE', 8000)))
    risk_pct  = float(os.getenv('RISK_PCT_PER_TRADE', 1.0))
    base_risk = round(capital * risk_pct / 100, 2)
    risk      = round(base_risk * 0.5, 2) if vix >= 30 else base_risk
    size_note = f'half size ${risk:.0f}' if vix >= 30 else f'normal ${risk:.0f}'
    log.info(
        f"session_gates: all green | SPY={spy_chg}% VIX={vix} "
        f"capital=${capital:.0f} risk_pct={risk_pct}% -> {size_note} risk"
    )
    return True, 'ok', risk, spy_chg
