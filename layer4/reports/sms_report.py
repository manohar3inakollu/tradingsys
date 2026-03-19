"""
Daily end-of-day SMS report — sent at 4:00 PM ET.

Summarises:
  - Session P&L (today's closed trades)
  - Trade count
  - Win rate for the day
  - Whether daily rules were followed (trade limit, loss limit)

Uses the same Twilio REST API as layer3/confirmation.py.

Required env vars (same as Layer 3):
  TWILIO_ACCOUNT_SID
  TWILIO_AUTH_TOKEN
  TWILIO_FROM          +E.164 Twilio number
  TWILIO_TO            +E.164 recipient number
"""

import os
import logging
from datetime import date

import requests

from db.connection import db_connection
from layer4.queries import get_todays_closed_trades, get_market_context

log = logging.getLogger(__name__)

_TWILIO_BASE = 'https://api.twilio.com/2010-04-01'


def _post_sms(body: str) -> bool:
    sid   = os.getenv('TWILIO_ACCOUNT_SID', '')
    token = os.getenv('TWILIO_AUTH_TOKEN', '')
    from_ = os.getenv('TWILIO_FROM', '')
    to_   = os.getenv('TWILIO_TO', '')

    if not (sid and token and from_ and to_):
        log.warning('EOD SMS skipped — Twilio env vars not set')
        return False

    try:
        r = requests.post(
            f'{_TWILIO_BASE}/Accounts/{sid}/Messages.json',
            auth=(sid, token),
            data={'From': from_, 'To': to_, 'Body': body},
            timeout=10,
        )
        if r.status_code in (200, 201):
            return True
        log.error('EOD SMS failed: %s %s', r.status_code, r.text[:200])
        return False
    except Exception as exc:
        log.error('EOD SMS exception: %s', exc)
        return False


def send_eod_sms() -> bool:
    """Build and send the 4 PM end-of-day SMS. Returns True on success."""
    try:
        with db_connection() as conn:
            trades = get_todays_closed_trades(conn)
            ctx    = get_market_context(conn)
    except Exception as exc:
        log.error('EOD SMS DB error: %s', exc)
        return False

    today = date.today().strftime('%a %b %d')
    count = len(trades)

    if count == 0:
        body = f"[{today}] No trades today. Session closed."
        return _post_sms(body)

    total_pnl = sum(float(t.get('pnl') or 0) for t in trades)
    wins       = sum(1 for t in trades if (t.get('r_multiple') or 0) > 0)
    win_rate   = round(wins / count * 100) if count else 0
    r_vals     = [float(t.get('r_multiple') or 0) for t in trades]
    avg_r      = round(sum(r_vals) / len(r_vals), 2) if r_vals else 0

    pnl_sign = '+' if total_pnl >= 0 else ''
    rules_ok = (
        count <= 3 and
        total_pnl > -240 and
        not ctx.get('session_halted', False)
    )
    rules_str = 'Rules ✓' if rules_ok else 'Rules ✗ — review journal'

    body = (
        f"[EOD {today}]\n"
        f"P&L: {pnl_sign}${total_pnl:.2f}\n"
        f"Trades: {count}/3  WR: {win_rate}%  Avg R: {avg_r}R\n"
        f"{rules_str}"
    )

    ok = _post_sms(body)
    if ok:
        log.info('EOD SMS sent: %s', body.replace('\n', ' | '))
    return ok
