"""
Human confirmation gate — Twilio SMS.

Flow:
  1. Send outbound SMS with full trade details
  2. Poll for inbound reply for up to 60 seconds
  3. Return 'YES', 'NO', or 'TIMEOUT'

Env vars required:
  TWILIO_ACCOUNT_SID
  TWILIO_AUTH_TOKEN
  TWILIO_FROM   — your Twilio number  e.g. +12025551234
  TWILIO_TO     — your mobile number  e.g. +12025559876
"""

import os
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from logger import setup_logger

load_dotenv()
log = setup_logger('layer3')

_TIMEOUT_S = 60
_POLL_S    = 3


def _creds() -> tuple[str, str]:
    return (
        os.getenv('TWILIO_ACCOUNT_SID', ''),
        os.getenv('TWILIO_AUTH_TOKEN',  ''),
    )


def _twilio_from() -> str:
    return os.getenv('TWILIO_FROM', '')


def _twilio_to() -> str:
    return os.getenv('TWILIO_TO', '')


def send_confirmation_sms(symbol: str, plan: dict) -> bool:
    """
    Send trade details to TWILIO_TO. Returns True if sent successfully.
    """
    sid, token = _creds()
    if not sid or not token:
        log.error("confirmation: TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN not set")
        return False

    body = (
        f"TRADE SIGNAL\n"
        f"Symbol : {symbol}\n"
        f"Entry  : ${plan['entry_price']:.2f}\n"
        f"Stop   : ${plan['stop_price']:.2f}\n"
        f"T1     : ${plan['t1_price']:.2f}\n"
        f"T2     : ${plan['t2_price']:.2f}\n"
        f"Shares : {plan['shares']}\n"
        f"Risk   : ${plan['risk_amount']:.2f}\n"
        f"Reply YES to trade or NO to skip (60s)"
    )

    try:
        r = requests.post(
            f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
            auth=(sid, token),
            data={
                'From': _twilio_from(),
                'To':   _twilio_to(),
                'Body': body,
            },
            timeout=10,
        )
        r.raise_for_status()
        log.info(f"confirmation: SMS sent for {symbol} — SID={r.json().get('sid')}")
        return True
    except Exception as e:
        log.error(f"confirmation: SMS send failed for {symbol}: {e}")
        return False


def wait_for_reply(timeout_s: int = _TIMEOUT_S, trade_id: int = None) -> str:
    """
    Poll for an inbound reply. Checks two sources each cycle:
      1. Web dashboard (in-memory, instant) — if trade_id is provided
      2. Twilio SMS inbound messages

    Returns 'YES', 'NO', or 'TIMEOUT'.
    """
    sid, token = _creds()
    sent_at    = datetime.now(timezone.utc)
    deadline   = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        time.sleep(_POLL_S)

        # ── Web dashboard reply (checked first — faster than SMS) ──
        if trade_id is not None:
            try:
                from layer3.confirmation_state import get_web_reply
                web = get_web_reply(trade_id)
                if web in ('YES', 'NO'):
                    log.info(f"confirmation: received web {web} for trade {trade_id}")
                    return web
            except Exception as e:
                log.warning(f"confirmation: web reply check failed: {e}")

        # ── Twilio SMS reply ───────────────────────────────────────
        try:
            r = requests.get(
                f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
                auth=(sid, token),
                params={
                    'To':       _twilio_from(),   # inbound → to our Twilio number
                    'From':     _twilio_to(),     # from the user's phone
                    'PageSize': 5,
                },
                timeout=5,
            )
            r.raise_for_status()
            messages = r.json().get('messages', [])

            for msg in messages:
                msg_time_str = msg.get('date_sent') or msg.get('date_created', '')
                try:
                    from email.utils import parsedate_to_datetime
                    msg_time = parsedate_to_datetime(msg_time_str)
                    if msg_time.tzinfo is None:
                        msg_time = msg_time.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                if msg_time < sent_at:
                    continue

                body = (msg.get('body') or '').strip().upper()
                if body.startswith('YES'):
                    log.info("confirmation: received SMS YES")
                    return 'YES'
                if body.startswith('NO'):
                    log.info("confirmation: received SMS NO")
                    return 'NO'

        except Exception as e:
            log.warning(f"confirmation: SMS poll error: {e}")

    log.info("confirmation: no reply within timeout — auto-executing")
    return 'TIMEOUT'
