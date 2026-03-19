"""
Layer 3 end-to-end flow test.

What it tests:
  1. DB seed  — inserts a fake AAPL TRADE candidate
  2. SMS send — fires a real Twilio message to your phone
  3. Confirmation overlay — appears on the Cockpit tab (/cockpit)
  4. Reply polling — waits for YES/NO from your phone OR the web dashboard
  5. Paper order — 1-share market buy + immediate close on Alpaca paper

How confirmation works across processes
  The test script and the Flask app are separate processes.
  Overlay:  /api/pending now falls back to a DB query, so the overlay appears
            as soon as the PENDING trade is inserted — no shared memory needed.
  Reply:    When you click YES/NO on the dashboard, Flask writes confirmed=T/F
            to the DB.  This script polls that column every 3 s.
  SMS:      If Twilio is configured, a parallel SMS poll also runs.

Usage (run from project root):
  python -m layer3.test_flow
  python -m layer3.test_flow --sms-only
  python -m layer3.test_flow --order-only
  python -m layer3.test_flow --symbol TSLA --timeout 90
"""

import argparse
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_ROOT, '.env'))

import requests as _requests
from db.connection import db_connection
from db.queries_layer3 import init_daily_session
from layer3.broker import AlpacaBroker

DEFAULT_SYMBOL = 'AAPL'
TEST_PLAN = {
    'entry_price': 195.00,
    'stop_price':  192.50,
    't1_price':    197.50,
    't2_price':    200.00,
    'shares':      1,
    'risk_amount': 2.50,
}

# ── output helpers ─────────────────────────────────────────────────────────────
def _sec(t):  print(f"\n── {t} {'─'*max(0,46-len(t))}")
def _ok(m):   print(f"  ✓ {m}")
def _fail(m): print(f"  ✗ {m}")
def _info(m): print(f"  · {m}")


# ── SMS: direct Twilio call with full error output ─────────────────────────────

def _check_twilio() -> dict:
    """Return dict of Twilio env vars; print status for each."""
    keys = ['TWILIO_ACCOUNT_SID', 'TWILIO_AUTH_TOKEN', 'TWILIO_FROM', 'TWILIO_TO']
    vals = {k: os.getenv(k, '') for k in keys}
    for k, v in vals.items():
        if v:
            display = v if k in ('TWILIO_FROM', 'TWILIO_TO') else f"{v[:8]}…"
            _ok(f"{k} = {display}")
        else:
            _fail(f"{k} = NOT SET")
    return vals


def _send_sms_direct(symbol: str, plan: dict, creds: dict) -> bool:
    """Send SMS using raw requests so we see the full Twilio response."""
    sid   = creds['TWILIO_ACCOUNT_SID']
    token = creds['TWILIO_AUTH_TOKEN']
    frm   = creds['TWILIO_FROM']
    to    = creds['TWILIO_TO']

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

    url = f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json'
    _info(f"POST {url}")
    _info(f"From={frm}  To={to}")

    try:
        r = _requests.post(
            url,
            auth=(sid, token),
            data={'From': frm, 'To': to, 'Body': body},
            timeout=10,
        )
        _info(f"HTTP {r.status_code}")
        data = r.json()

        if r.ok:
            msg_sid = data.get('sid')
            _ok(f"SMS accepted — Twilio SID: {msg_sid}")
            # Check delivery status after a short delay
            _check_delivery_status(sid, token, msg_sid)
            return True
        else:
            _fail(f"Twilio error {r.status_code}: {data.get('message', r.text)}")
            code = data.get('code')
            if code == 21608:
                _info("→ Trial accounts can only send to verified numbers.")
                _info("  Verify your number at twilio.com/console or upgrade your plan.")
            elif code == 21211:
                _info("→ 'To' number is invalid. Use E.164 format: +12025551234")
            elif code == 21212:
                _info("→ 'From' number is invalid. Use E.164 format: +12025551234")
            elif code in (20003, 20005):
                _info("→ Authentication failed. Check TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN.")
            return False
    except Exception as e:
        _fail(f"Request failed: {e}")
        return False


def _check_delivery_status(sid: str, token: str, msg_sid: str) -> None:
    """Poll Twilio message SID for delivery status (up to 15s)."""
    _info("Checking delivery status …")
    for attempt in range(5):
        time.sleep(3)
        try:
            r = _requests.get(
                f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages/{msg_sid}.json',
                auth=(sid, token),
                timeout=5,
            )
            if not r.ok:
                _info(f"Status check HTTP {r.status_code} — skipping")
                return
            status = r.json().get('status', '?')
            error_code = r.json().get('error_code')
            error_msg  = r.json().get('error_message', '')
            _info(f"Delivery status: {status}" + (f"  error_code={error_code}: {error_msg}" if error_code else ""))
            if status in ('delivered', 'undelivered', 'failed', 'canceled'):
                if status == 'undelivered':
                    _fail("Message NOT delivered to handset.")
                    _info("→ If FROM is a toll-free number (833/844/855/866/877/888), carriers")
                    _info("  silently filter it until Toll-Free Verification is approved.")
                    _info("  Register at: twilio.com/console → Phone Numbers → Regulatory Compliance")
                    _info("  Alternative: switch TWILIO_FROM to a 10DLC local number.")
                elif status == 'failed':
                    _fail(f"Message failed: {error_code} {error_msg}")
                elif status == 'delivered':
                    _ok("Message delivered to handset ✓")
                return
        except Exception as e:
            _info(f"Status check error: {e}")
    _info("Delivery status still pending after 15s — check Twilio console logs")


def _poll_sms_reply(creds: dict, sent_after, timeout_s: int) -> str | None:
    """Poll Twilio for an inbound YES/NO reply. Returns 'YES', 'NO', or None."""
    sid, token = creds['TWILIO_ACCOUNT_SID'], creds['TWILIO_AUTH_TOKEN']
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        time.sleep(3)
        try:
            r = _requests.get(
                f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
                auth=(sid, token),
                params={'To': creds['TWILIO_FROM'], 'From': creds['TWILIO_TO'], 'PageSize': 5},
                timeout=5,
            )
            if not r.ok:
                continue
            from email.utils import parsedate_to_datetime
            from datetime import timezone
            for msg in r.json().get('messages', []):
                try:
                    msg_time = parsedate_to_datetime(msg.get('date_sent') or msg.get('date_created',''))
                    if msg_time.tzinfo is None:
                        msg_time = msg_time.replace(tzinfo=timezone.utc)
                    if msg_time < sent_after:
                        continue
                except Exception:
                    continue
                body = (msg.get('body') or '').strip().upper()
                if body.startswith('YES'):
                    return 'YES'
                if body.startswith('NO'):
                    return 'NO'
        except Exception:
            pass
    return None


# ── DB helpers ─────────────────────────────────────────────────────────────────

def seed_db(symbol: str) -> dict:
    ids = {}
    with db_connection() as conn:
        with conn.cursor() as cur:
            init_daily_session(conn, risk_budget=80.0)

            cur.execute(
                "INSERT INTO tickers (symbol, type) VALUES (%s, 'stock') "
                "ON CONFLICT (symbol) DO UPDATE SET type='stock' RETURNING id",
                (symbol,))
            ids['ticker_id'] = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO daily_prices
                    (ticker_id, date, open, high, low, close, volume, gap_pct, atr_pct, vix)
                VALUES (%s, CURRENT_DATE, 195, 198, 193, 195.5, 40000000, 2.5, 1.8, 18.0)
                ON CONFLICT (ticker_id, date) DO UPDATE SET close = EXCLUDED.close
                RETURNING id""", (ids['ticker_id'],))
            ids['price_id'] = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO scan_results
                    (scan_date, ticker_id, price_id, passed_filters, orb_window, rank)
                VALUES (CURRENT_DATE, %s, %s, TRUE, 5, 1) RETURNING id""",
                (ids['ticker_id'], ids['price_id']))
            ids['scan_result_id'] = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO ai_scores (
                    scan_date, ticker_id, price_id, scan_result_id,
                    score_final, decision,
                    entry_price, stop_price, t1_price, t2_price,
                    shares, risk_amount, catalyst_type, headline, sentiment
                ) VALUES (
                    CURRENT_DATE, %s, %s, %s,
                    82.5, 'TRADE', 195.00, 192.50, 197.50, 200.00,
                    1, 2.50, 'news', '[TEST] Layer 3 flow test', 'bullish'
                )
                ON CONFLICT (scan_date, ticker_id)
                    DO UPDATE SET decision='TRADE', score_final=82.5
                RETURNING id""",
                (ids['ticker_id'], ids['price_id'], ids['scan_result_id']))
            ids['ai_score_id'] = cur.fetchone()[0]

            # PENDING trade — confirmed=NULL so the overlay shows
            cur.execute("""
                INSERT INTO trades (
                    ticker_id, ai_score_id, symbol,
                    orb_high, orb_low, orb_window,
                    entry_price, stop_price, t1_price, t2_price,
                    shares, risk_amount, l2_entry_estimate, l2_stop_estimate,
                    status
                ) VALUES (
                    %s, %s, %s, 195.50, 193.00, 5,
                    195.00, 192.50, 197.50, 200.00,
                    1, 2.50, 195.00, 192.50, 'PENDING'
                ) RETURNING id""",
                (ids['ticker_id'], ids['ai_score_id'], symbol))
            ids['trade_id'] = cur.fetchone()[0]

        conn.commit()
    _ok(f"trade_id={ids['trade_id']}")
    return ids


def cleanup_db(ids: dict) -> None:
    mapping = [
        ('trades',       'trade_id'),
        ('ai_scores',    'ai_score_id'),
        ('scan_results', 'scan_result_id'),
        ('daily_prices', 'price_id'),
    ]
    with db_connection() as conn:
        with conn.cursor() as cur:
            for table, key in mapping:
                if ids.get(key):
                    cur.execute(f"DELETE FROM {table} WHERE id = %s", (ids[key],))
        conn.commit()
    _ok("DB cleaned up")


def poll_db_reply(trade_id: int, timeout_s: int) -> str:
    """Poll trades.confirmed until TRUE/FALSE or timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        time.sleep(3)
        try:
            with db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT confirmed FROM trades WHERE id = %s", (trade_id,))
                    row = cur.fetchone()
                    if row and row[0] is True:  return 'YES'
                    if row and row[0] is False: return 'NO'
        except Exception:
            pass
    return 'TIMEOUT'


# ── order test ─────────────────────────────────────────────────────────────────

def test_order(symbol: str) -> bool:
    _sec("Alpaca Paper Order")
    api_key = os.getenv('ALPACA_API_KEY', '')
    if not api_key:
        _fail("ALPACA_API_KEY not set in .env")
        return False
    _info(f"ALPACA_API_KEY = {api_key[:8]}…  (paper)")

    broker = AlpacaBroker()
    print(f"  Placing market buy: 1 × {symbol} …")
    try:
        order = broker.place_market_buy(symbol, 1)
    except Exception as e:
        _fail(f"Buy failed: {e}")
        return False

    oid    = order.get('id')
    status = order.get('status', '?')
    _ok(f"Order submitted  id={oid}  status={status}")

    if status in ('rejected', 'expired'):
        _fail(f"Rejected: {order.get('reject_reason', 'unknown reason')}")
        _info("Normal outside market hours — API connectivity confirmed ✓")
        return True

    print(f"  Waiting up to 30s for fill …")
    filled = broker.wait_for_fill(oid, timeout_s=30)

    if filled.get('status') == 'filled':
        fill_price = float(filled.get('filled_avg_price') or 0)
        _ok(f"Filled @ ${fill_price:.2f}")
        print(f"  Closing position …")
        try:
            close = broker.place_market_sell(symbol, 1)
            broker.wait_for_fill(close.get('id', ''), timeout_s=30)
            _ok("Position closed — paper account restored")
        except Exception as e:
            _fail(f"Close failed: {e}")
            _info("Cancel manually at paper.alpaca.markets")
    else:
        _info(f"Status after 30s: {filled.get('status')} — market is closed")
        if broker.cancel_order(oid):
            _ok("Queued order cancelled")
        else:
            _info("Cancel manually at paper.alpaca.markets")
    return True


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--symbol',     default=DEFAULT_SYMBOL)
    p.add_argument('--sms-only',   action='store_true')
    p.add_argument('--order-only', action='store_true')
    p.add_argument('--timeout',    type=int, default=60)
    args = p.parse_args()
    symbol = args.symbol.upper()

    print(f"\n{'═'*54}")
    print(f"  Layer 3 Flow Test  ·  {symbol}")
    print(f"{'═'*54}")

    # ── Twilio check ──────────────────────────────────────────────────────────
    _sec("Twilio Env")
    creds = _check_twilio()
    twilio_ready = all(creds.values())

    # ── DB seed ───────────────────────────────────────────────────────────────
    _sec("DB Seed")
    try:
        ids = seed_db(symbol)
    except Exception as e:
        _fail(f"DB seed failed: {e}")
        sys.exit(1)

    trade_id = ids['trade_id']
    reply = 'YES'

    try:
        if not args.order_only:
            # ── SMS ───────────────────────────────────────────────────────────
            _sec("SMS Send")
            sent_at = None
            if twilio_ready:
                from datetime import datetime, timezone
                sent_at = datetime.now(timezone.utc)
                _send_sms_direct(symbol, TEST_PLAN, creds)
            else:
                _info("Twilio not configured — skipping SMS")

            # ── Overlay notice ────────────────────────────────────────────────
            print()
            _info("The confirmation overlay should now appear on the Cockpit tab.")
            _info(f"Open your browser → /cockpit  and click YES or NO.")
            _info(f"Waiting {args.timeout}s for reply …")
            print()

            # ── Poll for reply (SMS + DB in parallel) ─────────────────────────
            import threading
            result = ['TIMEOUT']

            def _db_poll():
                r = poll_db_reply(trade_id, args.timeout)
                if r in ('YES', 'NO'):
                    result[0] = r

            def _sms_poll():
                if twilio_ready and sent_at:
                    r = _poll_sms_reply(creds, sent_at, args.timeout)
                    if r in ('YES', 'NO'):
                        result[0] = r

            t_db  = threading.Thread(target=_db_poll,  daemon=True)
            t_sms = threading.Thread(target=_sms_poll, daemon=True)
            t_db.start(); t_sms.start()
            t_db.join(timeout=args.timeout + 3)
            t_sms.join(timeout=0.1)
            reply = result[0]

            if reply == 'YES':
                _ok("Reply: YES")
            elif reply == 'NO':
                _info("Reply: NO — order skipped")
            else:
                _info(f"No reply within {args.timeout}s — TIMEOUT (auto-executing)")

        # ── Order ─────────────────────────────────────────────────────────────
        if not args.sms_only:
            if args.order_only or reply in ('YES', 'TIMEOUT'):
                test_order(symbol)
            else:
                _info(f"Order skipped ({reply})")

    finally:
        _sec("Cleanup")
        try:
            cleanup_db(ids)
        except Exception as e:
            _fail(f"Cleanup error: {e}")

    print(f"\n{'═'*54}")
    print(f"  Done")
    print(f"{'═'*54}\n")


if __name__ == '__main__':
    main()
