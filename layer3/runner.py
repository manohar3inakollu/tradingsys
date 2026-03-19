"""
Layer 3 — Live execution engine.

Entry point: start_layer3()
Called by the APScheduler at 9:45 AM ET. Spawns a background monitor thread
that runs until 3:45 PM ET, then self-terminates after force exit.

Monitor loop (every 60 s):
  1. Check daily guards (trades ≥ 3 or loss ≥ $240) → halt if breached
  2. No-trade rule: if ORB low breaks before ORB high → skip symbol all day
  3. Dead zone 11:30–2:30 PM → pause new entries (existing positions managed)
  4. Evaluate all 5 criteria for each active candidate
  5. On breakout: live recalc → Twilio SMS → YES=place order, NO/TIMEOUT=skip
  6. Spawn a TradeManager thread to manage each live position
  7. At 3:45 PM: force exit all open positions and stop
"""

import os
import threading
import time
from datetime import datetime, time as dtime
from layer3.confirmation_state import set_pending, clear_pending

from dotenv import load_dotenv
from timing import ET

load_dotenv()
from logger import setup_logger
from db.connection import db_connection
from db.queries_layer3 import (
    init_daily_session, get_daily_session,
    get_trade_candidates, insert_trade,
    add_no_trade_ticker, halt_session, log_signal,
)
from layer3.guards import check_daily_guards, risk_budget, MAX_TRADES, MAX_LOSS
from layer3.orb import get_orb_range
from layer3.monitor import all_five_pass
from layer3.signal import live_trade_plan
from layer3.confirmation import send_confirmation_sms, wait_for_reply
from layer3.broker import AlpacaBroker
from layer3.trade_manager import TradeManager
from scanner.alpaca_client import AlpacaClient

log = setup_logger('layer3')

_POLL_S           = 60
_DEAD_ZONE_START  = dtime(11, 30)
_DEAD_ZONE_END    = dtime(14, 30)
_FORCE_EXIT_TIME  = dtime(15, 45)
_ENTRY_GATE_OPEN  = dtime(9, 45)

# Module-level state (reset each day by start_layer3)
_stop_event:    threading.Event           = threading.Event()
_manager_threads: list[threading.Thread] = []
_active_managers: list[TradeManager]     = []
_watchlist:     list[dict]               = []   # enriched candidates with ORB
_no_trade_set:  set[str]                 = set()
_fired_set:     set[str]                 = set()   # symbols that already triggered a signal today


# ── Public API ────────────────────────────────────────────────────────────────

def start_layer3() -> None:
    """Called at 9:45 AM ET by the scheduler. Non-blocking."""
    global _stop_event, _manager_threads, _active_managers
    global _watchlist, _no_trade_set, _fired_set

    # Reset state for today
    _stop_event    = threading.Event()
    _manager_threads = []
    _active_managers = []
    _no_trade_set  = set()
    _fired_set     = set()
    _watchlist     = []

    log.info("layer3: starting — initialising session and candidates")

    try:
        _bootstrap()
    except Exception as e:
        log.error(f"layer3: bootstrap failed — {e}")
        return

    thread = threading.Thread(target=_monitor_loop, name='layer3-monitor', daemon=True)
    thread.start()
    log.info("layer3: monitor thread started")


def stop_layer3() -> None:
    """Signal the monitor thread to stop (used on clean shutdown)."""
    _stop_event.set()
    log.info("layer3: stop signal sent")


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def _bootstrap() -> None:
    """
    Initialise daily session, determine risk budget from current VIX,
    fetch TRADE candidates, compute ORB ranges.
    """
    alpaca    = AlpacaClient()
    vix       = alpaca.get_vix()
    capital   = float(os.getenv('TRADING_CAPITAL',
                                os.getenv('ACCOUNT_START_BALANCE', 8000)))
    risk_pct  = float(os.getenv('RISK_PCT_PER_TRADE', 1.0))
    base      = round(capital * risk_pct / 100, 2)
    budget    = round(base * 0.5, 2) if vix >= 30 else base

    with db_connection() as conn:
        init_daily_session(conn, risk_budget=budget)
        candidates = get_trade_candidates(conn)

    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(
        f"layer3: [{now_et}] {len(candidates)} TRADE candidates | "
        f"VIX={vix} | risk_budget=${budget:.0f}"
    )

    if not candidates:
        log.info("layer3: no TRADE candidates — monitor will wait")
        return

    enriched = []
    for c in candidates:
        symbol = c['symbol']
        orb_window = int(c.get('orb_window') or (15 if c['ticker_type'] == 'etf' else 5))
        orb = get_orb_range(symbol, orb_window)
        if orb is None:
            log.warning(f"layer3: ORB unavailable for {symbol} — skipping")
            continue

        # Pre-insert a PENDING trade record with the ORB-based plan estimate so
        # the dashboard shows real values immediately.  Breakout will update it.
        preliminary_plan = live_trade_plan(
            entry_candle_low=orb['low'],
            current_close=orb['high'],
            risk_budget=budget,
        )
        trade_id = None
        if preliminary_plan is not None:
            with db_connection() as conn:
                trade_id = insert_trade(conn, {
                    'ticker_id':         c['ticker_id'],
                    'ai_score_id':       c.get('ai_score_id'),
                    'symbol':            symbol,
                    'orb_high':          orb['high'],
                    'orb_low':           orb['low'],
                    'orb_window':        orb_window,
                    'entry_price':       preliminary_plan['entry_price'],
                    'stop_price':        preliminary_plan['stop_price'],
                    't1_price':          preliminary_plan['t1_price'],
                    't2_price':          preliminary_plan['t2_price'],
                    'shares':            preliminary_plan['shares'],
                    'risk_amount':       preliminary_plan['risk_amount'],
                    'l2_entry_estimate': c.get('l2_entry_estimate'),
                    'l2_stop_estimate':  c.get('l2_stop_estimate'),
                })
            log.info(
                f"layer3: {symbol} ORB plan — "
                f"H={orb['high']} L={orb['low']} "
                f"entry={preliminary_plan['entry_price']} "
                f"stop={preliminary_plan['stop_price']} "
                f"T1={preliminary_plan['t1_price']} T2={preliminary_plan['t2_price']} "
                f"shares={preliminary_plan['shares']} risk=${preliminary_plan['risk_amount']}"
            )
        else:
            log.warning(
                f"layer3: {symbol} ORB plan invalid "
                f"(H={orb['high']} L={orb['low']}) — will re-evaluate at breakout"
            )

        enriched.append({**c, 'orb_high': orb['high'], 'orb_low': orb['low'], 'trade_id': trade_id})

    _watchlist.extend(enriched)
    log.info(
        f"layer3: {len(_watchlist)} candidates with ORB ranges: "
        + ", ".join(f"{c['symbol']}(H={c['orb_high']} L={c['orb_low']})" for c in _watchlist)
    )


# ── Monitor loop ──────────────────────────────────────────────────────────────

def _monitor_loop() -> None:
    """Background thread — runs every 60 s from 9:45 AM to 3:45 PM ET."""
    while not _stop_event.is_set():
        now = datetime.now(ET)
        t   = now.time().replace(second=0, microsecond=0)

        # Force exit and shut down at 3:45 PM
        if t >= _FORCE_EXIT_TIME:
            _force_exit_all()
            break

        # Periodic tick
        try:
            _tick(t)
        except Exception as e:
            log.error(f"layer3: monitor tick error: {e}")

        time.sleep(_POLL_S)

    log.info("layer3: monitor thread exiting")


def _tick(t: dtime) -> None:
    """One monitoring cycle."""
    now_et = datetime.now(ET).strftime('%H:%M ET')

    with db_connection() as conn:
        ok, halt_reason = check_daily_guards(conn)
        if not ok:
            log.info(f"layer3: [{now_et}] guards blocked — {halt_reason}")
            halt_session(conn, halt_reason)
            _stop_event.set()
            return

    # Dead zone — no new entries, but managers keep running
    in_dead_zone = _DEAD_ZONE_START <= t < _DEAD_ZONE_END
    if in_dead_zone:
        log.info(f"layer3: [{now_et}] dead zone — skipping new entries")
        return

    if not _watchlist:
        return

    # Evaluate each candidate
    for candidate in list(_watchlist):
        symbol = candidate['symbol']

        if symbol in _fired_set or symbol in _no_trade_set:
            continue

        orb_high     = candidate['orb_high']
        orb_low      = candidate['orb_low']
        avg_volume   = int(candidate.get('volume') or 0)

        # --- No-trade rule check ---
        # Fetch latest close to detect ORB low break before ORB high break
        try:
            import os
            import requests as req
            r = req.get(
                f'https://data.alpaca.markets/v2/stocks/{symbol}/bars/latest',
                headers={
                    'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
                    'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
                },
                params={'feed': 'iex'},
                timeout=3,
            )
            latest_close = float(r.json().get('bar', {}).get('c', 0))
        except Exception:
            latest_close = 0.0

        if latest_close > 0 and latest_close < orb_low:
            log.info(
                f"layer3: [{now_et}] NO-TRADE RULE — {symbol} "
                f"close={latest_close} < orb_low={orb_low}"
            )
            _no_trade_set.add(symbol)
            with db_connection() as conn:
                add_no_trade_ticker(conn, symbol)
                log_signal(conn, symbol, 'NO_TRADE_RULE',
                           f"close={latest_close} orb_low={orb_low}")
            continue

        # --- All 5 criteria ---
        all_pass, details = all_five_pass(symbol, orb_high, avg_volume)
        if not all_pass:
            continue

        # Breakout detected — handle signal
        log.info(f"layer3: [{now_et}] BREAKOUT detected for {symbol}")
        _handle_breakout(candidate, details)


# ── Breakout handler ──────────────────────────────────────────────────────────

def _handle_breakout(candidate: dict, details: dict) -> None:
    """
    Recalculate live plan, send SMS, wait for confirmation, place order.
    Blocks for up to 60 s during SMS confirmation window.
    """
    symbol = candidate['symbol']
    _fired_set.add(symbol)   # prevent re-triggering while waiting

    now_et = datetime.now(ET).strftime('%H:%M ET')

    # Live plan recalculation
    with db_connection() as conn:
        budget = risk_budget(conn)

    plan = live_trade_plan(
        entry_candle_low=details['entry_candle_low'],
        current_close=details['latest_close'],
        risk_budget=budget,
    )
    if plan is None:
        log.warning(f"layer3: [{now_et}] {symbol} — invalid plan, skipping")
        _fired_set.discard(symbol)
        return

    # Update pre-inserted trade record with real breakout plan,
    # or insert fresh if no record exists (bootstrap plan was invalid).
    trade_id = candidate.get('trade_id')
    with db_connection() as conn:
        if trade_id:
            from db.queries_layer3 import update_trade
            update_trade(conn, trade_id,
                entry_price=plan['entry_price'],
                stop_price=plan['stop_price'],
                t1_price=plan['t1_price'],
                t2_price=plan['t2_price'],
                shares=plan['shares'],
                risk_amount=plan['risk_amount'],
            )
        else:
            trade_id = insert_trade(conn, {
                'ticker_id':         candidate['ticker_id'],
                'ai_score_id':       candidate.get('ai_score_id'),
                'symbol':            symbol,
                'orb_high':          candidate['orb_high'],
                'orb_low':           candidate['orb_low'],
                'orb_window':        candidate.get('orb_window'),
                'entry_price':       plan['entry_price'],
                'stop_price':        plan['stop_price'],
                't1_price':          plan['t1_price'],
                't2_price':          plan['t2_price'],
                'shares':            plan['shares'],
                'risk_amount':       plan['risk_amount'],
                'l2_entry_estimate': candidate.get('l2_entry_estimate'),
                'l2_stop_estimate':  candidate.get('l2_stop_estimate'),
            })
        log_signal(conn, symbol, 'BREAKOUT',
                   f"close={details['latest_close']} plan={plan}", trade_id)

    log.info(
        f"layer3: [{now_et}] {symbol} — sending SMS confirmation "
        f"(trade_id={trade_id})"
    )

    # Register in confirmation state so the web dashboard can show the signal
    # and accept a YES/NO reply during the 60-second window.
    set_pending(trade_id, symbol, plan, time.time() + 60)

    # SMS confirmation gate
    sms_sent = send_confirmation_sms(symbol, plan)
    if not sms_sent:
        # SMS failed — still allow web confirmation for the remaining window
        log.warning(f"layer3: {symbol} — SMS failed, web confirmation still open")

    reply = wait_for_reply(timeout_s=60, trade_id=trade_id)
    clear_pending(trade_id)

    with db_connection() as conn:
        if reply == 'YES':
            from db.queries_layer3 import update_trade
            update_trade(conn, trade_id, confirmed=True,
                         confirmation_time='NOW()')
            log_signal(conn, symbol, 'CONFIRMED', trade_id=trade_id)
        elif reply == 'NO':
            from db.queries_layer3 import update_trade
            update_trade(conn, trade_id, status='REJECTED',
                         confirmed=False, confirmation_time='NOW()')
            log_signal(conn, symbol, 'REJECTED', trade_id=trade_id)
            log.info(f"layer3: {symbol} — manually rejected")
            return
        else:  # TIMEOUT — auto-execute (no veto received)
            from db.queries_layer3 import update_trade
            update_trade(conn, trade_id, confirmed=None,
                         confirmation_time='NOW()')
            log_signal(conn, symbol, 'TIMEOUT', trade_id=trade_id)
            log.info(f"layer3: {symbol} — no reply within timeout, auto-executing")
            _send_sms(f"[{symbol}] no reply — auto-executing trade")

    # Place entry order
    broker = AlpacaBroker()
    try:
        entry_order = broker.place_market_buy(symbol, plan['shares'])
        entry_order_id = entry_order.get('id', '')
    except Exception as e:
        log.error(f"layer3: {symbol} — entry order failed: {e}")
        with db_connection() as conn:
            from db.queries_layer3 import update_trade
            update_trade(conn, trade_id, status='REJECTED')
        return

    # Wait for fill
    filled_order = broker.wait_for_fill(entry_order_id)
    filled_price = float(filled_order.get('filled_avg_price') or plan['entry_price'])

    # Recalculate T1/T2 based on actual fill price
    risk_per_share = filled_price - plan['stop_price']
    t1_actual = round(filled_price + risk_per_share, 4)
    t2_actual = round(filled_price + 2 * risk_per_share, 4)

    # Place protective stop loss
    try:
        stop_order = broker.place_stop_sell(
            symbol, plan['shares'], plan['stop_price']
        )
        stop_order_id = stop_order.get('id', '')
    except Exception as e:
        log.error(f"layer3: {symbol} — stop order failed: {e}")
        stop_order_id = ''

    with db_connection() as conn:
        from db.queries_layer3 import update_trade
        update_trade(conn, trade_id,
            entry_order_id=entry_order_id,
            stop_order_id=stop_order_id,
            filled_entry=filled_price,
            t1_price=t1_actual,
            t2_price=t2_actual,
            status='OPEN')

    log.info(
        f"layer3: {symbol} — order placed | "
        f"filled=${filled_price:.4f} stop=${plan['stop_price']:.4f} "
        f"T1=${t1_actual:.4f} T2=${t2_actual:.4f}"
    )

    # Update plan with actual fill prices for the manager
    live_plan = {**plan, 'entry_price': filled_price,
                 't1_price': t1_actual, 't2_price': t2_actual}

    # Spawn TradeManager thread
    manager = TradeManager(trade_id, live_plan, symbol,
                           entry_order_id, stop_order_id)
    _active_managers.append(manager)

    t = threading.Thread(
        target=manager.run,
        name=f'trade-{symbol}',
        daemon=True,
    )
    _manager_threads.append(t)
    t.start()


# ── Force exit ────────────────────────────────────────────────────────────────

def _force_exit_all() -> None:
    """3:45 PM — close all open positions, stop all managers."""
    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(f"layer3: [{now_et}] FORCE EXIT — closing all positions")

    # Signal all active managers first (so they don't double-close)
    for mgr in _active_managers:
        if not mgr.closed:
            mgr.force_exit('FORCE_EXIT')

    # Belt-and-suspenders: close anything Alpaca still has open
    broker = AlpacaBroker()
    broker.close_all_positions()

    log.info(f"layer3: [{now_et}] force exit complete")


# ── SMS helper ────────────────────────────────────────────────────────────────

def _send_sms(message: str) -> None:
    import os
    import requests as req
    sid   = os.getenv('TWILIO_ACCOUNT_SID', '')
    token = os.getenv('TWILIO_AUTH_TOKEN',  '')
    frm   = os.getenv('TWILIO_FROM', '')
    to    = os.getenv('TWILIO_TO',   '')
    if not (sid and token and frm and to):
        return
    try:
        req.post(
            f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
            auth=(sid, token),
            data={'From': frm, 'To': to, 'Body': message},
            timeout=8,
        )
    except Exception as e:
        log.warning(f"layer3: SMS failed: {e}")
