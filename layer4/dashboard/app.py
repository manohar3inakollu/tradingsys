"""
Layer 4 dashboard — Flask web UI.

Routes:
  GET  /                         → redirect to /dash
  GET  /dash                     → morning dashboard
  GET  /cockpit                  → live trade cockpit
  GET  /analytics                → historical analytics
  GET  /progress                 → account progress vs plan
  GET  /reports                  → session history, trade log, downloads
  GET  /export/trades.csv        → all closed trades as CSV
  GET  /export/sessions.csv      → all sessions as CSV
  GET  /api/pending              → JSON list of trades awaiting web confirmation
  POST /api/confirm/<trade_id>   → web YES — confirm a pending trade
  POST /api/reject/<trade_id>    → web NO  — reject a pending trade
  POST /api/add-stock            → manually add a stock (full scoring pipeline)

Run on DASHBOARD_PORT (default 5000).
Start via layer4.runner.start_dashboard() — runs in a background thread.
"""

import csv
import io
import os
import functools
import hashlib
import secrets
from datetime import datetime, date

from flask import (
    Flask, render_template, redirect, url_for, jsonify,
    request, Response, session,
)
from dotenv import load_dotenv

load_dotenv()

from db.connection import db_connection
import requests as _req

from layer4.queries import (
    get_todays_layer1, get_todays_ai_scores, get_todays_l3_plans,
    get_market_context, get_live_trades, get_todays_closed_trades,
    get_win_rate_by_bucket, get_r_mult_by_catalyst,
    get_confirmation_outcomes, get_overall_stats,
    get_daily_pnl_series, get_all_sessions, get_signal_log_today,
    get_all_closed_trades, get_symbol_score_detail, get_symbol_history,
    get_symbol_info, get_per_symbol_stats, get_exit_reason_stats,
    get_time_of_day_stats, get_catalyst_log, get_recent_streak,
)
from timing import ET

app = Flask(__name__, template_folder='templates')
app.secret_key = os.getenv('DASH_SECRET_KEY', secrets.token_hex(24))


# ── Authentication ────────────────────────────────────────────────────────────

_DASH_USER = os.getenv('DASH_USER', 'admin')
_DASH_PASS = os.getenv('DASH_PASSWORD', '')


def login_required(f):
    """Decorator — redirects to /login if not authenticated."""
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return wrapped


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        user = request.form.get('username', '')
        pwd  = request.form.get('password', '')
        if user == _DASH_USER and pwd == _DASH_PASS:
            session['authenticated'] = True
            session['user'] = user
            next_url = request.args.get('next') or url_for('dash')
            return redirect(next_url)
        error = 'Invalid username or password'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


def _now_et():
    return datetime.now(ET)


def _csv_response(rows: list, filename: str) -> Response:
    """Serialize a list of dicts to a CSV download response."""
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            # Convert non-string types (date, datetime, Decimal) to str
            writer.writerow({k: (v.isoformat() if hasattr(v, 'isoformat') else v)
                             for k, v in row.items()})
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


# ── Alpaca helpers ─────────────────────────────────────────────────────────────

_ALPACA_TRADE_BASE = 'https://paper-api.alpaca.markets/v2'
_ALPACA_DATA_BASE  = 'https://data.alpaca.markets/v2'


def _alpaca_headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
        'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
    }


def _get_alpaca_account() -> dict:
    """Fetch live account data from Alpaca. Returns {} on failure."""
    try:
        r = _req.get(
            f'{_ALPACA_TRADE_BASE}/account',
            headers=_alpaca_headers(),
            timeout=5,
        )
        if r.ok:
            d = r.json()
            return {
                'equity':          float(d.get('equity') or 0),
                'last_equity':     float(d.get('last_equity') or 0),
                'buying_power':    float(d.get('buying_power') or 0),
                'cash':            float(d.get('cash') or 0),
                'portfolio_value': float(d.get('portfolio_value') or 0),
                'unrealized_pl':   float(d.get('unrealized_pl') or 0),
                'status':          d.get('status', ''),
            }
    except Exception:
        pass
    return {}


def _get_alpaca_snapshot(symbol: str) -> dict:
    """Fetch latest snapshot (quote + bars) for a symbol. Returns {} on failure."""
    try:
        r = _req.get(
            f'{_ALPACA_DATA_BASE}/stocks/{symbol}/snapshot',
            headers=_alpaca_headers(),
            params={'feed': 'iex'},
            timeout=4,
        )
        if r.ok:
            return r.json()
    except Exception:
        pass
    return {}


# ── Root redirect ──────────────────────────────────────────────────────────────

@app.route('/')
@login_required
def index():
    return redirect(url_for('dash'))


# ── Morning dashboard ──────────────────────────────────────────────────────────

@app.route('/dash')
@login_required
def dash():
    try:
        with db_connection() as conn:
            layer1    = get_todays_layer1(conn)
            ai_scores = get_todays_ai_scores(conn)
            l3_plans  = get_todays_l3_plans(conn)
            ctx       = get_market_context(conn)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

    has_scores = bool(ai_scores)
    score_map  = {r['symbol']: r for r in ai_scores}
    l3_map     = {r['symbol']: r for r in l3_plans}

    for row in layer1:
        sym = row['symbol']
        if sym in score_map:
            row.update(score_map[sym])
        if sym in l3_map:
            l3 = l3_map[sym]
            row['entry_price'] = float(l3['filled_entry'] or l3['entry_price'] or row.get('entry_price') or 0) or None
            row['stop_price']  = float(l3['stop_price'])  if l3['stop_price']  else row.get('stop_price')
            row['t1_price']    = float(l3['t1_price'])    if l3['t1_price']    else row.get('t1_price')
            row['t2_price']    = float(l3['t2_price'])    if l3['t2_price']    else row.get('t2_price')
            row['shares']      = int(l3['shares'])        if l3['shares']      else row.get('shares')
            row['risk_amount'] = float(l3['risk_amount']) if l3['risk_amount'] else row.get('risk_amount')
            row['orb_high']    = float(l3['orb_high'])    if l3['orb_high']    else None
            row['orb_low']     = float(l3['orb_low'])     if l3['orb_low']     else None
            row['l3_status']   = l3['status']

    # Risk remaining = budget per trade × trades left (max 3)
    risk_remaining = round(float(ctx.get('risk_budget', 80)) * max(3 - int(ctx.get('trades_count', 0)), 0), 2)

    return render_template(
        'dash.html',
        layer1=layer1,
        has_scores=has_scores,
        ctx=ctx,
        risk_remaining=risk_remaining,
        now=_now_et().strftime('%H:%M ET'),
        today=date.today().strftime('%a %b %d'),
    )


# ── Trade cockpit ──────────────────────────────────────────────────────────────

@app.route('/cockpit')
@login_required
def cockpit():
    try:
        with db_connection() as conn:
            live_trades   = get_live_trades(conn)
            closed_trades = get_todays_closed_trades(conn)
            ctx           = get_market_context(conn)
            signal_log    = get_signal_log_today(conn)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

    # Fetch live prices for open positions
    live_prices = {}
    if live_trades:
        try:
            headers = _alpaca_headers()
            for trade in live_trades:
                sym = trade.get('symbol', '')
                if not sym:
                    continue
                r = _req.get(
                    f'{_ALPACA_DATA_BASE}/stocks/{sym}/bars/latest',
                    headers=headers,
                    params={'feed': 'iex'},
                    timeout=3,
                )
                if r.ok:
                    live_prices[sym] = float(r.json().get('bar', {}).get('c', 0))
        except Exception:
            pass

    for t in live_trades:
        sym    = t.get('symbol', '')
        price  = live_prices.get(sym, 0.0)
        entry  = float(t.get('filled_entry') or t.get('entry_price') or 0)
        shares = int(t.get('shares') or 0)
        t['live_price'] = price
        t['live_pnl']   = round((price - entry) * shares, 2) if price and entry else 0.0
        t['r_current']  = round(
            (price - entry) / (entry - float(t.get('stop_price') or entry - 0.01)),
            2
        ) if price and entry and t.get('stop_price') else 0.0

    today_pnl = (
        sum(float(t.get('pnl') or 0) for t in closed_trades) +
        sum(t['live_pnl'] for t in live_trades)
    )

    # Format signal log times for display
    for entry in signal_log:
        ts = entry.get('log_time_et')
        if ts:
            entry['time_str'] = ts.strftime('%H:%M:%S') if hasattr(ts, 'strftime') else str(ts)[:8]

    return render_template(
        'cockpit.html',
        live_trades=live_trades,
        closed_trades=closed_trades,
        ctx=ctx,
        today_pnl=today_pnl,
        signal_log=signal_log,
        now=_now_et().strftime('%H:%M ET'),
        today=date.today().strftime('%a %b %d'),
    )


# ── Analytics ──────────────────────────────────────────────────────────────────

@app.route('/analytics')
@login_required
def analytics():
    try:
        with db_connection() as conn:
            buckets      = get_win_rate_by_bucket(conn)
            by_catalyst  = get_r_mult_by_catalyst(conn)
            by_confirm   = get_confirmation_outcomes(conn)
            overall      = get_overall_stats(conn)
            by_symbol    = get_per_symbol_stats(conn)
            by_exit      = get_exit_reason_stats(conn)
            by_hour      = get_time_of_day_stats(conn)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

    return render_template(
        'analytics.html',
        buckets=buckets,
        by_catalyst=by_catalyst,
        by_confirm=by_confirm,
        overall=overall,
        by_symbol=by_symbol,
        by_exit=by_exit,
        by_hour=by_hour,
        now=_now_et().strftime('%H:%M ET'),
    )


# ── Account progress ───────────────────────────────────────────────────────────

@app.route('/progress')
@login_required
def progress():
    try:
        with db_connection() as conn:
            daily_series = get_daily_pnl_series(conn)
            overall      = get_overall_stats(conn)
            streak       = get_recent_streak(conn)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

    # Live account data from Alpaca (graceful fallback to env if unavailable)
    alpaca_acct    = _get_alpaca_account()
    start_balance  = float(os.getenv('ACCOUNT_START_BALANCE', '8000'))
    monthly_target = float(os.getenv('ACCOUNT_MONTHLY_TARGET_PCT', '8'))
    daily_growth   = (1 + monthly_target / 100) ** (1 / 20)

    # Use Alpaca equity as the real current balance; fall back to DB calculation
    if alpaca_acct.get('equity'):
        current_balance = round(alpaca_acct['equity'], 2)
    else:
        current_balance = round(start_balance + float(overall.get('total_pnl') or 0), 2)

    cumulative = 0.0
    curve = []
    for i, row in enumerate(daily_series):
        cumulative += float(row['daily_pnl'])
        plan_balance   = round(start_balance * (daily_growth ** (i + 1)), 2)
        actual_balance = round(start_balance + cumulative, 2)
        curve.append({
            'date':           row['trade_date'].strftime('%m/%d'),
            'daily_pnl':      float(row['daily_pnl']),
            'actual_balance': actual_balance,
            'plan_balance':   plan_balance,
            'trades':         row['trades'],
        })

    days_traded = len(daily_series)
    # Day 0 = start balance; only advance the plan once trading has begun
    plan_today  = round(start_balance * (daily_growth ** days_traded), 2)
    vs_plan     = round(current_balance - plan_today, 2)

    peak = start_balance
    max_dd = 0.0
    running = start_balance
    for row in daily_series:
        running += float(row['daily_pnl'])
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    # Alpaca-specific extras (shown only when available)
    today_change = round(alpaca_acct.get('equity', 0) - alpaca_acct.get('last_equity', 0), 2) if alpaca_acct else 0.0

    return render_template(
        'progress.html',
        curve=curve,
        current_balance=current_balance,
        start_balance=start_balance,
        plan_today=plan_today,
        vs_plan=vs_plan,
        max_dd=round(max_dd, 2),
        monthly_target=monthly_target,
        days_traded=days_traded,
        overall=overall,
        alpaca=alpaca_acct,
        today_change=today_change,
        streak=list(reversed(streak)),
        now=_now_et().strftime('%H:%M ET'),
    )


# ── Reports & export ───────────────────────────────────────────────────────────

@app.route('/reports')
@login_required
def reports():
    try:
        with db_connection() as conn:
            sessions     = get_all_sessions(conn)
            trades       = get_all_closed_trades(conn)
            catalyst_log = get_catalyst_log(conn)
    except Exception as e:
        return render_template('error.html', error=str(e)), 500

    return render_template(
        'reports.html',
        sessions=sessions,
        trades=list(reversed(trades)),
        catalyst_log=catalyst_log,
        now=_now_et().strftime('%H:%M ET'),
        today=date.today().strftime('%a %b %d'),
        today_iso=date.today().isoformat(),
    )


@app.route('/export/trades.csv')
@login_required
def export_trades_csv():
    try:
        with db_connection() as conn:
            trades = get_all_closed_trades(conn)
    except Exception as e:
        return str(e), 500
    return _csv_response(trades, f'trades_{date.today()}.csv')


@app.route('/export/sessions.csv')
@login_required
def export_sessions_csv():
    try:
        with db_connection() as conn:
            sessions = get_all_sessions(conn)
    except Exception as e:
        return str(e), 500
    return _csv_response(sessions, f'sessions_{date.today()}.csv')


# ── Stock detail API ───────────────────────────────────────────────────────────

@app.route('/api/stock/<symbol>')
@login_required
def api_stock(symbol: str):
    """Return live quote + AI score + trade history for a symbol."""
    symbol = symbol.upper()

    # Live market snapshot from Alpaca
    snap = _get_alpaca_snapshot(symbol)
    daily_bar  = snap.get('dailyBar') or {}
    prev_bar   = snap.get('prevDailyBar') or {}
    latest_trade = snap.get('latestTrade') or {}
    latest_quote = snap.get('latestQuote') or {}
    minute_bar = snap.get('minuteBar') or {}

    last_price = float(latest_trade.get('p') or daily_bar.get('c') or 0)
    prev_close = float(prev_bar.get('c') or 0)
    day_change     = round(last_price - prev_close, 2) if last_price and prev_close else None
    day_change_pct = round(day_change / prev_close * 100, 2) if day_change and prev_close else None

    market = {
        'last':        last_price,
        'open':        float(daily_bar.get('o') or 0),
        'high':        float(daily_bar.get('h') or 0),
        'low':         float(daily_bar.get('l') or 0),
        'volume':      int(daily_bar.get('v') or 0),
        'vwap':        float(daily_bar.get('vw') or 0),
        'prev_close':  prev_close,
        'day_change':  day_change,
        'day_change_pct': day_change_pct,
        'bid':         float(latest_quote.get('bp') or 0),
        'ask':         float(latest_quote.get('ap') or 0),
        'bid_size':    int(latest_quote.get('bs') or 0),
        'ask_size':    int(latest_quote.get('as') or 0),
        'minute_vol':  int(minute_bar.get('v') or 0),
    }

    # DB: today's score + trade history + company info
    score = None
    history = []
    info = None
    try:
        with db_connection() as conn:
            score   = get_symbol_score_detail(conn, symbol)
            history = get_symbol_history(conn, symbol)
            info    = get_symbol_info(conn, symbol)
    except Exception:
        pass

    def _coerce(v):
        """Make psycopg3 Decimal / date / datetime JSON-safe."""
        if hasattr(v, 'isoformat'):
            return v.isoformat()
        try:
            return float(v)
        except (TypeError, ValueError):
            return v

    if score:
        score = {k: _coerce(v) for k, v in score.items()}
    for h in history:
        for k, v in list(h.items()):
            h[k] = _coerce(v)

    if info:
        info = {k: _coerce(v) for k, v in info.items()}

    # ORB-based plan — computed live from Alpaca, shown in the drawer for TRADE stocks
    orb_plan = None
    try:
        ticker_type = (info or {}).get('type', 'stock')
        orb_window  = 15 if ticker_type == 'etf' else 5
        from layer3.orb import get_orb_range
        from layer3.signal import live_trade_plan
        orb = get_orb_range(symbol, orb_window)
        if orb and orb['high'] > 0 and orb['low'] > 0:
            try:
                with db_connection() as conn2:
                    ctx = get_market_context(conn2)
            except Exception:
                ctx = {}
            risk_budget = float(ctx.get('risk_budget') or 80)
            plan = live_trade_plan(
                entry_candle_low=orb['low'],
                current_close=orb['high'],
                risk_budget=risk_budget,
            )
            if plan:
                orb_plan = {**plan, 'orb_high': orb['high'], 'orb_low': orb['low'],
                            'orb_window': orb_window}
    except Exception:
        pass

    return jsonify({
        'symbol':   symbol,
        'market':   market,
        'score':    score,
        'history':  history,
        'info':     info,
        'orb_plan': orb_plan,
    })


# ── Confirmation API ───────────────────────────────────────────────────────────

@app.route('/api/pending')
@login_required
def api_pending():
    """
    Return trades currently awaiting confirmation.
    Primary:  in-memory confirmation_state  (production — same process as layer3)
    Fallback: DB query for PENDING trades created in the last 90 seconds
              (test scripts that run in a separate process)
    """
    # Primary: in-memory (fast, used in production)
    try:
        from layer3.confirmation_state import get_all_pending
        pending = get_all_pending()
        if pending:
            return jsonify(pending)
    except Exception:
        pass

    # Fallback: DB scan for recently-created PENDING trades
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        tr.id,
                        tr.symbol,
                        tr.entry_price,
                        tr.stop_price,
                        tr.t1_price,
                        tr.t2_price,
                        tr.shares,
                        tr.risk_amount,
                        EXTRACT(EPOCH FROM (tr.created_at + INTERVAL '90 seconds'))
                            AS deadline_ts
                    FROM trades tr
                    WHERE tr.status   = 'PENDING'
                      AND tr.confirmed IS NULL
                      AND tr.trade_date = CURRENT_DATE
                      AND tr.created_at > NOW() - INTERVAL '90 seconds'
                    ORDER BY tr.created_at DESC
                """)
                rows = cur.fetchall()
        return jsonify([{
            'id':          r[0],
            'symbol':      r[1],
            'entry_price': float(r[2] or 0),
            'stop_price':  float(r[3] or 0),
            't1_price':    float(r[4] or 0),
            't2_price':    float(r[5] or 0),
            'shares':      r[6] or 0,
            'risk_amount': float(r[7] or 0),
            'deadline_ts': float(r[8] or 0),
        } for r in rows])
    except Exception:
        return jsonify([])


@app.route('/api/confirm/<int:trade_id>', methods=['POST'])
@login_required
def api_confirm(trade_id):
    """Web YES — confirm a pending trade."""
    try:
        from layer3.confirmation_state import set_web_reply
        set_web_reply(trade_id, 'YES')
        # Also persist to DB so separate processes (test scripts) can see it
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE trades SET confirmed = TRUE, confirmation_time = NOW()"
                    " WHERE id = %s",
                    (trade_id,)
                )
            conn.commit()
        return jsonify({'ok': True, 'action': 'confirmed', 'trade_id': trade_id})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/reject/<int:trade_id>', methods=['POST'])
@login_required
def api_reject(trade_id):
    """Web NO — reject a pending trade."""
    try:
        from layer3.confirmation_state import set_web_reply
        set_web_reply(trade_id, 'NO')
        # Also persist to DB so separate processes (test scripts) can see it
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE trades SET confirmed = FALSE, confirmation_time = NOW()"
                    " WHERE id = %s",
                    (trade_id,)
                )
            conn.commit()
        return jsonify({'ok': True, 'action': 'rejected', 'trade_id': trade_id})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/override/<int:ai_score_id>', methods=['POST'])
@login_required
def api_override(ai_score_id):
    """
    Override an AI decision (SKIP/WATCH → WATCH or TRADE).
    When upgrading to TRADE, recalculate entry/stop/T1/T2/shares using the
    current live price and today's risk budget.
    """
    body = request.get_json(silent=True) or {}
    decision = (body.get('decision') or '').upper()
    if decision not in ('WATCH', 'TRADE', 'SKIP'):
        return jsonify({'ok': False, 'error': 'decision must be WATCH, TRADE, or SKIP'}), 400

    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT t.symbol, t.type,
                           COALESCE(sr.orb_window, CASE WHEN t.type = 'etf' THEN 15 ELSE 5 END)
                               AS orb_window
                    FROM ai_scores a
                    JOIN tickers t ON t.id = a.ticker_id
                    LEFT JOIN scan_results sr ON sr.ticker_id = a.ticker_id
                                             AND sr.scan_date = CURRENT_DATE
                    WHERE a.id = %s AND a.scan_date = CURRENT_DATE
                    LIMIT 1
                """, (ai_score_id,))
                row = cur.fetchone()

            if not row:
                return jsonify({'ok': False, 'error': 'score not found for today'}), 404

            symbol, ticker_type, orb_window = row

            plan = None
            orb  = None
            if decision == 'TRADE':
                from layer3.orb import get_orb_range
                orb = get_orb_range(symbol, int(orb_window))

                if not (orb and orb['high'] > 0 and orb['low'] > 0):
                    return jsonify({
                        'ok':    False,
                        'error': f'ORB not available yet for {symbol} — try after the {int(orb_window)}-min opening window closes',
                    }), 422

                # entry = ORB high + 0.05%, stop = ORB low - 0.1%
                # (applied inside live_trade_plan)
                ctx         = get_market_context(conn)
                risk_budget = float(ctx.get('risk_budget') or 80)

                from layer3.signal import live_trade_plan
                plan = live_trade_plan(
                    entry_candle_low=orb['low'],
                    current_close=orb['high'],
                    risk_budget=risk_budget,
                )
                if plan is None:
                    return jsonify({
                        'ok':    False,
                        'error': f'ORB stop too wide ({orb["high"]} / {orb["low"]}) — plan rejected',
                    }), 422

            # Build update fields
            if plan:
                # Upgrading to TRADE with fresh plan
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ai_scores
                           SET decision    = %s,
                               entry_price = %s,
                               stop_price  = %s,
                               t1_price    = %s,
                               t2_price    = %s,
                               shares      = %s,
                               risk_amount = %s
                         WHERE id = %s AND scan_date = CURRENT_DATE
                    """, (decision,
                          plan['entry_price'], plan['stop_price'],
                          plan['t1_price'],    plan['t2_price'],
                          plan['shares'],      plan['risk_amount'],
                          ai_score_id))
                    updated = cur.rowcount
            elif decision in ('WATCH', 'SKIP'):
                # Downgrading — clear trade plan so Layer 3 ignores this symbol
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE ai_scores
                           SET decision    = %s,
                               entry_price = NULL,
                               stop_price  = NULL,
                               t1_price    = NULL,
                               t2_price    = NULL,
                               shares      = NULL,
                               risk_amount = NULL
                         WHERE id = %s AND scan_date = CURRENT_DATE
                    """, (decision, ai_score_id))
                    updated = cur.rowcount
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE ai_scores SET decision = %s"
                        " WHERE id = %s AND scan_date = CURRENT_DATE",
                        (decision, ai_score_id)
                    )
                    updated = cur.rowcount

            conn.commit()

        if updated == 0:
            return jsonify({'ok': False, 'error': 'score not found for today'}), 404

        resp = {'ok': True, 'decision': decision, 'ai_score_id': ai_score_id}
        if plan:
            resp['plan'] = plan
            resp['stop_source'] = f'ORB {int(orb_window)}min'
        return jsonify(resp)

    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/add-stock', methods=['POST'])
@login_required
def api_add_stock():
    """
    Manually add a stock to today's dashboard.
    Fetches market data from Tradier, runs full Layer 2 scoring (news + Haiku),
    inserts into tickers, daily_prices, scan_results, and ai_scores.
    """
    body = request.get_json(silent=True) or {}
    symbol = (body.get('symbol') or '').upper().strip()
    if not symbol or len(symbol) > 10:
        return jsonify({'ok': False, 'error': 'invalid symbol'}), 400

    try:
        from layer3.tradier_client import get_daily_data, get_premarket_data
        from scanner.alpaca_client import AlpacaClient
        from scanner.filters import calculate_atr_pct
        from db.queries import upsert_ticker, upsert_daily_price, insert_scan_result
        from timing import ETF_WATCHLIST

        # Check if already scanned today
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sr.id FROM scan_results sr
                    JOIN tickers t ON t.id = sr.ticker_id
                    WHERE t.symbol = %s AND sr.scan_date = CURRENT_DATE
                      AND sr.passed_filters = TRUE
                """, (symbol,))
                if cur.fetchone():
                    return jsonify({'ok': False, 'error': f'{symbol} is already on today\'s dashboard'}), 409

        # 1. Fetch market data from Tradier
        daily = get_daily_data(symbol)
        if not daily or not daily.get('close'):
            return jsonify({'ok': False, 'error': f'No market data for {symbol} — check ticker'}), 422

        pm_volume, pm_high, pm_low = get_premarket_data(symbol)

        # 2. Determine type and ORB window
        is_etf = symbol in ETF_WATCHLIST
        ticker_type = 'etf' if is_etf else 'stock'
        orb_window = 15 if is_etf else 5

        # 3. Fetch VIX
        vix = 0.0
        try:
            alpaca = AlpacaClient()
            vix = alpaca.get_vix()
        except Exception:
            pass

        price = daily['close']
        atr = daily.get('atr', 0)
        atr_pct = calculate_atr_pct(atr, price)
        gap_pct = daily.get('gap_pct', 0)
        prev_close = daily.get('prev_close', 0)

        today_str = datetime.now(ET).strftime('%Y-%m-%d')

        # 4. Persist: ticker -> daily_prices -> scan_results
        with db_connection() as conn:
            ticker_id = upsert_ticker(conn, symbol, ticker_type)

            price_id = upsert_daily_price(conn, ticker_id, {
                'date':       today_str,
                'open':       daily.get('open', price),
                'high':       daily.get('high', price),
                'low':        daily.get('low', price),
                'close':      price,
                'volume':     daily.get('volume', 0),
                'prev_close': prev_close,
                'gap_pct':    gap_pct,
                'pm_high':    pm_high,
                'pm_low':     pm_low,
                'pm_volume':  pm_volume,
                'atr_20':     atr,
                'atr_pct':    atr_pct,
                'vix':        vix,
            })

            scan_result_id = insert_scan_result(
                conn,
                scan_date=today_str,
                ticker_id=ticker_id,
                price_id=price_id,
                passed=True,
                failed=[],
                gap_pct=gap_pct,
                orb_window=orb_window,
                rank=99,  # manual adds rank last
            )

        # 5. Run Layer 2 scoring: news -> Haiku -> scoring model -> ai_scores
        candidate = {
            'ticker_id':      ticker_id,
            'symbol':         symbol,
            'type':           ticker_type,
            'sector':         None,
            'company':        '',
            'price_id':       price_id,
            'scan_result_id': scan_result_id,
            'open':           daily.get('open', price),
            'high':           daily.get('high', price),
            'low':            daily.get('low', price),
            'close':          price,
            'gap_pct':        gap_pct,
            'pm_high':        pm_high,
            'pm_low':         pm_low,
            'pm_volume':      pm_volume,
            'atr_20':         atr,
            'atr_pct':        atr_pct,
            'vix':            vix,
        }

        try:
            from layer2.news_client import fetch_news
            from layer2.haiku_scorer import score_catalyst
            from layer2.scoring import score_candidate
            from layer2.session_gates import get_spy_change
            from db.queries_layer2 import upsert_ai_score

            spy_change = get_spy_change()
            news = fetch_news(symbol, company_name='')
            catalyst = score_catalyst(symbol, news)
            scores = score_candidate(candidate, catalyst, spy_change)

            with db_connection() as conn:
                ai_score_id = upsert_ai_score(conn, candidate, news, catalyst, scores, {})

        except Exception as e:
            # Scoring failed — still show on dashboard with no AI score
            ai_score_id = None
            scores = None
            import traceback
            traceback.print_exc()

        return jsonify({
            'ok':       True,
            'symbol':   symbol,
            'type':     ticker_type,
            'gap_pct':  round(gap_pct, 1),
            'atr_pct':  round(atr_pct, 1),
            'pm_volume': pm_volume,
            'score':    scores.get('score_final') if scores else None,
            'decision': scores.get('decision') if scores else None,
            'ai_score_id': ai_score_id,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e)}), 500
