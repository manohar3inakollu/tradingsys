"""
Weekly Google Sheets fill — runs Sunday at 6:00 PM ET.

Writes four tabs:
  1. Trade Log       — every closed trade (all-time)
  2. Weekly Review   — last 7 days summary per day
  3. Catalyst Tracker — R-multiple breakdown by catalyst type
  4. Account Curve   — cumulative balance vs plan path

Required env vars:
  GOOGLE_SERVICE_ACCOUNT_JSON   path to service-account JSON file
  SHEETS_SPREADSHEET_ID         Google Sheets spreadsheet ID

Required packages:
  gspread
  google-auth
"""

import os
import logging
from datetime import date, timedelta

from db.connection import db_connection
from layer4.queries import (
    get_all_closed_trades,
    get_daily_pnl_series,
    get_r_mult_by_catalyst,
    get_overall_stats,
)

log = logging.getLogger(__name__)


def _get_client():
    """Return an authenticated gspread client."""
    import gspread
    from google.oauth2.service_account import Credentials

    sa_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', '')
    if not sa_path:
        raise ValueError('GOOGLE_SERVICE_ACCOUNT_JSON env var not set')

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def _ensure_tab(spreadsheet, title: str):
    """Return worksheet by title, creating it if absent."""
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        return spreadsheet.add_worksheet(title=title, rows=2000, cols=20)


def _clear_and_write(ws, rows: list[list]):
    """Clear worksheet and write all rows starting at A1."""
    ws.clear()
    if rows:
        ws.update('A1', rows)


# ── Tab writers ───────────────────────────────────────────────────────────────

def _write_trade_log(ws, trades: list):
    header = [
        'Date', 'Symbol', 'Catalyst', 'Score', 'Decision confirmed',
        'Entry', 'Stop', 'T1', 'T2', 'Shares', 'Risk $',
        'Filled entry', 'Exit price', 'Exit reason',
        'P&L', 'R-multiple', 'Status',
    ]
    rows = [header]
    for t in trades:
        rows.append([
            str(t.get('trade_date', '')),
            t.get('symbol', ''),
            t.get('catalyst_type', ''),
            t.get('score_final', ''),
            'YES' if t.get('confirmed') is True else ('NO' if t.get('confirmed') is False else 'TIMEOUT'),
            t.get('entry_price', ''),
            t.get('stop_price', ''),
            t.get('t1_price', ''),
            t.get('t2_price', ''),
            t.get('shares', ''),
            t.get('risk_amount', ''),
            t.get('filled_entry', ''),
            t.get('exit_price', ''),
            t.get('exit_reason', ''),
            t.get('pnl', ''),
            t.get('r_multiple', ''),
            t.get('status', ''),
        ])
    _clear_and_write(ws, rows)
    log.info('Trade Log tab: %d rows', len(rows) - 1)


def _write_weekly_review(ws, daily_series: list):
    """Last 7 trading days."""
    header = ['Date', 'Trades', 'Daily P&L', 'Cumulative P&L']
    rows   = [header]
    series = daily_series[-7:] if len(daily_series) > 7 else daily_series
    cum = 0.0
    for row in series:
        cum += float(row['daily_pnl'])
        rows.append([
            row['trade_date'].strftime('%Y-%m-%d'),
            row['trades'],
            round(float(row['daily_pnl']), 2),
            round(cum, 2),
        ])
    _clear_and_write(ws, rows)
    log.info('Weekly Review tab: %d rows', len(rows) - 1)


def _write_catalyst_tracker(ws, by_catalyst: list):
    header = ['Catalyst type', 'Trades', 'Wins', 'Win rate %', 'Avg R', 'Total P&L']
    rows   = [header]
    for r in by_catalyst:
        wr = round(r['wins'] / r['trades'] * 100, 1) if r['trades'] else 0
        rows.append([
            r.get('catalyst_type', ''),
            r['trades'],
            r['wins'],
            wr,
            r.get('avg_r', ''),
            round(float(r.get('total_pnl') or 0), 2),
        ])
    _clear_and_write(ws, rows)
    log.info('Catalyst Tracker tab: %d rows', len(rows) - 1)


def _write_account_curve(ws, daily_series: list, start_balance: float, monthly_target: float):
    header = ['Date', 'Daily P&L', 'Actual balance', 'Plan balance', 'vs Plan', 'Trades']
    rows   = [header]

    daily_growth = (1 + monthly_target / 100) ** (1 / 20)
    cumulative   = 0.0

    for i, row in enumerate(daily_series):
        cumulative     += float(row['daily_pnl'])
        plan_balance    = round(start_balance * (daily_growth ** (i + 1)), 2)
        actual_balance  = round(start_balance + cumulative, 2)
        rows.append([
            row['trade_date'].strftime('%Y-%m-%d'),
            round(float(row['daily_pnl']), 2),
            actual_balance,
            plan_balance,
            round(actual_balance - plan_balance, 2),
            row['trades'],
        ])
    _clear_and_write(ws, rows)
    log.info('Account Curve tab: %d rows', len(rows) - 1)


# ── Public entry point ────────────────────────────────────────────────────────

def fill_weekly_sheets() -> bool:
    """Write all four tabs to the configured spreadsheet. Returns True on success."""
    spreadsheet_id = os.getenv('SHEETS_SPREADSHEET_ID', '')
    if not spreadsheet_id:
        log.warning('Weekly sheets skipped — SHEETS_SPREADSHEET_ID not set')
        return False

    start_balance  = float(os.getenv('ACCOUNT_START_BALANCE', '8000'))
    monthly_target = float(os.getenv('ACCOUNT_MONTHLY_TARGET_PCT', '8'))

    try:
        with db_connection() as conn:
            trades       = get_all_closed_trades(conn)
            daily_series = get_daily_pnl_series(conn)
            by_catalyst  = get_r_mult_by_catalyst(conn)
    except Exception as exc:
        log.error('Weekly sheets DB error: %s', exc)
        return False

    try:
        gc           = _get_client()
        spreadsheet  = gc.open_by_key(spreadsheet_id)
    except Exception as exc:
        log.error('Weekly sheets gspread error: %s', exc)
        return False

    try:
        _write_trade_log(      _ensure_tab(spreadsheet, 'Trade Log'),        trades)
        _write_weekly_review(  _ensure_tab(spreadsheet, 'Weekly Review'),    daily_series)
        _write_catalyst_tracker(_ensure_tab(spreadsheet, 'Catalyst Tracker'), by_catalyst)
        _write_account_curve(  _ensure_tab(spreadsheet, 'Account Curve'),    daily_series, start_balance, monthly_target)
        log.info('Weekly sheets fill complete — spreadsheet %s', spreadsheet_id)
        return True
    except Exception as exc:
        log.error('Weekly sheets write error: %s', exc)
        return False
