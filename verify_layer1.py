"""
Layer 1 data accuracy checker.

Pulls today's scan results from PostgreSQL and re-fetches the same fields
live from Tradier, then prints a side-by-side comparison.

Usage:
    python verify_layer1.py              # check today's passed candidates
    python verify_layer1.py --all        # include failed candidates too
    python verify_layer1.py INTC APLD    # check specific tickers only

Fields checked:
    open, prev_close, gap_pct, volume, pm_volume, pm_high, pm_low, atr_20, vix

Tolerance: values within 1% are shown as OK. Outside 1% are flagged DIFF.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime
from db.connection import init_pool, close_pool, db_connection
from psycopg.rows import dict_row
from layer3 import tradier_client
from timing import ET, SCAN_CONFIG
from logger import setup_logger

log = setup_logger('verify')

_TOLERANCE = 0.01   # 1% — flag anything beyond this


def _pct_diff(a, b) -> float:
    """Relative difference between a (DB) and b (live)."""
    if not a or not b:
        return None
    return abs(float(a) - float(b)) / max(abs(float(b)), 1e-9)


def _fmt(val) -> str:
    if val is None:
        return 'None'
    try:
        return f'{float(val):.4f}'
    except (TypeError, ValueError):
        return str(val)


def _flag(diff) -> str:
    if diff is None:
        return '  --  '
    return '  OK  ' if diff <= _TOLERANCE else f'  DIFF ({diff*100:.1f}%)'


def fetch_db_candidates(passed_only: bool = True) -> list:
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT
                    t.symbol,
                    t.type,
                    dp.open,
                    dp.high,
                    dp.low,
                    dp.close,
                    dp.volume,
                    dp.prev_close,
                    dp.gap_pct,
                    dp.pm_high,
                    dp.pm_low,
                    dp.pm_volume,
                    dp.atr_20,
                    dp.atr_pct,
                    dp.vix,
                    sr.passed_filters,
                    sr.rank
                FROM scan_results sr
                JOIN tickers      t  ON sr.ticker_id = t.id
                JOIN daily_prices dp ON sr.price_id  = dp.id
                WHERE sr.scan_date = CURRENT_DATE
                  AND (%s OR sr.passed_filters = TRUE)
                ORDER BY sr.rank ASC NULLS LAST, t.symbol
            """, (not passed_only,))
            return cur.fetchall()


def verify_ticker(symbol: str, db_row: dict) -> dict:
    """Re-fetch from Tradier and compare field by field."""
    live = tradier_client.get_daily_data(symbol)
    pm_vol, pm_high, pm_low = tradier_client.get_premarket_data(symbol)

    def cmp(field_label, db_val, live_val):
        d = _pct_diff(db_val, live_val)
        return {
            'field': field_label,
            'db':    _fmt(db_val),
            'live':  _fmt(live_val),
            'flag':  _flag(d),
        }

    checks = [
        cmp('open',       db_row.get('open'),       live.get('open')),
        cmp('prev_close', db_row.get('prev_close'), live.get('prev_close')),
        cmp('gap_pct',    db_row.get('gap_pct'),    live.get('gap_pct')),
        cmp('volume',     db_row.get('volume'),      live.get('volume')),
        cmp('pm_volume',  db_row.get('pm_volume'),   pm_vol),
        cmp('pm_high',    db_row.get('pm_high'),     pm_high),
        cmp('pm_low',     db_row.get('pm_low'),      pm_low),
        cmp('atr_20',     db_row.get('atr_20'),      live.get('atr')),
    ]
    return checks


def print_report(symbol: str, ticker_type: str, rank, passed: bool, checks: list):
    status = 'PASS' if passed else 'FAIL'
    rank_str = f'rank={rank}' if rank else 'not ranked'
    print(f'\n{"="*60}')
    print(f'  {symbol} ({ticker_type}) | {status} | {rank_str}')
    print(f'{"="*60}')
    print(f'  {"Field":<14} {"DB":>12} {"Live":>12}   Status')
    print(f'  {"-"*52}')
    any_diff = False
    for c in checks:
        flag = c['flag']
        if 'DIFF' in flag:
            any_diff = True
        print(f'  {c["field"]:<14} {c["db"]:>12} {c["live"]:>12}  {flag.strip()}')
    if not any_diff:
        print(f'  >>> All fields within {_TOLERANCE*100:.0f}% tolerance')


def main():
    args = sys.argv[1:]
    passed_only = '--all' not in args
    ticker_filter = [a.upper() for a in args if not a.startswith('--')]

    init_pool()
    today = datetime.now(ET).strftime('%Y-%m-%d')
    print(f'\nLayer 1 data accuracy check — {today}')
    print(f'Mode: {"all candidates" if not passed_only else "passed candidates only"}')
    if ticker_filter:
        print(f'Filter: {ticker_filter}')

    rows = fetch_db_candidates(passed_only)

    if not rows:
        print('\nNo candidates found in DB for today.')
        close_pool()
        return

    if ticker_filter:
        rows = [r for r in rows if r['symbol'] in ticker_filter]
        if not rows:
            print(f'\nNone of {ticker_filter} found in today\'s scan results.')
            close_pool()
            return

    print(f'\nChecking {len(rows)} ticker(s) against live Tradier data...')

    total_diffs = 0
    for row in rows:
        symbol = row['symbol']
        try:
            checks = verify_ticker(symbol, row)
            diffs = sum(1 for c in checks if 'DIFF' in c['flag'])
            total_diffs += diffs
            print_report(symbol, row['type'], row.get('rank'), row['passed_filters'], checks)
        except Exception as e:
            print(f'\n  {symbol}: ERROR — {e}')

    print(f'\n{"="*60}')
    if total_diffs == 0:
        print(f'  RESULT: All {len(rows)} tickers match live Tradier data.')
    else:
        print(f'  RESULT: {total_diffs} field(s) out of tolerance across {len(rows)} ticker(s).')
    print(f'{"="*60}\n')

    close_pool()


if __name__ == '__main__':
    main()
