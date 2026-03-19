"""Merge stock and ETF scanner candidates, rank them, and persist to PostgreSQL."""
from datetime import datetime
from typing import List, Dict
from db.connection import db_connection
from db.queries import (
    upsert_ticker, upsert_daily_price,
    insert_scan_result,
)
from timing import ET, SCAN_CONFIG
from logger import setup_logger

log = setup_logger('layer1')


def _rank_candidates(candidates: List[Dict]) -> List[Dict]:
    """
    Sort order:
      1. ETFs first — QQQ always rank 1 if it passed
      2. Within ETFs — by gap_pct descending
      3. Stocks — by gap_pct descending
    """
    etfs   = [c for c in candidates if c['type'] == 'etf']
    stocks = [c for c in candidates if c['type'] == 'stock']

    etfs.sort(key=lambda x: (x['ticker'] != 'QQQ', -x['gap_pct']))
    stocks.sort(key=lambda x: -x['gap_pct'])

    ranked = etfs + stocks
    for i, c in enumerate(ranked, start=1):
        c['rank'] = i
    return ranked


def merge_and_save(stock_candidates: List[Dict],
                   etf_candidates:   List[Dict],
                   vix: float = 0.0) -> List[Dict]:
    """
    1. Filter to passed-only from both lists
    2. Rank combined list
    3. Cap at max_candidates
    4. Write to PostgreSQL (tickers, daily_prices, scan_results)
    5. Return saved candidates
    """
    all_candidates = list(stock_candidates + etf_candidates)
    passed = [c for c in all_candidates if c['passed']]

    if not passed:
        log.warning("merger: no candidates passed all filters today")

    ranked = _rank_candidates(passed)
    capped = ranked[:SCAN_CONFIG['max_candidates']]

    today = datetime.now(ET).strftime('%Y-%m-%d')
    saved = []

    with db_connection() as conn:
        for c in capped:
            # 1 — upsert ticker
            ticker_id = upsert_ticker(
                conn,
                symbol      = c['ticker'],
                ticker_type = c['type'],
                sector      = c.get('sector'),
                industry    = c.get('industry'),
                company     = c.get('company'),
            )

            # 2 — upsert daily price
            price_data = {
                'date':       today,
                'open':       c.get('open')  or c.get('price'),
                'high':       c.get('high')  or c.get('price'),
                'low':        c.get('low')   or c.get('price'),
                'close':      c.get('close') or c.get('price'),
                'volume':     c.get('volume', 0),
                'prev_close': c.get('prev_close'),
                'gap_pct':    c.get('gap_pct'),
                'pm_high':    c.get('pm_high'),
                'pm_low':     c.get('pm_low'),
                'pm_volume':  c.get('pm_volume', 0),
                'atr_20':     c.get('atr_20'),
                'atr_pct':    c.get('atr_pct'),
                'vix':        vix,
            }
            price_id = upsert_daily_price(conn, ticker_id, price_data)

            # 3 — insert scan result
            insert_scan_result(
                conn,
                scan_date  = today,
                ticker_id  = ticker_id,
                price_id   = price_id,
                passed     = True,
                failed     = [],
                gap_pct    = c.get('gap_pct', 0),
                orb_window = c.get('orb_window', 5),
                rank       = c.get('rank', 99),
            )

            c['ticker_id'] = ticker_id
            c['price_id']  = price_id
            saved.append(c)
            log.info(
                "merger: saved rank=%s %s (%s) gap=%s%% orb=%smin",
                c['rank'], c['ticker'], c['type'], c['gap_pct'], c['orb_window'],
            )

        # Also record failed candidates for analytics
        failed_all = [c for c in all_candidates if not c['passed']]
        for c in failed_all:
            ticker_id = upsert_ticker(
                conn,
                symbol      = c['ticker'],
                ticker_type = c['type'],
                sector      = c.get('sector'),
                industry    = c.get('industry'),
                company     = c.get('company'),
            )
            insert_scan_result(
                conn,
                scan_date  = today,
                ticker_id  = ticker_id,
                price_id   = None,
                passed     = False,
                failed     = c.get('failed', []),
                gap_pct    = c.get('gap_pct', 0),
                orb_window = c.get('orb_window', 5),
                rank       = None,
            )

    log.info(
        "merger: %s candidates saved to DB | %s failed recorded | VIX=%s",
        len(saved), len(failed_all), vix,
    )
    return saved
