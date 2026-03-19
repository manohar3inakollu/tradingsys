"""
Gap scanner: Finviz discovery → leveraged-product filtering → Tradier enrichment.

Filter order (cheapest checks first, before any API calls):
  1. Finviz industry label  — drops anything Finviz classifies as ETF
  2. Company name keywords  — drops leveraged/inverse ETNs and misclassified products
  3. LEVERAGED_ETF_BLACKLIST — explicit backstop for products that slip through both
  4. Tradier daily + premarket enrichment (parallel per ticker)
  5. passes_all() filter gates
"""
from concurrent.futures import ThreadPoolExecutor, as_completed

from finvizfinance.screener.overview import Overview
from scanner.retry import retry
from scanner.filters import passes_all, calculate_atr_pct
from layer3 import tradier_client
from timing import LEVERAGED_ETF_BLACKLIST
from logger import setup_logger

log = setup_logger('layer1')

# Leverage/inverse keywords in company name — catches ETNs and misclassified products
# that Finviz doesn't label as "Exchange Traded Fund".
_LEVERAGE_NAME_KEYWORDS = (
    '2x', '3x', '4x', '5x',
    'leveraged', 'inverse',
    'ultra short', 'ultrashort',
    'daily bull', 'daily bear',
    'bull 3', 'bear 3',
    'target etf', 'target etn',
)

FINVIZ_FILTERS = {
    'Country':        'USA',
    'Price':          'Over $10',
    'Gap':            'Up 3%',
    'Average Volume': 'Over 500K',
}


@retry(max_attempts=3, delay=3)
def _fetch_finviz() -> list:
    ov = Overview()
    ov.set_filter(filters_dict=FINVIZ_FILTERS)
    df = ov.screener_view()
    return df.to_dict('records') if df is not None else []


def _parse_price(v) -> float | None:
    try:
        s = str(v).strip().replace('$', '').replace(',', '')
        return float(s) if s not in ('', '-', 'None', 'N/A') else None
    except Exception:
        return None


def _parse_percent(v) -> float | None:
    try:
        s = str(v).strip().replace('%', '').replace('+', '').replace(',', '')
        return float(s) if s not in ('', '-', 'None', 'N/A') else None
    except Exception:
        return None


def _process_row(row: dict) -> dict | None:
    """
    Validate one Finviz row, apply pre-fetch filters, enrich via Tradier.
    Returns candidate dict or None to skip.
    """
    ticker   = str(row.get('Ticker',  '')).strip().upper()
    price    = _parse_price(row.get('Price', ''))
    finviz_gap = _parse_percent(row.get('Gap', ''))
    industry = str(row.get('Industry', '') or '').strip()
    sector   = str(row.get('Sector',   '') or '').strip()
    company  = str(row.get('Company',  '') or '').strip()
    company_lower = company.lower()

    if not ticker or price is None:
        return None

    # ── Pre-fetch filters (no API cost) ──────────────────────────────────────
    if 'exchange traded fund' in industry.lower() or industry.lower() == 'etf':
        log.debug('gap_scanner: %s skipped (Finviz industry=%r)', ticker, industry)
        return None

    if any(kw in company_lower for kw in _LEVERAGE_NAME_KEYWORDS):
        log.debug('gap_scanner: %s skipped (leverage keyword in name: %r)', ticker, company)
        return None

    if ticker in LEVERAGED_ETF_BLACKLIST:
        log.debug('gap_scanner: %s skipped (blacklist)', ticker)
        return None

    # ── Tradier enrichment (parallel daily + premarket) ──────────────────────
    with ThreadPoolExecutor(max_workers=2) as inner:
        fut_daily = inner.submit(tradier_client.get_daily_data,    ticker)
        fut_pm    = inner.submit(tradier_client.get_premarket_data, ticker)

    try:
        daily = fut_daily.result()
        if not daily:
            log.warning('gap_scanner: no daily data for %s — skipping', ticker)
            return None
    except Exception as e:
        log.warning('gap_scanner: daily data failed for %s: %s', ticker, e)
        return None

    try:
        pm_vol, pm_high, pm_low = fut_pm.result()
    except Exception as e:
        log.debug('gap_scanner: premarket data failed for %s: %s', ticker, e)
        pm_vol, pm_high, pm_low = 0, 0.0, 0.0

    atr        = daily['atr']
    close      = daily['close']
    prev_close = daily['prev_close']
    volume     = daily['volume']
    avg_volume = daily['avg_volume']
    atr_pct    = calculate_atr_pct(atr, close if close > 0 else price)

    # Gap: market open vs prev close (9:30 ET open, not pre-market)
    # Finviz gap is pre-market — only used as fallback if open not yet available
    if daily.get('open') and prev_close > 0:
        gap = round((daily['open'] - prev_close) / prev_close * 100, 4)
    elif finviz_gap is not None:
        gap = finviz_gap
    else:
        gap = daily.get('gap_pct')
        if gap is None:
            log.debug('gap_scanner: no gap data for %s — skipping', ticker)
            return None

    passed, failed = passes_all(ticker, price, gap, volume, pm_vol, atr, avg_volume)

    log.info(
        'gap_scanner: %s price=%s gap=%s%% vol=%s pm_vol=%s atr_pct=%s%% -> %s',
        ticker, price, gap, volume, pm_vol, atr_pct,
        'PASS' if passed else 'FAIL ' + str(failed),
    )

    return {
        'ticker':     ticker,
        'type':       'stock',
        'price':      round(price, 4),
        'open':       daily.get('open'),
        'high':       daily.get('high'),
        'low':        daily.get('low'),
        'close':      close,
        'volume':     volume,
        'prev_close': prev_close,
        'gap_pct':    round(gap, 4),
        'pm_volume':  pm_vol,
        'pm_high':    pm_high,
        'pm_low':     pm_low,
        'atr_20':     atr,
        'atr_pct':    atr_pct,
        'company':    company or None,
        'sector':     sector or None,
        'industry':   industry or None,
        'orb_window': 5,
        'passed':     passed,
        'failed':     failed,
    }


def scan_stocks() -> list:
    """
    Fetches Finviz gap list, filters leveraged/inverse products upfront,
    enriches survivors with Tradier SIP data in parallel.
    Returns list of candidate dicts (passed + failed for analytics).
    """
    log.info('gap_scanner: fetching Finviz gap list...')
    rows = _fetch_finviz()
    log.info('gap_scanner: Finviz returned %d raw results', len(rows))

    candidates = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_process_row, row): row for row in rows}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    candidates.append(result)
            except Exception as e:
                row = futures[future]
                log.warning('gap_scanner: row processing failed for %s: %s',
                            row.get('Ticker', '?'), e)

    passed_count = sum(1 for c in candidates if c['passed'])
    log.info('gap_scanner: %d stocks checked, %d passed all filters',
             len(candidates), passed_count)
    return candidates
