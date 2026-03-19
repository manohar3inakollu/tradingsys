from concurrent.futures import ThreadPoolExecutor, as_completed

from scanner.filters import passes_all, calculate_atr_pct
from layer3 import tradier_client
from timing import ETF_WATCHLIST, ETF_SCAN_CONFIG
from logger import setup_logger

log = setup_logger('layer1')

# Full names for ETFs in the watchlist — used by the web search fallback so
# "SPDR S&P Biotech ETF" is searched instead of just "XBI".
_ETF_NAMES: dict[str, str] = {
    'QQQ':  'Invesco QQQ Trust Nasdaq 100 ETF',
    'SPY':  'SPDR S&P 500 ETF Trust',
    'IWM':  'iShares Russell 2000 Small-Cap ETF',
    'USO':  'United States Oil Fund ETF',
    'XLE':  'Energy Select Sector SPDR ETF',
    'XOP':  'SPDR S&P Oil & Gas Exploration ETF',
    'XBI':  'SPDR S&P Biotech ETF',
    'IBB':  'iShares Nasdaq Biotechnology ETF',
    'GLD':  'SPDR Gold Shares ETF',
    'GDX':  'VanEck Gold Miners ETF',
    'XLK':  'Technology Select Sector SPDR ETF',
}


def _process_etf(ticker: str):
    """Enrich a single ETF using Tradier SIP data. Returns candidate dict or None."""
    # Fetch daily data and premarket data concurrently from Tradier
    with ThreadPoolExecutor(max_workers=2) as inner:
        fut_daily = inner.submit(tradier_client.get_daily_data, ticker)
        fut_pm    = inner.submit(tradier_client.get_premarket_data, ticker)

    try:
        daily = fut_daily.result()
        if not daily:
            log.warning(f"etf_scanner: no daily data for {ticker} — skipping")
            return None
    except Exception as e:
        log.warning(f"etf_scanner: daily data failed for {ticker}: {e}")
        return None

    try:
        pm_vol, pm_high, pm_low = fut_pm.result()
    except Exception as e:
        log.warning(f"etf_scanner: premarket data failed for {ticker}: {e}")
        pm_vol, pm_high, pm_low = 0, 0.0, 0.0

    price      = daily['open']
    gap_pct    = daily['gap_pct']
    atr        = daily['atr']
    close      = daily['close']
    volume     = daily['volume']
    avg_volume = daily['avg_volume']
    ref_price  = close if close > 0 else price
    atr_pct    = calculate_atr_pct(atr, ref_price)

    passed, failed = passes_all(ticker, price, gap_pct, volume, pm_vol, atr, avg_volume,
                                cfg_override=ETF_SCAN_CONFIG)

    log.info(
        f"etf_scanner: {ticker} price={price} gap={gap_pct}% "
        f"vol={volume} pm_vol={pm_vol} atr_pct={atr_pct}% "
        f"-> {'PASS' if passed else 'FAIL ' + str(failed)}"
    )

    return {
        'ticker':     ticker,
        'type':       'etf',
        'price':      round(price, 4),
        'open':       daily.get('open'),
        'high':       daily.get('high'),
        'low':        daily.get('low'),
        'close':      close,
        'volume':     volume,
        'prev_close': daily.get('prev_close'),
        'gap_pct':    round(gap_pct, 4),
        'pm_volume':  pm_vol,
        'pm_high':    pm_high,
        'pm_low':     pm_low,
        'atr_20':     atr,
        'atr_pct':    atr_pct,
        'company':    _ETF_NAMES.get(ticker),
        'sector':     'ETF',
        'industry':   None,
        'orb_window': 15,
        'passed':     passed,
        'failed':     failed,
    }


def scan_etfs() -> list:
    """
    Enrichment: Tradier SIP data for each ETF in the static watchlist.
    QQQ is always first — highest priority.
    Returns list of candidate dicts (passed + failed for analytics).
    """
    log.info(f"etf_scanner: checking {len(ETF_WATCHLIST)} ETFs via Tradier...")

    results = {}
    with ThreadPoolExecutor(max_workers=len(ETF_WATCHLIST)) as pool:
        futures = {pool.submit(_process_etf, ticker): ticker for ticker in ETF_WATCHLIST}
        for future in as_completed(futures):
            ticker = futures[future]
            result = future.result()
            if result is not None:
                results[ticker] = result

    # Preserve watchlist order (QQQ first = highest priority)
    candidates = [results[t] for t in ETF_WATCHLIST if t in results]

    passed_count = sum(1 for c in candidates if c['passed'])
    log.info(f"etf_scanner: {len(ETF_WATCHLIST)} ETFs checked, {passed_count} passed all filters")
    return candidates
