from typing import Dict, Tuple, List
from timing import SCAN_CONFIG

cfg = SCAN_CONFIG


def calculate_atr_pct(atr: float, price: float) -> float:
    if price <= 0:
        return 0.0
    return round(atr / price * 100, 4)


def passes_all(ticker: str, price: float, gap_pct: float,
               volume: int, pm_volume: int,
               atr: float, avg_volume: float = 0.0,
               cfg_override: Dict = None) -> Tuple[bool, List[str]]:
    """
    Returns (passed, failed_filter_names).
    All 5 must pass to be included in candidates.
    Pass cfg_override to use ETF_SCAN_CONFIG or any custom thresholds.
    """
    c = cfg_override if cfg_override is not None else cfg
    checks = {
        'price':     c['min_price'] <= price <= c['max_price'],
        'gap_pct':   c['min_gap_pct'] <= gap_pct <= c['max_gap_pct'],
        'volume':    volume >= c['min_volume'],
        'pm_volume': (pm_volume >= avg_volume * c['min_pm_vol_pct']) if avg_volume > 0 else False,
        'atr_pct':   calculate_atr_pct(atr, price) >= c['min_atr_pct'],
    }
    failed = [k for k, v in checks.items() if not v]
    return (len(failed) == 0, failed)
