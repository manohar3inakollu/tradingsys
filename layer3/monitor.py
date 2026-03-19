"""
Hybrid monitor — evaluates all 5 entry criteria every minute.

Watchman (Alpaca IEX, 1-min):
  1. Price closed above ORB high
  2. Price above cumulative VWAP (from 9:30 AM)
  3. SPY latest 1-min candle is green (close > open)

Validator (Tradier SIP, 5-min):
  4. Last completed 5-min bar volume >= 2x avg 5-min volume baseline
  5. 9-period EMA slope pointing up on 5-min closes

All 5 must pass simultaneously.
"""

import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from scanner.retry import retry
from timing import ET
from layer3.tradier_client import get_5min_bars
from logger import setup_logger

load_dotenv()
log = setup_logger('layer3')

_BASE = 'https://data.alpaca.markets/v2'


def _headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
        'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
    }


# ── Alpaca 1-min bars ─────────────────────────────────────────────────────────

@retry(max_attempts=3, delay=1)
def _fetch_intraday_bars(symbol: str) -> list:
    """All 1-min bars from 9:30 AM ET today up to now."""
    now_et   = datetime.now(ET)
    start_dt = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    r = requests.get(
        f'{_BASE}/stocks/{symbol}/bars',
        headers=_headers(),
        params={
            'timeframe': '1Min',
            'start':     start_dt.isoformat(),
            'feed':      'iex',
            'limit':     200,
        },
        timeout=3,
    )
    r.raise_for_status()
    return r.json().get('bars', [])


# ── VWAP ──────────────────────────────────────────────────────────────────────

def _cumulative_vwap(bars: list) -> float:
    """Cumulative VWAP from all provided bars (typical price × volume method)."""
    total_pv = 0.0
    total_v  = 0
    for b in bars:
        h = float(b.get('h', 0))
        l = float(b.get('l', 0))
        c = float(b.get('c', 0))
        v = int(b.get('v', 0))
        typical = (h + l + c) / 3
        total_pv += typical * v
        total_v  += v
    return total_pv / total_v if total_v > 0 else 0.0


# ── EMA ───────────────────────────────────────────────────────────────────────

def _ema_series(closes: list, period: int = 9) -> list:
    """Single-pass EMA seeded from the first close."""
    if not closes:
        return []
    k = 2 / (period + 1)
    emas = [closes[0]]
    for c in closes[1:]:
        emas.append(c * k + emas[-1] * (1 - k))
    return emas


def _ema9_slope_up(bars_5min: list) -> bool:
    """True if the 9-EMA of 5-min closes is trending upward."""
    if len(bars_5min) < 2:
        return True   # not enough data — pass by default
    closes = [b['close'] for b in bars_5min]
    emas = _ema_series(closes, period=9)
    return emas[-1] > emas[-2]


# ── Watchman (Alpaca, 1-min) ──────────────────────────────────────────────────

def check_watchman(symbol: str, orb_high: float) -> dict:
    """
    Returns:
      price_above_orb   — latest 1-min close > orb_high
      price_above_vwap  — latest 1-min close > cumulative VWAP
      spy_green         — latest SPY 1-min candle is green
      latest_close      — float
      entry_candle_low  — float (low of the bar that broke ORB)
    """
    result = {
        'price_above_orb':  False,
        'price_above_vwap': False,
        'spy_green':        False,
        'latest_close':     0.0,
        'entry_candle_low': 0.0,
    }

    try:
        bars = _fetch_intraday_bars(symbol)
    except Exception as e:
        log.warning(f"watchman: bars fetch failed for {symbol}: {e}")
        return result

    if not bars:
        return result

    latest = bars[-1]
    close  = float(latest.get('c', 0))
    vwap   = _cumulative_vwap(bars)

    result['latest_close']    = close
    result['entry_candle_low'] = float(latest.get('l', 0))
    result['price_above_orb']  = close > orb_high
    result['price_above_vwap'] = close > vwap if vwap > 0 else False

    # SPY green check
    try:
        spy_bars = _fetch_intraday_bars('SPY')
        if spy_bars:
            spy_last = spy_bars[-1]
            result['spy_green'] = (
                float(spy_last.get('c', 0)) > float(spy_last.get('o', 0))
            )
    except Exception as e:
        log.warning(f"watchman: SPY fetch failed: {e}")

    return result


# ── Validator (Tradier SIP, 5-min) ────────────────────────────────────────────

def check_validator(symbol: str, avg_daily_volume: int) -> dict:
    """
    Returns:
      volume_2x     — last 5-min bar volume >= 2x per-bar baseline
      ema9_slope_up — 9-EMA of 5-min closes is rising
    """
    result = {'volume_2x': False, 'ema9_slope_up': False}

    try:
        bars = get_5min_bars(symbol)
    except Exception as e:
        log.warning(f"validator: Tradier fetch failed for {symbol}: {e}")
        return result

    if not bars:
        return result

    # Volume baseline: avg daily volume ÷ 78 five-min bars per session
    baseline = avg_daily_volume / 78.0 if avg_daily_volume else 0
    last_vol  = bars[-1]['volume']
    result['volume_2x'] = (baseline > 0) and (last_vol >= 2 * baseline)

    result['ema9_slope_up'] = _ema9_slope_up(bars)

    return result


# ── Combined 5-criteria check ─────────────────────────────────────────────────

def all_five_pass(
    symbol: str,
    orb_high: float,
    avg_daily_volume: int,
) -> tuple[bool, dict]:
    """
    Evaluate all 5 entry criteria.
    Returns (all_pass: bool, details: dict).
    """
    watchman  = check_watchman(symbol, orb_high)
    validator = check_validator(symbol, avg_daily_volume)

    criteria = {
        'price_above_orb':  watchman['price_above_orb'],
        'volume_2x':        validator['volume_2x'],
        'price_above_vwap': watchman['price_above_vwap'],
        'ema9_slope_up':    validator['ema9_slope_up'],
        'spy_green':        watchman['spy_green'],
    }

    all_pass = all(criteria.values())

    details = {
        **criteria,
        'latest_close':    watchman['latest_close'],
        'entry_candle_low': watchman['entry_candle_low'],
    }

    log.info(
        f"monitor: {symbol} | "
        + ' | '.join(f"{k}={'Y' if v else 'N'}" for k, v in criteria.items())
        + f" | close={watchman['latest_close']}"
    )

    return all_pass, details
