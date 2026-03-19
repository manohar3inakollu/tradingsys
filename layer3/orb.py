"""
ORB (Opening Range Breakout) range calculation.

  Stocks → 5-min ORB  : 9:30–9:35 AM ET  (high/low of first 5 min)
  ETFs   → 15-min ORB : 9:30–9:45 AM ET  (high/low of first 15 min)

Uses Alpaca IEX — same feed as the live price checks in monitor.py.
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from scanner.retry import retry
from timing import ET  # noqa: F401 — used in datetime.now(ET)
from logger import setup_logger

load_dotenv()
log = setup_logger('layer3')

_BASE = 'https://data.alpaca.markets/v2'


def _headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
        'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
    }


@retry(max_attempts=3, delay=1)
def _fetch_orb_bar(symbol: str, start: str, end: str, window_mins: int) -> list:
    r = requests.get(
        f'{_BASE}/stocks/{symbol}/bars',
        headers=_headers(),
        params={
            'timeframe': f'{window_mins}Min',
            'start':     start,
            'end':       end,
            'feed':      'iex',
            'limit':     1,
        },
        timeout=3,
    )
    r.raise_for_status()
    return r.json().get('bars', [])


def get_orb_range(symbol: str, orb_window_mins: int) -> dict | None:
    """
    Fetch the single opening N-min candle from Alpaca IEX and return:
      {'high': float, 'low': float}
    or None if data unavailable.
    """
    now_et = datetime.now(ET)
    # Build tz-aware datetimes — isoformat() emits correct offset (EDT=-04:00, EST=-05:00)
    start_dt = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    end_dt   = now_et.replace(
        hour=(9 * 60 + 30 + orb_window_mins) // 60,
        minute=(9 * 60 + 30 + orb_window_mins) % 60,
        second=0, microsecond=0,
    ) - timedelta(seconds=1)   # 09:34:59 — exclude the next candle

    try:
        bars = _fetch_orb_bar(
            symbol, start_dt.isoformat(), end_dt.isoformat(), orb_window_mins
        )
    except requests.RequestException as e:
        log.warning('orb: bars fetch failed for %s: %s', symbol, e)
        return None

    if not bars:
        log.warning('orb: no bars returned for %s (window=%smin)', symbol, orb_window_mins)
        return None

    b = bars[0]
    orb_high = round(float(b.get('h', 0)), 4)
    orb_low  = round(float(b.get('l', 0)), 4)

    if orb_low <= 0 or orb_high <= 0:
        log.warning('orb: invalid range for %s: high=%s low=%s', symbol, orb_high, orb_low)
        return None

    log.info('orb: %s %s-min | high=%s low=%s', symbol, orb_window_mins, orb_high, orb_low)
    return {'high': orb_high, 'low': orb_low}
