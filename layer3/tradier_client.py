"""
Tradier SIP data client.

Provides:
  get_5min_bars      — intraday 5-min bars for ORB logic (Layer 3)
  get_daily_data     — daily OHLCV + ATR + gap_pct for scanning (Layer 1)
  get_premarket_data — premarket volume/high/low for scanning (Layer 1)
  get_quote          — real-time quote (used for VIX, SPY %)

Env vars required:
  TRADIER_TOKEN   — Tradier API access token
  TRADIER_ENV     — 'live' or 'sandbox' (default: 'sandbox')
"""

import os
import threading
import requests
from datetime import datetime
from dotenv import load_dotenv
from scanner.retry import retry
from timing import ET
from logger import setup_logger

# Rate-limit guard: Tradier developer tier ~200 req/min across all callers.
# Semaphore(4) on a 125-ticker scan ≈ 4 concurrent × ~1 s/req ≈ safe headroom.
_TRADIER_SEMAPHORE = threading.Semaphore(4)

load_dotenv()
log = setup_logger('layer3')

_env = os.getenv('TRADIER_ENV', 'sandbox')
_BASE = (
    'https://api.tradier.com/v1'
    if _env == 'live'
    else 'https://sandbox.tradier.com/v1'
)


def _headers() -> dict:
    token = os.getenv('TRADIER_TOKEN', '')
    if not token:
        log.warning("TRADIER_TOKEN not set — check .env")
    return {
        'Authorization': f'Bearer {token}',
        'Accept':        'application/json',
    }


@retry(max_attempts=3, delay=1)
def get_5min_bars(symbol: str) -> list:
    """
    Return completed 5-min bars from 9:30 AM ET today up to now.
    Each bar dict has: time, open, high, low, close, volume.
    """
    today = datetime.now(ET).strftime('%Y-%m-%d')
    now = datetime.now(ET).strftime('%H:%M')

    try:
        r = requests.get(
            f'{_BASE}/markets/timesales',
            headers=_headers(),
            params={
                'symbol':         symbol,
                'interval':       '5min',
                'start':          f'{today} 09:30',
                'end':            f'{today} {now}',
                'session_filter': 'open',
            },
            timeout=3,
        )
        r.raise_for_status()
    except requests.exceptions.Timeout:
        log.warning(f"tradier: timeout fetching {symbol} 5-min bars")
        return []
    except Exception as e:
        log.warning(f"tradier: fetch failed for {symbol}: {e}")
        return []

    data = r.json().get('series')
    if not data:
        return []

    raw = data.get('data', [])
    # API returns a dict (single bar) or list (multiple bars)
    if isinstance(raw, dict):
        raw = [raw]

    bars = []
    for b in raw:
        try:
            bars.append({
                'time':   b.get('time', ''),
                'open':   float(b.get('open',  0)),
                'high':   float(b.get('high',  0)),
                'low':    float(b.get('low',   0)),
                'close':  float(b.get('close', 0)),
                'volume': int(b.get('volume',  0)),
            })
        except (TypeError, ValueError):
            continue

    return bars


@retry(max_attempts=3, delay=1)
def get_quote(symbol: str) -> dict:
    """
    Real-time SIP quote for a single symbol.
    Returns dict with keys: last, open, prevclose, high, low, volume, etc.
    Used for VIX value and SPY % change.
    """
    with _TRADIER_SEMAPHORE:
        r = requests.get(
            f'{_BASE}/markets/quotes',
            headers=_headers(),
            params={'symbols': symbol, 'greeks': 'false'},
            timeout=5,
        )
    r.raise_for_status()
    quotes = r.json().get('quotes', {}).get('quote', {})
    if isinstance(quotes, list):
        quotes = quotes[0] if quotes else {}
    return quotes


@retry(max_attempts=3, delay=1)
def get_daily_data(symbol: str) -> dict:
    """
    Daily OHLCV for the last 30 trading days plus ATR-14 and gap_pct.
    Returns dict with: atr, close, prev_close, avg_volume, gap_pct,
                       open, high, low, volume.
    Returns {} if fewer than 2 bars are available.
    """
    today = datetime.now(ET).strftime('%Y-%m-%d')
    with _TRADIER_SEMAPHORE:
        r = requests.get(
            f'{_BASE}/markets/history',
            headers=_headers(),
            params={'symbol': symbol, 'interval': 'daily',
                    'start': _trading_days_ago(30), 'end': today},
            timeout=10,
        )
    r.raise_for_status()
    raw = r.json().get('history') or {}
    day_list = raw.get('day', [])
    if isinstance(day_list, dict):
        day_list = [day_list]
    if len(day_list) < 2:
        return {}

    # ATR-14 over available bars
    period = min(14, len(day_list) - 1)
    trs = []
    for i in range(1, len(day_list)):
        h  = float(day_list[i].get('high',  0))
        l  = float(day_list[i].get('low',   0))
        pc = float(day_list[i - 1].get('close', 0))
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = round(sum(trs[-period:]) / period, 4) if trs else 0.0

    last      = day_list[-1]
    prev      = day_list[-2]
    close     = float(last.get('close', 0))
    prev_close= float(prev.get('close', 0))
    today_open= float(last.get('open',  0))
    volume    = int(last.get('volume',  0))

    recent    = day_list[-14:]
    avg_vol   = round(sum(float(d.get('volume', 0)) for d in recent) / len(recent), 0)
    gap_pct   = round((today_open - prev_close) / prev_close * 100, 4) if prev_close > 0 else 0.0

    return {
        'atr':        atr,
        'close':      round(close, 4),
        'prev_close': round(prev_close, 4),
        'avg_volume': avg_vol,
        'gap_pct':    gap_pct,
        'open':       round(today_open, 4),
        'high':       round(float(last.get('high', 0)), 4),
        'low':        round(float(last.get('low',  0)), 4),
        'volume':     volume,
    }


@retry(max_attempts=2, delay=2)
def get_premarket_data(symbol: str) -> tuple:
    """
    Premarket 1-min bars 04:00–09:30 ET today.
    Returns (pm_volume, pm_high, pm_low).
    """
    today = datetime.now(ET).strftime('%Y-%m-%d')
    with _TRADIER_SEMAPHORE:
        r = requests.get(
            f'{_BASE}/markets/timesales',
            headers=_headers(),
            params={'symbol': symbol, 'interval': '1min',
                    'start': f'{today} 04:00', 'end': f'{today} 09:30'},
            timeout=5,
        )
    r.raise_for_status()
    series = r.json().get('series') or {}
    raw    = series.get('data', [])
    if isinstance(raw, dict):
        raw = [raw]
    if not raw:
        return 0, 0.0, 0.0

    pm_vol  = sum(int(b.get('volume', 0)) for b in raw)
    pm_high = max(float(b.get('high', 0)) for b in raw)
    pm_low  = min(float(b.get('low', float('inf'))) for b in raw)
    if pm_low == float('inf'):
        pm_low = 0.0
    return pm_vol, round(pm_high, 4), round(pm_low, 4)


def _trading_days_ago(n: int) -> str:
    """Approximate calendar date n trading days back (adds 40% buffer for weekends/holidays)."""
    from datetime import timedelta
    cal_days = int(n * 1.4) + 5
    return (datetime.now(ET) - timedelta(days=cal_days)).strftime('%Y-%m-%d')
