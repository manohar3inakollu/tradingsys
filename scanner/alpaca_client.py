"""
Alpaca IEX client — live price data only.

Used for:
  get_latest_bar   — latest 1-min bar (live price check, no-trade rule, Layer 4 P&L)
  get_premarket_bars — pre-market snapshot in run.py
  get_vix          — real VIX via Tradier, falls back to VIXY bar

All scanning (OHLCV, ATR, gap, premarket volume) uses Tradier SIP (layer3/tradier_client.py).
"""

import os
import threading
import requests
from datetime import datetime
from typing import Dict, Tuple
from dotenv import load_dotenv
from scanner.retry import retry
from timing import ET
from logger import setup_logger

load_dotenv()
log = setup_logger('layer1')

# Cap concurrent Alpaca HTTP requests to avoid rate-limit timeouts.
_ALPACA_SEMAPHORE = threading.Semaphore(10)

BASE = 'https://data.alpaca.markets/v2'


class AlpacaClient:
    def __init__(self):
        self.headers = {
            'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
            'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
        }
        if not self.headers['APCA-API-KEY-ID']:
            log.warning("ALPACA_API_KEY not set — check .env")

    # ── Live price ────────────────────────────────────────

    @retry(max_attempts=3, delay=2)
    def get_latest_bar(self, ticker: str) -> Dict:
        """Latest completed 1-min bar (IEX feed). Used for live price checks."""
        with _ALPACA_SEMAPHORE:
            r = requests.get(
                f'{BASE}/stocks/{ticker}/bars/latest',
                headers=self.headers,
                params={'feed': 'iex'},
                timeout=10,
            )
        r.raise_for_status()
        return r.json().get('bar', {})

    @retry(max_attempts=3, delay=2)
    def get_premarket_bars(self, ticker: str) -> Tuple[int, float, float]:
        """Returns (pm_volume, pm_high, pm_low) for today's pre-market (4:00–9:29 ET)."""
        now_et   = datetime.now(ET)
        start_dt = now_et.replace(hour=4,  minute=0,  second=0, microsecond=0)
        end_dt   = now_et.replace(hour=9,  minute=29, second=0, microsecond=0)
        with _ALPACA_SEMAPHORE:
            r = requests.get(
                f'{BASE}/stocks/{ticker}/bars',
                headers=self.headers,
                params={
                    'timeframe': '1Min',
                    'start':     start_dt.isoformat(),
                    'end':       end_dt.isoformat(),
                    'feed':      'iex',
                    'limit':     400,
                },
                timeout=15,
            )
        r.raise_for_status()
        bars = r.json().get('bars', [])
        if not bars:
            return 0, 0.0, 0.0
        pm_vol  = sum(int(b.get('v', 0)) for b in bars)
        pm_high = max(float(b.get('h', 0)) for b in bars)
        pm_low  = min(float(b.get('l', float('inf'))) for b in bars)
        if pm_low == float('inf'):
            pm_low = 0.0
        return pm_vol, round(pm_high, 4), round(pm_low, 4)

    # ── VIX ──────────────────────────────────────────────

    def get_vix(self) -> float:
        """
        Real VIX index value.
        Primary: Tradier quotes endpoint (symbol=VIX).
        Fallback: VIXY ETF price from Alpaca (proxy, not true VIX).
        """
        try:
            from layer3.tradier_client import get_quote
            quote = get_quote('VIX')
            value = float(quote.get('last') or quote.get('close') or 0)
            if value > 0:
                return round(value, 2)
        except Exception as e:
            log.warning(f"VIX Tradier fetch failed: {e}")

        try:
            bar = self.get_latest_bar('VIXY')
            log.warning("VIX: using VIXY price as fallback (not true VIX)")
            return round(float(bar.get('c', 0)), 2)
        except Exception as e:
            log.warning(f"VIX fetch failed entirely: {e} — returning 0")
            return 0.0
