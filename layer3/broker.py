"""
Alpaca paper-trading broker — order placement and position management.

Uses direct HTTP requests consistent with the rest of the codebase.
Paper trading only (ALPACA_ENV=paper, which is the default).
"""

import os
import time
import requests
from dotenv import load_dotenv
from scanner.retry import retry
from logger import setup_logger

load_dotenv()
log = setup_logger('layer3')

_env = os.getenv('ALPACA_ENV', 'paper')
_TRADE_BASE = (
    'https://paper-api.alpaca.markets/v2'
    if _env == 'paper'
    else 'https://api.alpaca.markets/v2'
)
_FILL_POLL_S     = 2
_FILL_TIMEOUT_S  = 30


class AlpacaBroker:
    def __init__(self):
        self.headers = {
            'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
            'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
            'Content-Type':        'application/json',
        }
        if not self.headers['APCA-API-KEY-ID']:
            log.warning("broker: ALPACA_API_KEY not set — check .env")

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_market_buy(self, symbol: str, qty: int) -> dict:
        """Submit a market buy order. Returns the order dict."""
        return self._submit_order({
            'symbol':        symbol,
            'qty':           str(qty),
            'side':          'buy',
            'type':          'market',
            'time_in_force': 'day',
        })

    def place_stop_sell(self, symbol: str, qty: int, stop_price: float) -> dict:
        """Submit a stop-market sell order (stop loss). Returns the order dict."""
        return self._submit_order({
            'symbol':        symbol,
            'qty':           str(qty),
            'side':          'sell',
            'type':          'stop',
            'stop_price':    f'{stop_price:.2f}',
            'time_in_force': 'day',
        })

    def place_market_sell(self, symbol: str, qty: int) -> dict:
        """Submit a market sell order. Returns the order dict."""
        return self._submit_order({
            'symbol':        symbol,
            'qty':           str(qty),
            'side':          'sell',
            'type':          'market',
            'time_in_force': 'day',
        })

    def replace_stop(self, order_id: str, new_stop_price: float) -> dict:
        """
        Replace (modify) an existing stop order's stop price.
        Returns the new order dict.
        """
        try:
            r = requests.patch(
                f'{_TRADE_BASE}/orders/{order_id}',
                headers=self.headers,
                json={'stop_price': f'{new_stop_price:.2f}'},
                timeout=5,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.error(f"broker: replace_stop failed (order={order_id}): {e}")
            return {}

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True on success."""
        try:
            r = requests.delete(
                f'{_TRADE_BASE}/orders/{order_id}',
                headers=self.headers,
                timeout=5,
            )
            return r.status_code in (200, 204)
        except Exception as e:
            log.warning(f"broker: cancel_order failed (order={order_id}): {e}")
            return False

    def get_order(self, order_id: str) -> dict:
        """Fetch a single order by ID."""
        try:
            r = requests.get(
                f'{_TRADE_BASE}/orders/{order_id}',
                headers=self.headers,
                timeout=5,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"broker: get_order failed (order={order_id}): {e}")
            return {}

    def wait_for_fill(self, order_id: str,
                      timeout_s: int = _FILL_TIMEOUT_S) -> dict:
        """
        Poll until the order is filled or timeout.
        Returns the order dict (check status == 'filled').
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            order = self.get_order(order_id)
            status = order.get('status', '')
            if status == 'filled':
                filled_price = float(order.get('filled_avg_price') or 0)
                log.info(
                    f"broker: order {order_id} filled @ ${filled_price:.4f}"
                )
                return order
            if status in ('cancelled', 'expired', 'rejected'):
                log.warning(f"broker: order {order_id} terminal status={status}")
                return order
            time.sleep(_FILL_POLL_S)

        log.warning(f"broker: wait_for_fill timeout after {timeout_s}s — {order_id}")
        return self.get_order(order_id)

    # ── Positions ─────────────────────────────────────────────────────────────

    def get_positions(self) -> list:
        """Return list of open positions."""
        try:
            r = requests.get(
                f'{_TRADE_BASE}/positions',
                headers=self.headers,
                timeout=5,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"broker: get_positions failed: {e}")
            return []

    def close_position(self, symbol: str) -> dict:
        """Liquidate all shares of a symbol at market."""
        try:
            r = requests.delete(
                f'{_TRADE_BASE}/positions/{symbol}',
                headers=self.headers,
                timeout=5,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"broker: close_position failed for {symbol}: {e}")
            return {}

    def close_all_positions(self) -> list:
        """
        Cancel all open orders, then liquidate all positions.
        Returns list of close-order dicts.
        """
        # Cancel all open orders first
        try:
            r = requests.delete(
                f'{_TRADE_BASE}/orders',
                headers=self.headers,
                timeout=10,
            )
            log.info(f"broker: cancel all orders -> status {r.status_code}")
        except Exception as e:
            log.warning(f"broker: cancel all orders failed: {e}")

        positions = self.get_positions()
        results = []
        for pos in positions:
            symbol = pos.get('symbol', '')
            if symbol:
                result = self.close_position(symbol)
                results.append(result)
                log.info(f"broker: force-closed {symbol}")

        return results

    # ── Internal ──────────────────────────────────────────────────────────────

    @retry(max_attempts=3, delay=1)
    def _submit_order(self, payload: dict) -> dict:
        r = requests.post(
            f'{_TRADE_BASE}/orders',
            headers=self.headers,
            json=payload,
            timeout=5,
        )
        r.raise_for_status()
        order = r.json()
        log.info(
            f"broker: order submitted — {payload.get('side')} "
            f"{payload.get('qty')} {payload.get('symbol')} "
            f"type={payload.get('type')} -> id={order.get('id')}"
        )
        return order
