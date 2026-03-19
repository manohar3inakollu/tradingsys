"""
Trade manager — monitors a single open position from entry to close.

Lifecycle:
  OPEN
    → price hits T1: sell 50%, move stop to breakeven  (PARTIAL)
    → price hits T2: sell remaining 50%, close trade   (CLOSED)
    → stop order fills: exit 100%, close trade          (CLOSED)
    → force_exit() called: market sell all, close trade (CLOSED)

Each check cycle polls Alpaca every 60 seconds.
Fires an SMS result on every close.
"""

import time
import math
from datetime import datetime
from db.connection import db_connection
from db.queries_layer3 import (
    update_trade, increment_trade_count, update_session_pnl, log_signal,
)
from layer3.broker import AlpacaBroker
from layer3.confirmation import send_confirmation_sms
from timing import ET
from logger import setup_logger

log = setup_logger('layer3')

_POLL_S = 60


class TradeManager:
    """
    Synchronously manages one live trade.
    Call run() from a dedicated thread; it blocks until the trade closes.
    """

    def __init__(self, trade_id: int, plan: dict, symbol: str,
                 entry_order_id: str, stop_order_id: str):
        self.trade_id       = trade_id
        self.symbol         = symbol
        self.plan           = plan          # entry, stop, t1, t2, shares
        self.entry_order_id = entry_order_id
        self.stop_order_id  = stop_order_id
        self.broker         = AlpacaBroker()
        self.closed            = False
        self.t1_hit            = False
        self.remaining_qty     = plan['shares']
        self.pre_stop_warned   = False

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Block until position is closed (T2, stop, or force exit)."""
        log.info(
            f"trade_manager: [{self.symbol}] monitoring started — "
            f"entry={self.plan['entry_price']} "
            f"stop={self.plan['stop_price']} "
            f"T1={self.plan['t1_price']} T2={self.plan['t2_price']}"
        )

        with db_connection() as conn:
            update_trade(conn, self.trade_id, status='OPEN')

        while not self.closed:
            time.sleep(_POLL_S)
            try:
                self._check_cycle()
            except Exception as e:
                log.error(f"trade_manager: [{self.symbol}] check error: {e}")

    def force_exit(self, reason: str = 'FORCE_EXIT') -> None:
        """Immediately close position at market (e.g. 3:45 PM deadline)."""
        if self.closed:
            return
        log.info(f"trade_manager: [{self.symbol}] force exit — {reason}")
        try:
            self.broker.cancel_order(self.stop_order_id)
            result = self.broker.close_position(self.symbol)
            exit_price = self._parse_fill_price(result)
        except Exception as e:
            log.error(f"trade_manager: [{self.symbol}] force exit error: {e}")
            exit_price = 0.0

        self._close_trade(exit_price, reason)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _check_cycle(self) -> None:
        """One monitoring tick — check stop fill, T1, T2."""
        # 1. Check if stop order already filled (Alpaca handled it)
        stop_order = self.broker.get_order(self.stop_order_id)
        if stop_order.get('status') == 'filled':
            exit_price = float(stop_order.get('filled_avg_price') or 0)
            log.info(
                f"trade_manager: [{self.symbol}] stop filled @ ${exit_price:.4f}"
            )
            self._close_trade(exit_price, 'STOP')
            return

        # 2. Get latest price from Alpaca
        latest_price = self._get_latest_price()
        if latest_price <= 0:
            return

        # 3. Pre-stop warning: price within 50% of entry→stop distance
        if not self.t1_hit and not self.pre_stop_warned:
            entry = self.plan['entry_price']
            stop  = self.plan['stop_price']
            warn_level = entry - (entry - stop) * 0.5
            if latest_price <= warn_level:
                self.pre_stop_warned = True
                log.warning(
                    f"trade_manager: [{self.symbol}] PRE-STOP WARNING "
                    f"price={latest_price:.4f} warn_level={warn_level:.4f}"
                )
                self._send_result_sms(
                    f"⚠ Pre-stop alert @ ${latest_price:.2f} "
                    f"(50% to stop ${stop:.2f})"
                )
                with db_connection() as conn:
                    log_signal(conn, self.symbol, 'PRE_STOP_WARN',
                               f"price={latest_price:.4f} warn_level={warn_level:.4f}",
                               self.trade_id)

        # 4. T1 check (only if not already hit)
        if not self.t1_hit and latest_price >= self.plan['t1_price']:
            self._handle_t1(latest_price)

        # 5. T2 check (only after T1 has been hit)
        if self.t1_hit and not self.closed and latest_price >= self.plan['t2_price']:
            self._handle_t2(latest_price)

    def _handle_t1(self, current_price: float) -> None:
        """T1 hit: sell half, move stop to breakeven."""
        t1_qty = math.floor(self.plan['shares'] / 2)
        if t1_qty <= 0:
            t1_qty = 1

        log.info(
            f"trade_manager: [{self.symbol}] T1 hit @ ${current_price:.4f} — "
            f"selling {t1_qty} shares, moving stop to entry"
        )

        try:
            self.broker.place_market_sell(self.symbol, t1_qty)
        except Exception as e:
            log.error(f"trade_manager: [{self.symbol}] T1 sell failed: {e}")
            return

        # Move stop to breakeven (entry price)
        try:
            new_stop = self.broker.replace_stop(
                self.stop_order_id,
                self.plan['entry_price'],
            )
            # If replace fails, cancel and re-place
            if not new_stop.get('id'):
                self.broker.cancel_order(self.stop_order_id)
                remaining = self.plan['shares'] - t1_qty
                if remaining > 0:
                    new_order = self.broker.place_stop_sell(
                        self.symbol, remaining, self.plan['entry_price']
                    )
                    self.stop_order_id = new_order.get('id', self.stop_order_id)
            else:
                self.stop_order_id = new_stop.get('id', self.stop_order_id)
        except Exception as e:
            log.warning(f"trade_manager: [{self.symbol}] stop move failed: {e}")

        self.t1_hit = True
        self.remaining_qty = self.plan['shares'] - t1_qty

        with db_connection() as conn:
            update_trade(conn, self.trade_id, status='PARTIAL')
            log_signal(conn, self.symbol, 'T1',
                       f"price={current_price:.4f} qty_sold={t1_qty}",
                       self.trade_id)

        self._send_result_sms(f"T1 hit @ ${current_price:.2f} — {t1_qty} shares sold, stop at entry")

    def _handle_t2(self, current_price: float) -> None:
        """T2 hit: sell remaining shares, close trade."""
        log.info(
            f"trade_manager: [{self.symbol}] T2 hit @ ${current_price:.4f} — "
            f"selling {self.remaining_qty} shares"
        )

        try:
            self.broker.cancel_order(self.stop_order_id)
            self.broker.place_market_sell(self.symbol, self.remaining_qty)
        except Exception as e:
            log.error(f"trade_manager: [{self.symbol}] T2 sell failed: {e}")

        self._close_trade(current_price, 'T2')

    def _close_trade(self, exit_price: float, reason: str) -> None:
        """Finalize the trade in DB, fire SMS, update session counters."""
        if self.closed:
            return
        self.closed = True

        entry = self.plan['entry_price']
        stop  = self.plan['stop_price']
        risk  = entry - stop if entry > stop else 0.01

        pnl = round((exit_price - entry) * self.plan['shares'], 2) if exit_price > 0 else 0.0
        r_mult = round((exit_price - entry) / risk, 2) if risk > 0 else 0.0

        now_et = datetime.now(ET).strftime('%H:%M ET')
        log.info(
            f"trade_manager: [{self.symbol}] closed — reason={reason} "
            f"exit=${exit_price:.4f} pnl=${pnl:.2f} R={r_mult:.2f}"
        )

        with db_connection() as conn:
            update_trade(conn, self.trade_id,
                exit_price=exit_price,
                exit_time='NOW()',
                exit_reason=reason,
                pnl=pnl,
                r_multiple=r_mult,
                status='CLOSED')
            log_signal(conn, self.symbol, reason,
                       f"exit={exit_price:.4f} pnl={pnl:.2f} R={r_mult:.2f}",
                       self.trade_id)
            increment_trade_count(conn, pnl_delta=pnl)

        self._send_result_sms(
            f"CLOSED {reason} @ ${exit_price:.2f} | "
            f"P&L ${pnl:+.2f} | {r_mult:+.2f}R"
        )

    def _get_latest_price(self) -> float:
        """Fetch the latest 1-min close for the symbol from Alpaca."""
        import os
        import requests as req
        headers = {
            'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
            'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
        }
        try:
            r = req.get(
                f'https://data.alpaca.markets/v2/stocks/{self.symbol}/bars/latest',
                headers=headers,
                params={'feed': 'iex'},
                timeout=3,
            )
            r.raise_for_status()
            return float(r.json().get('bar', {}).get('c', 0))
        except Exception as e:
            log.warning(f"trade_manager: price fetch failed for {self.symbol}: {e}")
            return 0.0

    def _parse_fill_price(self, order_or_close: dict) -> float:
        """Extract fill price from an order or position-close response."""
        for key in ('filled_avg_price', 'avg_entry_price', 'price'):
            val = order_or_close.get(key)
            if val:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
        return 0.0

    def _send_result_sms(self, message: str) -> None:
        """Send a brief result SMS. Reuses send_confirmation_sms structure."""
        import os
        import requests as req
        sid   = os.getenv('TWILIO_ACCOUNT_SID', '')
        token = os.getenv('TWILIO_AUTH_TOKEN',  '')
        frm   = os.getenv('TWILIO_FROM', '')
        to    = os.getenv('TWILIO_TO',   '')
        if not (sid and token and frm and to):
            return
        try:
            req.post(
                f'https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json',
                auth=(sid, token),
                data={'From': frm, 'To': to,
                      'Body': f"[{self.symbol}] {message}"},
                timeout=8,
            )
        except Exception as e:
            log.warning(f"trade_manager: result SMS failed: {e}")
