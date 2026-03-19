"""
In-memory confirmation state — shared between Layer 3 and Layer 4.

Both run in the same process (Layer 3 is a background thread, Flask is another),
so a thread-safe in-memory store is simpler and faster than polling the DB during
the 60-second confirmation window.

Layer 3 (runner.py) usage:
  set_pending(trade_id, symbol, plan, deadline_ts)  # before send_confirmation_sms
  clear_pending(trade_id)                            # after wait_for_reply returns

Layer 3 (confirmation.py) usage:
  get_web_reply(trade_id)   # returns 'YES', 'NO', or None

Layer 4 (app.py) usage:
  get_all_pending()                  # JSON list for /api/pending endpoint
  set_web_reply(trade_id, reply)     # on /api/confirm or /api/reject
  clear_pending(trade_id)            # remove from display after web action
"""

import threading
import time

_lock       = threading.Lock()
_pending: dict  = {}   # trade_id -> display dict
_replies: dict  = {}   # trade_id -> 'YES' | 'NO'


# ── Layer 3 writes ─────────────────────────────────────────────────────────────

def set_pending(trade_id: int, symbol: str, plan: dict, deadline_ts: float) -> None:
    """Register a trade as awaiting confirmation."""
    with _lock:
        _pending[trade_id] = {
            'id':           trade_id,
            'symbol':       symbol,
            'entry_price':  plan.get('entry_price'),
            'stop_price':   plan.get('stop_price'),
            't1_price':     plan.get('t1_price'),
            't2_price':     plan.get('t2_price'),
            'shares':       plan.get('shares'),
            'risk_amount':  plan.get('risk_amount'),
            'deadline_ts':  deadline_ts,
        }


def clear_pending(trade_id: int) -> None:
    """Remove trade from the pending display (confirmation window closed)."""
    with _lock:
        _pending.pop(trade_id, None)
        _replies.pop(trade_id, None)


# ── Layer 4 writes ─────────────────────────────────────────────────────────────

def set_web_reply(trade_id: int, reply: str) -> None:
    """Store the web YES/NO decision so confirmation.py can pick it up."""
    with _lock:
        _replies[trade_id] = reply
        _pending.pop(trade_id, None)   # hide from UI immediately


# ── Shared reads ───────────────────────────────────────────────────────────────

def get_web_reply(trade_id: int):
    """Return 'YES', 'NO', or None if not yet set."""
    with _lock:
        return _replies.get(trade_id)


def get_all_pending() -> list:
    """Return all pending trades whose deadline has not yet passed."""
    now = time.time()
    with _lock:
        return [
            dict(v) for v in _pending.values()
            if v['deadline_ts'] > now
        ]
