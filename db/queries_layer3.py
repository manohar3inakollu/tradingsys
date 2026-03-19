from psycopg.rows import dict_row
from logger import setup_logger

log = setup_logger('layer3')


# ── Daily session ─────────────────────────────────────────────────────────────

def init_daily_session(conn, risk_budget: float = 80.0) -> None:
    """Create today's session row if it doesn't exist yet."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO daily_sessions (session_date, risk_budget)
            VALUES (CURRENT_DATE, %s)
            ON CONFLICT (session_date) DO NOTHING
        """, (risk_budget,))


def get_daily_session(conn) -> dict:
    """Return today's session record or empty defaults."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT trades_count, total_pnl, risk_budget,
                   no_trade_tickers, session_halted, halt_reason
            FROM daily_sessions
            WHERE session_date = CURRENT_DATE
        """)
        row = cur.fetchone()
        if row:
            return dict(row)
        return {
            'trades_count': 0, 'total_pnl': 0.0, 'risk_budget': 80.0,
            'no_trade_tickers': [], 'session_halted': False, 'halt_reason': None,
        }


def increment_trade_count(conn, pnl_delta: float = 0.0) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE daily_sessions
            SET trades_count = trades_count + 1,
                total_pnl    = total_pnl + %s,
                updated_at   = NOW()
            WHERE session_date = CURRENT_DATE
        """, (pnl_delta,))


def update_session_pnl(conn, pnl_delta: float) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE daily_sessions
            SET total_pnl  = total_pnl + %s,
                updated_at = NOW()
            WHERE session_date = CURRENT_DATE
        """, (pnl_delta,))


def add_no_trade_ticker(conn, symbol: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE daily_sessions
            SET no_trade_tickers = array_append(no_trade_tickers, %s),
                updated_at       = NOW()
            WHERE session_date = CURRENT_DATE
              AND NOT (%s = ANY(no_trade_tickers))
        """, (symbol, symbol))


def halt_session(conn, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE daily_sessions
            SET session_halted = TRUE,
                halt_reason    = %s,
                updated_at     = NOW()
            WHERE session_date = CURRENT_DATE
        """, (reason,))


# ── Trade candidates ──────────────────────────────────────────────────────────

def get_trade_candidates(conn) -> list:
    """
    Return today's TRADE-decision rows from ai_scores, enriched with
    symbol, ticker_type, volume, pm_low, atr_20, and orb_window.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                a.id                AS ai_score_id,
                a.ticker_id,
                t.symbol,
                t.type              AS ticker_type,
                a.entry_price       AS l2_entry_estimate,
                a.stop_price        AS l2_stop_estimate,
                a.t1_price          AS l2_t1,
                a.t2_price          AS l2_t2,
                a.shares            AS l2_shares,
                a.risk_amount       AS l2_risk,
                a.score_final,
                dp.open             AS day_open,
                dp.volume,
                dp.pm_low,
                dp.pm_high,
                dp.atr_20,
                COALESCE(
                    sr.orb_window,
                    CASE WHEN t.type = 'etf' THEN 15 ELSE 5 END
                )                   AS orb_window
            FROM ai_scores a
            JOIN tickers t       ON t.id = a.ticker_id
            JOIN daily_prices dp ON dp.ticker_id = a.ticker_id
                                 AND dp.date = CURRENT_DATE
            LEFT JOIN scan_results sr ON sr.id = a.scan_result_id
            WHERE a.scan_date = CURRENT_DATE
              AND a.decision  = 'TRADE'
            ORDER BY a.score_final DESC NULLS LAST
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Trades ────────────────────────────────────────────────────────────────────

def insert_trade(conn, data: dict) -> int:
    """Insert a new trade row and return its id."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO trades (
                ticker_id, ai_score_id, symbol,
                orb_high, orb_low, orb_window,
                entry_price, stop_price, t1_price, t2_price,
                shares, risk_amount,
                l2_entry_estimate, l2_stop_estimate,
                status
            ) VALUES (
                %(ticker_id)s, %(ai_score_id)s, %(symbol)s,
                %(orb_high)s, %(orb_low)s, %(orb_window)s,
                %(entry_price)s, %(stop_price)s, %(t1_price)s, %(t2_price)s,
                %(shares)s, %(risk_amount)s,
                %(l2_entry_estimate)s, %(l2_stop_estimate)s,
                'PENDING'
            )
            RETURNING id
        """, data)
        row = cur.fetchone()
        return row['id'] if row else None


def update_trade(conn, trade_id: int, **kwargs) -> None:
    """Update arbitrary fields on a trade row."""
    if not kwargs:
        return
    kwargs['updated_at'] = 'NOW()'
    set_clauses = []
    params = {}
    for k, v in kwargs.items():
        if v == 'NOW()':
            set_clauses.append(f"{k} = NOW()")
        else:
            set_clauses.append(f"{k} = %({k})s")
            params[k] = v
    params['trade_id'] = trade_id
    sql = f"UPDATE trades SET {', '.join(set_clauses)} WHERE id = %(trade_id)s"
    with conn.cursor() as cur:
        cur.execute(sql, params)


def get_open_trades(conn) -> list:
    """Return all trades with status OPEN or PARTIAL for today."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT *
            FROM trades
            WHERE trade_date = CURRENT_DATE
              AND status IN ('OPEN', 'PARTIAL')
            ORDER BY created_at
        """)
        return [dict(r) for r in cur.fetchall()]


def get_trade_by_id(conn, trade_id: int) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM trades WHERE id = %s", (trade_id,))
        row = cur.fetchone()
        return dict(row) if row else {}


# ── Signal log ────────────────────────────────────────────────────────────────

def log_signal(conn, symbol: str, event_type: str,
               details: str = None, trade_id: int = None) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO signal_log (symbol, event_type, details, trade_id)
            VALUES (%s, %s, %s, %s)
        """, (symbol, event_type, details, trade_id))
