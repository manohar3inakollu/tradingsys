"""
Layer 4 — read-only DB queries.
Never writes to any table.
"""

from psycopg.rows import dict_row
from logger import setup_logger

log = setup_logger('layer4')


# ── Morning dash ──────────────────────────────────────────────────────────────

def get_todays_layer1(conn) -> list:
    """All Layer 1 candidates for today (passed + failed)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                t.symbol,
                t.type          AS ticker_type,
                t.sector,
                dp.open,
                dp.gap_pct,
                dp.pm_volume,
                dp.pm_high,
                dp.pm_low,
                dp.atr_pct,
                dp.vix,
                sr.passed_filters,
                sr.orb_window,
                sr.rank
            FROM scan_results sr
            JOIN tickers      t  ON t.id  = sr.ticker_id
            JOIN daily_prices dp ON dp.id = sr.price_id
            WHERE sr.scan_date = CURRENT_DATE
            ORDER BY sr.passed_filters DESC, sr.rank ASC NULLS LAST
        """)
        return [dict(r) for r in cur.fetchall()]


def get_todays_ai_scores(conn) -> list:
    """Today's ai_scores for all scored candidates."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                a.id                    AS ai_score_id,
                t.symbol,
                t.type                  AS ticker_type,
                a.score_final,
                a.decision,
                a.catalyst_type,
                a.catalyst_score,
                a.catalyst_confidence,
                a.catalyst_reasoning,
                a.score_catalyst,
                a.score_volume,
                a.score_gap,
                a.score_atr,
                a.score_spy,
                a.vix_multiplier,
                a.skip_reason,
                a.entry_price,
                a.stop_price,
                a.t1_price,
                a.t2_price,
                a.shares,
                a.risk_amount,
                a.headline,
                a.sentiment,
                a.created_at
            FROM ai_scores a
            JOIN tickers t ON t.id = a.ticker_id
            WHERE a.scan_date = CURRENT_DATE
            ORDER BY a.score_final DESC NULLS LAST
        """)
        return [dict(r) for r in cur.fetchall()]


def get_todays_l3_plans(conn) -> list:
    """
    Layer 3 real ORB-based trade plans for today.
    Returns the latest trade record per symbol (PENDING/OPEN/PARTIAL/CLOSED).
    These values override Layer 2 pm_low estimates in the morning dashboard.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT DISTINCT ON (symbol)
                symbol,
                orb_high,
                orb_low,
                entry_price,
                stop_price,
                t1_price,
                t2_price,
                shares,
                risk_amount,
                filled_entry,
                status
            FROM trades
            WHERE trade_date = CURRENT_DATE
              AND status NOT IN ('REJECTED', 'TIMEOUT')
            ORDER BY symbol, created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_market_context(conn) -> dict:
    """VIX + session status for today."""
    with conn.cursor(row_factory=dict_row) as cur:
        # VIX from latest daily_price entry
        cur.execute("""
            SELECT vix FROM daily_prices
            WHERE date = CURRENT_DATE
            ORDER BY id DESC LIMIT 1
        """)
        row = cur.fetchone()
        vix = float(row['vix']) if row and row['vix'] else 0.0

        # Today's session
        cur.execute("""
            SELECT trades_count, total_pnl, risk_budget,
                   session_halted, halt_reason
            FROM daily_sessions
            WHERE session_date = CURRENT_DATE
        """)
        sess = cur.fetchone()

    return {
        'vix':            vix,
        'trades_count':   sess['trades_count']  if sess else 0,
        'total_pnl':      float(sess['total_pnl']) if sess else 0.0,
        'risk_budget':    float(sess['risk_budget']) if sess else 80.0,
        'session_halted': sess['session_halted'] if sess else False,
        'halt_reason':    sess['halt_reason']    if sess else None,
    }


# ── Trade cockpit ─────────────────────────────────────────────────────────────

def get_live_trades(conn) -> list:
    """Open or partial trades for today."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT *
            FROM trades
            WHERE trade_date = CURRENT_DATE
              AND status IN ('OPEN', 'PARTIAL')
            ORDER BY created_at
        """)
        return [dict(r) for r in cur.fetchall()]


def get_todays_closed_trades(conn) -> list:
    """Closed trades for today with P&L."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                t.symbol,
                tr.entry_price,
                tr.filled_entry,
                tr.stop_price,
                tr.t1_price,
                tr.t2_price,
                tr.exit_price,
                tr.exit_reason,
                tr.pnl,
                tr.r_multiple,
                tr.shares,
                tr.status,
                tr.confirmed,
                tr.created_at,
                tr.exit_time
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            WHERE tr.trade_date = CURRENT_DATE
              AND tr.status NOT IN ('OPEN', 'PARTIAL', 'PENDING')
            ORDER BY tr.created_at
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Analytics ─────────────────────────────────────────────────────────────────

def get_win_rate_by_bucket(conn) -> list:
    """Win rate and avg R for score buckets 65-74 and 75+."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                CASE WHEN a.score_final >= 75 THEN '75+'
                     ELSE '65-74'
                END                                                 AS bucket,
                COUNT(*)                                            AS trades,
                SUM(CASE WHEN tr.r_multiple > 0 THEN 1 ELSE 0 END) AS wins,
                ROUND(AVG(tr.r_multiple)::numeric, 2)               AS avg_r,
                ROUND(SUM(tr.pnl)::numeric, 2)                      AS total_pnl
            FROM trades tr
            JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE tr.status = 'CLOSED'
            GROUP BY bucket
            ORDER BY bucket
        """)
        return [dict(r) for r in cur.fetchall()]


def get_r_mult_by_catalyst(conn) -> list:
    """Average R-multiple grouped by catalyst type."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                COALESCE(a.catalyst_type, 'unknown')                AS catalyst_type,
                COUNT(*)                                            AS trades,
                SUM(CASE WHEN tr.r_multiple > 0 THEN 1 ELSE 0 END) AS wins,
                ROUND(AVG(tr.r_multiple)::numeric, 2)               AS avg_r,
                ROUND(SUM(tr.pnl)::numeric, 2)                      AS total_pnl
            FROM trades tr
            JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE tr.status = 'CLOSED'
            GROUP BY catalyst_type
            ORDER BY avg_r DESC NULLS LAST
        """)
        return [dict(r) for r in cur.fetchall()]


def get_confirmation_outcomes(conn) -> list:
    """Outcomes split by YES / NO / TIMEOUT confirmation reply."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                CASE
                    WHEN confirmed = TRUE  THEN 'YES'
                    WHEN confirmed = FALSE THEN 'NO'
                    ELSE 'TIMEOUT'
                END                                                        AS reply,
                COUNT(*)                                                   AS signals,
                SUM(CASE WHEN status = 'CLOSED' AND r_multiple > 0 THEN 1
                         ELSE 0 END)                                       AS wins,
                ROUND(AVG(CASE WHEN status = 'CLOSED'
                               THEN r_multiple END)::numeric, 2)           AS avg_r,
                ROUND(SUM(CASE WHEN status = 'CLOSED'
                               THEN pnl END)::numeric, 2)                  AS total_pnl
            FROM trades
            GROUP BY reply
            ORDER BY reply
        """)
        return [dict(r) for r in cur.fetchall()]


def get_overall_stats(conn) -> dict:
    """Aggregate stats across all closed trades."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                             AS total_trades,
                SUM(CASE WHEN r_multiple > 0 THEN 1 ELSE 0 END)     AS wins,
                ROUND(AVG(r_multiple)::numeric, 2)                   AS avg_r,
                ROUND(SUM(pnl)::numeric, 2)                          AS total_pnl,
                ROUND(MAX(r_multiple)::numeric, 2)                   AS best_r,
                ROUND(MIN(r_multiple)::numeric, 2)                   AS worst_r
            FROM trades
            WHERE status = 'CLOSED'
        """)
        row = cur.fetchone()
        if not row or not row['total_trades']:
            return {'total_trades': 0, 'wins': 0, 'avg_r': 0,
                    'total_pnl': 0, 'best_r': 0, 'worst_r': 0, 'win_rate': 0}
        result = dict(row)
        result['win_rate'] = round(
            result['wins'] / result['total_trades'] * 100, 1
        ) if result['total_trades'] else 0
        return result


# ── Account progress ──────────────────────────────────────────────────────────

def get_daily_pnl_series(conn) -> list:
    """Daily P&L from all closed trades, ordered by date."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                trade_date,
                ROUND(SUM(pnl)::numeric, 2)   AS daily_pnl,
                COUNT(*)                       AS trades
            FROM trades
            WHERE status = 'CLOSED'
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Reports ───────────────────────────────────────────────────────────────────

def get_yesterday_summary(conn) -> dict:
    """Yesterday's closed trades and session summary for morning email."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                tr.symbol,
                tr.entry_price,
                tr.exit_price,
                tr.exit_reason,
                tr.pnl,
                tr.r_multiple,
                tr.shares,
                a.score_final,
                a.catalyst_type
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            LEFT JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE tr.trade_date = CURRENT_DATE - 1
              AND tr.status     = 'CLOSED'
            ORDER BY tr.exit_time
        """)
        trades = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT trades_count, total_pnl, session_halted, halt_reason
            FROM daily_sessions
            WHERE session_date = CURRENT_DATE - 1
        """)
        sess = cur.fetchone()

    return {
        'trades':         trades,
        'trades_count':   sess['trades_count']  if sess else 0,
        'total_pnl':      float(sess['total_pnl']) if sess else 0.0,
        'session_halted': sess['session_halted'] if sess else False,
    }


def get_todays_watchlist_for_email(conn) -> list:
    """Today's Layer 1 candidates for morning email."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT t.symbol, t.type, dp.gap_pct, dp.pm_volume, sr.rank
            FROM scan_results sr
            JOIN tickers      t  ON t.id  = sr.ticker_id
            JOIN daily_prices dp ON dp.id = sr.price_id
            WHERE sr.scan_date      = CURRENT_DATE
              AND sr.passed_filters = TRUE
            ORDER BY sr.rank ASC
            LIMIT 10
        """)
        return [dict(r) for r in cur.fetchall()]


def get_weekly_trades(conn) -> list:
    """All closed trades from the past 7 days for weekly Sheets fill."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                tr.trade_date,
                t.symbol,
                t.type              AS ticker_type,
                tr.orb_window,
                tr.entry_price,
                tr.filled_entry,
                tr.stop_price,
                tr.t1_price,
                tr.t2_price,
                tr.exit_price,
                tr.exit_reason,
                tr.pnl,
                tr.r_multiple,
                tr.shares,
                tr.confirmed,
                tr.status,
                a.score_final,
                a.catalyst_type,
                a.catalyst_score,
                a.catalyst_confidence
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            LEFT JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE tr.trade_date >= CURRENT_DATE - 7
              AND tr.status = 'CLOSED'
            ORDER BY tr.trade_date, tr.created_at
        """)
        return [dict(r) for r in cur.fetchall()]


def get_all_sessions(conn) -> list:
    """All daily sessions ordered newest first — for the reports page."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT session_date, trades_count,
                   ROUND(total_pnl::numeric, 2)    AS total_pnl,
                   ROUND(risk_budget::numeric, 2)   AS risk_budget,
                   session_halted, halt_reason
            FROM daily_sessions
            ORDER BY session_date DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_signal_log_today(conn) -> list:
    """Today's signal_log entries, newest first (max 100)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT symbol, event_type, details,
                   log_time AT TIME ZONE 'America/New_York' AS log_time_et
            FROM signal_log
            WHERE log_time::date = CURRENT_DATE
            ORDER BY log_time DESC
            LIMIT 100
        """)
        return [dict(r) for r in cur.fetchall()]


def get_symbol_score_detail(conn, symbol: str) -> dict | None:
    """Today's AI score detail for a specific symbol."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                a.id                    AS ai_score_id,
                a.score_final,
                a.decision,
                a.catalyst_type,
                a.catalyst_score,
                a.catalyst_confidence,
                a.catalyst_reasoning,
                a.headline,
                a.sentiment,
                a.score_catalyst,
                a.score_volume,
                a.score_gap,
                a.score_atr,
                a.score_spy,
                a.vix_multiplier,
                a.skip_reason,
                a.entry_price,
                a.stop_price,
                a.t1_price,
                a.t2_price,
                a.shares,
                a.risk_amount
            FROM ai_scores a
            JOIN tickers t ON t.id = a.ticker_id
            WHERE t.symbol = %s
              AND a.scan_date = CURRENT_DATE
            ORDER BY a.created_at DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_symbol_history(conn, symbol: str) -> list:
    """All closed trades for a specific symbol (newest first, capped at 20)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                tr.trade_date,
                tr.entry_price,
                tr.filled_entry,
                tr.exit_price,
                tr.exit_reason,
                tr.pnl,
                tr.r_multiple,
                tr.shares,
                tr.confirmed,
                a.score_final,
                a.catalyst_type
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            LEFT JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE t.symbol = %s
              AND tr.status = 'CLOSED'
            ORDER BY tr.trade_date DESC, tr.created_at DESC
            LIMIT 20
        """, (symbol,))
        return [dict(r) for r in cur.fetchall()]


def get_all_closed_trades(conn) -> list:
    """All closed trades ever, for Sheets trade log tab."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                tr.trade_date,
                t.symbol,
                t.type              AS ticker_type,
                tr.entry_price,
                tr.filled_entry,
                tr.stop_price,
                tr.t1_price,
                tr.t2_price,
                tr.exit_price,
                tr.exit_reason,
                tr.pnl,
                tr.r_multiple,
                tr.shares,
                tr.confirmed,
                a.score_final,
                a.catalyst_type,
                a.catalyst_score
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            LEFT JOIN ai_scores a ON a.id = tr.ai_score_id
            WHERE tr.status = 'CLOSED'
            ORDER BY tr.trade_date, tr.created_at
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Symbol company info ────────────────────────────────────────────────────────

def get_symbol_info(conn, symbol: str) -> dict | None:
    """Company name, industry, sector for a symbol."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT company, industry, sector, type
            FROM tickers WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        return dict(row) if row else None


# ── Per-symbol analytics ───────────────────────────────────────────────────────

def get_per_symbol_stats(conn) -> list:
    """Win rate, avg R, total P&L per symbol across all closed trades."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                t.symbol,
                COUNT(*)                                             AS trades,
                SUM(CASE WHEN tr.r_multiple > 0 THEN 1 ELSE 0 END) AS wins,
                ROUND(AVG(tr.r_multiple)::numeric, 2)               AS avg_r,
                ROUND(SUM(tr.pnl)::numeric, 2)                      AS total_pnl,
                ROUND(MAX(tr.r_multiple)::numeric, 2)               AS best_r
            FROM trades tr
            JOIN tickers t ON t.id = tr.ticker_id
            WHERE tr.status = 'CLOSED'
            GROUP BY t.symbol
            ORDER BY total_pnl DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_exit_reason_stats(conn) -> list:
    """P&L, win rate, avg R grouped by exit reason."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                COALESCE(exit_reason, 'UNKNOWN')                    AS exit_reason,
                COUNT(*)                                             AS trades,
                SUM(CASE WHEN r_multiple > 0 THEN 1 ELSE 0 END)    AS wins,
                ROUND(AVG(r_multiple)::numeric, 2)                  AS avg_r,
                ROUND(SUM(pnl)::numeric, 2)                         AS total_pnl
            FROM trades
            WHERE status = 'CLOSED'
            GROUP BY exit_reason
            ORDER BY total_pnl DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_time_of_day_stats(conn) -> list:
    """Win rate and avg R by entry hour (ET), 9–16."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int AS hour,
                COUNT(*)                                             AS trades,
                SUM(CASE WHEN r_multiple > 0 THEN 1 ELSE 0 END)    AS wins,
                ROUND(AVG(r_multiple)::numeric, 2)                  AS avg_r,
                ROUND(SUM(pnl)::numeric, 2)                         AS total_pnl
            FROM trades
            WHERE status = 'CLOSED'
            GROUP BY hour
            ORDER BY hour
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Catalyst log ───────────────────────────────────────────────────────────────

def get_catalyst_log(conn) -> list:
    """News catalysts with trade outcomes for the last 30 days."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                a.scan_date,
                t.symbol,
                a.headline,
                a.sentiment,
                a.catalyst_type,
                a.catalyst_confidence,
                a.decision,
                tr.pnl,
                tr.r_multiple,
                tr.exit_reason,
                tr.status AS trade_status
            FROM ai_scores a
            JOIN tickers t ON t.id = a.ticker_id
            LEFT JOIN trades tr ON tr.ai_score_id = a.id AND tr.status = 'CLOSED'
            WHERE a.scan_date >= CURRENT_DATE - 30
              AND a.headline IS NOT NULL
            ORDER BY a.scan_date DESC, a.score_final DESC NULLS LAST
            LIMIT 60
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Session streak ─────────────────────────────────────────────────────────────

def get_recent_streak(conn) -> list:
    """Last 5 trading days with daily P&L (newest first)."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT trade_date, ROUND(SUM(pnl)::numeric, 2) AS daily_pnl
            FROM trades
            WHERE status = 'CLOSED'
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 5
        """)
        return [dict(r) for r in cur.fetchall()]
