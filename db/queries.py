from psycopg.rows import dict_row
from logger import setup_logger

log = setup_logger('layer1')


# ── tickers ──────────────────────────────────────────────

def upsert_ticker(conn, symbol: str, ticker_type: str,
                  sector: str = None, industry: str = None,
                  company: str = None) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO tickers (symbol, type, sector, industry, company)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                sector   = COALESCE(EXCLUDED.sector,   tickers.sector),
                industry = COALESCE(EXCLUDED.industry, tickers.industry),
                company  = COALESCE(EXCLUDED.company,  tickers.company)
            RETURNING id
        """, (symbol, ticker_type, sector, industry, company))
        return cur.fetchone()['id']


# ── daily_prices ─────────────────────────────────────────

def upsert_daily_price(conn, ticker_id: int, data: dict) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO daily_prices (
                ticker_id, date, open, high, low, close, volume,
                prev_close, gap_pct, pm_high, pm_low, pm_volume,
                atr_20, atr_pct, vix
            ) VALUES (
                %(ticker_id)s, %(date)s, %(open)s, %(high)s, %(low)s,
                %(close)s, %(volume)s, %(prev_close)s, %(gap_pct)s,
                %(pm_high)s, %(pm_low)s, %(pm_volume)s,
                %(atr_20)s, %(atr_pct)s, %(vix)s
            )
            ON CONFLICT (ticker_id, date) DO UPDATE SET
                open      = EXCLUDED.open,
                high      = EXCLUDED.high,
                low       = EXCLUDED.low,
                close     = EXCLUDED.close,
                volume    = EXCLUDED.volume,
                gap_pct   = EXCLUDED.gap_pct,
                pm_high   = EXCLUDED.pm_high,
                pm_low    = EXCLUDED.pm_low,
                pm_volume = EXCLUDED.pm_volume,
                atr_20    = EXCLUDED.atr_20,
                atr_pct   = EXCLUDED.atr_pct,
                vix       = EXCLUDED.vix
            RETURNING id
        """, {**data, 'ticker_id': ticker_id})
        return cur.fetchone()['id']


def update_premarket_levels(conn, ticker_id: int, date: str,
                            pm_high: float, pm_low: float,
                            pm_volume: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE daily_prices
            SET pm_high = %s, pm_low = %s, pm_volume = %s
            WHERE ticker_id = %s AND date = %s
        """, (pm_high, pm_low, pm_volume, ticker_id, date))
        log.info(f"Updated pm levels ticker_id={ticker_id} "
                 f"pm_high={pm_high} pm_low={pm_low} pm_vol={pm_volume}")


# ── scan_results ─────────────────────────────────────────

def insert_scan_result(conn, scan_date: str, ticker_id: int,
                       price_id: int, passed: bool,
                       failed: list, gap_pct: float,
                       orb_window: int, rank: int) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO scan_results (
                scan_date, ticker_id, price_id, passed_filters,
                failed_filters, gap_pct, orb_window, rank
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (scan_date, ticker_id, price_id, passed,
              failed, gap_pct, orb_window, rank))
        return cur.fetchone()['id']


def get_todays_candidates(conn) -> list:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                t.id AS ticker_id,
                t.symbol,
                t.type,
                t.sector,
                dp.gap_pct,
                dp.pm_high,
                dp.pm_low,
                dp.pm_volume,
                dp.atr_pct,
                dp.vix,
                sr.orb_window,
                sr.rank
            FROM scan_results sr
            JOIN tickers t      ON sr.ticker_id = t.id
            JOIN daily_prices dp ON sr.price_id  = dp.id
            WHERE sr.scan_date    = CURRENT_DATE
              AND sr.passed_filters = TRUE
            ORDER BY sr.rank ASC
        """)
        return cur.fetchall()


def get_todays_candidates_full(conn) -> list:
    """Layer 2 version: includes open, atr_20, price_id, scan_result_id."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                t.id        AS ticker_id,
                t.symbol,
                t.type,
                t.sector,
                t.company,
                dp.id       AS price_id,
                dp.open,
                dp.high,
                dp.low,
                dp.close,
                dp.gap_pct,
                dp.pm_high,
                dp.pm_low,
                dp.pm_volume,
                dp.atr_20,
                dp.atr_pct,
                dp.vix,
                sr.id       AS scan_result_id,
                sr.orb_window,
                sr.rank
            FROM scan_results sr
            JOIN tickers      t  ON sr.ticker_id = t.id
            JOIN daily_prices dp ON sr.price_id  = dp.id
            WHERE sr.scan_date      = CURRENT_DATE
              AND sr.passed_filters = TRUE
            ORDER BY sr.rank ASC
        """)
        return cur.fetchall()
