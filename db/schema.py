"""
Run once:  python -m db.schema
Creates all Layer 1 tables in PostgreSQL.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.connection import db_connection
from logger import setup_logger

log = setup_logger('layer1')

SCHEMA = """
CREATE TABLE IF NOT EXISTS tickers (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10) UNIQUE NOT NULL,
    type        VARCHAR(10) NOT NULL CHECK (type IN ('stock', 'etf')),
    sector      VARCHAR(100),
    industry    VARCHAR(150),
    company     TEXT,
    first_seen  DATE DEFAULT CURRENT_DATE,
    notes       TEXT
);

CREATE TABLE IF NOT EXISTS daily_prices (
    id          SERIAL PRIMARY KEY,
    ticker_id   INTEGER NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    date        DATE NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    prev_close  NUMERIC(12,4),
    gap_pct     NUMERIC(8,4),
    pm_high     NUMERIC(12,4),
    pm_low      NUMERIC(12,4),
    pm_volume   BIGINT,
    atr_20      NUMERIC(10,4),
    atr_pct     NUMERIC(8,4),
    vix         NUMERIC(8,4),
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker_id, date)
);

CREATE TABLE IF NOT EXISTS scan_results (
    id              SERIAL PRIMARY KEY,
    scan_date       DATE NOT NULL DEFAULT CURRENT_DATE,
    scan_time       TIMESTAMPTZ DEFAULT NOW(),
    ticker_id       INTEGER NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    price_id        INTEGER REFERENCES daily_prices(id),
    passed_filters  BOOLEAN NOT NULL,
    failed_filters  TEXT[],
    gap_pct         NUMERIC(8,4),
    orb_window      SMALLINT CHECK (orb_window IN (5, 15)),
    rank            SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date   ON daily_prices(date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices(ticker_id, date);
CREATE INDEX IF NOT EXISTS idx_scan_results_date   ON scan_results(scan_date);
CREATE INDEX IF NOT EXISTS idx_scan_results_ticker ON scan_results(ticker_id);
"""


SCHEMA_LAYER2 = """
CREATE TABLE IF NOT EXISTS ai_scores (
    id                  SERIAL PRIMARY KEY,
    scan_date           DATE NOT NULL DEFAULT CURRENT_DATE,
    ticker_id           INTEGER NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    price_id            INTEGER REFERENCES daily_prices(id),
    scan_result_id      INTEGER REFERENCES scan_results(id),
    -- news
    headline            TEXT,
    news_source         VARCHAR(30),
    sentiment           VARCHAR(10),
    -- haiku output
    catalyst_score      SMALLINT,
    catalyst_direction  VARCHAR(10),
    catalyst_confidence VARCHAR(10),
    catalyst_type       VARCHAR(20),
    catalyst_reasoning  TEXT,
    catalyst_skipped    BOOLEAN DEFAULT FALSE,
    -- component scores
    score_catalyst      SMALLINT,
    score_volume        SMALLINT,
    score_gap           SMALLINT,
    score_atr           SMALLINT,
    score_spy           SMALLINT,
    score_raw           SMALLINT,
    score_final         NUMERIC(6,2),
    vix_multiplier      NUMERIC(4,2),
    -- decision
    decision            VARCHAR(10) CHECK (decision IN ('TRADE','WATCH','SKIP')),
    skip_reason         TEXT,
    -- trade plan (TRADE only, null otherwise)
    entry_price         NUMERIC(12,4),
    stop_price          NUMERIC(12,4),
    t1_price            NUMERIC(12,4),
    t2_price            NUMERIC(12,4),
    shares              INTEGER,
    risk_amount         NUMERIC(8,2),
    -- meta
    vix                 NUMERIC(8,4),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (scan_date, ticker_id)
);

CREATE INDEX IF NOT EXISTS idx_ai_scores_date    ON ai_scores(scan_date);
CREATE INDEX IF NOT EXISTS idx_ai_scores_decision ON ai_scores(scan_date, decision);
"""


SCHEMA_LAYER3 = """
CREATE TABLE IF NOT EXISTS daily_sessions (
    id                  SERIAL PRIMARY KEY,
    session_date        DATE NOT NULL UNIQUE DEFAULT CURRENT_DATE,
    trades_count        SMALLINT DEFAULT 0,
    total_pnl           NUMERIC(10,2) DEFAULT 0,
    risk_budget         NUMERIC(8,2) DEFAULT 80,
    no_trade_tickers    TEXT[] DEFAULT ARRAY[]::TEXT[],
    session_halted      BOOLEAN DEFAULT FALSE,
    halt_reason         TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trades (
    id                  SERIAL PRIMARY KEY,
    trade_date          DATE NOT NULL DEFAULT CURRENT_DATE,
    ticker_id           INTEGER NOT NULL REFERENCES tickers(id) ON DELETE CASCADE,
    ai_score_id         INTEGER REFERENCES ai_scores(id),
    symbol              VARCHAR(10) NOT NULL,
    -- ORB context
    orb_high            NUMERIC(12,4),
    orb_low             NUMERIC(12,4),
    orb_window          SMALLINT,
    -- live plan (recalculated at breakout)
    entry_price         NUMERIC(12,4),
    stop_price          NUMERIC(12,4),
    t1_price            NUMERIC(12,4),
    t2_price            NUMERIC(12,4),
    shares              INTEGER,
    risk_amount         NUMERIC(8,2),
    -- layer 2 estimates for comparison
    l2_entry_estimate   NUMERIC(12,4),
    l2_stop_estimate    NUMERIC(12,4),
    -- execution
    entry_order_id      VARCHAR(50),
    stop_order_id       VARCHAR(50),
    filled_entry        NUMERIC(12,4),
    -- confirmation
    confirmed           BOOLEAN,
    confirmation_time   TIMESTAMPTZ,
    -- exit
    exit_price          NUMERIC(12,4),
    exit_time           TIMESTAMPTZ,
    exit_reason         VARCHAR(20),
    -- results
    pnl                 NUMERIC(10,2),
    r_multiple          NUMERIC(6,2),
    -- status: PENDING | OPEN | PARTIAL | CLOSED | REJECTED | TIMEOUT | NO_TRADE
    status              VARCHAR(20) DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signal_log (
    id          SERIAL PRIMARY KEY,
    log_time    TIMESTAMPTZ DEFAULT NOW(),
    symbol      VARCHAR(10) NOT NULL,
    event_type  VARCHAR(30) NOT NULL,
    details     TEXT,
    trade_id    INTEGER REFERENCES trades(id)
);

CREATE INDEX IF NOT EXISTS idx_trades_date       ON trades(trade_date);
CREATE INDEX IF NOT EXISTS idx_trades_ticker     ON trades(ticker_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_signal_log_time   ON signal_log(log_time);
"""


def create_schema():
    log.info("Creating schema...")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
    log.info("Schema ready -- tables: tickers, daily_prices, scan_results")
    create_schema_layer2()


def create_schema_layer2():
    log.info("Creating Layer 2 schema...")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_LAYER2)
    log.info("Schema ready -- table: ai_scores")


def create_schema_layer3():
    log.info("Creating Layer 3 schema...")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_LAYER3)
    log.info("Schema ready -- tables: daily_sessions, trades, signal_log")


if __name__ == '__main__':
    create_schema()
    create_schema_layer3()
