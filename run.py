"""
Trading System — Gap Scanner + AI Scorer + Live Execution + Dashboard
======================================================================
Runs scheduled jobs every market day (ET):

  8:00 AM  — morning email (yesterday summary + today's watchlist)
  9:25 AM  — pre-market snapshot (refresh pm highs/lows)
  9:28 AM  — health check
  9:30 AM  — main gap scan -> PostgreSQL
  9:33 AM  — Layer 2 scoring (Haiku + 100pt model)
  9:45 AM  — Layer 3 live execution (ORB + 5-criteria + SMS gate)
  4:00 PM  — end-of-day SMS summary
  Sun 6 PM — weekly Google Sheets fill

Flask dashboard runs on DASHBOARD_PORT (default 5000) in background thread.

Usage:
  python run.py               # start scheduler + dashboard
  python run.py --now         # run main scan immediately
  python run.py --layer2      # run Layer 2 scoring immediately
  python run.py --layer3      # start Layer 3 monitor immediately
  python run.py --snapshot    # run pre-market snapshot immediately
  python run.py --health      # run health check only
  python run.py --setup       # create DB schema and exit
  python run.py --demo        # weekend/offline test: skips Finviz & market-day guard,
                              #   uses hardcoded tickers + live Alpaca historical data
"""

from scanner.merger import merge_and_save
from scanner.etf_scanner import scan_etfs
from scanner.gap_scanner import scan_stocks
from scanner.alpaca_client import AlpacaClient
from layer4.runner import start_dashboard, schedule_reports
from layer3.runner import start_layer3, stop_layer3
from layer2.runner import run_layer2
from db.schema import create_schema, create_schema_layer2, create_schema_layer3
from db.queries import get_todays_candidates, update_premarket_levels
from db.connection import db_connection, init_pool, close_pool
from logger import setup_logger
from timing import ET, MARKET_HOLIDAYS_2026, SCAN_CONFIG
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import sys
import time
import os
sys.path.insert(0, os.path.dirname(__file__))


log = setup_logger('layer1')


# ── Guards ────────────────────────────────────────────────────────────────────

def is_market_day() -> bool:
    now = datetime.now(ET)
    today = now.strftime('%Y-%m-%d')
    weekday = now.weekday()          # 0=Mon … 4=Fri, 5=Sat, 6=Sun

    if weekday >= 5:
        log.info(f"Weekend ({now.strftime('%A')}) — no scan")
        return False
    if today in MARKET_HOLIDAYS_2026:
        log.info(f"Market holiday ({today}) — no scan")
        return False
    return True


# ── Health check ──────────────────────────────────────────────────────────────

def health_check() -> bool:
    if not is_market_day():
        return False

    now_et = datetime.now(ET).strftime('%H:%M ET')

    try:
        from health import check_all
        results = check_all()
    except Exception as e:
        log.error(f"[{now_et}] health_check: failed to import health module: {e}")
        return False

    non_fatal = {'anthropic', 'sheets'}

    for source, status in results.items():
        level = 'info' if status.startswith('ok') or status.startswith('skipped') else 'error'
        getattr(log, level)(f"[{now_et}] health_check {source}: {status}")

    failed = [k for k, v in results.items() if 'FAIL' in v and k not in non_fatal]
    if failed:
        log.error(f"[{now_et}] health_check FAILED: {failed} — scan will abort")
        return False

    nonfatal_failed = [k for k, v in results.items() if 'FAIL' in v and k in non_fatal]
    if nonfatal_failed:
        log.warning(f"[{now_et}] health_check non-fatal failures: {nonfatal_failed} — continuing")

    log.info(f"[{now_et}] health_check: all systems green")
    return True


# ── Pre-market snapshot ───────────────────────────────────────────────────────

def run_premarket_snapshot():
    if not is_market_day():
        return
    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(f"[{now_et}] Pre-market snapshot starting...")

    alpaca = AlpacaClient()
    today = datetime.now(ET).strftime('%Y-%m-%d')

    with db_connection() as conn:
        rows = get_todays_candidates(conn)

    if not rows:
        log.info(f"[{now_et}] No candidates from today yet — skipping snapshot")
        return

    for row in rows:
        ticker = row['symbol']
        try:
            pm_vol, pm_high, pm_low = alpaca.get_premarket_bars(ticker)
            with db_connection() as conn:
                update_premarket_levels(
                    conn, row['ticker_id'] if 'ticker_id' in row.keys()
                    else _get_ticker_id(conn, ticker),
                    today, pm_high, pm_low, pm_vol
                )
            log.info(
                f"[{now_et}] snapshot {ticker}: "
                f"pm_high={pm_high} pm_low={pm_low} vol={pm_vol}"
            )
        except Exception as e:
            log.warning(f"[{now_et}] snapshot failed for {ticker}: {e}")

    log.info(
        f"[{now_et}] Pre-market snapshot complete — {len(rows)} tickers updated")


def _get_ticker_id(conn, symbol: str) -> int:
    with conn.cursor() as cur:
        cur.execute('SELECT id FROM tickers WHERE symbol = %s', (symbol,))
        row = cur.fetchone()
        return row[0] if row else None


# ── Main scan ─────────────────────────────────────────────────────────────────

def run_layer1_main():
    if not is_market_day():
        return

    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(f"[{now_et}] --- Layer 1 starting ---")

    # Abort if health check fails
    if not health_check():
        log.error(f"[{now_et}] Health check failed — Layer 1 aborted")
        return

    # 60-second buffer for Finviz to reflect true opening gap
    delay = SCAN_CONFIG['finviz_delay_s']
    log.info(f"[{now_et}] Market open — waiting {delay}s for Finviz to update...")
    time.sleep(delay)

    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(f"[{now_et}] Starting gap scan...")

    alpaca = AlpacaClient()

    # Fetch VIX proxy first — single call
    vix = alpaca.get_vix()
    log.info(f"[{now_et}] VIX: {vix}")

    # Run both scanners (Finviz discovery + Tradier enrichment — no Alpaca)
    stocks = scan_stocks()
    etfs = scan_etfs()

    # Merge, rank, save
    saved = merge_and_save(stocks, etfs, vix=vix)

    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(
        f"[{now_et}] --- Layer 1 complete ---\n"
        f"  stocks checked : {len(stocks)}\n"
        f"  ETFs checked   : {len(etfs)}\n"
        f"  saved to DB    : {len(saved)}\n"
        f"  VIX            : {vix}\n"
        f"  top candidates : "
        f"{[c['ticker'] for c in saved[:5]]}\n"
        f"  Layer 2 has until 9:45 AM ET to score"
    )


# ── Demo mode (weekend / offline testing) ────────────────────────────────────

# Liquid, volatile stocks likely to be in the $10–$100 range for filter testing
_DEMO_TICKERS = [
    'MARA', 'RIOT', 'COIN', 'PLUG', 'SOFI',
    'RIVN', 'XPEV', 'NIO',  'LCID', 'SOUN',
]


def _demo_scan_stocks() -> list:
    """Like scan_stocks() but uses a hardcoded list instead of Finviz (Tradier enrichment)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from layer3 import tradier_client
    from scanner.filters import passes_all, calculate_atr_pct
    log.info(f"demo_scanner: checking {len(_DEMO_TICKERS)} hardcoded tickers via Tradier...")
    candidates = []

    def _process(ticker):
        daily = tradier_client.get_daily_data(ticker)
        if not daily:
            return None
        pm_vol, pm_high, pm_low = tradier_client.get_premarket_data(ticker)
        price      = daily['open']
        gap_pct    = daily['gap_pct']
        volume     = daily['volume']
        atr        = daily['atr']
        close      = daily['close']
        avg_volume = daily['avg_volume']
        atr_pct    = calculate_atr_pct(atr, close if close > 0 else price)
        passed, failed = passes_all(ticker, price, gap_pct, volume, pm_vol, atr, avg_volume)
        log.info(
            f"demo_scanner: {ticker} price={price} gap={gap_pct}% "
            f"vol={volume} pm_vol={pm_vol} atr_pct={atr_pct}% "
            f"-> {'PASS' if passed else 'FAIL ' + str(failed)}"
        )
        return {
            'ticker': ticker, 'type': 'stock',
            'price': round(price, 4), 'open': daily.get('open'),
            'high': daily.get('high'), 'low': daily.get('low'),
            'close': close, 'volume': volume, 'prev_close': daily.get('prev_close'),
            'gap_pct': round(gap_pct, 4), 'pm_volume': pm_vol,
            'pm_high': pm_high, 'pm_low': pm_low,
            'atr_20': atr, 'atr_pct': atr_pct,
            'sector': None, 'industry': None, 'orb_window': 5,
            'passed': passed, 'failed': failed,
        }

    with ThreadPoolExecutor(max_workers=len(_DEMO_TICKERS)) as pool:
        futures = {pool.submit(_process, t): t for t in _DEMO_TICKERS}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    candidates.append(result)
            except Exception as e:
                log.warning(f"demo_scanner: {futures[fut]} failed: {e}")

    passed_count = sum(1 for c in candidates if c['passed'])
    log.info(f"demo_scanner: {len(candidates)} stocks checked, {passed_count} passed all filters")
    return candidates


def run_demo():
    """Weekend/offline test: bypasses market-day guard and Finviz.
    Uses hardcoded tickers + Tradier data → PostgreSQL."""
    log.info("--- Demo mode starting (no market-day guard, no Finviz) ---")

    alpaca = AlpacaClient()

    vix = alpaca.get_vix()
    log.info(f"demo: VIX: {vix}")

    stocks = _demo_scan_stocks()
    etfs = scan_etfs()
    saved = merge_and_save(stocks, etfs, vix=vix)

    log.info(
        f"--- Demo complete ---\n"
        f"  stocks checked : {len(stocks)}\n"
        f"  ETFs checked   : {len(etfs)}\n"
        f"  saved to DB    : {len(saved)}\n"
        f"  VIX            : {vix}\n"
        f"  top candidates : {[c['ticker'] for c in saved[:5]]}"
    )


# ── Scheduler ────────────────────────────────────────────────────────────────

def start_scheduler():
    scheduler = BlockingScheduler(timezone=ET)

    # Layer 4 — dashboard (background) + report jobs
    start_dashboard()
    schedule_reports(scheduler)

    scheduler.add_job(
        run_premarket_snapshot,
        CronTrigger(day_of_week='mon-fri', hour=9, minute=25, timezone=ET),
        id='premarket_snapshot',
        name='Pre-market snapshot 9:25 AM ET',
    )
    scheduler.add_job(
        health_check,
        CronTrigger(day_of_week='mon-fri', hour=9, minute=28, timezone=ET),
        id='health_check',
        name='Health check 9:28 AM ET',
    )
    scheduler.add_job(
        run_layer1_main,
        CronTrigger(day_of_week='mon-fri', hour=9, minute=30, timezone=ET),
        id='layer1_main',
        name='Layer 1 main scan 9:30 AM ET',
    )
    scheduler.add_job(
        run_layer2,
        CronTrigger(day_of_week='mon-fri', hour=9, minute=33, timezone=ET),
        id='layer2_main',
        name='Layer 2 scoring 9:33 AM ET',
    )
    scheduler.add_job(
        start_layer3,
        CronTrigger(day_of_week='mon-fri', hour=9, minute=45, timezone=ET),
        id='layer3_main',
        name='Layer 3 live execution 9:45 AM ET',
    )

    now_et = datetime.now(ET).strftime('%Y-%m-%d %H:%M %Z')
    log.info(f"Scheduler started | current time: {now_et}")
    log.info("Jobs scheduled:")
    log.info("  8:00 AM ET -- morning email (yesterday + watchlist)")
    log.info("  9:25 AM ET -- pre-market snapshot")
    log.info("  9:28 AM ET -- health check")
    log.info("  9:30 AM ET -- main scan (+ 60s Finviz buffer)")
    log.info("  9:33 AM ET -- Layer 2 scoring (Haiku + 100pt model)")
    log.info("  9:45 AM ET -- Layer 3 live execution (ORB + 5-criteria + SMS gate)")
    log.info("  4:00 PM ET -- EOD SMS summary")
    log.info("  Sun 6PM ET -- weekly Google Sheets fill")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped")
        stop_layer3()
        close_pool()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_pool()

    if '--setup' in sys.argv:
        create_schema()
        create_schema_layer2()
        create_schema_layer3()
        print("Schema created. Run 'python run.py' to start scheduler.")

    elif '--layer2' in sys.argv:
        log.info("Layer 2 manual run triggered (--layer2)")
        run_layer2()
        close_pool()

    elif '--layer3' in sys.argv:
        log.info("Layer 3 manual run triggered (--layer3)")
        start_layer3()
        # Block main thread so daemon monitor thread stays alive
        try:
            while True:
                time.sleep(10)
        except (KeyboardInterrupt, SystemExit):
            stop_layer3()
            close_pool()

    elif '--demo' in sys.argv:
        log.info("Demo run triggered (--demo)")
        run_demo()
        close_pool()

    elif '--now' in sys.argv:
        log.info("Manual run triggered (--now)")
        run_layer1_main()
        close_pool()

    elif '--snapshot' in sys.argv:
        log.info("Manual snapshot triggered (--snapshot)")
        run_premarket_snapshot()
        close_pool()

    elif '--health' in sys.argv:
        ok = health_check()
        close_pool()
        sys.exit(0 if ok else 1)

    else:
        start_scheduler()
