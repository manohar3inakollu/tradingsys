"""
Layer 2 main runner.
Reads today's Layer 1 candidates, scores with Haiku, writes ai_scores to DB.
Scheduled at 9:33 AM ET, must complete before 9:45 AM ET.
"""
from datetime import datetime
from timing import ET
from logger import setup_logger
from db.connection import db_connection
from db.queries import get_todays_candidates_full
from db.queries_layer2 import upsert_ai_score
from scanner.alpaca_client import AlpacaClient
from layer2.session_gates import check_session_gates
from layer2.news_client import fetch_news
from layer2.haiku_scorer import score_catalyst
from layer2.scoring import score_candidate

log = setup_logger('layer1')


def run_layer2():
    now_et = datetime.now(ET).strftime('%H:%M ET')
    log.info(f"[{now_et}] --- Layer 2 starting ---")

    alpaca = AlpacaClient()
    vix    = alpaca.get_vix()

    go, reason, risk_per_trade, spy_change = check_session_gates(vix)
    if not go:
        log.warning(f"[{now_et}] Layer 2 gate FAILED: {reason} -- no trades today")
        return

    with db_connection() as conn:
        candidates = get_todays_candidates_full(conn)

    if not candidates:
        log.info(f"[{now_et}] Layer 2: no Layer 1 candidates found")
        return

    log.info(
        f"[{now_et}] Layer 2: {len(candidates)} candidates | "
        f"VIX={vix} SPY={spy_change}% risk=${risk_per_trade}"
    )

    results = []
    for c in candidates:
        ticker = c['symbol']
        log.info(f"[{now_et}] scoring {ticker} (rank={c.get('rank')})...")

        news     = fetch_news(ticker, company_name=c.get('company', ''))
        catalyst = score_catalyst(ticker, news)
        scores   = score_candidate(c, catalyst, spy_change)

        with db_connection() as conn:
            upsert_ai_score(conn, c, news, catalyst, scores, {})

        results.append({'ticker': ticker, 'decision': scores['decision'],
                        'score': scores['score_final']})

    trades  = [r for r in results if r['decision'] == 'TRADE']
    watches = [r for r in results if r['decision'] == 'WATCH']
    skips   = [r for r in results if r['decision'] == 'SKIP']
    now_et  = datetime.now(ET).strftime('%H:%M ET')

    log.info(
        f"[{now_et}] --- Layer 2 complete ---\n"
        f"  scored  : {len(results)}\n"
        f"  TRADE   : {[r['ticker'] for r in trades]}\n"
        f"  WATCH   : {[r['ticker'] for r in watches]}\n"
        f"  SKIP    : {len(skips)}\n"
        f"  VIX={vix} risk=${risk_per_trade}"
    )
