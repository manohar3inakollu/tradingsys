from psycopg.rows import dict_row
from logger import setup_logger

log = setup_logger('layer1')


def upsert_ai_score(conn, candidate: dict, news: dict,
                    catalyst: dict, scores: dict, plan: dict) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            INSERT INTO ai_scores (
                scan_date, ticker_id, price_id, scan_result_id,
                headline, news_source, sentiment,
                catalyst_score, catalyst_direction, catalyst_confidence,
                catalyst_type, catalyst_reasoning, catalyst_skipped,
                score_catalyst, score_volume, score_gap, score_atr, score_spy,
                score_raw, score_final, vix_multiplier,
                decision, skip_reason,
                entry_price, stop_price, t1_price, t2_price,
                shares, risk_amount, vix
            ) VALUES (
                CURRENT_DATE, %(ticker_id)s, %(price_id)s, %(scan_result_id)s,
                %(headline)s, %(news_source)s, %(sentiment)s,
                %(catalyst_score)s, %(catalyst_direction)s, %(catalyst_confidence)s,
                %(catalyst_type)s, %(catalyst_reasoning)s, %(catalyst_skipped)s,
                %(score_catalyst)s, %(score_volume)s, %(score_gap)s,
                %(score_atr)s, %(score_spy)s,
                %(score_raw)s, %(score_final)s, %(vix_multiplier)s,
                %(decision)s, %(skip_reason)s,
                %(entry_price)s, %(stop_price)s, %(t1_price)s, %(t2_price)s,
                %(shares)s, %(risk_amount)s, %(vix)s
            )
            ON CONFLICT (scan_date, ticker_id) DO UPDATE SET
                headline            = EXCLUDED.headline,
                news_source         = EXCLUDED.news_source,
                sentiment           = EXCLUDED.sentiment,
                catalyst_score      = EXCLUDED.catalyst_score,
                catalyst_direction  = EXCLUDED.catalyst_direction,
                catalyst_confidence = EXCLUDED.catalyst_confidence,
                catalyst_type       = EXCLUDED.catalyst_type,
                catalyst_reasoning  = EXCLUDED.catalyst_reasoning,
                catalyst_skipped    = EXCLUDED.catalyst_skipped,
                score_catalyst      = EXCLUDED.score_catalyst,
                score_volume        = EXCLUDED.score_volume,
                score_gap           = EXCLUDED.score_gap,
                score_atr           = EXCLUDED.score_atr,
                score_spy           = EXCLUDED.score_spy,
                score_raw           = EXCLUDED.score_raw,
                score_final         = EXCLUDED.score_final,
                vix_multiplier      = EXCLUDED.vix_multiplier,
                decision            = EXCLUDED.decision,
                skip_reason         = EXCLUDED.skip_reason,
                entry_price         = EXCLUDED.entry_price,
                stop_price          = EXCLUDED.stop_price,
                t1_price            = EXCLUDED.t1_price,
                t2_price            = EXCLUDED.t2_price,
                shares              = EXCLUDED.shares,
                risk_amount         = EXCLUDED.risk_amount,
                vix                 = EXCLUDED.vix,
                created_at          = NOW()
            RETURNING id
        """, {
            'ticker_id':          candidate.get('ticker_id'),
            'price_id':           candidate.get('price_id'),
            'scan_result_id':     candidate.get('scan_result_id'),
            'headline':           news.get('headline'),
            'news_source':        news.get('source'),
            'sentiment':          news.get('sentiment'),
            'catalyst_score':     catalyst.get('score'),
            'catalyst_direction': catalyst.get('direction'),
            'catalyst_confidence':catalyst.get('confidence'),
            'catalyst_type':      catalyst.get('type'),
            'catalyst_reasoning': catalyst.get('reasoning'),
            'catalyst_skipped':   catalyst.get('skipped', False),
            'score_catalyst':     scores.get('score_catalyst'),
            'score_volume':       scores.get('score_volume'),
            'score_gap':          scores.get('score_gap'),
            'score_atr':          scores.get('score_atr'),
            'score_spy':          scores.get('score_spy'),
            'score_raw':          scores.get('score_raw'),
            'score_final':        scores.get('score_final'),
            'vix_multiplier':     scores.get('vix_multiplier'),
            'decision':           scores.get('decision'),
            'skip_reason':        scores.get('skip_reason'),
            'entry_price':        plan.get('entry_price'),
            'stop_price':         plan.get('stop_price'),
            't1_price':           plan.get('t1_price'),
            't2_price':           plan.get('t2_price'),
            'shares':             plan.get('shares'),
            'risk_amount':        plan.get('risk_amount'),
            'vix':                candidate.get('vix'),
        })
        row = cur.fetchone()
        return row['id'] if row else None
