"""
100-point scoring model.
Catalyst: 0-40 (Haiku)  Volume: 0-20  Gap: 0-20  ATR%: 0-10  SPY: 0-10
VIX 30-40 applies 0.5x multiplier to final score.
"""
from logger import setup_logger

log = setup_logger('layer1')


def _score_volume(pm_volume: int) -> int:
    if pm_volume >= 1_000_000: return 20
    if pm_volume >= 500_000:   return 17
    if pm_volume >= 250_000:   return 13
    if pm_volume >= 100_000:   return 8
    if pm_volume >= 50_000:    return 3
    return 0


def _score_gap(gap_pct: float) -> int:
    if gap_pct >= 12: return 20
    if gap_pct >= 10: return 19
    if gap_pct >= 8:  return 17
    if gap_pct >= 6:  return 14
    if gap_pct >= 4:  return 10
    return 0


def _score_gap_etf(gap_pct: float) -> int:
    """ETFs gap less by nature — scale scoring to their 1–5% filter range."""
    if gap_pct >= 4: return 20
    if gap_pct >= 3: return 15
    if gap_pct >= 2: return 10
    if gap_pct >= 1: return 5
    return 0


def _score_atr(atr_pct: float) -> int:
    if atr_pct >= 6: return 10
    if atr_pct >= 4: return 8
    if atr_pct >= 3: return 6
    if atr_pct >= 2: return 3
    return 0


def _score_spy(spy_change: float) -> int:
    if spy_change > 0:    return 10
    if spy_change >= -1:  return 3
    return 0


def score_candidate(candidate: dict, catalyst: dict, spy_change: float) -> dict:
    """Returns full scoring breakdown and TRADE/WATCH/SKIP decision."""
    vix = float(candidate.get('vix') or 0)

    is_etf     = candidate.get('type') == 'etf'
    s_catalyst = int(catalyst.get('score', 0))
    s_volume   = _score_volume(int(candidate.get('pm_volume') or 0))
    s_gap      = (_score_gap_etf if is_etf else _score_gap)(float(candidate.get('gap_pct') or 0))
    s_atr      = _score_atr(float(candidate.get('atr_pct') or 0))
    s_spy      = _score_spy(spy_change)

    # Hard rules -- checked before confidence cap
    skip_reason = None
    if catalyst.get('direction') == 'bearish':
        skip_reason = 'bearish direction'
    elif s_catalyst < 20:
        skip_reason = f'catalyst score {s_catalyst} < 20 minimum'

    # Confidence cap: low confidence -> cap catalyst contribution at 20 (the gate minimum).
    # Capping at 15 killed valid trades where the headline was vague but the catalyst was real.
    # Capping at 20 lets low-confidence candidates reach the gate but not inflate the score.
    if not skip_reason and catalyst.get('confidence') == 'low' and s_catalyst > 20:
        log.info(f"scoring: {candidate.get('symbol')} low confidence cap applied (was {s_catalyst})")
        s_catalyst = 20

    raw_score   = s_catalyst + s_volume + s_gap + s_atr + s_spy
    multiplier  = 0.5 if vix >= 30 else 1.0
    final_score = round(raw_score * multiplier, 2)

    if skip_reason:
        decision = 'SKIP'
    elif final_score >= 65:
        decision = 'TRADE'
    elif final_score >= 45:
        decision = 'WATCH'
    else:
        decision = 'SKIP'
        if not skip_reason:
            skip_reason = f'score {final_score} < 45'

    log.info(
        f"scoring: {candidate.get('symbol')} "
        f"C={s_catalyst} V={s_volume} G={s_gap} A={s_atr} S={s_spy} "
        f"raw={raw_score} x{multiplier}={final_score} -> {decision}"
        + (f" ({skip_reason})" if skip_reason else "")
    )

    return {
        'score_catalyst': s_catalyst,
        'score_volume':   s_volume,
        'score_gap':      s_gap,
        'score_atr':      s_atr,
        'score_spy':      s_spy,
        'score_raw':      raw_score,
        'score_final':    final_score,
        'vix_multiplier': multiplier,
        'decision':       decision,
        'skip_reason':    skip_reason,
    }
