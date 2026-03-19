"""
News fetching: Alpaca Benzinga + Finnhub fetched in parallel, merged and ranked
by catalyst tier so the strongest article wins regardless of publication order.

Fallback chain:
  1. Alpaca Benzinga  (parallel with Finnhub)
  2. Finnhub          (parallel with Alpaca)
  3. Claude web search (only if both above return nothing)

Article ranking tiers (higher = stronger catalyst):
  Tier 3 (30): FDA approval/rejection, Phase 2/3, clinical trial result,
               earnings beat/miss, merger/acquisition, deal, partnership
  Tier 2 (20): Analyst upgrade/downgrade, price target change, initiation
  Tier 1 (10): Guidance, conference, general company news
  Tier 0 ( 0): Fallback — unclassified
"""
import os
import json as _json
import anthropic
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv
from timing import ET
from logger import setup_logger
from scanner.retry import retry

load_dotenv()
log = setup_logger('layer1')

_ALPACA_NEWS = 'https://data.alpaca.markets/v1beta1/news'
_FINNHUB     = 'https://finnhub.io/api/v1'

# ── Apology phrases shared with haiku_scorer.py ──────────────────────────────
FAILED_LOOKUP_PHRASES = (
    'unable to', 'could not', "couldn't",
    'cannot find', "can't find",
    'no specific', 'no news', 'failed to find',
    'i was unable', 'i could not', 'not find',
)

# ── ETF focus keywords — boost articles specific to each ETF's underlying theme ─
# When a broad macro article ties with a theme-specific article, the focused
# one wins. Boost = +15 tier points (enough to beat Tier 10 generic news).
_ETF_FOCUS_BOOST: int = 15

_ETF_FOCUS: dict[str, tuple[str, ...]] = {
    # Nasdaq / mega-cap tech
    'QQQ':  ('nasdaq', 'big tech', 'apple', 'microsoft', 'meta', 'amazon',
             'alphabet', 'nvidia', 'tesla', 'technology sector'),
    # Technology sector SPDR
    'XLK':  ('technology', 'semiconductor', 'software', 'artificial intelligence',
             ' ai ', 'chip', 'apple', 'microsoft', 'nvidia', 'broadcom',
             'amd', 'qualcomm', 'intel', 'salesforce'),
    # Russell 2000 small-cap
    'IWM':  ('small cap', 'small-cap', 'russell 2000', 'interest rate',
             'federal reserve', 'rate cut', 'rate hike', 'consumer spending',
             'regional bank', 'domestic economy'),
    # S&P 500
    'SPY':  ('s&p 500', 'sp500', 'federal reserve', 'inflation', 'jobs report',
             'gdp', 'cpi', 'pce', 'economic data'),
    # Financials
    'XLF':  ('bank', 'financial sector', 'federal reserve', 'interest rate',
             'jpmorgan', 'goldman sachs', 'morgan stanley', 'wells fargo',
             'bank of america', 'insurance'),
    # Energy
    'XLE':  ('oil', 'crude', 'energy sector', 'opec', 'natural gas',
             'exxon', 'chevron', 'refiner', 'pipeline'),
    # Health care
    'XLV':  ('health care', 'biotech', 'pharmaceutical', 'fda', 'drug approval',
             'clinical trial', 'johnson', 'unitedhealth', 'pfizer', 'abbvie'),
    # Gold
    'GLD':  ('gold', 'precious metal', 'inflation hedge', 'dollar index',
             'safe haven', 'bullion'),
    # Long treasuries
    'TLT':  ('treasury', 'bond yield', 'federal reserve', 'rate cut', 'rate hike',
             'duration', 'fixed income', '10-year', '30-year'),
    # Japan
    'EWJ':  ('japan', 'yen', 'bank of japan', 'nikkei', 'boj', 'japanese'),
    # China
    'FXI':  ('china', 'chinese', 'beijing', 'pboc', 'csi 300', 'alibaba',
             'tencent', 'baidu', 'hang seng'),
    # Semiconductors
    'SOXX': ('semiconductor', 'chip', 'nvidia', 'amd', 'intel', 'tsmc',
             'qualcomm', 'broadcom', 'micron', 'wafer'),
}

# ── Catalyst tier keywords ────────────────────────────────────────────────────
_TIER3_KEYWORDS = (
    'fda', 'phase 2', 'phase 3', 'phase ii', 'phase iii',
    'clinical trial', 'approval', 'approved', 'nda', 'bla', 'pdufa',
    'earnings', 'beat', 'miss', 'revenue', 'eps',
    'merger', 'acquisition', 'acquir', 'deal', 'buyout', 'takeover',
    'partnership', 'license', 'collaboration', 'agreement',
)
_TIER2_KEYWORDS = (
    'upgrade', 'downgrade', 'price target', 'initiates', 'initiated',
    'overweight', 'outperform', 'buy rating', 'sell rating',
    'raises target', 'lowers target', 'cuts target',
)


def _tier(headline: str) -> int:
    h = headline.lower()
    if any(k in h for k in _TIER3_KEYWORDS):
        return 30
    if any(k in h for k in _TIER2_KEYWORDS):
        return 20
    return 10


def _alpaca_headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.getenv('ALPACA_API_KEY', ''),
        'APCA-API-SECRET-KEY': os.getenv('ALPACA_SECRET_KEY', ''),
    }


@retry(max_attempts=2, delay=2)
def _fetch_alpaca_news(ticker: str) -> list[dict]:
    today = datetime.now(ET).strftime('%Y-%m-%d')
    r = requests.get(
        _ALPACA_NEWS,
        headers=_alpaca_headers(),
        params={'symbols': ticker, 'limit': 10, 'sort': 'desc',
                'start': f'{today}T00:00:00Z'},
        timeout=10,
    )
    r.raise_for_status()
    articles = r.json().get('news', [])
    return [
        {'headline': a.get('headline', ''), 'src': 'alpaca_benzinga'}
        for a in articles if a.get('headline')
    ]


@retry(max_attempts=2, delay=2)
def _fetch_finnhub_news(ticker: str) -> list[dict]:
    token = os.getenv('FINNHUB_API_KEY', '')
    if not token:
        return []
    today = datetime.now(ET).strftime('%Y-%m-%d')
    r = requests.get(
        f'{_FINNHUB}/company-news',
        params={'symbol': ticker, 'from': today, 'to': today, 'token': token},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    articles = data if isinstance(data, list) else []
    return [
        {'headline': a.get('headline', ''), 'src': 'finnhub'}
        for a in articles if a.get('headline')
    ]


@retry(max_attempts=2, delay=2)
def _fetch_finnhub_sentiment(ticker: str) -> str:
    token = os.getenv('FINNHUB_API_KEY', '')
    if not token:
        return 'neutral'
    r = requests.get(
        f'{_FINNHUB}/news-sentiment',
        params={'symbol': ticker, 'token': token},
        timeout=10,
    )
    if r.status_code != 200:
        return 'neutral'
    score = r.json().get('companyNewsScore', 0.5)
    if score >= 0.6:
        return 'positive'
    if score <= 0.4:
        return 'negative'
    return 'neutral'


def _merge_and_rank(
    alpaca: list[dict],
    finnhub: list[dict],
    focus_keywords: tuple[str, ...] = (),
) -> list[dict]:
    """
    Merge articles from both sources, deduplicate by 40-char headline prefix,
    and sort by catalyst tier (highest first).

    If focus_keywords are provided (ETF-specific themes), articles matching
    those keywords receive a +_ETF_FOCUS_BOOST bonus so theme-relevant
    headlines beat generic macro articles of the same base tier.
    """
    seen: set[str] = set()
    merged: list[dict] = []
    for article in alpaca + finnhub:
        h = article['headline'].strip()
        if not h:
            continue
        key = h[:40].lower()
        if key in seen:
            continue
        seen.add(key)
        h_lower = h.lower()
        tier    = _tier(h)
        if focus_keywords and any(k in h_lower for k in focus_keywords):
            tier += _ETF_FOCUS_BOOST
        merged.append({'headline': h, 'src': article['src'], 'tier': tier})

    merged.sort(key=lambda a: a['tier'], reverse=True)
    return merged


def _fetch_web_news(ticker: str, company_name: str = '') -> tuple[str, str, str]:
    """
    Last-resort fallback: Claude Haiku with web search.
    Searches by ticker AND company name so mid-cap stocks with sparse
    ticker-symbol coverage (e.g. APLD / Applied Digital) are still found.
    Returns (headline, summary, source) or ('', '', 'none') on failure.
    """
    today = datetime.now(ET).strftime('%B %d, %Y')
    # Build search subject: "APLD (Applied Digital)" beats bare "APLD" for
    # companies whose press releases mention the full name but not the ticker.
    subject = f'{ticker} ({company_name})' if company_name else ticker
    client = anthropic.Anthropic()

    # Two-turn approach:
    # Turn 1: force a web search by setting tool_choice={"type": "any"}.
    #         This guarantees the web_search tool is actually called rather
    #         than Haiku responding from training data with an apology.
    # Turn 2: ask Haiku to summarise the search results as JSON.
    search_query = (
        f'Search for news about {subject} stock on {today}. '
        + (f'Try both "{ticker}" and "{company_name}". ' if company_name else '')
        + 'Find any press releases, FDA decisions, earnings, clinical trial results, '
        + 'M&A announcements, or analyst upgrades from today.'
    )
    turn1 = client.messages.create(
        model       = 'claude-haiku-4-5-20251001',
        max_tokens  = 1000,
        tools       = [{'type': 'web_search_20250305', 'name': 'web_search', 'max_uses': 3}],
        tool_choice = {'type': 'any'},   # force the search tool to be called
        messages    = [{'role': 'user', 'content': search_query}],
    )

    # Check whether the tool was actually used
    tool_used = any(getattr(b, 'type', '') == 'tool_use' for b in turn1.content)
    log.info(
        'news_client: web_search %s — turn1 stop_reason=%s tool_used=%s',
        ticker, turn1.stop_reason, tool_used,
    )

    # Turn 2: take the full turn1 exchange and ask for JSON summary
    turn1_messages = [
        {'role': 'user',      'content': search_query},
        {'role': 'assistant', 'content': turn1.content},
    ]
    # Append tool results so the model can read them
    tool_results = []
    for b in turn1.content:
        if getattr(b, 'type', '') == 'tool_result':
            tool_results.append(b)
    if tool_results:
        turn1_messages.append({'role': 'user', 'content': tool_results})

    msg = client.messages.create(
        model      = 'claude-haiku-4-5-20251001',
        max_tokens = 400,
        messages   = turn1_messages + [{'role': 'user', 'content': (
            f'Based on the search results above, return ONLY a JSON object — no other text:\n'
            f'{{"headline": "<one factual sentence describing the specific catalyst for {ticker}>", '
            f'"summary": "<2-3 sentences with key details>"}}\n'
            f'If no specific catalyst was found today, return: '
            f'{{"headline": "", "summary": ""}}'
        )}],
    )

    # Take the last text block
    text_blocks = [
        b.text.strip() for b in msg.content
        if getattr(b, 'type', '') == 'text' and b.text.strip()
    ]
    log.info(
        'news_client: web_search %s — turn2 stop_reason=%s text_blocks=%d',
        ticker, msg.stop_reason, len(text_blocks),
    )
    if not text_blocks:
        log.warning('news_client: web_search %s — no text blocks in response', ticker)
        return '', '', 'none'

    raw = text_blocks[-1]
    log.debug('news_client: web_search %s raw=%r', ticker, raw[:200])
    if '```' in raw:
        raw = raw.split('```')[1]
        if raw.startswith('json'):
            raw = raw[4:]

    try:
        data     = _json.loads(raw.strip())
        headline = str(data.get('headline', '')).strip()[:300]
        summary  = str(data.get('summary',  '')).strip()[:500]
        if not headline:
            log.warning('news_client: web_search %s — empty headline in JSON', ticker)
            return '', '', 'none'
        if any(p in headline.lower() for p in FAILED_LOOKUP_PHRASES):
            log.warning(
                'news_client: web_search %s — apology phrase detected in headline: %r',
                ticker, headline[:80],
            )
            return '', '', 'none'
        return headline, summary, 'web_search'
    except _json.JSONDecodeError as e:
        log.warning(
            'news_client: web_search %s — JSON parse failed: %s | raw=%r',
            ticker, e, raw[:200],
        )
        return '', '', 'none'


def fetch_news(ticker: str, company_name: str = '') -> dict:
    """
    Returns {headline, summary, source, sentiment}.

    Alpaca + Finnhub news are fetched in parallel. Articles are merged,
    deduplicated, and ranked by catalyst tier — the highest-tier article
    becomes the headline regardless of publication order. If both APIs return
    nothing, falls back to Claude web search using both ticker and company name
    so mid-cap stocks with sparse ticker coverage are still found.
    Finnhub sentiment is always fetched in parallel and included.
    """
    alpaca_articles:   list[dict] = []
    finnhub_articles:  list[dict] = []
    sentiment:         str        = 'neutral'
    fetch_errors:      list[str]  = []

    # Fetch Alpaca news, Finnhub news, and Finnhub sentiment in parallel
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(_fetch_alpaca_news,      ticker): 'alpaca',
            pool.submit(_fetch_finnhub_news,     ticker): 'finnhub',
            pool.submit(_fetch_finnhub_sentiment,ticker): 'sentiment',
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                result = future.result()
                if label == 'alpaca':
                    alpaca_articles = result
                elif label == 'finnhub':
                    finnhub_articles = result
                else:
                    sentiment = result
            except Exception as e:
                fetch_errors.append(f'{label}: {e}')
                log.warning(f'news_client: {ticker} {label} fetch failed: {e}')

    focus_keywords = _ETF_FOCUS.get(ticker.upper(), ())
    ranked = _merge_and_rank(alpaca_articles, finnhub_articles, focus_keywords)

    headline = ''
    summary  = ''
    source   = 'none'

    _web_search_needed = not ranked or ranked[0]['tier'] < 20

    if ranked:
        best     = ranked[0]
        headline = best['headline']
        source   = best['src']
        # Summary = pipe-separated top 5 ranked headlines (strongest catalyst first)
        summary  = ' | '.join(a['headline'] for a in ranked[:5])[:500]

        log.info(
            f'news_client: {ticker} merged {len(ranked)} articles '
            f'(alpaca={len(alpaca_articles)} finnhub={len(finnhub_articles)}) '
            f'best_tier={best["tier"]} source={source}'
            + (f' [etf_focus={len(focus_keywords)}kw]' if focus_keywords else '')
            + (' [weak — will try web search]' if _web_search_needed else '')
        )

    if _web_search_needed:
        log.info(
            f'news_client: {ticker} best_tier<20 — '
            f'trying web search (company={repr(company_name) if company_name else "none"})'
        )
        try:
            ws_headline, ws_summary, ws_source = _fetch_web_news(ticker, company_name)
            # Only use web result if it's a real headline (not an apology)
            if ws_headline:
                headline = ws_headline
                summary  = ws_summary
                source   = ws_source
        except Exception as e:
            log.warning(f'news_client: {ticker} web search fallback failed: {e}')

    log.info(
        f'news_client: {ticker} source={source} sentiment={sentiment} '
        f'headline={repr(headline[:60])}'
    )
    return {'headline': headline, 'summary': summary,
            'source': source, 'sentiment': sentiment}
