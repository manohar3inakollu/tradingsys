"""
Claude Haiku catalyst scorer.
Sentiment shortcut: negative -> score=0, skip Haiku.
Low confidence cap: catalyst score capped at 15 in scoring.py.
"""
import json
import anthropic
from logger import setup_logger
from layer2.news_client import FAILED_LOOKUP_PHRASES

log = setup_logger('layer1')

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = anthropic.Anthropic()
    return _client


_PROMPT = """You are a pre-market gap-up trading analyst. Evaluate this news catalyst for a stock gapping up today.

Stock: {ticker}
Top Headline: {headline}
All Headlines Today (pipe-separated): {summary}
Finnhub Sentiment: {sentiment}

Score based on the STRONGEST catalyst across all headlines above.
Rate this as a catalyst for a long ORB (opening range breakout) trade.
Return ONLY a JSON object with no other text:
{{
  "score": <integer 0-40>,
  "direction": "<bullish|bearish|neutral>",
  "confidence": "<high|medium|low>",
  "type": "<earnings|fda|merger|guidance|analyst|macro|other>",
  "reasoning": "<one sentence>"
}}

Scoring:
35-40: Strong specific catalyst (earnings beat, FDA approval, major deal)
25-34: Good catalyst with clear upside driver
20-24: Moderate catalyst
10-19: Weak or vague
0-9:   No meaningful catalyst or negative news"""


def score_catalyst(ticker: str, news: dict) -> dict:
    """Returns score(0-40), direction, confidence, type, reasoning."""

    if news.get('sentiment') == 'negative':
        log.info(f"haiku_scorer: {ticker} negative sentiment shortcut -> score=0")
        return {'score': 0, 'direction': 'bearish', 'confidence': 'high',
                'type': 'other', 'reasoning': 'Negative sentiment shortcut',
                'skipped': True}

    if not news.get('headline'):
        log.info(f"haiku_scorer: {ticker} no headline -> score=5")
        return {'score': 5, 'direction': 'neutral', 'confidence': 'low',
                'type': 'other', 'reasoning': 'No news headline available',
                'skipped': False}

    # Failed news lookup guard — news client returned a failure/apology string
    # instead of a real headline. Sending this to Haiku produces unreliable scores
    # (APLD scored 22 on "Unable to identify specific news catalyst").
    if any(p in news['headline'].lower() for p in FAILED_LOOKUP_PHRASES):
        log.info(f"haiku_scorer: {ticker} failed-news headline detected -> score=5")
        return {'score': 5, 'direction': 'neutral', 'confidence': 'low',
                'type': 'other', 'reasoning': 'News lookup returned no real catalyst',
                'skipped': False}

    prompt = _PROMPT.format(
        ticker    = ticker,
        headline  = news.get('headline', ''),
        summary   = news.get('summary', ''),
        sentiment = news.get('sentiment', 'neutral'),
    )

    try:
        msg = _get_client().messages.create(
            model       = 'claude-haiku-4-5-20251001',
            max_tokens  = 256,
            temperature = 0,
            messages    = [{'role': 'user', 'content': prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        result = json.loads(raw.strip())

        result['score'] = max(0, min(40, int(result.get('score', 0))))
        result.setdefault('direction',  'neutral')
        result.setdefault('confidence', 'low')
        result.setdefault('type',       'other')
        result.setdefault('reasoning',  '')
        result['skipped'] = False

        log.info(
            f"haiku_scorer: {ticker} score={result['score']} "
            f"dir={result['direction']} conf={result['confidence']} "
            f"type={result['type']}"
        )
        return result

    except Exception as e:
        log.warning(f"haiku_scorer: {ticker} error: {e} -> score=5")
        return {'score': 5, 'direction': 'neutral', 'confidence': 'low',
                'type': 'other', 'reasoning': f'Haiku error: {e}',
                'skipped': False}
