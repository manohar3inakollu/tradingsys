import os
import requests
import concurrent.futures
from typing import Dict

from db.connection import db_connection

try:
    from scanner.alpaca_client import AlpacaClient
except Exception:
    AlpacaClient = None

try:
    from finvizfinance.screener.overview import Overview
except Exception:
    Overview = None

try:
    from twilio.rest import Client as TwilioClient
except Exception:
    TwilioClient = None

try:
    import gspread
except Exception:
    gspread = None


DEFAULT_TIMEOUT = 5


def check_postgres() -> str:
    try:
        with db_connection() as conn:
            conn.cursor().execute('SELECT 1')
        return 'ok'
    except Exception as e:
        return f'FAIL: {e}'


def check_alpaca() -> str:
    if AlpacaClient is None:
        return 'skipped (Alpaca client unavailable)'
    try:
        bar = AlpacaClient().get_latest_bar('SPY')
        price = bar.get('c', 0) if isinstance(
            bar, dict) else getattr(bar, 'c', 0)
        return f'ok (SPY={price})'
    except Exception as e:
        return f'FAIL: {e}'


def check_finviz() -> str:
    # Only verifies the package is importable and instantiable — does not make a network call.
    if Overview is None:
        return 'skipped (finviz package missing)'
    try:
        Overview()
        return 'ok (package available)'
    except Exception as e:
        return f'FAIL: {e}'


def check_finnhub() -> str:
    key = os.getenv('FINNHUB_API_KEY')
    if not key:
        return 'skipped (no FINNHUB_API_KEY)'
    try:
        url = f'https://finnhub.io/api/v1/quote?symbol=SPY&token={key}'
        r = requests.get(url, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            return 'ok'
        return f'FAIL: status {r.status_code}'
    except Exception as e:
        return f'FAIL: {e}'


def check_tradier() -> str:
    token = os.getenv('TRADIER_TOKEN')
    env = os.getenv('TRADIER_ENV', 'live')
    if not token:
        return 'skipped (no TRADIER_TOKEN)'
    try:
        base = 'https://sandbox.tradier.com' if env == 'sandbox' else 'https://api.tradier.com'
        url = f"{base}/v1/markets/quotes?symbols=SPY"
        headers = {'Authorization': f'Bearer {token}',
                   'Accept': 'application/json'}
        r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            return 'ok'
        return f'FAIL: status {r.status_code} body={r.text[:200]}'
    except Exception as e:
        return f'FAIL: {e}'


def check_twilio() -> str:
    sid = os.getenv('TWILIO_ACCOUNT_SID')
    token = os.getenv('TWILIO_AUTH_TOKEN')
    if not sid or not token:
        return 'skipped (no Twilio creds)'
    if TwilioClient is None:
        return 'skipped (twilio package missing)'
    try:
        client = TwilioClient(sid, token)
        acct = client.api.accounts(sid).fetch()
        return f"ok (sid={acct.sid})"
    except Exception as e:
        return f'FAIL: {e}'


def check_anthropic() -> str:
    key = os.getenv('ANTHROPIC_API_KEY')
    if not key:
        return 'skipped (no ANTHROPIC_API_KEY)'
    try:
        # Anthropic requires an API version header. Allow override via env var.
        version = os.getenv('ANTHROPIC_API_VERSION', '2023-06-01')
        headers = {'x-api-key': key, 'anthropic-version': version}
        url = 'https://api.anthropic.com/v1/models'
        r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
        if r.status_code == 200:
            return 'ok'
        # include server error body snippet for diagnostics
        body = r.text[:400] if r.text else ''
        return f'FAIL: status {r.status_code} body={body}'
    except Exception as e:
        return f'FAIL: {repr(e)}'


def check_google_sheets() -> str:
    sa = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
    sid = os.getenv('SHEETS_SPREADSHEET_ID')
    if not sa or not sid:
        return 'skipped (no Sheets config)'
    if gspread is None:
        return 'skipped (gspread missing)'
    try:
        # service account JSON path expected in env var
        if not os.path.exists(sa):
            return f'FAIL: service account file not found: {sa}'
        gc = gspread.service_account(filename=sa)
        sh = gc.open_by_key(sid)
        # small sanity: list worksheets
        _ = sh.worksheets()
        return 'ok'
    except Exception as e:
        return f'FAIL: {repr(e)}'


def check_all() -> Dict[str, str]:
    """Run all health checks and return a mapping of service->status.

    - Required checks (postgres, alpaca, finviz) run first sequentially.
    - Optional network checks run concurrently to keep total time under one timeout window.
    - Required or core services return 'ok...' or 'FAIL: ...'
    - Optional integrations return 'skipped (...)' when not configured.
    """
    results = {}
    results['postgres'] = check_postgres()
    results['alpaca']   = check_alpaca()
    results['finviz']   = check_finviz()

    optional = {
        'finnhub':   check_finnhub,
        'tradier':   check_tradier,
        'twilio':    check_twilio,
        'anthropic': check_anthropic,
        'sheets':    check_google_sheets,
    }
    with concurrent.futures.ThreadPoolExecutor() as ex:
        futures = {name: ex.submit(fn) for name, fn in optional.items()}
        for name, fut in futures.items():
            results[name] = fut.result()

    return results
