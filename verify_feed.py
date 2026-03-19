"""
Verify Alpaca data feed for a given stock ticker.

Usage:
  python verify_feed.py AAPL
  python verify_feed.py USO --date 2026-03-16
  python verify_feed.py TSLA --window 15 --timeframe 5Min
  python verify_feed.py NVDA --snapshot
  python verify_feed.py NVDA --orb

Options:
  --date      YYYY-MM-DD  (default: today)
  --window    ORB window in minutes (default: 5 for stocks, 15 for known ETFs)
  --timeframe bar size for detailed view: 1Min, 5Min, 15Min (default: 1Min)
  --snapshot  show live quote / snapshot instead of bars
  --orb       show ORB range only (high/low of opening window)
"""

import argparse
import os
import sys
from datetime import datetime, date

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_ROOT, '.env'))

import requests

API_KEY    = os.getenv('ALPACA_API_KEY', '')
API_SECRET = os.getenv('ALPACA_SECRET_KEY', '')
BASE_URL   = 'https://data.alpaca.markets/v2'

HEADERS = {
    'APCA-API-KEY-ID':     API_KEY,
    'APCA-API-SECRET-KEY': API_SECRET,
}

# Known ETF tickers — get 15-min ORB by default
_ETF_SET = {'SPY','QQQ','IWM','DIA','GLD','SLV','USO','UNG','XLF','XLE','XLK',
            'XLV','XLI','ARKK','TQQQ','SQQQ','SPXU','UPRO'}

def _sec(t):  print(f'\n── {t} {"─"*max(0,48-len(t))}')
def _ok(m):   print(f'  ✓ {m}')
def _info(m): print(f'  · {m}')
def _warn(m): print(f'  ⚠ {m}')
def _fail(m): print(f'  ✗ {m}')


def _get_bars(symbol: str, feed: str, timeframe: str, start: str, end: str, limit: int = 50) -> list:
    r = requests.get(
        f'{BASE_URL}/stocks/{symbol}/bars',
        headers=HEADERS,
        params={'feed': feed, 'timeframe': timeframe, 'start': start, 'end': end, 'limit': limit},
        timeout=8,
    )
    if not r.ok:
        _warn(f'{feed.upper()} bars HTTP {r.status_code}: {r.text[:120]}')
        return []
    return r.json().get('bars', [])


def _print_bars(bars: list) -> None:
    if not bars:
        _warn('No bars returned')
        return
    for b in bars:
        print(f"    {b['t']}  O={b['o']:.2f}  H={b['h']:.2f}  L={b['l']:.2f}  C={b['c']:.2f}  V={b['v']:,}")


def _orb_summary(label: str, bars: list, window: int) -> None:
    if not bars:
        return
    orb_high = max(b['h'] for b in bars)
    orb_low  = min(b['l'] for b in bars)
    vol      = sum(b['v'] for b in bars)
    print(f'  {label:<6} {window}min ORB │ high={orb_high:.4f}  low={orb_low:.4f}  vol={vol:,}')


def cmd_bars(symbol: str, trade_date: str, timeframe: str, window: int) -> None:
    """Show minute bars for the ORB window on IEX and SIP."""
    # Build start/end with correct ET offset
    # Detect DST: EDT=-04:00 Mar–Nov, EST=-05:00 Nov–Mar
    dt = datetime.strptime(trade_date, '%Y-%m-%d')
    offset = '-04:00' if 3 <= dt.month <= 11 else '-05:00'
    # End at HH:MM:59 — same logic as orb.py — so the next candle (9:35 / 9:45) is excluded
    end_min = 9 * 60 + 30 + window - 1   # last minute of the window
    end_h, end_m = divmod(end_min, 60)
    start = f'{trade_date}T09:30:00{offset}'
    end   = f'{trade_date}T{end_h:02d}:{end_m:02d}:59{offset}'

    _sec(f'IEX bars  {symbol}  {trade_date}  {timeframe}  ({window}min window 9:30–{end_h}:{end_m:02d} ET)')
    iex_bars = _get_bars(symbol, 'iex', timeframe, start, end)
    _print_bars(iex_bars)

    _sec(f'SIP bars  {symbol}  {trade_date}  {timeframe}  ({window}min window 9:30–{end_h}:{end_m:02d} ET)')
    sip_bars = _get_bars(symbol, 'sip', timeframe, start, end)
    _print_bars(sip_bars)

    _sec('ORB Comparison')
    _orb_summary('IEX', iex_bars, window)
    _orb_summary('SIP', sip_bars, window)

    if iex_bars and sip_bars:
        iex_h = max(b['h'] for b in iex_bars)
        iex_l = min(b['l'] for b in iex_bars)
        sip_h = max(b['h'] for b in sip_bars)
        sip_l = min(b['l'] for b in sip_bars)
        if abs(iex_h - sip_h) < 0.05 and abs(iex_l - sip_l) < 0.05:
            _ok('IEX and SIP feeds agree')
        else:
            _warn(f'Feed divergence: high diff={abs(iex_h-sip_h):.4f}  low diff={abs(iex_l-sip_l):.4f}')


def cmd_orb(symbol: str, trade_date: str, window: int) -> None:
    """Show ORB range only."""
    dt = datetime.strptime(trade_date, '%Y-%m-%d')
    offset = '-04:00' if 3 <= dt.month <= 11 else '-05:00'
    end_min = 9 * 60 + 30 + window - 1
    end_h, end_m = divmod(end_min, 60)
    start = f'{trade_date}T09:30:00{offset}'
    end   = f'{trade_date}T{end_h:02d}:{end_m:02d}:59{offset}'

    _sec(f'ORB  {symbol}  {window}min  (9:30–{end_h}:{end_m:02d} ET  {trade_date})')
    for feed in ('iex', 'sip'):
        bars = _get_bars(symbol, feed, f'{window}Min', start, end, limit=1)
        if bars:
            b = bars[0]
            print(f'  {feed.upper():<4}  high={b["h"]:.4f}  low={b["l"]:.4f}  '
                  f'open={b["o"]:.4f}  close={b["c"]:.4f}  vol={b["v"]:,}')
        else:
            _warn(f'{feed.upper()} — no data')


def cmd_snapshot(symbol: str) -> None:
    """Show live quote + snapshot."""
    _sec(f'Snapshot  {symbol}')
    r = requests.get(
        f'{BASE_URL}/stocks/{symbol}/snapshot',
        headers=HEADERS,
        params={'feed': 'iex'},
        timeout=8,
    )
    if not r.ok:
        _fail(f'HTTP {r.status_code}: {r.text[:200]}')
        return

    d = r.json()
    lt  = d.get('latestTrade')  or {}
    lq  = d.get('latestQuote') or {}
    db  = d.get('dailyBar')    or {}
    pb  = d.get('prevDailyBar') or {}
    mb  = d.get('minuteBar')   or {}

    last = float(lt.get('p') or db.get('c') or 0)
    prev = float(pb.get('c') or 0)
    chg  = round(last - prev, 2) if last and prev else None
    pct  = round(chg / prev * 100, 2) if chg and prev else None

    _info(f'Last trade  : ${last:.2f}' + (f'  ({chg:+.2f} / {pct:+.2f}%)' if chg else ''))
    _info(f'Bid/Ask     : ${lq.get("bp",0):.2f} ×{lq.get("bs",0)}  /  ${lq.get("ap",0):.2f} ×{lq.get("as",0)}')
    _info(f'Day O/H/L/C : ${db.get("o",0):.2f} / ${db.get("h",0):.2f} / ${db.get("l",0):.2f} / ${db.get("c",0):.2f}')
    _info(f'Day volume  : {int(db.get("v",0)):,}   VWAP=${db.get("vw",0):.2f}')
    _info(f'Prev close  : ${prev:.2f}')
    _info(f'1-min bar   : O={mb.get("o",0):.2f}  H={mb.get("h",0):.2f}  L={mb.get("l",0):.2f}  C={mb.get("c",0):.2f}  V={int(mb.get("v",0)):,}')

    if not API_KEY:
        _warn('ALPACA_API_KEY not set — using empty credentials')


def main():
    p = argparse.ArgumentParser(description='Verify Alpaca data feed for a ticker')
    p.add_argument('symbol',                   help='Ticker symbol, e.g. AAPL')
    p.add_argument('--date',      default=None, help='Trade date YYYY-MM-DD (default: today)')
    p.add_argument('--window',    type=int, default=None,
                   help='ORB window in minutes (default: 15 for ETFs, 5 for stocks)')
    p.add_argument('--timeframe', default='1Min',
                   help='Bar timeframe for detailed view (default: 1Min)')
    p.add_argument('--snapshot',  action='store_true', help='Show live snapshot instead of bars')
    p.add_argument('--orb',       action='store_true', help='Show ORB range only')
    args = p.parse_args()

    symbol = args.symbol.upper()
    trade_date = args.date or date.today().isoformat()
    window = args.window or (15 if symbol in _ETF_SET else 5)

    print(f'\n{"═"*54}')
    print(f'  Verify Feed  ·  {symbol}  ·  {trade_date}')
    print(f'{"═"*54}')

    if not API_KEY:
        _warn('ALPACA_API_KEY not found in .env — requests may fail')

    if args.snapshot:
        cmd_snapshot(symbol)
    elif args.orb:
        cmd_orb(symbol, trade_date, window)
    else:
        cmd_bars(symbol, trade_date, args.timeframe, window)

    print(f'\n{"═"*54}\n')


if __name__ == '__main__':
    main()
