"""
Morning email report — sent at 8:00 AM ET.

Pulls:
  - Yesterday's session summary (P&L, trades, win rate)
  - Today's watchlist (TRADE-decision symbols with scores/setups)

Sends via SMTP (Gmail app password or any SMTP server).

Required env vars:
  EMAIL_FROM       sender address
  EMAIL_TO         recipient address (comma-sep for multiple)
  EMAIL_PASSWORD   SMTP password / app password
  EMAIL_SMTP_HOST  defaults to smtp.gmail.com
  EMAIL_SMTP_PORT  defaults to 587
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date

from db.connection import db_connection
from layer4.queries import get_yesterday_summary, get_todays_watchlist_for_email

log = logging.getLogger(__name__)


def _build_html(yesterday: dict, watchlist: list) -> str:
    # ── yesterday summary block ───────────────────────────────────────────────
    if yesterday:
        pnl      = float(yesterday.get('total_pnl') or 0)
        trades   = yesterday.get('trades_count', 0)
        wr       = yesterday.get('win_rate', 0)
        avg_r    = yesterday.get('avg_r', '—')
        pnl_col  = '#9fe1cb' if pnl >= 0 else '#f09595'
        pnl_str  = f"{'+'if pnl>=0 else ''}${pnl:.2f}"
        yest_html = f"""
        <tr><td style="padding:6px 10px;color:#9c9a91">P&amp;L</td>
            <td style="padding:6px 10px;color:{pnl_col};font-weight:600">{pnl_str}</td></tr>
        <tr><td style="padding:6px 10px;color:#9c9a91">Trades</td>
            <td style="padding:6px 10px;color:#dedcd1">{trades}</td></tr>
        <tr><td style="padding:6px 10px;color:#9c9a91">Win rate</td>
            <td style="padding:6px 10px;color:#dedcd1">{wr}%</td></tr>
        <tr><td style="padding:6px 10px;color:#9c9a91">Avg R</td>
            <td style="padding:6px 10px;color:#dedcd1">{avg_r}R</td></tr>
        """
    else:
        yest_html = '<tr><td colspan="2" style="padding:10px;color:#5a5a54;font-style:italic">No trades yesterday</td></tr>'

    # ── watchlist block ───────────────────────────────────────────────────────
    if watchlist:
        rows = []
        for r in watchlist:
            score  = r.get('score_final')
            score_s = f"{score:.0f}" if score else '—'
            entry  = r.get('entry_price')
            stop   = r.get('stop_price')
            t1     = r.get('t1_price')
            cat    = r.get('catalyst_type') or '—'
            rows.append(f"""
            <tr>
              <td style="padding:6px 10px;color:#faf9f5;font-weight:600">{r['symbol']}</td>
              <td style="padding:6px 10px;color:#9fe1cb;font-weight:600">{score_s}</td>
              <td style="padding:6px 10px;color:#9c9a91;font-size:11px">{cat}</td>
              <td style="padding:6px 10px;color:#9fe1cb">${entry:.2f if entry else '—'}</td>
              <td style="padding:6px 10px;color:#f09595">${stop:.2f if stop else '—'}</td>
              <td style="padding:6px 10px;color:#dedcd1">${t1:.2f if t1 else '—'}</td>
            </tr>""")
        watch_html = ''.join(rows)
        watch_header = """
        <tr style="border-bottom:1px solid rgba(222,220,209,0.1)">
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">Symbol</th>
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">Score</th>
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">Catalyst</th>
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">Entry est.</th>
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">Stop est.</th>
          <th style="padding:6px 10px;color:#9c9a91;font-size:11px;text-transform:uppercase;text-align:left">T1 est.</th>
        </tr>"""
    else:
        watch_header = ''
        watch_html = '<tr><td colspan="6" style="padding:10px;color:#5a5a54;font-style:italic">No TRADE-rated candidates for today</td></tr>'

    today_str = date.today().strftime('%A, %B %d, %Y')

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="background:#1a1a17;color:#dedcd1;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',monospace,sans-serif;font-size:13px;margin:0;padding:20px">
  <div style="max-width:600px;margin:0 auto">

    <div style="margin-bottom:20px">
      <span style="color:#9fe1cb;font-weight:700;font-size:14px;letter-spacing:1px">TRADING SYS</span>
      <span style="color:#5a5a54;font-size:12px;margin-left:12px">Morning Brief — {today_str}</span>
    </div>

    <div style="background:#262622;border:1px solid rgba(222,220,209,0.12);border-radius:8px;padding:16px;margin-bottom:16px">
      <div style="font-size:11px;font-weight:600;color:#c2c0b6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Yesterday</div>
      <table style="width:100%;border-collapse:collapse">{yest_html}</table>
    </div>

    <div style="background:#262622;border:1px solid rgba(222,220,209,0.12);border-radius:8px;padding:16px;margin-bottom:16px">
      <div style="font-size:11px;font-weight:600;color:#c2c0b6;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px">Today's watchlist ({len(watchlist)} candidate{'s' if len(watchlist) != 1 else ''})</div>
      <table style="width:100%;border-collapse:collapse">
        {watch_header}
        {watch_html}
      </table>
    </div>

    <div style="color:#3a3a34;font-size:11px;text-align:center;padding:10px 0">
      Layer 4 — read only — never places orders
    </div>
  </div>
</body>
</html>"""


def send_morning_email() -> bool:
    """Build and send the 8 AM morning email. Returns True on success."""
    from_addr = os.getenv('EMAIL_FROM', '')
    to_raw    = os.getenv('EMAIL_TO', '')
    password  = os.getenv('EMAIL_PASSWORD', '')

    if not (from_addr and to_raw and password):
        log.warning('Morning email skipped — EMAIL_FROM / EMAIL_TO / EMAIL_PASSWORD not set')
        return False

    to_addrs = [a.strip() for a in to_raw.split(',') if a.strip()]

    try:
        with db_connection() as conn:
            yesterday = get_yesterday_summary(conn)
            watchlist = get_todays_watchlist_for_email(conn)
    except Exception as exc:
        log.error('Morning email DB error: %s', exc)
        return False

    html = _build_html(yesterday, watchlist)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"Morning Brief — {date.today().strftime('%a %b %d')} — {len(watchlist)} candidate(s)"
    msg['From']    = from_addr
    msg['To']      = ', '.join(to_addrs)
    msg.attach(MIMEText(html, 'html'))

    smtp_host = os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('EMAIL_SMTP_PORT', '587'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(from_addr, password)
            smtp.sendmail(from_addr, to_addrs, msg.as_string())
        log.info('Morning email sent to %s (%d candidates)', to_addrs, len(watchlist))
        return True
    except Exception as exc:
        log.error('Morning email SMTP error: %s', exc)
        return False
