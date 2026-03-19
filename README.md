# Trading System — v1.0

A four-layer automated day trading system.
Paper-trading only (Alpaca paper account).
**Layer 4 never places orders — read only.**

---

## Architecture

```
Layer 1  9:30 AM  Gap scan — Finviz + Alpaca → PostgreSQL
Layer 2  9:33 AM  AI scoring — Claude Haiku + 100-pt model → TRADE / WATCH / SKIP
Layer 3  9:45 AM  Live execution — ORB breakout + 5-criteria + SMS confirmation
Layer 4  always   Dashboard + reports — Flask, email, SMS, Google Sheets
```

---

## Quick start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

## Health check

Run a consolidated health check for all core and optional integrations:

```bash
python run.py --health
```

What it checks:

- **postgres** — makes a simple `SELECT 1` to the configured database
- **alpaca** — fetches latest bar for `SPY` (requires Alpaca credentials)
- **finviz** — exercises the Finviz screener (package must be installed)
- **finnhub** — quick quote request when `FINNHUB_API_KEY` is set
- **tradier / twilio / anthropic / sheets** — reported as `skipped (...)` unless their env vars are configured

Active checks performed when credentials are present:

- **Tradier**: requests a quote for `SPY` using your `TRADIER_TOKEN` (sandbox vs live controlled by `TRADIER_ENV`).
- **Twilio**: fetches account metadata (no SMS sent) to verify Twilio credentials.
- **Anthropic**: attempts a lightweight `models` query to verify API reachability and key validity.
- **Google Sheets**: opens the spreadsheet by `SHEETS_SPREADSHEET_ID` using the service account JSON and lists worksheets.

Environment notes for deeper checks:

- `ANTHROPIC_API_VERSION` — (optional) send this header value with Anthropic API requests if your account requires a specific API version. Default used by health check: `2024-11-22`.
- `GOOGLE_SERVICE_ACCOUNT_JSON` must point to an accessible service account JSON file path (absolute or relative to repo); health checks will attempt to open the spreadsheet and list worksheets.

Developer notes — running tests for health checks

- The test `tests/test_health.py` is a small pytest unit test that mocks DB and Alpaca. To run it reliably, use Python 3.11 (pytest and some plugins may be incompatible with Python 3.14):

```powershell
# create a Python 3.11 venv and run the single test
py -3.11 -m venv .venv311
.venv311\Scripts\activate
pip install -r requirements.txt pytest
pytest tests/test_health.py -q
```

If you can't use Python 3.11, running `python run.py --health` is still a useful manual smoke test — optional integrations will be skipped with clear messages when not configured.

Security note: active checks will make read-only API requests and will not send messages or place orders. Keep your API keys secret.

### 2. Configure environment

```bash
cp .env.example .env
# Fill in all values — see Environment Variables section below
```

### 3. Create PostgreSQL database

```sql
CREATE DATABASE trading_system;
```

### 4. Create schema

```bash
python run.py --setup
```

### 5. Start the system

```bash
python run.py
```

The scheduler starts, the Flask dashboard launches at `http://localhost:5000`, and all jobs run automatically on market days.

---

## CLI commands

| Command                    | What it does                                        |
| -------------------------- | --------------------------------------------------- |
| `python run.py`            | Start full system (scheduler + dashboard)           |
| `python run.py --now`      | Run Layer 1 scan immediately                        |
| `python run.py --layer2`   | Run Layer 2 scoring immediately                     |
| `python run.py --layer3`   | Start Layer 3 monitor immediately                   |
| `python run.py --snapshot` | Run pre-market snapshot immediately                 |
| `python run.py --health`   | Health check (postgres + alpaca + finviz)           |
| `python run.py --setup`    | Create DB schema and exit                           |
| `python run.py --demo`     | Weekend test — skips Finviz, uses hardcoded tickers |

---

## Daily schedule (all times ET, market days only)

| Time     | Job                                               |
| -------- | ------------------------------------------------- |
| 8:00 AM  | Morning email — yesterday P&L + today's watchlist |
| 9:25 AM  | Pre-market snapshot — refresh pm highs/lows       |
| 9:28 AM  | Health check — postgres, alpaca, finviz           |
| 9:30 AM  | Layer 1 scan fires (+ 60s Finviz buffer)          |
| 9:33 AM  | Layer 2 scoring — Claude Haiku + 100-pt model     |
| 9:45 AM  | Layer 3 entry gate opens — ORB monitor starts     |
| 11:30 AM | Dead zone begins — no new entries                 |
| 2:30 PM  | Dead zone ends — entries resume                   |
| 3:45 PM  | Force exit — all open positions closed at market  |
| 4:00 PM  | EOD SMS — session summary                         |
| Sun 6 PM | Weekly Google Sheets fill                         |

---

## Layer 1 — Gap Scanner

Finds opening-gap candidates from Finviz (stocks) and a static ETF watchlist.

**Data sources:**

| Source | Role |
| --- | --- |
| Finviz | Stock discovery — returns ticker list with coarse pre-filters |
| Tradier SIP | All enrichment — OHLCV, ATR, avg volume, pre-market bars |
| Alpaca IEX | VIX fallback only — not used in the scan path |

**Stocks** are discovered via Finviz, enriched via Tradier (quote + 14-day history + premarket bars). **ETFs** come from a static 11-ticker watchlist in `timing.py` (QQQ, SPY, IWM, USO, XLE, XOP, XBI, IBB, GLD, GDX, XLK), same enrichment.

**Filters — stocks and ETFs use different thresholds:**

| Filter | Stocks | ETFs |
| --- | --- | --- |
| Price | $15 – $80 | $15 – $2,000 |
| Gap % | 3.5% – 6% | 1% – 5% |
| Volume (today) | ≥ 750,000 | ≥ 1,000,000 |
| Pre-market volume | ≥ 10% of 14-day avg vol | ≥ 0.8% of 14-day avg vol |
| ATR % | ≥ 1.5% | ≥ 0.7% |

Passed candidates are ranked (ETFs first, QQQ always rank 1, then by gap% descending) and capped at **6 total**.

**PostgreSQL tables written:** `tickers`, `daily_prices`, `scan_results`

---

## Layer 2 — AI Scorer

Runs at 9:33 AM on every candidate that passed Layer 1 filters.

- Fetches news headlines from Finnhub
- Sends to Claude Haiku with a structured 100-point scoring prompt
- Scores: catalyst type + strength, momentum, risk/reward, volume quality
- Computes entry / stop / T1 / T2 prices from the score
- Decision: **TRADE** (≥ 65), **WATCH** (50–64), **SKIP** (< 50)

**PostgreSQL table written:** `ai_scores`

---

## Layer 3 — Live Execution

Starts at 9:45 AM. Runs a 60-second monitor loop in a background thread.

**ORB windows:** 5 min for stocks (9:30–9:35 ET), 15 min for ETFs (9:30–9:45 ET)
ORB range (high/low) is fetched from Alpaca IEX as a single N-min bar.

At bootstrap (before the monitor loop starts), a **PENDING trade record is written** to the `trades` table immediately using the ORB range as the preliminary plan estimate. This is updated with live values when the breakout fires.

**Entry criteria — all 5 must pass simultaneously:**

| #   | Check                       | Source            |
| --- | --------------------------- | ----------------- |
| 1   | Price above ORB high        | Alpaca 1-min IEX  |
| 2   | Price above cumulative VWAP | Alpaca 1-min IEX  |
| 3   | SPY green (close > open)    | Alpaca 1-min IEX  |
| 4   | Volume 2× baseline          | Tradier SIP 5-min |
| 5   | 9-EMA slope up              | Tradier SIP 5-min |

**Daily guards:**

- Max 3 trades per session
- Session halt if loss ≥ $240
- No-trade rule: if ORB low breaks before ORB high → skip symbol all day

**Trade lifecycle:**

1. Bootstrap → ORB computed → PENDING record inserted with preliminary plan
2. Breakout detected → live trade plan recalculated (entry = breakout close, stop = candle low, T1 = +1R, T2 = +2R) → record updated
3. SMS confirmation sent — 60s to reply YES/NO (timeout = skip)
4. Market buy + stop-loss order placed on Alpaca paper account
5. TradeManager monitors every 60s:
   - 50% to stop → pre-stop SMS warning
   - T1 hit → sell 50%, move stop to breakeven
   - T2 hit → sell remainder, close
   - Stop fills → close
6. 3:45 PM → force-exit all positions

**Risk sizing:** `TRADING_CAPITAL × RISK_PCT_PER_TRADE` (default: $8,000 × 1% = $80/trade).
VIX ≥ 30 halves the budget (effectively 0.5% risk).

**PostgreSQL tables written:** `daily_sessions`, `trades`, `signal_log`

---

## Layer 4 — Dashboard & Reports

**Never writes to the database. Never places orders.**

### Flask dashboard — `http://localhost:5000`

| Route        | Page                                                              |
| ------------ | ----------------------------------------------------------------- |
| `/dash`      | Morning dashboard — Layer 1 candidates + AI scores                |
| `/cockpit`   | Live trade cockpit — open positions + live P&L                    |
| `/analytics` | Historical analytics — win rate by bucket, catalyst, confirmation |
| `/progress`  | Account progress — actual balance vs compound plan                |

Dashboard auto-refreshes every 30 seconds.

### Scheduled reports

| Report        | When              | Method      |
| ------------- | ----------------- | ----------- |
| Morning email | 8:00 AM ET        | SMTP (HTML) |
| EOD SMS       | 4:00 PM ET        | Twilio      |
| Weekly Sheets | Sunday 6:00 PM ET | gspread     |

**Google Sheets tabs written:** Trade Log, Weekly Review, Catalyst Tracker, Account Curve

---

## Environment variables

Copy `.env.example` to `.env` and fill in all values.

### Required

| Variable             | Description                                         |
| -------------------- | --------------------------------------------------- |
| `ALPACA_API_KEY`     | Alpaca API key                                      |
| `ALPACA_SECRET_KEY`  | Alpaca secret key                                   |
| `ALPACA_ENV`         | `paper` or `live`                                   |
| `DB_HOST`            | PostgreSQL host (e.g. `localhost`)                  |
| `DB_PORT`            | PostgreSQL port (e.g. `5432`)                       |
| `DB_NAME`            | Database name (e.g. `trading_system`)               |
| `DB_USER`            | PostgreSQL user                                     |
| `DB_PASSWORD`        | PostgreSQL password                                 |
| `ANTHROPIC_API_KEY`  | Claude API key (Layer 2 scoring)                    |
| `FINNHUB_API_KEY`    | Finnhub API key (news headlines)                    |
| `TRADIER_TOKEN`      | Tradier API token (SIP 5-min bars)                  |
| `TRADIER_ENV`        | `sandbox` or `live`                                 |
| `TWILIO_ACCOUNT_SID` | Twilio account SID                                  |
| `TWILIO_AUTH_TOKEN`  | Twilio auth token                                   |
| `TWILIO_FROM`        | Twilio outbound number (E.164, e.g. `+18339595082`) |
| `TWILIO_TO`          | Your phone number (E.164, e.g. `+18728186717`)      |

### Optional (have defaults)

| Variable                     | Default | Description                                           |
| ---------------------------- | ------- | ----------------------------------------------------- |
| `TRADING_CAPITAL`            | `8000`  | Current account balance used for risk sizing          |
| `RISK_PCT_PER_TRADE`         | `1.0`   | % of capital to risk per trade (1% = $80 on $8k)     |
| `MAX_POSITION_PCT`           | `95.0`  | Max single-position % of capital (risk% controls real exposure) |
| `DASHBOARD_PORT`             | `5000`  | Flask dashboard port                                  |
| `ACCOUNT_START_BALANCE`      | `8000`  | Starting account balance for progress page            |
| `ACCOUNT_MONTHLY_TARGET_PCT` | `8`     | Monthly growth target % (compounded, 20 trading days) |

### Optional (reports disabled if unset)

| Variable                      | Description                                 |
| ----------------------------- | ------------------------------------------- |
| `EMAIL_FROM`                  | Sender email address                        |
| `EMAIL_TO`                    | Recipient address(es), comma-separated      |
| `EMAIL_PASSWORD`              | SMTP password or Gmail app password         |
| `EMAIL_SMTP_HOST`             | SMTP host (default: `smtp.gmail.com`)       |
| `EMAIL_SMTP_PORT`             | SMTP port (default: `587`)                  |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Path to Google service account `.json` file |
| `SHEETS_SPREADSHEET_ID`       | Google Sheets spreadsheet ID                |

> Reports are silently skipped if their env vars are missing — the rest of the system continues normally.

---

## File structure

```
trading_appv1.0/
├── run.py                        # entry point + APScheduler
├── timing.py                     # ET timezone, holidays, ETF watchlist, scan config
├── logger.py                     # ET-timestamped rotating log
├── requirements.txt
├── .env
├── .env.example
│
├── scanner/                      # Layer 1 — gap scanner
│   ├── alpaca_client.py          # Alpaca IEX — live price, pre-market bars, VIX
│   ├── gap_scanner.py            # Finviz stock scan
│   ├── etf_scanner.py            # static ETF watchlist scan
│   ├── filters.py                # 5 shared filter functions
│   ├── merger.py                 # combine + rank + write to DB
│   └── retry.py                  # retry decorator (3 attempts, exp back-off)
│
├── layer2/                       # Layer 2 — AI scorer
│   ├── haiku_scorer.py           # Claude Haiku API call + JSON parse
│   ├── news_client.py            # Finnhub headlines fetch
│   ├── scoring.py                # 100-pt model, TRADE/WATCH/SKIP decision
│   ├── session_gates.py          # VIX + market-day session guards
│   ├── trade_plan.py             # entry / stop / T1 / T2 price calculation
│   └── runner.py                 # orchestrates Layer 2 for all candidates
│
├── layer3/                       # Layer 3 — live execution
│   ├── guards.py                 # daily trade + loss limits
│   ├── orb.py                    # ORB range from Alpaca 1-min bars
│   ├── tradier_client.py         # Tradier SIP 5-min bars
│   ├── monitor.py                # watchman (1-min Alpaca) + validator (5-min Tradier)
│   ├── signal.py                 # live trade plan (entry, stop, shares, T1, T2)
│   ├── confirmation.py           # Twilio SMS send + 60s reply poll
│   ├── broker.py                 # Alpaca paper order execution
│   ├── trade_manager.py          # single-trade lifecycle (T1/T2/stop/force-exit)
│   └── runner.py                 # monitor loop orchestrator
│
├── layer4/                       # Layer 4 — dashboard & reports (read only)
│   ├── queries.py                # all read-only DB queries
│   ├── runner.py                 # start_dashboard() + schedule_reports()
│   ├── dashboard/
│   │   ├── app.py                # Flask routes
│   │   └── templates/
│   │       ├── base.html         # dark theme, nav, shared CSS
│   │       ├── dash.html         # morning candidates view
│   │       ├── cockpit.html      # live positions + closed today
│   │       ├── analytics.html    # historical win rate / R / P&L
│   │       └── progress.html     # balance vs compound plan curve
│   └── reports/
│       ├── email_report.py       # 8 AM morning HTML email
│       ├── sms_report.py         # 4 PM EOD SMS
│       └── sheets.py             # Sunday weekly Google Sheets fill
│
├── db/
│   ├── connection.py             # psycopg3 ConnectionPool + context manager
│   ├── schema.py                 # CREATE TABLE — all layers
│   ├── queries.py                # Layer 1 insert/update/select
│   ├── queries_layer2.py         # Layer 2 insert/update/select
│   └── queries_layer3.py         # Layer 3 insert/update/select
│
└── logs/
    └── layer1_YYYYMMDD.log
```

---

## PostgreSQL tables

| Table            | Layer | Description                                             |
| ---------------- | ----- | ------------------------------------------------------- |
| `tickers`        | 1     | Symbol, type, sector, industry                          |
| `daily_prices`   | 1     | OHLCV, pm high/low, ATR, gap%, VIX                      |
| `scan_results`   | 1     | Rank, ORB window, passed/failed filters                 |
| `ai_scores`      | 2     | Score, decision, catalyst, entry/stop/T1/T2 estimates   |
| `daily_sessions` | 3     | Trade count, P&L, risk budget, halt status              |
| `trades`         | 3     | Full trade record — from signal to exit                 |
| `signal_log`     | 3     | Event log (breakout, T1, T2, stop, pre-stop warn, etc.) |

---

## Google Sheets setup

1. Create a Google Cloud project and enable the Sheets + Drive APIs
2. Create a service account and download the JSON key file
3. Share the target spreadsheet with the service account email (Editor access)
4. Set `GOOGLE_SERVICE_ACCOUNT_JSON` to the path of the JSON key file
5. Set `SHEETS_SPREADSHEET_ID` to the spreadsheet ID from the URL

The four tabs (Trade Log, Weekly Review, Catalyst Tracker, Account Curve) are created automatically on first run.

---

## Gmail app password setup

1. Enable 2-factor authentication on your Google account
2. Go to **Google Account → Security → App passwords**
3. Create an app password for "Mail"
4. Set `EMAIL_PASSWORD` to the 16-character app password (no spaces)
5. Set `EMAIL_FROM` and `EMAIL_TO` to your Gmail address
