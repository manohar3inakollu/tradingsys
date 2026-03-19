"""
Layer 4 orchestrator.

  start_dashboard()   — starts Flask in a background daemon thread
  schedule_reports()  — registers 8 AM email, 4 PM SMS, Sun 6 PM sheets
                        jobs on the provided APScheduler instance
"""

import logging
import os
import threading

log = logging.getLogger(__name__)

_dashboard_thread: threading.Thread | None = None


# ── Flask dashboard ───────────────────────────────────────────────────────────

def start_dashboard() -> None:
    """Launch the Flask dashboard in a daemon thread. No-op if already running."""
    global _dashboard_thread

    if _dashboard_thread and _dashboard_thread.is_alive():
        log.debug('Dashboard already running')
        return

    port = int(os.getenv('DASHBOARD_PORT', '5000'))

    def _run():
        from layer4.dashboard.app import app
        # use_reloader=False — critical, we're inside a thread
        log.info('Dashboard starting on port %d', port)
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

    _dashboard_thread = threading.Thread(target=_run, name='layer4-dashboard', daemon=True)
    _dashboard_thread.start()
    log.info('Dashboard thread started (port %d)', port)


# ── Scheduled report jobs ─────────────────────────────────────────────────────

def job_morning_email() -> None:
    """APScheduler job: 8:00 AM ET — morning watchlist email."""
    log.info('Running morning email report')
    try:
        from layer4.reports.email_report import send_morning_email
        send_morning_email()
    except Exception as exc:
        log.error('Morning email job failed: %s', exc)


def job_eod_sms() -> None:
    """APScheduler job: 4:00 PM ET — end-of-day SMS summary."""
    log.info('Running EOD SMS report')
    try:
        from layer4.reports.sms_report import send_eod_sms
        send_eod_sms()
    except Exception as exc:
        log.error('EOD SMS job failed: %s', exc)


def job_weekly_sheets() -> None:
    """APScheduler job: Sunday 6:00 PM ET — Google Sheets fill."""
    log.info('Running weekly Sheets fill')
    try:
        from layer4.reports.sheets import fill_weekly_sheets
        fill_weekly_sheets()
    except Exception as exc:
        log.error('Weekly sheets job failed: %s', exc)


def schedule_reports(scheduler) -> None:
    """
    Add Layer 4 report jobs to an existing APScheduler instance.

    Called from run.py after the scheduler is created but before .start().
    Expects the scheduler to use America/New_York timezone.

    Args:
        scheduler: apscheduler.schedulers.blocking.BlockingScheduler instance
    """
    from apscheduler.triggers.cron import CronTrigger
    from timing import ET

    scheduler.add_job(
        job_morning_email,
        CronTrigger(hour=8, minute=0, timezone=ET),
        id='layer4_morning_email',
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        job_eod_sms,
        CronTrigger(hour=16, minute=0, timezone=ET),
        id='layer4_eod_sms',
        replace_existing=True,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        job_weekly_sheets,
        CronTrigger(day_of_week='sun', hour=18, minute=0, timezone=ET),
        id='layer4_weekly_sheets',
        replace_existing=True,
        misfire_grace_time=600,
    )
    log.info('Layer 4 report jobs scheduled (8AM email, 4PM SMS, Sun 6PM sheets)')
