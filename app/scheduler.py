"""Background polling scheduler using APScheduler."""

import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.sheets import fetch_sel_stock_list
from app.snapshots import (
    save_snapshot,
    get_latest_snapshot,
    diff_snapshots,
    save_alerts,
    cleanup_old_snapshots,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# In-memory data store (module-level)
# ---------------------------------------------------------------------------

stock_data: list[dict] = []
changed_tickers: set[str] = set()
last_refreshed: datetime | None = None

# ---------------------------------------------------------------------------
# Market hours check
# ---------------------------------------------------------------------------


def is_market_open(now: datetime | None = None) -> bool:
    """Check if Indian stock market is open (9 AM – 4 PM IST, Mon–Fri)."""
    now = now or datetime.now(IST)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    hour = now.hour
    minute = now.minute
    # Market is open from 09:00 to 16:00 IST
    if hour < 9 or hour >= 16:
        return False
    return True


def get_next_open(now: datetime | None = None) -> datetime:
    """Return the next market open time."""
    now = now or datetime.now(IST)
    # Start from tomorrow at 9 AM
    candidate = now.replace(hour=9, minute=0, second=0, microsecond=0)

    # If it's before 9 AM on a weekday, next open is today
    if now.weekday() < 5 and now.hour < 9:
        return candidate

    # Otherwise, find next weekday
    from datetime import timedelta

    candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# Poll job
# ---------------------------------------------------------------------------


def poll_job():
    """Fetch fresh data from Sheets, diff, save snapshot, update in-memory store."""
    global stock_data, changed_tickers, last_refreshed

    try:
        logger.info("Starting poll job...")

        # 1. Fetch fresh data from Google Sheets (always required)
        new_data = fetch_sel_stock_list()
        if not new_data:
            logger.warning("Poll returned empty data, skipping")
            return

        # 2–5. Firestore snapshot/diff/alerts — non-fatal if Firestore is unavailable
        changed_tickers_set: set[str] = set()
        try:
            prev_snapshot = get_latest_snapshot()
            changes = []
            if prev_snapshot and prev_snapshot.get("data"):
                changes = diff_snapshots(prev_snapshot["data"], new_data)
                changed_tickers_set = {c["ticker"] for c in changes}

            save_snapshot(new_data)

            if changes:
                save_alerts(changes)
                logger.info("Detected %d changes", len(changes))
        except Exception as fs_err:
            logger.warning(
                "Firestore unavailable — snapshot/diff skipped: %s", fs_err
            )

        # 6. Mark changed stocks in data
        for stock in new_data:
            stock["changed"] = stock["ticker"] in changed_tickers_set

        # 7. Update in-memory store
        stock_data = new_data
        changed_tickers = changed_tickers_set
        last_refreshed = datetime.now(IST)

        # 8. Cleanup old snapshots (non-fatal)
        try:
            cleanup_old_snapshots(days=30)
        except Exception as cleanup_err:
            logger.warning("Snapshot cleanup skipped: %s", cleanup_err)

        logger.info("Poll complete. %d stocks loaded.", len(stock_data))

    except Exception as e:
        logger.error("Poll job failed: %s", e, exc_info=True)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

scheduler = BackgroundScheduler(timezone=IST)


def start_scheduler():
    """Start the background scheduler with market-aware polling."""

    # During market hours: every 10 min. Outside: every 60 min.
    # We use a single job that checks market hours internally
    # and an interval of 10 minutes; the job itself is lightweight
    # when called outside hours (just a quick check).
    # For simplicity, we run every 10 min always and adjust behaviour
    # via a wrapper.

    def smart_poll():
        now = datetime.now(IST)
        if is_market_open(now):
            poll_job()
        else:
            # Outside market hours: only run if it's been >= 55 min
            # since last refresh (to approximate hourly polling with
            # 10-min interval ticks)
            global last_refreshed
            if last_refreshed is None:
                poll_job()
            else:
                elapsed = (now - last_refreshed).total_seconds()
                if elapsed >= 55 * 60:
                    poll_job()

    scheduler.add_job(
        smart_poll,
        trigger=IntervalTrigger(minutes=10),
        id="sheet_poll",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started")


def stop_scheduler():
    """Shutdown the scheduler gracefully."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
