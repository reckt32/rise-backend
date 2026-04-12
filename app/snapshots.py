"""Firestore snapshot management — save, diff, alerts, cleanup."""

import logging
from datetime import datetime, timedelta
from typing import Optional

import pytz
import firebase_admin
from firebase_admin import credentials, firestore

from app.config import settings

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Firebase init
# ---------------------------------------------------------------------------

_db = None


def _get_db():
    global _db
    if _db is None:
        if not firebase_admin._apps:
            cred = credentials.Certificate(
                {
                    "type": "service_account",
                    "project_id": settings.firebase_project_id,
                    "private_key": settings.firebase_private_key.replace("\\n", "\n"),
                    "client_email": settings.firebase_client_email,
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            )
            firebase_admin.initialize_app(cred)
        _db = firestore.client()
    return _db


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


def save_snapshot(data: list[dict]) -> str:
    """Save a full snapshot to Firestore. Returns the document ID."""
    db = _get_db()
    now = datetime.now(IST)
    doc_ref = db.collection("snapshots").document()
    doc_ref.set(
        {
            "timestamp": now.isoformat(),
            "data": data,
        }
    )
    logger.info("Saved snapshot %s at %s", doc_ref.id, now.isoformat())
    return doc_ref.id


def get_latest_snapshot() -> Optional[dict]:
    """Return the most recent snapshot document, or None."""
    db = _get_db()
    docs = (
        db.collection("snapshots")
        .order_by("timestamp", direction=firestore.Query.DESCENDING)
        .limit(1)
        .stream()
    )
    for doc in docs:
        return doc.to_dict()
    return None


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def diff_snapshots(
    old_data: list[dict], new_data: list[dict]
) -> list[dict]:
    """
    Compare two snapshot data arrays.
    Returns list of changed items with previous and current values.
    """
    old_map = {s["ticker"]: s for s in old_data}
    changes = []

    for stock in new_data:
        ticker = stock["ticker"]
        prev = old_map.get(ticker)
        if prev is None:
            continue

        output_changed = prev.get("output") != stock.get("output")
        car_changed = prev.get("car_rating") != stock.get("car_rating")

        if output_changed or car_changed:
            changes.append(
                {
                    "ticker": ticker,
                    "previous_trend": prev.get("output"),
                    "current_trend": stock.get("output"),
                    "previous_car": prev.get("car_rating"),
                    "current_car": stock.get("car_rating"),
                }
            )

    return changes


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


def save_alerts(changes: list[dict]) -> None:
    """Save alert documents to Firestore."""
    if not changes:
        return
    db = _get_db()
    now = datetime.now(IST).isoformat()
    batch = db.batch()
    for change in changes:
        ref = db.collection("alerts").document()
        batch.set(
            ref,
            {
                **change,
                "timestamp": now,
            },
        )
    batch.commit()
    logger.info("Saved %d alerts", len(changes))


def get_recent_alerts(limit: int = 50) -> list[dict]:
    """Fetch recent alerts from Firestore, ordered by timestamp descending."""
    db = _get_db()
    docs = (
        db.collection("alerts")
        .order_by("timestamp", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    return [doc.to_dict() for doc in docs]


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_old_snapshots(days: int = 30) -> int:
    """Delete snapshots older than `days` days. Returns count deleted."""
    db = _get_db()
    cutoff = (datetime.now(IST) - timedelta(days=days)).isoformat()
    old_docs = (
        db.collection("snapshots")
        .where("timestamp", "<", cutoff)
        .stream()
    )
    count = 0
    batch = db.batch()
    for doc in old_docs:
        batch.delete(doc.reference)
        count += 1
        if count % 400 == 0:  # Firestore batch limit is 500
            batch.commit()
            batch = db.batch()
    if count % 400 != 0:
        batch.commit()
    logger.info("Cleaned up %d old snapshots", count)
    return count
