"""FastAPI application — entry point, route registration, CORS."""

import logging
from contextlib import asynccontextmanager
from datetime import datetime

import pytz
from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.auth import verify_token
from app.models import (
    HealthResponse,
    CategoryResponse,
    StockResponse,
    AlertResponse,
    MarketStatusResponse,
)
from app.sheets import fetch_sel_stock_list, fetch_category_map
from app.filter import filter_stocks, map_trend, map_car
from app.snapshots import get_recent_alerts
import app.scheduler as _scheduler
from app.scheduler import (
    start_scheduler,
    stop_scheduler,
    poll_job,
    is_market_open,
    get_next_open,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

IST = pytz.timezone("Asia/Kolkata")

# ---------------------------------------------------------------------------
# In-memory category cache
# ---------------------------------------------------------------------------

_category_map: dict[str, list[str]] = {}
_category_list: list[dict] = []


def _category_sort_key(name: str) -> tuple:
    """Sort key that pins NIFTY and BANKNIFTY categories to the top."""
    upper = name.upper()
    # Exact NIFTY/BANKNIFTY matches first, then partial matches
    if upper in ("NIFTY", "NIFTY 50"):
        return (0, upper)
    if upper in ("BANKNIFTY", "BANK NIFTY", "NIFTY BANK"):
        return (1, upper)
    if "NIFTY" in upper or "BANKNIFTY" in upper:
        return (2, upper)
    return (3, upper)


def _refresh_categories():
    global _category_map, _category_list
    _category_map = fetch_category_map()
    # Sort so NIFTY and BANKNIFTY categories appear first
    sorted_items = sorted(_category_map.items(), key=lambda item: _category_sort_key(item[0]))
    _category_list = [
        {"id": str(i + 1), "name": name, "stock_count": len(codes)}
        for i, (name, codes) in enumerate(sorted_items)
    ]


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown events."""
    logger.info("Starting RISE backend...")
    # Register category refresh so it runs on every poll tick automatically
    _scheduler.on_poll_start.append(_refresh_categories)
    # Load categories + initial data
    _refresh_categories()
    poll_job()
    # Start scheduler — categories are refreshed on every poll tick
    start_scheduler()
    yield
    stop_scheduler()
    logger.info("RISE backend stopped.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="RISE API",
    description="Market intelligence API for Indian equity markets",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health():
    now = datetime.now(IST)
    return HealthResponse(status="ok", timestamp=now.isoformat())


@app.get("/categories", response_model=list[CategoryResponse])
async def categories(user: dict = Depends(verify_token)):
    return [CategoryResponse(**c) for c in _category_list]


@app.get("/stocks", response_model=list[StockResponse])
async def stocks(
    category: str = Query(..., description="Category id or name"),
    filter: str | None = Query(None, description="Filter type"),
    user: dict = Depends(verify_token),
):
    from app.scheduler import stock_data as current_data

    # Resolve category — try by id first, then by name
    cat_tickers = None
    for cat in _category_list:
        if cat["id"] == category or cat["name"].lower() == category.lower():
            cat_tickers = _category_map.get(cat["name"], [])
            break

    if cat_tickers is None:
        return []

    result = filter_stocks(current_data, cat_tickers, filter)
    return [StockResponse(**s) for s in result]


@app.get("/alerts", response_model=list[AlertResponse])
async def alerts(user: dict = Depends(verify_token)):
    try:
        raw_alerts = get_recent_alerts()
    except Exception as e:
        logger.warning("Could not fetch alerts from Firestore: %s", e)
        raw_alerts = []
    mapped = []
    for a in raw_alerts:
        mapped.append(
            AlertResponse(
                ticker=a["ticker"],
                previous_trend=map_trend(a.get("previous_trend")),
                current_trend=map_trend(a.get("current_trend")),
                previous_car=map_car(a.get("previous_car")),
                current_car=map_car(a.get("current_car")),
                timestamp=a.get("timestamp", ""),
            )
        )
    return mapped


@app.get("/market-status", response_model=MarketStatusResponse)
async def market_status(user: dict = Depends(verify_token)):
    now = datetime.now(IST)
    lr = _scheduler.last_refreshed  # always read the live module attribute
    return MarketStatusResponse(
        is_open=is_market_open(now),
        next_open=get_next_open(now).isoformat(),
        last_refreshed=lr.isoformat() if lr else now.isoformat(),
    )


@app.post("/refresh", status_code=200)
async def force_refresh(user: dict = Depends(verify_token)):
    """Force an immediate data + category refresh (useful after adding a new sheet)."""
    try:
        _refresh_categories()
        poll_job()
        return {"status": "ok", "categories": len(_category_list), "stocks": len(_scheduler.stock_data)}
    except Exception as e:
        logger.error("Force refresh failed: %s", e, exc_info=True)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))
