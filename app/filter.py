"""In-memory filtering and sorting logic for stock data."""

from typing import Optional

# ---------------------------------------------------------------------------
# Raw sheet value → internal API value mapping (used ONLY for alerts/snapshots)
# ---------------------------------------------------------------------------

_TREND_MAP = {
    "In Bull Run": "bull_run",
    "In Bear Run": "bear_run",
    "Unconfirmed": "unconfirmed",
}

_CAR_MAP = {
    "Buy/Average Out": "meets_car",
    "Avoid/Hold": "not_car",
    "Short History": "insufficient_data",
    "TICKER NOT FOUND": None,
}

# Filter param → raw sheet Output value (filter param IS the raw sheet value)
_FILTER_OUTPUT = {
    "In Bull Run": "In Bull Run",
    "In Bear Run": "In Bear Run",
    "Unconfirmed": "Unconfirmed",
}

# Filter param → raw sheet CAR Rating value
_FILTER_CAR = {
    "Buy/Average Out": "Buy/Average Out",
    "Avoid/Hold": "Avoid/Hold",
}


def map_trend(raw: Optional[str]) -> Optional[str]:
    """Map raw Output value to API trend value (used for alerts only)."""
    if raw is None:
        return None
    return _TREND_MAP.get(raw)


def map_car(raw: Optional[str]) -> Optional[str]:
    """Map raw CAR Rating value to API car_status value (used for alerts only)."""
    if raw is None:
        return None
    return _CAR_MAP.get(raw)


def filter_stocks(
    all_stocks: list[dict],
    category_tickers: list[str],
    filter_type: Optional[str] = None,
) -> list[dict]:
    """
    Filter stocks:
    1. Keep only stocks in category_tickers
    2. Optionally filter by filter_type (raw sheet values)
    3. Sort by diff_200dma descending (nulls last)
    """
    # Category filter
    cat_set = set(t.upper() for t in category_tickers)
    filtered = [s for s in all_stocks if s["ticker"] in cat_set]

    # Filter type — now uses raw sheet values directly
    if filter_type:
        if filter_type in _FILTER_OUTPUT:
            target = _FILTER_OUTPUT[filter_type]
            filtered = [s for s in filtered if s.get("output") == target]
        elif filter_type in _FILTER_CAR:
            target = _FILTER_CAR[filter_type]
            filtered = [s for s in filtered if s.get("car_rating") == target]

    # Sort by diff_200dma descending, nulls last
    def sort_key(s):
        v = s.get("diff_200dma")
        if v is None:
            return (1, 0)
        return (0, -v)

    filtered.sort(key=sort_key)

    # Map to API response format — pass raw sheet values through directly
    result = []
    for s in filtered:
        result.append(
            {
                "ticker": s["ticker"],
                "cmp": s.get("cmp"),
                "diff_200dma": s.get("diff_200dma"),
                "trend": s.get("output"),        # raw sheet value
                "car_status": s.get("car_rating"),  # raw sheet value
                "changed": s.get("changed", False),
            }
        )
    return result
