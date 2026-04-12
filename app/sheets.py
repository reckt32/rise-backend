"""Google Sheets API reader — fetches stock data and category map."""

import json
import logging
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from app.config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# ---------------------------------------------------------------------------
# Sheets client
# ---------------------------------------------------------------------------

_service = None


def _get_service():
    global _service
    if _service is None:
        creds_info = json.loads(settings.google_sheets_credentials_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        _service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _service


def _read_sheet(range_name: str) -> list[list[Any]]:
    """Read a range from the configured spreadsheet."""
    service = _get_service()
    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=settings.spreadsheet_id,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    return result.get("values", [])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INVALID_VALUES = {"#N/A", "#REF!", "#VALUE!", "#ERROR!", "Loading...", "N/A", ""}


def _clean(value: Any) -> Any:
    """Return None for any invalid / loading cell value."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in _INVALID_VALUES:
        return None
    return value


def _clean_float(value: Any) -> float | None:
    v = _clean(value)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _strip_nse(code: Any) -> str | None:
    """Strip 'NSE:'/'INDEXNSE:' prefix and return uppercased ticker, or None.

    Returns None for section-label cells like '(13)ALL TIME HIGH' that
    appear as the first row of each column in the LIST sheet.
    """
    v = _clean(code)
    if v is None:
        return None
    s = str(v).strip().upper()
    if s.startswith("INDEXNSE:"):
        s = s[9:]
    elif s.startswith("NSE:"):
        s = s[4:]
    # Reject section labels like "(13)ALL TIME HIGH"
    if s.startswith("("):
        return None
    return s if s else None


# ---------------------------------------------------------------------------
# Sel Stock List
# ---------------------------------------------------------------------------

def fetch_sel_stock_list() -> list[dict]:
    """Fetch all rows from 'Sel Stock List' and return as list of dicts."""
    rows = _read_sheet("Sel Stock List")
    if not rows:
        return []

    # First row is headers; data starts from row 2
    # Expected columns (0-indexed):
    # 0: NSECode, 1: CMP, 2: 50DMA, 3: 100DMA, 4: 200DMA,
    # 5: Output, 6: Diff from 200 DMA, 7: CAR Rating
    stocks = []
    for row in rows[1:]:
        # Pad row to avoid index errors
        while len(row) < 8:
            row.append(None)

        ticker = _strip_nse(row[0])
        if not ticker:
            continue

        stocks.append(
            {
                "ticker": ticker,
                "cmp": _clean_float(row[1]),
                "dma_50": _clean_float(row[2]),
                "dma_100": _clean_float(row[3]),
                "dma_200": _clean_float(row[4]),
                "output": _clean(row[5]),
                "diff_200dma": _clean_float(row[6]),
                "car_rating": _clean(row[7]),
            }
        )
    logger.info("Fetched %d stocks from Sel Stock List", len(stocks))
    return stocks


# ---------------------------------------------------------------------------
# LIST — category map
# ---------------------------------------------------------------------------

def fetch_category_map() -> dict[str, list[str]]:
    """
    Read the LIST sheet and return { category_name: [ticker, ...] }.
    Each column header is a category name; tickers are listed below.
    """
    rows = _read_sheet("LIST")
    if not rows:
        return {}

    headers = rows[0]
    category_map: dict[str, list[str]] = {}

    for col_idx, header in enumerate(headers):
        cat_name = _clean(header)
        if cat_name is None:
            continue
        cat_name = str(cat_name).strip()
        if not cat_name:
            continue

        tickers: list[str] = []
        for row in rows[1:]:
            if col_idx >= len(row):
                continue
            t = _strip_nse(row[col_idx])
            if t:
                tickers.append(t)

        category_map[cat_name] = tickers

    logger.info(
        "Built category map with %d categories", len(category_map)
    )
    return category_map
