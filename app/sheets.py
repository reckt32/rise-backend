"""Google Sheets API reader — fetches stock data and category map."""

import json
import logging
import time
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


def _reset_service():
    """Invalidate the cached Sheets service so it is rebuilt on next use."""
    global _service
    _service = None


def _get_service():
    global _service
    if _service is None:
        raw = settings.google_sheets_credentials_json.strip()
        try:
            creds_info = json.loads(raw)
        except json.JSONDecodeError:
            # Azure App Settings converts \n escapes to real newlines,
            # which breaks JSON string values (e.g. private_key).
            # Fix by replacing real newlines inside string values only.
            import re
            fixed = re.sub(
                r'("private_key"\s*:\s*")(.*?)(")',
                lambda m: m.group(1) + m.group(2).replace("\n", "\\n") + m.group(3),
                raw,
                flags=re.DOTALL,
            )
            # Also ensure trailing brace exists (Azure may strip it)
            fixed = fixed.rstrip()
            if not fixed.endswith("}"):
                fixed += " }"
            creds_info = json.loads(fixed)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        _service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _service


def _read_sheet(range_name: str, _retry: bool = True) -> list[list[Any]]:
    """Read a range from the configured spreadsheet."""
    service = _get_service()
    try:
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
    except Exception as exc:
        logger.warning("Sheets API call failed (%s); resetting service", exc)
        _reset_service()
        if _retry:
            time.sleep(1)
            return _read_sheet(range_name, _retry=False)
        raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INVALID_PREFIXES = ("#",)  # catches #N/A, #REF!, and long error strings
_INVALID_VALUES = {"N/A", "", "Loading...", "TICKER NOT FOUND"}


def _clean(value: Any) -> Any:
    """Return None for any invalid / loading cell value."""
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if s in _INVALID_VALUES:
            return None
        if s.startswith(_INVALID_PREFIXES):
            return None  # catches '#N/A (Historical GOOGLEFINANCE...)', '#REF!', etc.
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

    # The sheet may have blank rows before the actual header.
    # Find the header row by looking for a row that contains 'CMP'.
    # Expected columns (0-indexed):
    # 0: NSECode, 1: CMP, 2: 50DMA, 3: 100DMA, 4: 200DMA,
    # 5: Output, 6: Diff from 200 DMA, 7: CAR Rating
    header_idx = 0
    for i, row in enumerate(rows):
        if any(isinstance(cell, str) and cell.strip().upper() == "CMP" for cell in row):
            header_idx = i
            break

    stocks = []
    for row in rows[header_idx + 1:]:
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

    Sheet layout:
      Row 0 — metadata (ignored)
      Row 1 — category labels in each column, e.g. "(1)NIFTY MICROCAP 250"
              Column 1 has "(16)FnOSTOCKS" (master list of ticker names)
              Columns 5+ have index category labels
      Row 2+ — tickers listed below each category column
    """
    rows = _read_sheet("LIST")
    if not rows or len(rows) < 3:
        return {}

    # Row 1 contains category names
    header_row = rows[1]
    category_map: dict[str, list[str]] = {}

    for col_idx, header in enumerate(header_row):
        raw = _clean(header)
        if raw is None:
            continue
        cat_name = str(raw).strip()
        if not cat_name:
            continue

        # Only process columns that have a category label (starts with "(")
        if not cat_name.startswith("("):
            continue

        # Strip the "(N)" prefix to get the display name, e.g. "(1)NIFTY MICROCAP 250" → "NIFTY MICROCAP 250"
        paren_end = cat_name.find(")")
        if paren_end != -1:
            display_name = cat_name[paren_end + 1:].strip()
        else:
            display_name = cat_name

        if not display_name:
            continue

        tickers: list[str] = []
        for row in rows[2:]:  # Data starts at row 2
            if col_idx >= len(row):
                continue
            cell = _clean(row[col_idx])
            if cell is None:
                continue
            # Tickers in category columns are plain names (no NSE: prefix)
            t = str(cell).strip().upper()
            if t and not t.startswith("("):
                tickers.append(t)

        if tickers:
            category_map[display_name] = tickers

    logger.info(
        "Built category map with %d categories", len(category_map)
    )
    return category_map
