"""Google Sheets utilities for the Streamlit dashboard.

Used by the Allocation Manager page to delete rows from agent sheets
when an allocation is removed from the DB.
"""

from __future__ import annotations

import os
from pathlib import Path

import gspread

# dashboard/utils/sheets.py → .parent = utils → .parent = dashboard → .parent = SQL Migration (project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# Tab names — must match what allocation_engine.py writes
SUBSHEET_NAMES = {
    0: "New Contact",
    1: "FU1",
    2: "FU2",
    3: "FU3",
    4: "FU4",
    5: "FU5",
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _find_creds_file() -> str:
    """Locate credentials.json — env var first, then project root/credentials.json."""
    load_dotenv()
    creds_file = os.getenv("GOOGLE_CREDS_FILE", "")
    if creds_file and Path(creds_file).exists():
        return creds_file
    default = PROJECT_ROOT / "credentials.json"
    if default.exists():
        return str(default)
    raise FileNotFoundError(
        "credentials.json not found. "
        "Set GOOGLE_CREDS_FILE in your .env or place credentials.json in the project root."
    )


def get_gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_file(_find_creds_file(), scopes=SCOPES)
    return gspread.authorize(creds)


def delete_contact_from_sheet(sheet_id: str, unique_id: str) -> tuple[bool, str]:
    """
    Search all 6 tabs of an agent's sheet for a row whose column-A value
    matches unique_id (e.g. 'RR | 75863932') and delete that row.

    Searches all tabs so the caller doesn't need to know which tab the contact
    landed in (tab is determined by attempt_number at allocation time).

    Returns (success: bool, message: str).
    """
    if not sheet_id:
        return False, "No sheet ID configured for this agent."

    try:
        gc = get_gspread_client()
    except FileNotFoundError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Could not connect to Google Sheets: {e}"

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        return False, f"Cannot open sheet (id={sheet_id}): {e}"

    uid = unique_id.strip()

    for tab_name in SUBSHEET_NAMES.values():
        try:
            ws = sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            continue

        try:
            col_a = ws.col_values(1)  # includes header at index 0
        except Exception:
            continue

        # Skip header row (index 0 = row 1), data starts at index 1 = row 2
        for idx, val in enumerate(col_a[1:], start=2):
            if val.strip() == uid:
                ws.delete_rows(idx)
                return True, f"Deleted from '{tab_name}' tab (row {idx})."

    return (
        False,
        f"'{unique_id}' not found in any sheet tab. "
        "It may have already been deleted from the sheet manually.",
    )


def sheet_is_configured(sheet_id: str | None) -> bool:
    return bool(sheet_id and sheet_id.strip())
