"""
mql_pro_sheet_backfill.py
==========================
Backfill historical MQL call data from row-per-call CSV into agent Google Sheets.

The CSV file contains historical calls that have already been migrated to the DB
(in mql_call_attempts table), but NOT yet written to the agent Google Sheets.

This script:
1. Reads historical CSV (default: data/mql_pro_from_team.csv)
2. For each row, finds the corresponding allocation (agent + contact)
3. Determines the FU stage number
4. Writes to the agent's Google Sheet in the appropriate FU block
5. Marks rows as synced (✓) to avoid re-inserting

Sheet structure per agent:
  Tab "MQL FU 1-15"  → Contact info (12 cols) + FU1-FU15 blocks (11 cols each)
  Tab "MQL FU 16-30" → Contact info (12 cols) + FU16-FU30 blocks (11 cols each)

Each FU block:
  FU#_Category | FU#_Call Status | FU#_Current State | FU#_Call Duration |
  FU#_Remark | FU#_Recording Link | FU#_Transcript | FU#_Message Status |
  FU#_Timestamp | FU#_Follow-up Stage | FU#_Sync Status

Usage:
  python mql_pro_sheet_backfill.py --dry-run
  python mql_pro_sheet_backfill.py --apply
  python mql_pro_sheet_backfill.py --apply --agent-id 5

Dependencies: PostgreSQL, gspread (Google Sheets API)
"""

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import gspread
import psycopg2
import psycopg2.extras
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── LOAD ENV FIRST ────────────────────────────────────────────
load_dotenv(PROJECT_ROOT / ".env")

# ── CONFIG ────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "crm_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

GOOGLE_CREDS_FILE = os.getenv(
    "GOOGLE_CREDS_FILE", str(PROJECT_ROOT / "credentials.json")
)

CSV_FILE = PROJECT_ROOT / "data" / "mql_pro.csv"
DEFAULT_TEAM_CSV_FILE = PROJECT_ROOT / "data" / "mql_pro_from_team.csv"

MQL_TAB_1 = "MQL FU 1-15"
MQL_TAB_2 = "MQL FU 16-30"

CONTACT_COLS = 12  # A-L: contact info
FU_BLOCK_SIZE = 11  # columns per FU block

# FU block column offsets
FU_CATEGORY = 0
FU_CALL_STATUS = 1
FU_CURRENT_STATE = 2
FU_CALL_DURATION = 3
FU_REMARK = 4
FU_RECORDING_LINK = 5
FU_TRANSCRIPT = 6
FU_MESSAGE_STATUS = 7
FU_TIMESTAMP = 8
FU_FOLLOW_STAGE = 9
FU_SYNC_STATUS = 10

CONTACT_HEADERS = [
    "Unique ID",
    "Company Name",
    "Person Name",
    "Phone",
    "Email",
    "BD Agent",
    "BD Call Date",
    "BD Remark",
    "BD Recording Link",
    "Category",
    "BD Transcript",
    "Dream Snapshot",
]

FU_BLOCK_HEADERS = [
    "MQL Category",
    "Call Status",
    "Current State",
    "Call Duration",
    "Remark",
    "Recording Link",
    "Transcript",
    "Message Status",
    "Timestamp",
    "Follow-up Stage",
    "Sync Status",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("mql.backfill")


# ── DATABASE ──────────────────────────────────────────────────
def get_connection():
    """Connect to PostgreSQL with explicit password handling."""
    try:
        # Build connection string explicitly
        conn_params = {
            "host": DB_CONFIG["host"],
            "port": int(DB_CONFIG["port"]),
            "database": DB_CONFIG["dbname"],
            "user": DB_CONFIG["user"],
        }
        
        # Only add password if it exists and is not empty
        if DB_CONFIG["password"] and DB_CONFIG["password"].strip():
            conn_params["password"] = DB_CONFIG["password"]
        
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        return conn
        
    except psycopg2.OperationalError as e:
        error_msg = str(e).lower()
        if "password" in error_msg or "authentication" in error_msg:
            log.error(f"✗ Password authentication failed")
            log.error(f"  Check .env: DB_PASSWORD={DB_CONFIG['password'][:3]}{'*' * max(0, len(DB_CONFIG['password'])-3) if DB_CONFIG['password'] else '[empty]'}")
            log.error(f"  Connection attempt: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        else:
            log.error(f"Database connection failed: {e}")
        sys.exit(1)
    except (psycopg2.Error, ValueError) as e:
        log.error(f"Failed to connect to database: {e}")
        sys.exit(1)


def find_allocation(
    conn,
    unique_id: str,
    assigned_agent: str,
    campaign: str = "consulting",
) -> Optional[dict]:
    """
    Find the mql_allocation row for a contact by Unique ID and agent name.

    Returns: dict with keys: contact_id, allocation_id, agent_id, agent_name, sheet_id
    Or None if not found.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Parse Unique ID (e.g. "RR | 62462790" -> source="rocketreach", source_id="62462790")
        source = None
        source_id = None

        if " | " in unique_id:
            prefix, source_id = unique_id.split(" | ", 1)
            prefix_map = {
                "RR": "rocketreach",
                "MS": "msme",
                "PH": "pharma",
                "MN": "manual",
                "BD": "manual",  # legacy
                "CC": "manual",  # legacy
            }
            source = prefix_map.get(prefix, "rocketreach")
        elif unique_id.startswith("ID-"):
            # Legacy IDs often come as ID-... and map to manual source records.
            source = "manual"
            source_id = unique_id
        else:
            log.warning(f"Invalid Unique ID format: {unique_id}")
            return None

        try:
            cur.execute(
                f"""
                SELECT
                    c.id            AS contact_id,
                    ma.id           AS allocation_id,
                    ma.agent_id,
                    a.name          AS agent_name,
                    ags.sheet_id
                FROM contacts c
                JOIN mql_allocations ma ON ma.contact_id = c.id
                JOIN agents a ON a.id = ma.agent_id
                LEFT JOIN agent_sheets ags
                    ON ags.agent_id = a.id
                    AND ags.campaign = %s
                    AND ags.sheet_type = 'calling'
                WHERE c.source = %s
                  AND c.source_id = %s
                                    AND LOWER(TRIM(a.name)) = LOWER(TRIM(%s))
                                ORDER BY ma.allocated_at DESC, ma.id DESC
                LIMIT 1
                """,
                (campaign, source, source_id, assigned_agent),
            )
            result = cur.fetchone()
            if result:
                return dict(result)
            return None
        except psycopg2.Error as e:
            log.error(f"Query error finding allocation: {e}")
            return None


def parse_fu_number(followup_stage: str) -> Optional[int]:
    """
    Parse follow-up number from text like "Follow-up 1", "FU1", "FU 2", etc.
    Returns 1-30, or None if invalid.
    """
    if not followup_stage:
        return None

    followup_stage = followup_stage.strip().lower()

    # Try "follow-up X" format
    if "follow-up" in followup_stage:
        parts = followup_stage.split()
        if len(parts) >= 2:
            try:
                return int(parts[-1])
            except ValueError:
                pass

    # Try "fu X" or "fuX" format
    if followup_stage.startswith("fu"):
        try:
            return int(followup_stage[2:].strip())
        except ValueError:
            pass

    # Try plain numeric stage like "1", "2", ..., "30"
    if followup_stage.isdigit():
        try:
            return int(followup_stage)
        except ValueError:
            pass

    return None


def normalize_date(date_str: str) -> Optional[str]:
    """
    Parse various date formats and return ISO format (YYYY-MM-DD).
    CSV has format: DD/MM/YYYY
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    try:
        # Try DD/MM/YYYY or DD-MM-YYYY
        for sep in ("/", "-"):
            if sep in date_str:
                parts = date_str.split(sep)
                if len(parts) == 3:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    dt = datetime(year, month, day)
                    return dt.strftime("%Y-%m-%d")
        return None
    except (ValueError, AttributeError):
        return None


def get_fu_stage_text(row_data: dict) -> str:
    """Return FU stage text from CSV, falling back to Call Type when Followups is blank."""
    followups = (row_data.get("Followups", "") or row_data.get("Follow-up Stage", "") or "").strip()
    if followups:
        return followups

    call_type = (row_data.get("Call Type", "") or "").strip()
    if call_type:
        return call_type

    return ""


def build_fu_row(row_data: dict) -> list:
    """
    Build the 11-column FU block row from CSV.

    Input dict keys:
      Call Type (category)
      Call Status
      Current State
      Call Duration
      Remark
      Recording Link
      Transcript
      Message Status (unused)
      Date (timestamp)
      Follow-ups (FU stage)

    Returns: [category, call_status, current_state, call_duration, remark,
              recording_link, transcript, message_status, timestamp, fu_stage, sync_status]
    """
    return [
        row_data.get("Lead Category", ""),  # category (MQL Category)
        row_data.get("Call Status", ""),  # call_status
        row_data.get("Current State", ""),  # current_state
        row_data.get("Call Duration", ""),  # call_duration (will be empty in CSV)
        row_data.get("Remark", ""),  # remark
        row_data.get("Recording Link", ""),  # recording_link
        row_data.get("Transcript", ""),  # transcript
        "",  # message_status (not in CSV, leave blank)
        normalize_date(row_data.get("Date", "")) or "",  # timestamp
        get_fu_stage_text(row_data),  # follow-up stage text
        "✓ Synced",  # sync_status (mark as already synced)
    ]


def build_tab_headers(fu_start: int, fu_end: int) -> list[str]:
    """Build full header row for a tab covering fu_start..fu_end."""
    headers = list(CONTACT_HEADERS)
    for fu_num in range(fu_start, fu_end + 1):
        for col in FU_BLOCK_HEADERS:
            headers.append(f"FU{fu_num} — {col}")
    return headers


def build_contact_row_from_csv(row_data: dict, assigned_agent: str) -> list[str]:
    """Build a fallback 12-column contact row from CSV when DB context is unavailable."""
    person_name = (row_data.get("Person Name", "") or "").strip()
    company_name = (row_data.get("Company Name", "") or "").strip()
    phone = (row_data.get("Phone No", "") or "").strip()
    email = (row_data.get("Email", "") or "").strip()
    bd_call_date = normalize_date(row_data.get("Date", "")) or ""
    bd_remark = (row_data.get("Remark", "") or "").strip()
    bd_recording = (row_data.get("Recording Link", "") or "").strip()
    category = (row_data.get("Category", "") or "").strip()
    bd_transcript = (row_data.get("Transcript", "") or "").strip()
    snapshot = (row_data.get("Dream Snapshot", "") or "").strip()

    return [
        (row_data.get("Unique ID", "") or "").strip(),
        company_name,
        person_name,
        phone,
        email,
        assigned_agent,
        bd_call_date,
        bd_remark,
        bd_recording,
        category,
        bd_transcript,
        snapshot,
    ]


def build_contact_row_from_db(conn, alloc: dict, row_data: dict) -> list[str]:
    """
    Build the 12 contact-info columns from DB so BD context stays accurate.
    Falls back to CSV values only when DB values are missing.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                c.source,
                c.source_id,
                c.first_name,
                c.last_name,
                c.campaign,
                c.bd_category,
                co.name AS company_name,
                ph.phone_number,
                em.email,
                ag.name AS bd_agent_name,
                ca.called_at,
                ca.remark AS bd_remark,
                ca.recording_link AS bd_recording_link,
                ca.transcript_link AS bd_transcript,
                ca.dream_snapshot_link AS bd_snapshot
            FROM contacts c
            LEFT JOIN companies co ON co.id = c.company_id
            LEFT JOIN LATERAL (
                SELECT cp.phone_number
                FROM contact_phones cp
                WHERE cp.contact_id = c.id AND cp.is_invalid = FALSE
                ORDER BY cp.rank
                LIMIT 1
            ) ph ON TRUE
            LEFT JOIN LATERAL (
                SELECT ce.email
                FROM contact_emails ce
                WHERE ce.contact_id = c.id
                ORDER BY ce.rank
                LIMIT 1
            ) em ON TRUE
            LEFT JOIN LATERAL (
                SELECT ca2.*
                FROM call_actions ca2
                WHERE ca2.contact_id = c.id
                ORDER BY ca2.called_at DESC, ca2.id DESC
                LIMIT 1
            ) ca ON TRUE
            LEFT JOIN agents ag ON ag.id = ca.agent_id
            WHERE c.id = %s
            """,
            (alloc["contact_id"],),
        )
        info = cur.fetchone()

    # Hard fallback to old behavior if contact is unexpectedly missing.
    if not info:
        return build_contact_row_from_csv(row_data, alloc.get("agent_name", ""))

    source = (info["source"] or "").strip().lower()
    source_id = (info["source_id"] or "").strip()
    pfx_map = {
        "rocketreach": "RR",
        "msme": "MS",
        "pharma": "PH",
        "manual": "MN",
    }
    unique_id = (row_data.get("Unique ID", "") or "").strip()
    if not unique_id and source_id:
        if source == "manual" and "-" in source_id:
            legacy_pfx, rest = source_id.split("-", 1)
            if legacy_pfx in {"BD", "CC", "AV", "BW"}:
                unique_id = f"{legacy_pfx} | {rest}"
            else:
                unique_id = f"MN | {source_id}"
        else:
            unique_id = f"{pfx_map.get(source, 'RR')} | {source_id}"

    first = (info["first_name"] or "").strip()
    last = (info["last_name"] or "").strip()
    person_name = f"{first} {last}".strip() or (row_data.get("Person Name", "") or "").strip()
    company_name = (info["company_name"] or "").strip() or (row_data.get("Company Name", "") or "").strip()
    phone = (info["phone_number"] or "").strip() or (row_data.get("Phone No", "") or "").strip()
    email = (info["email"] or "").strip() or (row_data.get("Email", "") or "").strip()
    bd_agent = (info["bd_agent_name"] or "").strip()
    bd_call_date = info["called_at"].strftime("%Y-%m-%d") if info["called_at"] else ""
    bd_remark = (info["bd_remark"] or "").strip() or (row_data.get("Remark", "") or "").strip()
    bd_recording = (info["bd_recording_link"] or "").strip() or (row_data.get("Recording Link", "") or "").strip()
    bd_transcript = (info["bd_transcript"] or "").strip() or (row_data.get("Transcript", "") or "").strip()
    snapshot = (info["bd_snapshot"] or "").strip() or (row_data.get("Dream Snapshot", "") or "").strip()
    category = (info["bd_category"] or info["campaign"] or "").strip() or (row_data.get("Category", "") or "").strip()

    return [
        unique_id,
        company_name,
        person_name,
        phone,
        email,
        bd_agent,
        bd_call_date,
        bd_remark,
        bd_recording,
        category,
        bd_transcript,
        snapshot,
    ]


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def call_with_retry(func, *args, max_retries: int = 5, base_delay: float = 1.0, **kwargs):
    """Retry gspread operations on temporary quota/rate-limit errors."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            text = str(e)
            is_quota_error = (
                "429" in text
                or "Quota exceeded" in text
                or "rate limit" in text.lower()
            )
            if not is_quota_error or attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)
            log.warning(f"  Sheets API throttled (429). Retrying in {delay:.1f}s...")
            time.sleep(delay)


def find_fu_block_column(headers: list[str], fu_num: int) -> Optional[int]:
    """
    Find the column number (0-indexed) where FU<fu_num> block starts.

    FU blocks start at: CONTACT_COLS + (fu_num-1) * FU_BLOCK_SIZE
    Column index of first column in block.

    Returns column number (0-indexed), or None if not found.
    """
    # Expected header for FU<fu_num>: "FU{fu_num} — MQL Category"
    for i, header in enumerate(headers):
        if f"FU{fu_num} —" in header and "MQL Category" in header:
            return i
    return None


def find_contact_in_sheet(ws, unique_id: str) -> Optional[int]:
    """
    Find row number (1-indexed) in sheet where contact's Unique ID is located.
    Searches column A (Unique ID).

    Returns row number (1-indexed), or None if not found.
    """
    col_a = ws.col_values(1)  # 1-indexed, returns list of strings
    for i, val in enumerate(col_a):
        if val.strip() == unique_id.strip():
            return i + 1  # Convert to 1-indexed row number
    return None


def ensure_mql_tabs(sh, agent_name: str):
    """Create/repair MQL FU tabs with expected headers if needed."""
    existing = [ws.title for ws in sh.worksheets()]

    for tab_name, fu_start, fu_end in [
        (MQL_TAB_1, 1, 15),
        (MQL_TAB_2, 16, 30),
    ]:
        headers = build_tab_headers(fu_start, fu_end)
        total_cols = len(headers)

        if tab_name not in existing:
            ws = sh.add_worksheet(title=tab_name, rows=2000, cols=total_cols + 5)
            ws.update(range_name="A1", values=[headers])
            log.info(f"  [{agent_name}] Created tab: {tab_name} ({total_cols} cols)")
        else:
            ws = sh.worksheet(tab_name)
            existing_h = ws.row_values(1)
            if len(existing_h) != total_cols or existing_h[0] != headers[0]:
                ws.update(range_name="A1", values=[headers])
                log.info(f"  [{agent_name}] Updated header: {tab_name}")


def write_fu_to_sheet(
    gc,
    sheet_id: str,
    agent_name: str,
    unique_id: str,
    fu_num: int,
    fu_row_data: list,
    source_row: dict,
    contact_row_data: list,
    write_fu_block: bool = True,
    dry_run: bool = False,
    cache: Optional[dict] = None,
) -> bool:
    """
    Write FU block data to agent's Google Sheet.

    Args:
        gc: gspread client
        sheet_id: spreadsheet ID
        agent_name: for logging
        unique_id: contact's Unique ID (to find row)
        fu_num: FU stage number (1-30)
        fu_row_data: 11-element list for FU block
        source_row: original CSV row (used to insert contact row if missing)
        dry_run: if True, don't actually write

    Returns: True if successful, False otherwise
    """
    if cache is None:
        cache = {}

    sheet_cache = cache.setdefault("sheet", {})
    ws_cache = cache.setdefault("worksheet", {})
    headers_cache = cache.setdefault("headers", {})
    contacts_cache = cache.setdefault("contacts", {})
    refreshed_contacts = cache.setdefault("refreshed_contacts", set())
    missing_tabs = cache.setdefault("missing_tabs", set())
    initialized_tabs = cache.setdefault("initialized_tabs", set())

    try:
        sh = sheet_cache.get(sheet_id)
        if sh is None:
            sh = call_with_retry(gc.open_by_key, sheet_id)
            sheet_cache[sheet_id] = sh
    except gspread.exceptions.SpreadsheetNotFound:
        log.error(f"  Sheet not found for {agent_name} (ID={sheet_id[:12]}...)")
        return False
    except Exception as e:
        log.error(f"  Cannot open sheet for {agent_name}: {e}")
        return False

    # Choose tab based on FU number
    tab_name = MQL_TAB_1 if fu_num <= 15 else MQL_TAB_2
    ws_key = (sheet_id, tab_name)

    if ws_key in missing_tabs:
        return False

    try:
        ws = ws_cache.get(ws_key)
        if ws is None:
            ws = call_with_retry(sh.worksheet, tab_name)
            ws_cache[ws_key] = ws
    except gspread.exceptions.WorksheetNotFound:
        if sheet_id not in initialized_tabs:
            call_with_retry(ensure_mql_tabs, sh, agent_name)
            initialized_tabs.add(sheet_id)
            # Clear cached negatives and retry once.
            missing_tabs.discard((sheet_id, MQL_TAB_1))
            missing_tabs.discard((sheet_id, MQL_TAB_2))
            ws = call_with_retry(sh.worksheet, tab_name)
            ws_cache[ws_key] = ws
        else:
            missing_tabs.add(ws_key)
            log.warning(f"  Tab {tab_name} not found in {agent_name} sheet, skipping")
            return False

    # Find contact row
    contact_rows = contacts_cache.get(ws_key)
    if contact_rows is None:
        col_a = call_with_retry(ws.col_values, 1)
        contact_rows = {v.strip(): idx + 1 for idx, v in enumerate(col_a) if v and v.strip()}
        contacts_cache[ws_key] = contact_rows

    row_num = contact_rows.get(unique_id.strip())
    if not row_num:
        # Insert a new contact row so historical FU logs can be backfilled.
        if dry_run:
            log.info(
                f"  [DRY RUN] Would add missing contact {unique_id} in {agent_name}/{tab_name}"
            )
            return True

        contact_row = contact_row_data
        total_cols = CONTACT_COLS + 15 * FU_BLOCK_SIZE
        row_payload = contact_row + ([""] * (total_cols - CONTACT_COLS))
        call_with_retry(ws.append_row, row_payload, value_input_option="USER_ENTERED")

        # Refresh local contact map and fetch inserted row number.
        col_a = call_with_retry(ws.col_values, 1)
        contact_rows = {v.strip(): idx + 1 for idx, v in enumerate(col_a) if v and v.strip()}
        contacts_cache[ws_key] = contact_rows
        row_num = contact_rows.get(unique_id.strip())
        if not row_num:
            log.warning(f"  Contact {unique_id} could not be inserted in {agent_name} {tab_name}")
            return False
        log.info(f"  [{agent_name}] Added missing contact row for {unique_id} in {tab_name}")

    # Keep contact columns aligned with DB (fixes stale/incorrect BD metadata),
    # but refresh each contact row once per run to avoid write-quota spikes.
    refresh_key = (sheet_id, tab_name, row_num)
    if refresh_key not in refreshed_contacts:
        if dry_run:
            log.debug(f"  [DRY RUN] Would refresh contact columns A-L for {unique_id} in {agent_name}/{tab_name}")
        else:
            call_with_retry(ws.update, f"A{row_num}:L{row_num}", [contact_row_data])
        refreshed_contacts.add(refresh_key)

    if not write_fu_block:
        if dry_run:
            log.info(
                f"  [DRY RUN] Refreshed contact columns for {unique_id} in {agent_name}/{tab_name}"
            )
        return True

    # Find FU block column
    headers = headers_cache.get(ws_key)
    if headers is None:
        headers = call_with_retry(ws.row_values, 1)
        headers_cache[ws_key] = headers

    col_num = find_fu_block_column(headers, fu_num)
    if col_num is None:
        log.warning(f"  FU{fu_num} block not found in {agent_name} {tab_name}, skipping")
        return False

    if dry_run:
        log.info(
            f"  [DRY RUN] Would write FU{fu_num} for {unique_id} at "
            f"{agent_name}/{tab_name}!{rowcol_to_a1(row_num, col_num + 1)}"
        )
        return True

    # Write FU block (11 cells horizontally)
    try:
        start_cell = rowcol_to_a1(row_num, col_num + 1)
        end_cell = rowcol_to_a1(row_num, col_num + FU_BLOCK_SIZE)
        call_with_retry(ws.update, f"{start_cell}:{end_cell}", [fu_row_data])
        start_cell = rowcol_to_a1(row_num, col_num + 1)
        log.info(
            f"  [{agent_name}] FU{fu_num} written for {unique_id} at {tab_name}!{start_cell}"
        )
        return True

    except Exception as e:
        log.error(f"  Failed to write FU{fu_num} for {unique_id}: {e}")
        return False


# ── CSV PROCESSING ───────────────────────────────────────────
def read_csv(csv_path: Path) -> list[dict]:
    """
    Read mql_pro.csv and return list of row dicts.
    Expected columns: Unique ID, Call Status, Current State, Date, Followups, etc.
    """
    rows = []
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                log.error(f"CSV file {csv_path} is empty or unreadable")
                return []

            for i, row in enumerate(reader, start=2):  # start=2 because row 1 is header
                rows.append(row)

        log.info(f"Read {len(rows)} rows from {csv_path.name}")
        return rows
    except Exception as e:
        log.error(f"Failed to read CSV: {e}")
        return []


def backfill_mql_sheets(
    csv_path: Path,
    dry_run: bool = False,
    agent_id_filter: Optional[int] = None,
    refresh_contact_only: bool = False,
) -> tuple[int, int]:
    """
    Main backfill logic.

    Args:
        dry_run: if True, don't write to sheets
        agent_id_filter: if provided, only backfill this agent

    Returns: (total_rows_processed, rows_written)
    """
    if not csv_path.exists():
        log.error(f"CSV file not found: {csv_path}")
        return 0, 0

    log.info(f"\n{'=' * 70}")
    log.info(f"Starting MQL Pro backfill...")
    log.info(f"CSV source: {csv_path}")
    log.info(f"{'=' * 70}\n")

    if dry_run:
        log.info("[DRY RUN MODE] — No sheets will be modified\n")

    # Read CSV
    csv_rows = read_csv(csv_path)
    if not csv_rows:
        return 0, 0

    # Connect to DB
    conn = get_connection()
    gc = get_gspread_client()

    total_processed = 0
    total_written = 0

    try:
        agents_processed = set()
        cache = {}

        for i, csv_row in enumerate(csv_rows, start=2):  # row 2 in file
            total_processed += 1

            # Extract key fields
            unique_id = csv_row.get("Unique ID", "").strip()
            assigned_agent = csv_row.get("Assigned", "").strip()
            followup_stage = get_fu_stage_text(csv_row)

            if not unique_id or not assigned_agent:
                log.warning(f"  Row {i}: Missing Unique ID or Assigned agent, skipping")
                continue

            # Parse FU number
            if refresh_contact_only:
                fu_num = 1  # use tab1 for contact refresh path
            else:
                fu_num = parse_fu_number(followup_stage)
                if not fu_num or fu_num < 1 or fu_num > 30:
                    log.warning(
                        f"  Row {i}: Invalid FU number '{followup_stage}' for {unique_id}, skipping"
                    )
                    continue

            # Find allocation
            alloc = find_allocation(conn, unique_id, assigned_agent)
            if not alloc:
                log.debug(
                    f"  Row {i}: No allocation found for {unique_id} under {assigned_agent}, skipping"
                )
                continue

            if agent_id_filter is not None and alloc.get("agent_id") != agent_id_filter:
                continue

            if not alloc.get("sheet_id"):
                log.warning(
                    f"  Row {i}: No sheet_id for agent {alloc['agent_name']}, skipping"
                )
                continue

            # Build FU row
            fu_row = build_fu_row(csv_row)
            contact_row = build_contact_row_from_db(conn, alloc, csv_row)

            # Write to sheet
            success = write_fu_to_sheet(
                gc,
                alloc["sheet_id"],
                alloc["agent_name"],
                unique_id,
                fu_num,
                fu_row,
                csv_row,
                contact_row,
                write_fu_block=not refresh_contact_only,
                dry_run=dry_run,
                cache=cache,
            )

            if success or dry_run:
                total_written += 1
                agents_processed.add(alloc["agent_name"])

        log.info(f"\n{'=' * 70}")
        log.info(f"Backfill complete:")
        log.info(f"  Rows processed: {total_processed}")
        log.info(f"  Rows written: {total_written}")
        log.info(f"  Agents touched: {len(agents_processed)}")
        log.info(f"{'=' * 70}\n")

        return total_processed, total_written

    except Exception as e:
        log.error(f"Backfill failed: {e}")
        return total_processed, total_written
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical MQL calls from CSV to agent Google Sheets"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backfilled without modifying sheets",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (required to actually write to sheets)",
    )
    parser.add_argument(
        "--agent-id",
        type=int,
        help="Backfill only a specific agent",
    )
    parser.add_argument(
        "--refresh-contact-only",
        action="store_true",
        help="Only refresh contact columns A-L from DB (BD Agent/BD links), do not write FU blocks",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Input row-per-call CSV path (default: data/mql_pro_from_team.csv if present, else data/mql_pro.csv)",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        log.error("Must specify either --dry-run or --apply")
        parser.print_help()
        sys.exit(1)

    csv_path = Path(args.file) if args.file else (DEFAULT_TEAM_CSV_FILE if DEFAULT_TEAM_CSV_FILE.exists() else CSV_FILE)

    processed, written = backfill_mql_sheets(
        csv_path=csv_path,
        dry_run=args.dry_run,
        agent_id_filter=args.agent_id,
        refresh_contact_only=args.refresh_contact_only,
    )

    if args.apply and written > 0:
        log.info(f"✓ Successfully backfilled {written} MQL records to agent sheets")

    sys.exit(0)


if __name__ == "__main__":
    main()
