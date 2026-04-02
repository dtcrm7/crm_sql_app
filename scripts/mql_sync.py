"""
mql_sync.py
===========
Reads MQL agent Google Sheets and syncs filled follow-up data to the DB.

Run BEFORE mql_allocation_engine.py each morning:
  01:00 AM → mql_sync.py           (reads outcomes → DB)
  02:00 AM → mql_allocation_engine (writes today's contacts → sheets)
  03:00 PM → MQL agents start shift

Sheet structure per agent:
  Tab "MQL FU 1-15"  → rows = contacts, columns = FU1-FU15 (11 cols each after 12 contact cols)
  Tab "MQL FU 16-30" → rows = contacts, columns = FU16-FU30 (11 cols each after 12 contact cols)

For each row, the script:
  1. Reads each FU block (checks Timestamp column)
  2. If Timestamp filled and Sync Status not "✓ Synced":
       a. Inserts row into mql_call_attempts
       b. Updates contact_flag based on outcome
       c. If final outcome (Meeting Scheduled / Not interested / max FU30):
            → closes mql_allocation with correct close_reason
            → updates mql_analysis outcome
  3. Marks FU block Sync Status = "✓ Synced"

Usage:
  python mql_sync.py                       # sync all unsynced rows
  python mql_sync.py --dry-run             # preview only
  python mql_sync.py --date 2026-03-20     # sync specific date
  python mql_sync.py --campaign consulting
"""

import os
import re
import sys
import logging
import argparse
import unicodedata
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import gspread
from google.oauth2.service_account import Credentials

from sheet_values_config import (
    MQL_CALL_STATUS_ALIASES,
    MQL_CLOSE_QUALIFIED_STATES,
    MQL_CLOSE_REJECTED_STATES,
    MQL_CURRENT_STATE_ALIASES,
)

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── CONFIG ────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "crm_db"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

GOOGLE_CREDS_FILE = os.getenv(
    "GOOGLE_CREDS_FILE", str(PROJECT_ROOT / "credentials.json")
)
MEETING_SHEET_ID = os.getenv("MEETING_SHEET_ID", "")

MQL_TAB_1 = "MQL FU 1-15"
MQL_TAB_2 = "MQL FU 16-30"
MEETING_CONTEXT_TAB = "Meeting Context"
MEETING_CONTEXT_NORM_TAB = "Meeting Context Normalized"

CONTACT_COLS  = 12
FU_BLOCK_SIZE = 11

# Offsets within a FU block (relative to block start)
FU_LEAD_CATEGORY  = 0
FU_CALL_STATUS    = 1
FU_CURRENT_STATE  = 2
FU_CALL_DURATION  = 3
FU_REMARK         = 4
FU_RECORDING_LINK = 5
FU_TRANSCRIPT     = 6
FU_MESSAGE_STATUS = 7
FU_TIMESTAMP      = 8
FU_FOLLOW_STAGE   = 9
FU_SYNC_STATUS    = 10

# Map tab → FU range
TAB_FU_RANGE = {
    MQL_TAB_1: (1, 15),
    MQL_TAB_2: (16, 30),
}

DASH_VARIANTS_RE = r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212]"
ALT_REMARK_SEGMENT_RE = re.compile(r"(?:\s*\|\s*)?ALT\s*:\s*[0-9,\s()+-]*", re.IGNORECASE)

# Call statuses that close the MQL allocation
# Meeting Scheduled does NOT close the allocation — same agent continues in same sheet
MQL_CLOSE_QUALIFIED = set(MQL_CLOSE_QUALIFIED_STATES)
MQL_CLOSE_REJECTED  = set(MQL_CLOSE_REJECTED_STATES)
MQL_MAX_FU          = 30


# ── NORMALISATIONS ────────────────────────────────────────────
CALL_STATUS_NORM = dict(MQL_CALL_STATUS_ALIASES)
CURRENT_STATE_NORM = dict(MQL_CURRENT_STATE_ALIASES)

FLAG_MAP = {
    # Meeting track — allocation stays open, same agent continues in same sheet
    "Meeting Scheduled":            "meeting_in_progress",
    "Meeting Held":                 "meeting_in_progress",
    "Solution Sent":                "meeting_in_progress",
    # SQL outcomes — close allocation
    "Solution Picked":              "mql_qualified",
    "Picked Solution":              "mql_qualified",
    # Rejected outcomes
    "Not interested":               "mql_rejected",
    "Do not Disturb":               "mql_rejected",
    "Attempt Again after 3 months": "mql_rejected",
    "Allocate Again 3 months":      "attempt_3_months",
    "Reffered":                     "mql_rejected",
    "Irrelevant":                   "mql_rejected",
}


# ── Meeting sheet columns (A=0 … R=17) ───────────────────────
MTG_MEETING_ID    = 0   # A — DB meetings.id (written by mql_sync)
MTG_COMPANY       = 1   # B — company name
MTG_PERSON        = 2   # C — contact full name
MTG_PHONE         = 3   # D — phone used
MTG_EMAIL         = 4   # E — email
MTG_AGENT         = 5   # F — MQL agent name
MTG_CAMPAIGN      = 6   # G — campaign
MTG_SCHEDULED     = 7   # H — scheduled date (from FU timestamp)
MTG_FU_AT_SCHED   = 8   # I — MQL FU# where scheduled
MTG_BD_REMARK     = 9   # J — BD remark (context)
MTG_BD_SNAPSHOT   = 10  # K — BD snapshot link
MTG_MQL_REMARK    = 11  # L — last MQL remark (when meeting was scheduled)
# ── Agent fills below ─────────────────────────────────────────
MTG_MEETING_DATE  = 12  # M — actual meeting date
MTG_DURATION      = 13  # N — meeting duration (minutes)
MTG_CONCLUSION    = 14  # O — problems identified / conclusion
MTG_SOLUTION      = 15  # P — solution proposed
MTG_SOL_LINK      = 16  # Q — solution link
MTG_SYNC_STATUS   = 17  # R — "✓ Synced DD/MM HH:MM" (meeting_sync.py)

MEETING_SHEET_HEADERS = [
    "Meeting ID", "Company", "Person", "Phone", "Email",
    "MQL Agent", "Campaign", "Scheduled Date", "MQL FU#",
    "BD Remark", "BD Snapshot Link", "Last MQL Remark",
    "Meeting Date", "Duration (min)", "Problems Identified",
    "Solution Proposed", "Solution Link", "Sync Status",
]


def build_unique_id(source: str, source_id: str | None) -> str:
    """Build display ID like RR | 6175204 or CC | 9723049295."""
    if source_id:
        for pfx in ("BD", "CC", "AV", "BW"):
            if source_id.startswith(f"{pfx}-"):
                return f"{pfx} | {source_id[len(pfx)+1:]}"
        if source_id.startswith("ID-"):
            return source_id

    prefix_map = {
        "rocketreach": "RR",
        "msme": "MS",
        "pharma": "PH",
        "manual": "MN",
    }
    prefix = prefix_map.get((source or "").lower(), "RR")
    return f"{prefix} | {source_id}" if source_id else prefix


def meeting_context_headers() -> list[str]:
    headers = [
        "Meeting ID",
        "Unique ID",
        "Company",
        "Person",
        "Campaign",
        "MQL Agent",
        "Meeting Scheduled Date",
        "Shared Story Date",
        "Shared Story Recording Link",
        "Shared Story Transcript",
    ]
    for fu in range(1, 31):
        headers.extend([
            f"FU{fu} Date",
            f"FU{fu} Recording Link",
            f"FU{fu} Transcript",
        ])
    return headers


def meeting_context_normalized_headers() -> list[str]:
    return [
        "Meeting ID",
        "Unique ID",
        "Company",
        "Person",
        "Campaign",
        "MQL Agent",
        "Meeting Scheduled Date",
        "Stage",
        "FU Number",
        "Date",
        "Recording Link",
        "Transcript",
    ]


def fu_col_offset(fu_number: int, tab_fu_start: int) -> int:
    """Column index (0-based) where FU block starts in this tab's row."""
    local_fu = fu_number - tab_fu_start   # 0-based within tab
    return CONTACT_COLS + local_fu * FU_BLOCK_SIZE


def safe_col(row: list, idx: int) -> str:
    return row[idx].strip() if idx < len(row) else ""


def norm_status(raw: str) -> str:
    return CALL_STATUS_NORM.get(raw.strip().lower(), raw.strip())


def norm_state(raw: str) -> str | None:
    if not raw or not raw.strip():
        return None
    return CURRENT_STATE_NORM.get(raw.strip().lower(), raw.strip())


def derive_call_status(current_state_raw: str) -> str:
    """Infer call_status from current_state (same logic as BD sync)."""
    s = current_state_raw.strip().lower()
    if "invalid" in s:
        return "Invalid Number"
    if "do not disturb" in s:
        return "Do not Disturb"
    if "referred" in s:
        return "Referred"
    if s in ("call back later", "rescheduled"):
        return "Call back later"
    if s in ("attempt again", "attempt again after 3 months",
             "not interested", "shared story", "snapshot sent",
             "snapshot confirmed", "dream snapshot confirmed",
             "interested", "allocate again", "allocate again 3 months",
             "meeting requested", "meeting scheduled",
             "meeting held", "solution sent", "solution picked",
             "escalate", "respondent", "picked solution",
             "irrelevant"):
        return "Connected"
    if "reffered" in s:
        return "Referred"
    return "Did not connect"


def parse_duration(raw: str) -> int | None:
    if not raw or not raw.strip() or raw.strip() == "0":
        return None
    raw = raw.strip().lstrip(":")
    m = re.match(r'^(\d+):(\d{2}):(\d{2})$', raw)
    if m:
        secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        return secs if secs > 0 else None
    try:
        frac = float(raw)
        if 0 < frac < 1:
            return round(frac * 86400) or None
    except ValueError:
        pass
    return None


def parse_timestamp(raw: str) -> datetime | None:
    if not raw or not raw.strip():
        return None
    for fmt in ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S",
                "%m/%d/%Y", "%m/%d/%Y %H:%M:%S",
                "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_phone(raw: str) -> str | None:
    if not raw:
        return None
    digits = re.sub(r'\D', '', raw.strip())
    return digits[-10:] if len(digits) >= 10 else None


def _get_active_phone_digits(cur, contact_id: int) -> list[str]:
    """Return non-invalid contact phone digits in rank order, deduplicated."""
    cur.execute(
        """
        SELECT phone_number
        FROM contact_phones
        WHERE contact_id = %s
          AND COALESCE((to_jsonb(contact_phones)->>'is_invalid')::boolean, FALSE) = FALSE
        ORDER BY rank, id
        """,
        (contact_id,),
    )
    phones = []
    seen = set()
    for (phone_raw,) in cur.fetchall() or []:
        digits = parse_phone(phone_raw or "")
        if digits and digits not in seen:
            phones.append(digits)
            seen.add(digits)
    return phones


def _strip_alt_segment(remark: str) -> str:
    """Remove any previously appended ALT segment from the remark."""
    if not remark:
        return ""
    cleaned = ALT_REMARK_SEGMENT_RE.sub("", remark)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip()
    cleaned = re.sub(r"\s*\|\s*$", "", cleaned).strip()
    return cleaned


def _format_remark_with_alt(remark: str, alternates: list[str]) -> str:
    """Keep original remark text, but refresh trailing ALT segment."""
    base = _strip_alt_segment(remark or "")
    if not alternates:
        return base
    alt_note = f"ALT: {', '.join(alternates)}"
    return f"{base} | {alt_note}" if base else alt_note


def _build_remark_with_alt(cur, contact_id: int, primary_phone: str, current_remark: str) -> str:
    """Build remark with ALT numbers excluding the current primary in column D."""
    primary_digits = parse_phone(primary_phone or "") or ""
    active_digits = _get_active_phone_digits(cur, contact_id)
    alternates = [digits for digits in active_digits if digits != primary_digits]
    return _format_remark_with_alt(current_remark or "", alternates)


def parse_unique_id(uid: str) -> tuple[str, str | None]:
    """
    'RR | 6175204'    → ('rocketreach', '6175204')
    'CC | 9723049295' → ('manual',      'CC-9723049295')
    'BD | 9810001001' → ('manual',      'BD-9810001001')
    'ID-12345'        → ('manual',      'ID-12345')
    """
    if not uid or not uid.strip():
        return "rocketreach", None
    uid = uid.strip()
    parts = uid.split("|")
    prefix = parts[0].strip().upper()

    # Standard source map
    src_map = {"RR": "rocketreach", "MS": "msme", "PH": "pharma", "MN": "manual"}

    if prefix in ("BD", "CC", "AV", "BW"):
        # Manual contacts stored as 'BD-xxxx', 'CC-xxxx', etc.
        raw_id    = parts[1].strip() if len(parts) > 1 else ""
        source_id = f"{prefix}-{raw_id}" if raw_id else None
        return "manual", source_id
    elif uid.startswith("ID-"):
        return "manual", uid
    else:
        source    = src_map.get(prefix, "rocketreach")
        source_id = parts[1].strip() if len(parts) > 1 else None
        return source, source_id


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging(dry_run=False):
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = "_dryrun" if dry_run else ""
    log_file = str(PROJECT_ROOT / "logs" / f"mql_sync_{ts}{suffix}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ]
    )
    return log_file


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_sheets_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
    return gspread.authorize(creds)


# ── DB HELPERS ────────────────────────────────────────────────
def get_contact(cur, source: str, source_id: str):
    cur.execute("""
        SELECT id, contact_flag FROM contacts
        WHERE source = %s AND source_id = %s LIMIT 1
    """, (source, source_id))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            SELECT id, contact_flag FROM contacts
            WHERE source_id = %s LIMIT 1
        """, (source_id,))
        row = cur.fetchone()
    return row  # (id, contact_flag) or None


def get_mql_allocation(cur, contact_id: int, agent_id: int):
    cur.execute("""
        SELECT id FROM mql_allocations
        WHERE contact_id = %s AND agent_id = %s AND closed_at IS NULL
        LIMIT 1
    """, (contact_id, agent_id))
    row = cur.fetchone()
    return row[0] if row else None


def attempt_already_synced(cur, contact_id: int, agent_id: int,
                            fu_number: int, called_at: datetime) -> bool:
    cur.execute("""
        SELECT 1 FROM mql_call_attempts
        WHERE contact_id       = %s
          AND agent_id         = %s
          AND follow_up_number = %s
          AND called_at        = %s
        LIMIT 1
    """, (contact_id, agent_id, fu_number, called_at))
    return bool(cur.fetchone())


def get_highest_fu(cur, contact_id: int, agent_id: int) -> int:
    """Returns the highest follow_up_number synced so far for this contact."""
    cur.execute("""
        SELECT COALESCE(MAX(follow_up_number), 0)
        FROM mql_call_attempts
        WHERE contact_id = %s AND agent_id = %s
    """, (contact_id, agent_id))
    return cur.fetchone()[0]


def _sync_primary_phone_from_sheet(cur, contact_id: int, phone: str,
                                   called_at: datetime, current_state: str | None,
                                   dry_run: bool, stats: dict) -> bool:
    """
    Keep contact phone data aligned with sheet-confirmed phone (column D).
    Rules:
      - Only numbers already present as non-invalid contact phones are accepted.
      - For normal outcomes, chosen number is promoted to rank=1 (primary).
      - For Invalid Number outcome, that number is marked invalid and the next
        valid alternate is auto-promoted to rank=1.
      - In dry-run mode, no DB writes are performed.
    """
    if not phone:
        return False

    def _normalized(raw: str) -> str:
        return re.sub(r"\D", "", (raw or ""))[-10:]

    def _rerank_rows(contact_id_val: int, ordered_ids: list[int]) -> None:
        if not ordered_ids:
            return
        values_sql = ", ".join(["(%s, %s)"] * len(ordered_ids))
        params = []
        for idx, pid in enumerate(ordered_ids, start=1):
            params.extend([pid, idx])
        params.append(contact_id_val)
        cur.execute(
            f"""
            UPDATE contact_phones cp
            SET rank = v.rank
            FROM (VALUES {values_sql}) AS v(id, rank)
            WHERE cp.id = v.id
              AND cp.contact_id = %s
            """,
            params,
        )

    cur.execute("""
        SELECT
            id,
            phone_number,
            rank,
            COALESCE((to_jsonb(contact_phones)->>'is_invalid')::boolean, FALSE) AS is_invalid
        FROM contact_phones
        WHERE contact_id = %s
        ORDER BY rank, id
    """, (contact_id,))
    raw_rows = cur.fetchall()

    rows = []
    for row_id, row_phone, row_rank, row_invalid in raw_rows:
        rows.append({
            "id": int(row_id),
            "phone": row_phone or "",
            "rank": int(row_rank) if row_rank is not None else 99,
            "is_invalid": bool(row_invalid),
            "digits": _normalized(row_phone),
        })

    if not rows:
        if dry_run:
            stats["phones_confirmed"] = stats.get("phones_confirmed", 0) + 1
            return True
        cur.execute(
            """
            INSERT INTO contact_phones (contact_id, phone_number, rank, is_invalid, invalidated_at)
            VALUES (%s, %s, 1, FALSE, NULL)
            ON CONFLICT (contact_id, phone_number) DO UPDATE
                SET rank = 1,
                    is_invalid = FALSE,
                    invalidated_at = NULL
            """,
            (contact_id, phone),
        )
        stats["phones_confirmed"] = stats.get("phones_confirmed", 0) + 1
        return True

    active_rows = [r for r in rows if not r["is_invalid"] and r["digits"]]
    active_digits = {r["digits"] for r in active_rows}

    if phone not in active_digits:
        stats["skipped_phone_not_allowed"] = stats.get("skipped_phone_not_allowed", 0) + 1
        logging.warning(
            "  Skipping row sync: phone %s is not an allowed active number for contact_id=%s",
            phone,
            contact_id,
        )
        return False

    selected = next((r for r in active_rows if r["digits"] == phone), None)
    if not selected:
        stats["skipped_phone_not_allowed"] = stats.get("skipped_phone_not_allowed", 0) + 1
        logging.warning(
            "  Skipping row sync: phone %s could not be matched for contact_id=%s",
            phone,
            contact_id,
        )
        return False

    selected_id = selected["id"]
    selected_rank = selected["rank"]

    is_invalid_outcome = (current_state or "").strip().lower() == "invalid number"
    if is_invalid_outcome:
        remaining_active = [r for r in active_rows if r["id"] != selected_id]

        if dry_run:
            stats["phones_marked_invalid"] = stats.get("phones_marked_invalid", 0) + 1
            if remaining_active:
                stats["phones_auto_promoted_after_invalid"] = (
                    stats.get("phones_auto_promoted_after_invalid", 0) + 1
                )
            return True

        cur.execute(
            """
            UPDATE contact_phones
            SET is_invalid = TRUE,
                invalidated_at = %s
            WHERE id = %s
            """,
            (called_at, selected_id),
        )

        cur.execute(
            """
            SELECT
                id,
                phone_number,
                rank,
                COALESCE((to_jsonb(contact_phones)->>'is_invalid')::boolean, FALSE) AS is_invalid
            FROM contact_phones
            WHERE contact_id = %s
            ORDER BY rank, id
            """,
            (contact_id,),
        )
        refreshed = cur.fetchall()
        non_invalid_ids = [
            int(r[0]) for r in refreshed
            if not bool(r[3]) and _normalized(r[1])
        ]
        invalid_ids = [
            int(r[0]) for r in refreshed
            if bool(r[3])
        ]
        ordered_ids = (non_invalid_ids + invalid_ids)[:3]
        _rerank_rows(contact_id, ordered_ids)

        stats["phones_marked_invalid"] = stats.get("phones_marked_invalid", 0) + 1
        if non_invalid_ids:
            stats["phones_auto_promoted_after_invalid"] = (
                stats.get("phones_auto_promoted_after_invalid", 0) + 1
            )
        return True

    non_invalid_ids = [selected_id] + [
        r["id"] for r in rows
        if (not r["is_invalid"]) and r["id"] != selected_id and r["digits"]
    ]
    invalid_ids = [r["id"] for r in rows if r["is_invalid"]]
    ordered_ids = (non_invalid_ids + invalid_ids)[:3]

    if dry_run:
        stats["phones_confirmed"] = stats.get("phones_confirmed", 0) + 1
        if selected_rank != 1:
            stats["phones_promoted_primary"] = stats.get("phones_promoted_primary", 0) + 1
        return True

    _rerank_rows(contact_id, ordered_ids)

    cur.execute(
        """
        UPDATE contact_phones
        SET is_invalid = FALSE,
            invalidated_at = NULL
        WHERE id = %s
        """,
        (selected_id,),
    )

    stats["phones_confirmed"] = stats.get("phones_confirmed", 0) + 1
    if selected_rank != 1:
        stats["phones_promoted_primary"] = stats.get("phones_promoted_primary", 0) + 1
    return True


# ── CORE: PROCESS ONE FU BLOCK ────────────────────────────────
def process_fu_block(cur, gc, row: list, agent_id: int, agent_name: str,
                     fu_number: int, tab_fu_start: int,
                     sync_date, dry_run: bool, stats: dict,
                     rewrite_bd_remark: bool = True) -> tuple[bool, str | None]:
    """Process one FU block and return (synced, rewritten_bd_remark_or_none)."""
    col_start = fu_col_offset(fu_number, tab_fu_start)

    # ── Parse timestamp (required) ──
    ts_raw    = safe_col(row, col_start + FU_TIMESTAMP)
    called_at = parse_timestamp(ts_raw)
    if not called_at:
        return False, None

    # Filter by date if requested
    if sync_date and called_at.date() != sync_date:
        return False, None

    # ── Parse call data ──
    current_state_raw = safe_col(row, col_start + FU_CURRENT_STATE)
    if not current_state_raw:
        stats["skipped_no_status"] += 1
        return False, None

    call_status    = derive_call_status(current_state_raw)
    current_state  = norm_state(current_state_raw)

    # ── Skip if already synced ──
    sync_col = safe_col(row, col_start + FU_SYNC_STATUS)
    if sync_col.startswith("✓") and current_state != "Escalate":
        return False, None
    if sync_col.startswith("✓") and current_state == "Escalate":
        logging.info(
            f"  FU{fu_number}: already marked synced, reprocessing escalation update"
        )

    # ── Parse contact identity ──
    uid_raw = safe_col(row, 0)
    source, source_id = parse_unique_id(uid_raw)
    if not source_id:
        stats["skipped_no_id"] += 1
        return False, None

    phone = parse_phone(safe_col(row, 3))
    if not phone:
        stats["skipped_no_phone"] += 1
        return False, None

    lead_category  = safe_col(row, col_start + FU_LEAD_CATEGORY) or None
    call_duration  = parse_duration(safe_col(row, col_start + FU_CALL_DURATION))
    remark_raw     = safe_col(row, col_start + FU_REMARK)
    remark_for_db  = remark_raw or None
    rec_link       = safe_col(row, col_start + FU_RECORDING_LINK) or None
    transcript     = safe_col(row, col_start + FU_TRANSCRIPT) or None
    msg_status_raw = safe_col(row, col_start + FU_MESSAGE_STATUS)
    message_status = msg_status_raw.lower() in ("yes", "true", "1", "✓") if msg_status_raw else False

    # ── Look up contact ──
    contact_row = get_contact(cur, source, source_id)
    if not contact_row:
        stats["skipped_no_contact"] += 1
        return False, None
    contact_id, current_flag = contact_row

    # ── Look up MQL allocation ──
    alloc_id = get_mql_allocation(cur, contact_id, agent_id)
    if not alloc_id:
        stats["skipped_no_allocation"] += 1
        return False, None

    # ── Duplicate check ──
    if attempt_already_synced(cur, contact_id, agent_id, fu_number, called_at) and current_state != "Escalate":
        stats["skipped_duplicate"] += 1
        return False, None

    # Keep DB phone mapping aligned to the confirmed number on sheet column D.
    phone_sync_ok = _sync_primary_phone_from_sheet(
        cur,
        contact_id,
        phone,
        called_at,
        current_state,
        dry_run,
        stats,
    )
    if not phone_sync_ok:
        return False, None

    # Keep FU remark unchanged; update BD Remark (column H) only when requested.
    rewritten_bd_remark = None
    if rewrite_bd_remark:
        bd_remark_raw = safe_col(row, 7)
        rewritten = _build_remark_with_alt(cur, contact_id, phone, bd_remark_raw)
        if rewritten != bd_remark_raw:
            counter_key = "remarks_would_rewrite" if dry_run else "remarks_rewritten"
            stats[counter_key] = stats.get(counter_key, 0) + 1
        rewritten_bd_remark = rewritten

    if dry_run:
        logging.info(
            f"  [DRY RUN] FU{fu_number}: {uid_raw} | {phone} | "
            f"{call_status} | {current_state} | {called_at}"
        )
        stats["would_sync"] += 1
        return True, rewritten_bd_remark

    # ── Insert mql_call_attempt ──
    cur.execute("""
        INSERT INTO mql_call_attempts (
            contact_id, allocation_id, agent_id, follow_up_number,
            phone_number_used, call_status, lead_category, current_state,
            call_duration, remark, recording_link, transcript_link,
            message_status, called_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (contact_id, agent_id, follow_up_number, called_at)
        DO UPDATE SET
            phone_number_used = EXCLUDED.phone_number_used,
            call_status = EXCLUDED.call_status,
            lead_category = EXCLUDED.lead_category,
            current_state = EXCLUDED.current_state,
            call_duration = EXCLUDED.call_duration,
            remark = EXCLUDED.remark,
            recording_link = EXCLUDED.recording_link,
            transcript_link = EXCLUDED.transcript_link,
            message_status = EXCLUDED.message_status
    """, (
        contact_id, alloc_id, agent_id, fu_number,
        phone, call_status, lead_category, current_state,
        call_duration, remark_for_db, rec_link, transcript,
        message_status, called_at,
    ))
    stats["attempts_inserted"] += 1

    # ── Determine new contact_flag ──
    # Ordered progression — never downgrade a contact's flag
    FLAG_ORDER = ["mql_in_progress", "meeting_in_progress", "mql_qualified", "mql_rejected"]
    new_flag = FLAG_MAP.get(current_state, "mql_in_progress")
    cur_rank = FLAG_ORDER.index(current_flag) if current_flag in FLAG_ORDER else 0
    new_rank = FLAG_ORDER.index(new_flag)     if new_flag     in FLAG_ORDER else 0
    final_flag = new_flag if new_rank > cur_rank else current_flag

    if final_flag != current_flag:
        cur.execute("""
            UPDATE contacts SET contact_flag = %s, flag_updated_at = NOW()
            WHERE id = %s
        """, (final_flag, contact_id))
        stats["flags_updated"] += 1

    # ── Close allocation if final outcome ──
    close_reason = None
    escalated_now = (current_state == "Escalate")
    reenter_3m_now = (current_state == "Allocate Again 3 months")
    if current_state in MQL_CLOSE_QUALIFIED:
        close_reason = "qualified"
    elif escalated_now:
        close_reason = "escalated"
    elif reenter_3m_now:
        # Pause this allocation and return the contact to manual MQL queue after 90 days.
        close_reason = "reallocated"
    elif current_state in MQL_CLOSE_REJECTED or call_status == "Do not Disturb":
        close_reason = "rejected"
    elif fu_number == MQL_MAX_FU:
        close_reason = "stalled"

    if close_reason:
        cur.execute("""
            UPDATE mql_allocations
            SET closed_at = %s, close_reason = %s
            WHERE id = %s AND closed_at IS NULL
        """, (called_at, close_reason, alloc_id))

        # Map close_reason → mql_analysis outcome
        outcome_map = {
            "qualified":  "sql",
            "rejected":   "back_to_bd",
            "stalled":    "disqualified",
            "escalated": "back_to_bd",
            "reallocated": "pending",
        }
        outcome = outcome_map.get(close_reason, "pending")

        highest_fu = get_highest_fu(cur, contact_id, agent_id)

        cur.execute("""
            UPDATE mql_analysis
            SET outcome       = %s,
                outcome_reason = %s,
                outcome_date  = %s,
                last_follow_up = %s,
                lead_category  = %s,
                updated_at     = NOW()
            WHERE allocation_id = %s
        """, (
            outcome,
            "escalated" if escalated_now else ("attempt_3_months" if reenter_3m_now else None),
            called_at,
            max(highest_fu, fu_number),
            lead_category,
            alloc_id,
        ))
        stats["allocations_closed"] += 1

    # ── Update meetings metadata for meeting-track states ─────
    # Meeting stays in MQL sheet — allocation stays open.
    # We keep the meetings table in sync and append to shared meeting sheet.
    if current_state == "Meeting Scheduled":
        # Read BD context columns from sheet row (contact info block)
        company_name  = safe_col(row, 1)   # col B
        person_name   = safe_col(row, 2)   # col C
        phone_used    = phone or ""
        bd_remark     = safe_col(row, 7)   # col H — BD Remark
        bd_snapshot   = safe_col(row, 11)  # col L — Dream Snapshot link
        email_used    = safe_col(row, 4)   # col E

        cur.execute("""
            INSERT INTO meetings
                (contact_id, mql_allocation_id, mql_agent_id, campaign,
                 status, scheduled_date, scheduled_at_fu, created_at, updated_at)
            VALUES (%s, %s, %s,
                (SELECT campaign FROM mql_allocations WHERE id = %s),
                'scheduled', %s, %s, NOW(), NOW())
            ON CONFLICT (mql_allocation_id)
            WHERE status NOT IN ('lost')
            DO UPDATE SET
                status          = 'scheduled',
                scheduled_date  = EXCLUDED.scheduled_date,
                scheduled_at_fu = EXCLUDED.scheduled_at_fu,
                updated_at      = NOW()
            RETURNING id
        """, (contact_id, alloc_id, agent_id, alloc_id,
              called_at.date(), fu_number))
        mtg_row = cur.fetchone()
        meeting_id = mtg_row[0] if mtg_row else None

        stats["meetings_created"] = stats.get("meetings_created", 0) + 1
        logging.info(f"  Meeting record created/updated for contact_id={contact_id}")

        if meeting_id:
            # Fetch campaign for this allocation
            cur.execute("SELECT campaign FROM mql_allocations WHERE id = %s", (alloc_id,))
            camp_row  = cur.fetchone()
            campaign  = camp_row[0] if camp_row else ""
            append_to_meeting_sheet(gc, meeting_id, {
                "company":       company_name,
                "person":        person_name,
                "phone":         phone_used,
                "email":         email_used,
                "agent_name":    agent_name,
                "campaign":      campaign,
                "scheduled_date": str(called_at.date()),
                "fu_number":     fu_number,
                "bd_remark":     bd_remark,
                "bd_snapshot":   bd_snapshot,
                "mql_remark":    remark_raw or "",
            })
            upsert_meeting_context_normalized_sheet(
                cur,
                gc,
                meeting_id=meeting_id,
                allocation_id=alloc_id,
                contact_id=contact_id,
                agent_name=agent_name,
                campaign=campaign,
                scheduled_date=called_at.date(),
            )

    elif current_state == "Meeting Held":
        cur.execute("""
            UPDATE meetings
            SET status      = 'held',
                held_at     = %s,
                held_at_fu  = %s,
                conclusion  = COALESCE(%s, conclusion),
                updated_at  = NOW()
            WHERE mql_allocation_id = %s AND status != 'lost'
        """, (called_at, fu_number, remark_for_db, alloc_id))

    elif current_state == "Solution Sent":
        cur.execute("""
            UPDATE meetings
            SET status           = 'solution_sent',
                solution_sent_at = %s,
                solution_link    = COALESCE(%s, solution_link),
                updated_at       = NOW()
            WHERE mql_allocation_id = %s AND status != 'lost'
        """, (called_at, rec_link, alloc_id))

    elif current_state in ("Solution Picked", "Picked Solution"):
        cur.execute("""
            UPDATE meetings
            SET status               = 'solution_picked',
                solution_picked_at   = %s,
                updated_at           = NOW()
            WHERE mql_allocation_id = %s AND status != 'lost'
        """, (called_at, alloc_id))

    return True, rewritten_bd_remark


# ── MEETINGS SHEET APPEND ─────────────────────────────────────
def append_to_meeting_sheet(gc, meeting_id: int, contact_row_data: dict):
    """Append one row to the shared meeting sheet when Meeting Scheduled is detected."""
    if not MEETING_SHEET_ID:
        logging.warning("MEETING_SHEET_ID not set in .env — skipping meeting sheet append")
        return

    try:
        sh = gc.open_by_key(MEETING_SHEET_ID)
    except Exception as e:
        logging.error(f"  Cannot open meeting sheet (id={MEETING_SHEET_ID}): {e}")
        return

    # Get or create the single tab "Meetings"
    try:
        ws = sh.worksheet("Meetings")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Meetings", rows=1000, cols=20)
        ws.append_row(MEETING_SHEET_HEADERS, value_input_option="RAW")
        logging.info("  Created 'Meetings' tab in meeting sheet")

    # Ensure headers exist (first row)
    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "Meeting ID":
        ws.insert_row(MEETING_SHEET_HEADERS, index=1, value_input_option="RAW")

    row = [""] * len(MEETING_SHEET_HEADERS)
    row[MTG_MEETING_ID]   = meeting_id
    row[MTG_COMPANY]      = contact_row_data.get("company", "")
    row[MTG_PERSON]       = contact_row_data.get("person", "")
    row[MTG_PHONE]        = contact_row_data.get("phone", "")
    row[MTG_EMAIL]        = contact_row_data.get("email", "")
    row[MTG_AGENT]        = contact_row_data.get("agent_name", "")
    row[MTG_CAMPAIGN]     = contact_row_data.get("campaign", "")
    row[MTG_SCHEDULED]    = contact_row_data.get("scheduled_date", "")
    row[MTG_FU_AT_SCHED]  = contact_row_data.get("fu_number", "")
    row[MTG_BD_REMARK]    = contact_row_data.get("bd_remark", "")
    row[MTG_BD_SNAPSHOT]  = contact_row_data.get("bd_snapshot", "")
    row[MTG_MQL_REMARK]   = contact_row_data.get("mql_remark", "")
    # Columns M–R left blank for agent to fill

    ws.append_row(row, value_input_option="USER_ENTERED")
    logging.info(
        f"  Appended meeting row: Meeting ID={meeting_id}, "
        f"company={contact_row_data.get('company')}, "
        f"person={contact_row_data.get('person')}"
    )


def upsert_meeting_context_sheet(cur, gc, meeting_id: int, allocation_id: int,
                                 contact_id: int, agent_name: str,
                                 campaign: str, scheduled_date):
    """
    Upsert a single row in MEETING_CONTEXT_TAB with:
      - Shared Story details
      - FU1..FU30 date/recording/transcript side-by-side columns.
    """
    if not MEETING_SHEET_ID:
        logging.warning("MEETING_SHEET_ID not set in .env — skipping meeting context sheet update")
        return

    # Contact identity + basic metadata
    cur.execute("""
        SELECT
            c.source,
            c.source_id,
            co.name AS company_name,
            c.first_name,
            c.last_name
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE c.id = %s
        LIMIT 1
    """, (contact_id,))
    c_row = cur.fetchone()
    if not c_row:
        return

    source, source_id, company_name, first_name, last_name = c_row
    person_name = f"{first_name or ''} {last_name or ''}".strip()
    unique_id = build_unique_id(source or "", source_id)

    # Latest Shared Story from BD call_actions
    cur.execute("""
        SELECT
            called_at::date,
            recording_link,
            transcript_link
        FROM call_actions
        WHERE contact_id = %s
          AND current_state = 'Shared Story'
        ORDER BY called_at DESC NULLS LAST, id DESC
        LIMIT 1
    """, (contact_id,))
    s_row = cur.fetchone()
    story_date = str(s_row[0]) if s_row and s_row[0] else ""
    story_recording = s_row[1] if s_row and s_row[1] else ""
    story_transcript = s_row[2] if s_row and s_row[2] else ""

    # Latest synced attempt per FU number
    cur.execute("""
        SELECT DISTINCT ON (follow_up_number)
            follow_up_number,
            called_at::date,
            recording_link,
            transcript_link
        FROM mql_call_attempts
        WHERE allocation_id = %s
          AND follow_up_number BETWEEN 1 AND 30
        ORDER BY follow_up_number, called_at DESC NULLS LAST, id DESC
    """, (allocation_id,))
    fu_rows = cur.fetchall() or []
    fu_map = {
        int(r[0]): (str(r[1]) if r[1] else "", r[2] or "", r[3] or "")
        for r in fu_rows
    }

    headers = meeting_context_headers()
    row = [""] * len(headers)
    row[0] = meeting_id
    row[1] = unique_id
    row[2] = company_name or ""
    row[3] = person_name
    row[4] = campaign or ""
    row[5] = agent_name or ""
    row[6] = str(scheduled_date) if scheduled_date else ""
    row[7] = story_date
    row[8] = story_recording
    row[9] = story_transcript

    base = 10
    for fu in range(1, 31):
        d, rec, trn = fu_map.get(fu, ("", "", ""))
        offset = base + (fu - 1) * 3
        row[offset] = d
        row[offset + 1] = rec
        row[offset + 2] = trn

    try:
        sh = gc.open_by_key(MEETING_SHEET_ID)
    except Exception as e:
        logging.error(f"  Cannot open meeting sheet for context tab (id={MEETING_SHEET_ID}): {e}")
        return

    try:
        ws = sh.worksheet(MEETING_CONTEXT_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=MEETING_CONTEXT_TAB, rows=1000, cols=len(headers) + 5)
        ws.append_row(headers, value_input_option="RAW")
        logging.info(f"  Created '{MEETING_CONTEXT_TAB}' tab in meeting sheet")

    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "Meeting ID" or len(first_row) != len(headers):
        ws.update(range_name="A1", values=[headers])

    meeting_ids = ws.col_values(1)
    target_row = None
    needle = str(meeting_id)
    for idx, val in enumerate(meeting_ids[1:], start=2):
        if str(val).strip() == needle:
            target_row = idx
            break

    if target_row is None:
        ws.append_row(row, value_input_option="USER_ENTERED")
        logging.info(f"  Appended meeting context row for Meeting ID={meeting_id}")
    else:
        end_col = col_index_to_letter(len(headers) - 1)
        ws.update(
            range_name=f"A{target_row}:{end_col}{target_row}",
            values=[row],
            value_input_option="USER_ENTERED",
        )
        logging.info(f"  Updated meeting context row for Meeting ID={meeting_id}")


def upsert_meeting_context_normalized_sheet(cur, gc, meeting_id: int, allocation_id: int,
                                            contact_id: int, agent_name: str,
                                            campaign: str, scheduled_date):
    """Write normalized timeline rows (Shared Story + FU1..FU30) for this meeting."""
    if not MEETING_SHEET_ID:
        return

    cur.execute("""
        SELECT
            c.source,
            c.source_id,
            co.name AS company_name,
            c.first_name,
            c.last_name
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE c.id = %s
        LIMIT 1
    """, (contact_id,))
    c_row = cur.fetchone()
    if not c_row:
        return

    source, source_id, company_name, first_name, last_name = c_row
    unique_id = build_unique_id(source or "", source_id)
    person_name = f"{first_name or ''} {last_name or ''}".strip()

    cur.execute("""
        SELECT
            called_at::date,
            recording_link,
            transcript_link
        FROM call_actions
        WHERE contact_id = %s
          AND current_state = 'Shared Story'
        ORDER BY called_at DESC NULLS LAST, id DESC
        LIMIT 1
    """, (contact_id,))
    s_row = cur.fetchone()

    cur.execute("""
        SELECT DISTINCT ON (follow_up_number)
            follow_up_number,
            called_at::date,
            recording_link,
            transcript_link
        FROM mql_call_attempts
        WHERE allocation_id = %s
          AND follow_up_number BETWEEN 1 AND 30
        ORDER BY follow_up_number, called_at DESC NULLS LAST, id DESC
    """, (allocation_id,))
    fu_rows = cur.fetchall() or []

    rows = []
    if s_row:
        rows.append([
            meeting_id,
            unique_id,
            company_name or "",
            person_name,
            campaign or "",
            agent_name or "",
            str(scheduled_date) if scheduled_date else "",
            "Shared Story",
            "",
            str(s_row[0]) if s_row[0] else "",
            s_row[1] or "",
            s_row[2] or "",
        ])

    for r in fu_rows:
        fu_num = int(r[0])
        rows.append([
            meeting_id,
            unique_id,
            company_name or "",
            person_name,
            campaign or "",
            agent_name or "",
            str(scheduled_date) if scheduled_date else "",
            f"FU{fu_num}",
            fu_num,
            str(r[1]) if r[1] else "",
            r[2] or "",
            r[3] or "",
        ])

    if not rows:
        return

    try:
        sh = gc.open_by_key(MEETING_SHEET_ID)
    except Exception as e:
        logging.error(f"  Cannot open normalized meeting context tab (id={MEETING_SHEET_ID}): {e}")
        return

    headers = meeting_context_normalized_headers()
    try:
        ws = sh.worksheet(MEETING_CONTEXT_NORM_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=MEETING_CONTEXT_NORM_TAB, rows=2000, cols=len(headers) + 3)
        ws.append_row(headers, value_input_option="RAW")
        logging.info(f"  Created '{MEETING_CONTEXT_NORM_TAB}' tab in meeting sheet")

    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "Meeting ID" or len(first_row) != len(headers):
        ws.update(range_name="A1", values=[headers])

    existing = ws.get_all_values()
    delete_row_indices = []
    needle = str(meeting_id)
    if len(existing) > 1:
        for idx, row in enumerate(existing[1:], start=2):
            if row and str(row[0]).strip() == needle:
                delete_row_indices.append(idx)

    for idx in sorted(delete_row_indices, reverse=True):
        ws.delete_rows(idx)

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    logging.info(f"  Upserted normalized meeting context rows for Meeting ID={meeting_id}")


def rewrite_bd_remarks_for_tab(cur, data_rows: list[list[str]],
                               dry_run: bool, stats: dict) -> dict[int, dict[int, str]]:
    """
    One-time rewrite mode for BD remark (column H) in a tab.
    Rewrites ALT segment for every contact row that resolves to a contact + phone.
    """
    rows_to_update: dict[int, dict[int, str]] = {}
    touched_rows: set[int] = set()

    for row_idx, row in enumerate(data_rows, start=2):
        uid_raw = safe_col(row, 0)
        source, source_id = parse_unique_id(uid_raw)
        if not source_id:
            continue

        contact_row = get_contact(cur, source, source_id)
        if not contact_row:
            continue
        contact_id = contact_row[0]

        phone = parse_phone(safe_col(row, 3))
        if not phone:
            continue

        bd_remark_col_idx = 7
        current_remark = safe_col(row, bd_remark_col_idx)
        rewritten = _build_remark_with_alt(cur, contact_id, phone, current_remark)
        if rewritten == current_remark:
            continue

        counter_key = "remarks_would_rewrite" if dry_run else "remarks_rewritten"
        stats[counter_key] = stats.get(counter_key, 0) + 1
        touched_rows.add(row_idx)

        if not dry_run:
            rows_to_update.setdefault(row_idx, {})[bd_remark_col_idx] = rewritten

    stats["remarks_full_rewrite_rows"] = stats.get("remarks_full_rewrite_rows", 0) + len(touched_rows)
    return rows_to_update


# ── CORE: PROCESS ONE AGENT ───────────────────────────────────
def sync_mql_agent(gc, conn, agent_id: int, agent_name: str, sheet_id: str,
                   sync_date, dry_run: bool, stats: dict,
                   rewrite_bd_remark_all: bool = False):
    logging.info(f"\n  MQL Agent: {agent_name} (id={agent_id})")

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        logging.error(f"  Cannot open sheet for {agent_name}: {e}")
        stats["agents_failed"] += 1
        return

    def _normalize_sheet_title(title: str) -> str:
        """Normalize tab titles so dashes/spaces/case variants still match."""
        text = unicodedata.normalize("NFKC", str(title or ""))
        text = text.replace("\u00A0", " ")
        text = re.sub(DASH_VARIANTS_RE, "-", text)
        text = re.sub(r"\s*-\s*", "-", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        return text

    def _worksheet_by_title_flexible(tab_name: str):
        try:
            return sh.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            target = _normalize_sheet_title(tab_name)
            for ws_candidate in sh.worksheets():
                if _normalize_sheet_title(ws_candidate.title) == target:
                    logging.info(
                        f"  Resolved tab '{tab_name}' to existing worksheet '{ws_candidate.title}'"
                    )
                    return ws_candidate
            raise

    for tab_name, (fu_start, fu_end) in TAB_FU_RANGE.items():
        try:
            ws = _worksheet_by_title_flexible(tab_name)
        except gspread.WorksheetNotFound:
            available = ", ".join([w.title for w in sh.worksheets()])
            logging.warning(
                f"  Tab '{tab_name}' not found — skipping. Available tabs: {available}"
            )
            continue

        all_values = ws.get_all_values()
        if len(all_values) < 2:
            continue

        data_rows = all_values[1:]
        rows_to_update: dict[int, dict[int, str]] = {}  # row_idx -> {col_idx: value}

        with conn.cursor() as cur:
            if rewrite_bd_remark_all:
                rewritten_rows = rewrite_bd_remarks_for_tab(
                    cur,
                    data_rows,
                    dry_run,
                    stats,
                )
                for row_idx, col_map in rewritten_rows.items():
                    rows_to_update.setdefault(row_idx, {}).update(col_map)
                logging.info(
                    "  %s: full BD Remark rewrite rows touched=%s",
                    tab_name,
                    len(rewritten_rows),
                )
            else:
                for row_idx, row in enumerate(data_rows, start=2):
                    for fu_num in range(fu_start, fu_end + 1):
                        synced, rewritten_bd_remark = process_fu_block(
                            cur, gc, row, agent_id, agent_name, fu_num,
                            fu_start, sync_date, dry_run, stats,
                            rewrite_bd_remark=True,
                        )

                        if rewritten_bd_remark is not None and not dry_run:
                            bd_remark_col_idx = 7
                            current_bd_remark = safe_col(row, bd_remark_col_idx)
                            if rewritten_bd_remark != current_bd_remark:
                                rows_to_update.setdefault(row_idx, {})[bd_remark_col_idx] = rewritten_bd_remark

                        if synced and not dry_run:
                            sync_col_idx = fu_col_offset(fu_num, fu_start) + FU_SYNC_STATUS
                            rows_to_update.setdefault(row_idx, {})[sync_col_idx] = (
                                f"✓ Synced {datetime.now().strftime('%d/%m %H:%M')}"
                            )

                        if synced:
                            stats["fu_blocks_synced"] += 1

                if not dry_run:
                    conn.commit()

        if rows_to_update and not dry_run:
            cell_updates = []
            for row_idx, col_map in rows_to_update.items():
                for col_idx, value in col_map.items():
                    col_letter = col_index_to_letter(col_idx)
                    cell_updates.append({
                        "range":  f"{col_letter}{row_idx}",
                        "values": [[value]],
                    })
            if cell_updates:
                ws.batch_update(cell_updates)

        if rewrite_bd_remark_all:
            logging.info(f"  {tab_name}: BD Remark rewrite completed")
        else:
            logging.info(f"  {tab_name}: FU blocks synced in this run noted above")

    stats["agents_synced"] += 1


def col_index_to_letter(idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, ..., Z, AA, AB, ...)."""
    result = ""
    idx += 1  # 1-based
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        result = chr(65 + remainder) + result
    return result


# ── MAIN ──────────────────────────────────────────────────────
def run_mql_sync(
    dry_run:   bool       = False,
    sync_date: date | None = None,
    campaign:  str        = "consulting",
    rewrite_bd_remark_all: bool = False,
) -> tuple[bool, str]:
    """Entry point for CLI and dashboard. Returns (success, log_output)."""
    import io, logging as _logging

    log_stream = io.StringIO()
    handler    = _logging.StreamHandler(log_stream)
    handler.setFormatter(_logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    root = _logging.getLogger()
    root.setLevel(_logging.DEBUG)   # must set before basicConfig or it stays at WARNING
    root.addHandler(handler)

    log_file = setup_logging(dry_run)

    logging.info("=" * 60)
    logging.info("  MQL SYNC")
    logging.info(f"  Date:     {date.today()}")
    logging.info(f"  Campaign: {campaign}")
    logging.info(f"  Mode:     {'DRY RUN' if dry_run else 'LIVE'}")
    if rewrite_bd_remark_all:
        logging.info("  BD Remark rewrite: FULL (all rows)")
    if sync_date:
        logging.info(f"  Syncing:  {sync_date} only")
    logging.info("=" * 60)

    stats = {
        "agents_synced":       0,
        "agents_failed":       0,
        "fu_blocks_synced":    0,
        "attempts_inserted":   0,
        "allocations_closed":  0,
        "flags_updated":       0,
        "skipped_no_id":       0,
        "skipped_no_phone":    0,
        "skipped_no_status":   0,
        "skipped_no_contact":  0,
        "skipped_no_allocation": 0,
        "skipped_duplicate":   0,
        "skipped_phone_not_allowed": 0,
        "would_sync":          0,
        "phones_confirmed":    0,
        "phones_promoted_primary": 0,
        "phones_marked_invalid": 0,
        "phones_auto_promoted_after_invalid": 0,
        "remarks_rewritten": 0,
        "remarks_would_rewrite": 0,
        "remarks_full_rewrite_rows": 0,
    }

    try:
        gc = get_sheets_client()
        logging.info("Google Sheets client connected")
    except Exception as e:
        logging.error(f"Google Sheets auth failed: {e}")
        root.removeHandler(handler)
        return False, log_stream.getvalue()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    success = True

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.name, ags.sheet_id
                FROM agents a
                LEFT JOIN agent_sheets ags
                       ON ags.agent_id = a.id
                      AND ags.campaign = %s
                      AND ags.sheet_type = 'calling'
                WHERE a.status = 'active' AND a.team = 'mql'
                ORDER BY a.id
            """, (campaign,))
            mql_agents = cur.fetchall()

        logging.info(f"MQL agents: {[a[1] for a in mql_agents]}")

        for agent_id, agent_name, sheet_id in mql_agents:
            if not sheet_id:
                logging.warning(
                    f"No sheet_id for {agent_name} (id={agent_id}) — "
                    f"set it in the Agents page."
                )
                continue
            sync_mql_agent(gc, conn, agent_id, agent_name, sheet_id,
                           sync_date, dry_run, stats,
                           rewrite_bd_remark_all=rewrite_bd_remark_all)

    except Exception as e:
        conn.rollback()
        logging.error(f"MQL sync failed: {e}", exc_info=True)
        success = False
    finally:
        conn.close()

    # ── Summary ──
    logging.info("\n" + "=" * 60)
    logging.info("  MQL SYNC COMPLETE" + (" — DRY RUN" if dry_run else ""))
    logging.info("=" * 60)
    logging.info(f"  Agents synced:          {stats['agents_synced']}")
    logging.info(f"  Agents failed:          {stats['agents_failed']}")
    if dry_run:
        logging.info(f"  FU blocks would sync:   {stats['would_sync']}")
    else:
        logging.info(f"  FU blocks synced:       {stats['fu_blocks_synced']}")
        logging.info(f"  Attempts inserted:      {stats['attempts_inserted']}")
        logging.info(f"  Allocations closed:     {stats['allocations_closed']}")
        logging.info(f"  Meetings created:       {stats.get('meetings_created', 0)}")
        logging.info(f"  Contact flags updated:  {stats['flags_updated']}")
    logging.info(f"  Skipped (no ID):        {stats['skipped_no_id']}")
    logging.info(f"  Skipped (no phone):     {stats['skipped_no_phone']}")
    logging.info(f"  Skipped (no contact):   {stats['skipped_no_contact']}")
    logging.info(f"  Skipped (no alloc):     {stats['skipped_no_allocation']}")
    logging.info(f"  Skipped (duplicate):    {stats['skipped_duplicate']}")
    logging.info(f"  Skipped (phone rule):   {stats['skipped_phone_not_allowed']}")
    if dry_run:
        logging.info(f"  BD Remark rewrites (dry): {stats['remarks_would_rewrite']}")
    else:
        logging.info(f"  BD Remark rewrites:     {stats['remarks_rewritten']}")
    logging.info(f"  Full rewrite rows:      {stats['remarks_full_rewrite_rows']}")
    logging.info(f"  Phones confirmed:       {stats['phones_confirmed']}")
    logging.info(f"  Phones promoted:        {stats['phones_promoted_primary']}")
    logging.info(f"  Phones marked invalid:  {stats['phones_marked_invalid']}")
    logging.info(f"  Auto-promoted alternates: {stats['phones_auto_promoted_after_invalid']}")
    logging.info(f"  Log: {log_file}")
    logging.info("=" * 60)

    root.removeHandler(handler)
    return success, log_stream.getvalue()


# ── CLI ────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQL Sync — sheet outcomes → DB")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--date", help="Sync only rows from this date (YYYY-MM-DD)")
    parser.add_argument("--campaign", type=str, default="consulting")
    parser.add_argument(
        "--rewrite-bd-remark-all",
        action="store_true",
        help="Rewrite ALT segment in BD Remark (col H) for all rows.",
    )
    parser.add_argument(
        "--rewrite-last-remark-all",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    sync_date = None
    if args.date:
        try:
            sync_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: Invalid date '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)

    success, output = run_mql_sync(
        dry_run   = args.dry_run,
        sync_date = sync_date,
        campaign  = args.campaign,
        rewrite_bd_remark_all = (
            args.rewrite_bd_remark_all or args.rewrite_last_remark_all
        ),
    )
    print(output)
    sys.exit(0 if success else 1)
