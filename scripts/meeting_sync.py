"""
meeting_sync.py
===============
Reads the shared meetings Google Sheet and syncs agent-filled conclusions
back to:
  1. The DB (meetings table — status, held_at, conclusion, solution fields)
  2. The agent's MQL calling sheet — auto-fills the next FU block with
     "Meeting Held" so the agent can continue the follow-up normally.

Flow:
  mql_sync (detects "Meeting Scheduled")
    → creates meetings DB row
    → appends row to shared meeting sheet (cols A–L pre-filled, M–R blank)

  Agent opens meeting sheet, fills:
    M  Meeting Date       (actual date the meeting was held)
    N  Duration (min)
    O  Problems Identified / Conclusion
    P  Solution Proposed
    Q  Solution Link

  meeting_sync (this script):
    → reads meeting sheet
    → for each row where Meeting Date filled + Sync Status not "✓ Synced":
        a. Updates meetings table in DB
        b. Finds agent's MQL calling sheet
        c. Auto-fills the next empty FU block with "Meeting Held" + conclusion
        d. Writes "✓ Synced DD/MM HH:MM" to col R (Sync Status)

Run AFTER mql_sync, BEFORE mql_allocation_engine:
  01:00 AM → mql_sync.py
  01:30 AM → meeting_sync.py    ← this script
  02:00 AM → mql_allocation_engine.py
  03:00 PM → agents start shift

Usage:
  python meeting_sync.py                 # sync all unsynced rows
  python meeting_sync.py --dry-run       # preview only
  python meeting_sync.py --campaign consulting
"""

import os
import re
import sys
import logging
import argparse
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import gspread
from google.oauth2.service_account import Credentials

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

# Meeting sheet column indices (0-based, cols A–R)
MTG_MEETING_ID    = 0   # A — DB meetings.id
MTG_COMPANY       = 1   # B
MTG_PERSON        = 2   # C
MTG_PHONE         = 3   # D
MTG_EMAIL         = 4   # E
MTG_AGENT         = 5   # F — MQL agent name
MTG_CAMPAIGN      = 6   # G
MTG_SCHEDULED     = 7   # H — scheduled date
MTG_FU_AT_SCHED   = 8   # I — FU# where scheduled
MTG_BD_REMARK     = 9   # J
MTG_BD_SNAPSHOT   = 10  # K
MTG_MQL_REMARK    = 11  # L
MTG_MEETING_DATE  = 12  # M — agent fills: actual meeting date
MTG_DURATION      = 13  # N — agent fills: duration (min)
MTG_CONCLUSION    = 14  # O — agent fills: problems identified
MTG_SOLUTION      = 15  # P — agent fills: solution proposed
MTG_SOL_LINK      = 16  # Q — agent fills: solution link
MTG_SYNC_STATUS   = 17  # R — system: "✓ Synced DD/MM HH:MM"

# MQL sheet constants (same as mql_sync.py)
MQL_TAB_1       = "MQL FU 1-15"
MQL_TAB_2       = "MQL FU 16-30"
CONTACT_COLS    = 12
FU_BLOCK_SIZE   = 11
FU_CURRENT_STATE = 2
FU_REMARK        = 4
FU_TIMESTAMP     = 8
FU_SYNC_STATUS   = 10
TAB_FU_RANGE    = {MQL_TAB_1: (1, 15), MQL_TAB_2: (16, 30)}

MEETING_SHEET_HEADERS = [
    "Meeting ID", "Company", "Person", "Phone", "Email",
    "MQL Agent", "Campaign", "Scheduled Date", "MQL FU#",
    "BD Remark", "BD Snapshot Link", "Last MQL Remark",
    "Meeting Date", "Duration (min)", "Problems Identified",
    "Solution Proposed", "Solution Link", "Sync Status",
]

CONTACT_HEADERS = [
    "Unique ID", "Company Name", "Person Name", "Phone", "Email",
    "BD Agent", "BD Call Date", "BD Remark", "BD Recording Link",
    "Category", "BD Transcript", "Dream Snapshot",
]

FU_BLOCK_HEADERS = [
    "MQL Category", "Call Status", "Current State", "Call Duration",
    "Remark", "Recording Link", "Transcript", "Message Status",
    "Timestamp", "Follow-up Stage", "Sync Status",
]


def safe_col(row: list, idx: int) -> str:
    return row[idx].strip() if idx < len(row) else ""


def parse_date(raw: str) -> date | None:
    if not raw or not raw.strip():
        return None
    for fmt in ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S",
                "%m/%d/%Y", "%m/%d/%Y %H:%M:%S",
                "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def fu_col_offset(fu_number: int, tab_fu_start: int) -> int:
    return CONTACT_COLS + (fu_number - tab_fu_start) * FU_BLOCK_SIZE


def col_index_to_letter(idx: int) -> str:
    result = ""
    idx += 1
    while idx > 0:
        idx, remainder = divmod(idx - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _build_mql_tab_headers(fu_start: int, fu_end: int) -> list[str]:
    headers = list(CONTACT_HEADERS)
    for fu_num in range(fu_start, fu_end + 1):
        for col in FU_BLOCK_HEADERS:
            headers.append(f"FU{fu_num} - {col}")
    return headers


def _ensure_mql_tab(sheet, tab_name: str, fu_start: int, fu_end: int):
    headers = _build_mql_tab_headers(fu_start, fu_end)
    total_cols = len(headers)

    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=2000, cols=total_cols + 5)
        ws.update(range_name="A1", values=[headers])
        logging.info(f"  Created missing tab '{tab_name}' in agent sheet")
        return ws

    row1 = ws.row_values(1)
    if len(row1) != total_cols or (row1 and row1[0] != headers[0]):
        ws.update(range_name="A1", values=[headers])
        logging.info(f"  Repaired headers for tab '{tab_name}' in agent sheet")
    return ws


def _ensure_meetings_tab(sheet):
    try:
        ws = sheet.worksheet("Meetings")
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title="Meetings", rows=1000, cols=20)
        ws.update(range_name="A1", values=[MEETING_SHEET_HEADERS])
        logging.info("Created missing 'Meetings' tab in meeting sheet")
        return ws

    row1 = ws.row_values(1)
    if len(row1) != len(MEETING_SHEET_HEADERS) or (row1 and row1[0] != "Meeting ID"):
        ws.update(range_name="A1", values=[MEETING_SHEET_HEADERS])
        logging.info("Repaired headers in 'Meetings' tab")
    return ws


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging(dry_run=False):
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = "_dryrun" if dry_run else ""
    log_file = str(PROJECT_ROOT / "logs" / f"meeting_sync_{ts}{suffix}.log")
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


# ── WRITE MEETING HELD BLOCK TO MQL SHEET ────────────────────
def write_meeting_held_to_mql_sheet(gc, agent_name: str, campaign: str,
                                    contact_id: int, fu_at_sched: int,
                                    meeting_date: date, conclusion: str,
                                    dry_run: bool):
    """
    Finds the agent's MQL calling sheet and pre-fills the FU block immediately
    after the 'Meeting Scheduled' FU with current_state='Meeting Held'.
    The agent then takes over for the remaining FU calls.
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ags.sheet_id
                FROM agents a
                JOIN agent_sheets ags ON ags.agent_id = a.id
                WHERE a.name      = %s
                  AND ags.campaign   = %s
                  AND ags.sheet_type = 'calling'
                LIMIT 1
            """, (agent_name, campaign))
            row = cur.fetchone()

    if not row or not row[0]:
        logging.warning(
            f"  No calling sheet found for agent '{agent_name}' / campaign '{campaign}' "
            f"— skipping MQL sheet write-back"
        )
        return False

    sheet_id = row[0]
    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        logging.error(f"  Cannot open MQL sheet for {agent_name}: {e}")
        return False

    # Contact info col A = Unique ID; we need to find the row for this contact_id
    # We match by checking col A (Unique ID) against the source_id from DB
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT source, source_id FROM contacts WHERE id = %s", (contact_id,)
            )
            row = cur.fetchone()

    if not row:
        logging.warning(f"  Contact id={contact_id} not found in DB")
        return False

    source, source_id = row
    src_prefix = {"rocketreach": "RR", "msme": "MS", "pharma": "PH", "manual": "MN"}.get(source, "RR")
    uid_pattern = source_id  # match by source_id substring

    # Determine which tab and FU block to write (fu_at_sched + 1)
    target_fu = fu_at_sched + 1
    target_tab = MQL_TAB_1 if target_fu <= 15 else MQL_TAB_2
    tab_fu_start = TAB_FU_RANGE[target_tab][0]

    fu_start, fu_end = TAB_FU_RANGE[target_tab]
    try:
        ws = _ensure_mql_tab(sh, target_tab, fu_start, fu_end)
    except Exception as e:
        logging.warning(f"  Could not ensure tab '{target_tab}' in sheet for {agent_name}: {e}")
        return False

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        return False

    # Find the contact row
    contact_row_idx = None
    for i, sheet_row in enumerate(all_values[1:], start=2):
        uid_cell = safe_col(sheet_row, 0)
        if source_id and source_id in uid_cell:
            contact_row_idx = i
            break

    if contact_row_idx is None:
        logging.warning(
            f"  Could not find contact (source_id={source_id}) "
            f"in sheet for {agent_name} — skipping write-back"
        )
        return False

    sheet_row = all_values[contact_row_idx - 1]
    col_start  = fu_col_offset(target_fu, tab_fu_start)

    # Only write if the target FU block's Timestamp is empty (don't overwrite agent work)
    ts_existing = safe_col(sheet_row, col_start + FU_TIMESTAMP)
    if ts_existing.strip():
        logging.info(
            f"  FU{target_fu} Timestamp already filled for {uid_pattern} "
            f"— agent has already worked this block, skipping write-back"
        )
        return False

    ts_str   = meeting_date.strftime("%d/%m/%Y")
    remark   = f"Meeting held. {conclusion}" if conclusion else "Meeting held."

    if dry_run:
        logging.info(
            f"  [DRY RUN] Would write Meeting Held to FU{target_fu} "
            f"row {contact_row_idx} in {target_tab} for {agent_name}"
        )
        return True

    updates = [
        {"range": f"{col_index_to_letter(col_start + FU_CURRENT_STATE)}{contact_row_idx}",
         "values": [["Meeting Held"]]},
        {"range": f"{col_index_to_letter(col_start + FU_REMARK)}{contact_row_idx}",
         "values": [[remark]]},
        {"range": f"{col_index_to_letter(col_start + FU_TIMESTAMP)}{contact_row_idx}",
         "values": [[ts_str]]},
    ]
    ws.batch_update(updates)
    logging.info(
        f"  Wrote Meeting Held → FU{target_fu} row {contact_row_idx} "
        f"in {target_tab} for {agent_name} | contact_id={contact_id}"
    )
    return True


# ── MAIN SYNC ─────────────────────────────────────────────────
def run_meeting_sync(dry_run: bool = False, campaign: str = "consulting") -> tuple[bool, str]:
    import io, logging as _logging

    log_stream = io.StringIO()
    handler    = _logging.StreamHandler(log_stream)
    handler.setFormatter(_logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    root = _logging.getLogger()
    root.setLevel(_logging.DEBUG)
    root.addHandler(handler)

    log_file = setup_logging(dry_run)

    logging.info("=" * 60)
    logging.info("  MEETING SYNC")
    logging.info(f"  Date:     {date.today()}")
    logging.info(f"  Campaign: {campaign}")
    logging.info(f"  Mode:     {'DRY RUN' if dry_run else 'LIVE'}")
    logging.info("=" * 60)

    if not MEETING_SHEET_ID:
        logging.error("MEETING_SHEET_ID not set in .env — aborting")
        root.removeHandler(handler)
        return False, log_stream.getvalue()

    stats = {
        "rows_read":        0,
        "rows_synced":      0,
        "db_updated":       0,
        "mql_written_back": 0,
        "skipped_no_date":  0,
        "skipped_synced":   0,
        "errors":           0,
    }

    try:
        gc = get_sheets_client()
        logging.info("Google Sheets client connected")
    except Exception as e:
        logging.error(f"Google Sheets auth failed: {e}")
        root.removeHandler(handler)
        return False, log_stream.getvalue()

    try:
        sh = gc.open_by_key(MEETING_SHEET_ID)
    except Exception as e:
        logging.error(f"Cannot open meeting sheet: {e}")
        root.removeHandler(handler)
        return False, log_stream.getvalue()

    try:
        ws = _ensure_meetings_tab(sh)
    except Exception as e:
        logging.error(f"Cannot ensure 'Meetings' tab: {e}")
        root.removeHandler(handler)
        return False, log_stream.getvalue()

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        logging.info("Meeting sheet is empty — nothing to sync")
        root.removeHandler(handler)
        return True, log_stream.getvalue()

    data_rows = all_values[1:]  # skip header
    sync_ts   = datetime.now().strftime("%d/%m %H:%M")
    cell_updates: list = []

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    success = True

    try:
        for row_idx, row in enumerate(data_rows, start=2):
            stats["rows_read"] += 1

            # Skip if already synced
            sync_status = safe_col(row, MTG_SYNC_STATUS)
            if sync_status.startswith("✓"):
                stats["skipped_synced"] += 1
                continue

            # Skip if Meeting Date not filled (agent hasn't filled it yet)
            meeting_date_raw = safe_col(row, MTG_MEETING_DATE)
            meeting_date     = parse_date(meeting_date_raw)
            if not meeting_date:
                stats["skipped_no_date"] += 1
                continue

            meeting_id_raw = safe_col(row, MTG_MEETING_ID)
            try:
                meeting_id = int(meeting_id_raw)
            except (ValueError, TypeError):
                logging.warning(f"  Row {row_idx}: invalid Meeting ID '{meeting_id_raw}' — skipping")
                stats["errors"] += 1
                continue

            agent_name   = safe_col(row, MTG_AGENT)
            conclusion   = safe_col(row, MTG_CONCLUSION)
            solution     = safe_col(row, MTG_SOLUTION)
            sol_link     = safe_col(row, MTG_SOL_LINK)
            duration_raw = safe_col(row, MTG_DURATION)
            fu_at_sched_raw = safe_col(row, MTG_FU_AT_SCHED)

            try:
                fu_at_sched = int(fu_at_sched_raw)
            except (ValueError, TypeError):
                fu_at_sched = 1

            logging.info(
                f"  Row {row_idx}: Meeting ID={meeting_id} | "
                f"agent={agent_name} | date={meeting_date}"
            )

            if dry_run:
                logging.info(
                    f"  [DRY RUN] Would update DB + write Meeting Held to "
                    f"FU{fu_at_sched + 1} in MQL sheet"
                )
                stats["rows_synced"] += 1
                continue

            # ── 1. Update meetings table in DB ──
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE meetings
                    SET status          = 'held',
                        held_at         = %s,
                        conclusion      = COALESCE(NULLIF(%s, ''), conclusion),
                        solution_proposed = COALESCE(NULLIF(%s, ''), solution_proposed),
                        solution_link   = COALESCE(NULLIF(%s, ''), solution_link),
                        updated_at      = NOW()
                    WHERE id = %s AND status NOT IN ('solution_picked', 'lost')
                """, (meeting_date, conclusion, solution, sol_link, meeting_id))

                # Fetch contact_id for the meeting to use in write-back
                cur.execute(
                    "SELECT contact_id FROM meetings WHERE id = %s", (meeting_id,)
                )
                mtg_row = cur.fetchone()
                contact_id = mtg_row[0] if mtg_row else None

            conn.commit()
            stats["db_updated"] += 1

            # ── 2. Write "Meeting Held" back to agent's MQL calling sheet ──
            if contact_id and agent_name and fu_at_sched:
                wrote = write_meeting_held_to_mql_sheet(
                    gc, agent_name, campaign,
                    contact_id, fu_at_sched,
                    meeting_date, conclusion, dry_run
                )
                if wrote:
                    stats["mql_written_back"] += 1

            # ── 3. Mark this row as synced ──
            sync_col_letter = col_index_to_letter(MTG_SYNC_STATUS)
            cell_updates.append({
                "range":  f"{sync_col_letter}{row_idx}",
                "values": [[f"✓ Synced {sync_ts}"]],
            })
            stats["rows_synced"] += 1

        # Batch-update sync status cells
        if cell_updates and not dry_run:
            ws.batch_update(cell_updates)

    except Exception as e:
        conn.rollback()
        logging.error(f"Meeting sync failed: {e}", exc_info=True)
        success = False
    finally:
        conn.close()

    # ── Summary ──
    logging.info("\n" + "=" * 60)
    logging.info("  MEETING SYNC COMPLETE" + (" — DRY RUN" if dry_run else ""))
    logging.info("=" * 60)
    logging.info(f"  Rows read:           {stats['rows_read']}")
    logging.info(f"  Rows synced:         {stats['rows_synced']}")
    logging.info(f"  DB meetings updated: {stats['db_updated']}")
    logging.info(f"  MQL sheet write-back:{stats['mql_written_back']}")
    logging.info(f"  Skipped (no date):   {stats['skipped_no_date']}")
    logging.info(f"  Skipped (synced):    {stats['skipped_synced']}")
    logging.info(f"  Errors:              {stats['errors']}")
    logging.info(f"  Log: {log_file}")
    logging.info("=" * 60)

    root.removeHandler(handler)
    return success, log_stream.getvalue()


# ── CLI ────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Meeting Sync — meeting sheet conclusions → DB + MQL sheets")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--campaign", type=str, default="consulting")
    args = parser.parse_args()

    success, output = run_meeting_sync(
        dry_run  = args.dry_run,
        campaign = args.campaign,
    )
    print(output)
    sys.exit(0 if success else 1)
