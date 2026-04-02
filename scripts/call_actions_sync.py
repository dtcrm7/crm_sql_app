"""
call_actions_sync.py
====================
Runs every morning BEFORE allocation_engine.py.
Reads what agents filled in their Google Sheets yesterday,
and pushes all outcomes into the database.

Daily run order:
  01:00 AM → call_actions_sync.py   (reads yesterday's outcomes → DB)
  02:00 AM → allocation_engine.py   (builds today's sheets from updated DB)
  03:00 PM → Agents start shift

What this script does per agent:
    1. Opens each sub-sheet (New Contact, FU1 … FU5)
  2. Reads rows that have a Timestamp (= agent filled them in)
  3. For each filled row:
       a. Looks up contact + open allocation in DB
       b. Inserts call_action (one row per phone number)
       c. Updates contact_flag (highest priority wins)
       d. If Call Status = Invalid Number → marks phone is_invalid
       e. If outcome is final (DND / Referred / max attempts):
            → closes allocation (closed_at, close_reason)
       f. If outcome is non-final (Connected/Did not connect):
            → increments attempt_number for next FU
  4. Marks synced rows in sheet with "✓ Synced" to avoid double-sync

Run:
  python call_actions_sync.py              # normal run
  python call_actions_sync.py --dry-run    # print only, no DB writes
  python call_actions_sync.py --date 2026-03-13  # sync specific date

Requirements:
  pip install psycopg2-binary python-dotenv gspread google-auth
"""

import os
import re
import sys
import logging
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import gspread
from google.oauth2.service_account import Credentials

from sheet_values_config import (
    BD_CALL_STATUS_ALIASES,
    BD_CURRENT_STATE_ALIASES,
    BD_FINAL_CLOSE_STATUSES,
    BD_STOP_FOLLOWUP_STATES,
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

GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", str(PROJECT_ROOT / "credentials.json"))
# Sheet IDs are stored in agents.sheet_id in the DB — no hardcoded dict needed.

# Sub-sheet names → attempt_number (must match allocation_engine.py)
SUBSHEET_MAP = {
    "New Contact": 0,
    "FU1":         1,
    "FU2":         2,
    "FU3":         3,
    "FU4":         4,
    "FU5":         5,
}

# Column positions — New Contact sheet (13 data cols, no context)
# Unique ID | Company | Name | Phone | Title |
# Call Duration | Call Status | Current State | Remark |
# Recording Link | Transcript | Dream Snapshot | Timestamp | Sync Status
COL_NEW = {
    "unique_id":        0,
    "company":          1,
    "name":             2,
    "phone":            3,
    "title":            4,
    "call_duration":    5,
    "call_status":      6,   # agent fills: Connected / Did not connect / etc.
    "current_state":    7,
    "remark":           8,
    "recording_link":   9,
    "transcript_link":  10,
    "dream_snapshot":   11,
    "timestamp":        12,
    "campaign":         13,  # pre-filled by allocation engine
    "sync_status":      14,  # written at runtime by sync script
}

# Column positions — FU1-FU5 sheets (3 extra pre-filled context cols after Title)
# Unique ID | Company | Name | Phone | Title |
# Last Status | Last Remark | Last Called |
# Call Duration | Call Status | Current State | Remark |
# Recording Link | Transcript | Dream Snapshot | Timestamp | Sync Status
COL_FU = {
    "unique_id":        0,
    "company":          1,
    "name":             2,
    "phone":            3,
    "title":            4,
    "last_status":      5,   # pre-filled by engine
    "last_remark":      6,   # pre-filled by engine
    "last_called":      7,   # pre-filled by engine
    "call_duration":    8,
    "call_status":      9,   # agent fills: Connected / Did not connect / etc.
    "current_state":    10,
    "remark":           11,
    "recording_link":   12,
    "transcript_link":  13,
    "dream_snapshot":   14,
    "timestamp":        15,
    "campaign":         16,  # pre-filled by allocation engine
    "sync_status":      17,  # written at runtime by sync script
}

def get_col(attempt_number):
    """Returns correct column map based on which sub-sheet we're reading."""
    return COL_NEW if attempt_number == 0 else COL_FU

# Call statuses that permanently close an allocation
FINAL_CLOSE_STATUSES = set(BD_FINAL_CLOSE_STATUSES)

# Current states that should not move to the next FU page.
STOP_FOLLOWUP_STATES = set(BD_STOP_FOLLOWUP_STATES)

# Call statuses that close after max attempts (handled separately)
# All others keep allocation open, increment attempt_number

# ── NORMALISATIONS ────────────────────────────────────────────
CALL_STATUS_NORM = dict(BD_CALL_STATUS_ALIASES)
CURRENT_STATE_NORM = dict(BD_CURRENT_STATE_ALIASES)

FLAG_RANK = {
    "fresh":            0,
    "in_progress":      1,
    "needs_followup":   2,
    "attempt_3_months": 3,
    "not_interested":   4,
    "dnd":              5,
    "snapshot_sent":    6,
    "shared_story":     7,
    "language_issue":   8,
    "referred":         9,
    "invalid_number":   10,
}

def norm_status(raw):
    return CALL_STATUS_NORM.get(raw.strip().lower(), raw.strip())


def norm_state(raw):
    if not raw or not raw.strip():
        return None
    key = raw.strip().lower().rstrip(".")
    return CURRENT_STATE_NORM.get(key, raw.strip())


def derive_flag(call_status, current_state):
    cs  = (call_status  or "").strip().lower()
    cur = (current_state or "").strip().lower().rstrip(".")
    if "invalid" in cs:
        return "invalid_number"
    if cs == "referred":
        return "referred"
    if "do not disturb" in cs or "do not disturb" in cur:
        return "dnd"
    if cur == "shared story":
        return "shared_story"
    if cur == "snapshot sent":
        return "snapshot_sent"
    if cur == "allocate again":
        return "fresh"
    if "3 months" in cur:
        return "attempt_3_months"
    if cur == "not interested":
        return "not_interested"
    return "in_progress"


def best_flag(a, b):
    return a if FLAG_RANK.get(a, 0) >= FLAG_RANK.get(b, 0) else b


def parse_duration(raw):
    """HH:MM:SS or decimal fraction → integer seconds. None if blank."""
    if not raw or not raw.strip() or raw.strip() == "0":
        return None
    raw = raw.strip().lstrip(":")
    m = re.match(r'^(\d+):(\d{2}):(\d{2})$', raw)
    if m:
        secs = int(m.group(1))*3600 + int(m.group(2))*60 + int(m.group(3))
        return secs if secs > 0 else None
    try:
        frac = float(raw)
        if 0 < frac < 1:
            return round(frac * 86400) or None
    except ValueError:
        pass
    return None


def parse_phone(raw):
    """Extract 10-digit number. Returns None if invalid."""
    if not raw:
        return None
    digits = re.sub(r'\D', '', raw.strip())
    return digits[-10:] if len(digits) >= 10 else None


def parse_timestamp(raw):
    """Parse sheet timestamp into datetime. Returns None if blank."""
    if not raw or not raw.strip():
        return None
    for fmt in ["%d/%m/%Y %H:%M:%S", "%d/%m/%Y",
                "%m/%d/%Y %H:%M:%S", "%m/%d/%Y",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    return None


def parse_unique_id(uid_str):
    """'RR | 6175204' → ('rocketreach', '6175204')"""
    if not uid_str or not uid_str.strip():
        return None, None
    parts = uid_str.strip().split("|")
    src_map = {"RR": "rocketreach", "MS": "msme",
               "PH": "pharma",      "MN": "manual"}
    prefix    = parts[0].strip().upper()
    source    = src_map.get(prefix, "rocketreach")
    source_id = parts[1].strip() if len(parts) > 1 else None
    return source, source_id


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging(dry_run=False):
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = "_dryrun" if dry_run else ""
    log_file = str(PROJECT_ROOT / "logs" / f"sync_{ts}{suffix}.log")
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
    creds  = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE, scopes=scopes
    )
    return gspread.authorize(creds)


# ── DB HELPERS ────────────────────────────────────────────────
def get_contact(cur, source, source_id):
    cur.execute("""
        SELECT id, contact_flag FROM contacts
        WHERE source = %s AND source_id = %s LIMIT 1
    """, (source, source_id))
    row = cur.fetchone()
    if not row:
        # Fallback — source_id only
        cur.execute("""
            SELECT id, contact_flag FROM contacts
            WHERE source_id = %s LIMIT 1
        """, (source_id,))
        row = cur.fetchone()
    return row  # (id, contact_flag) or None


def get_open_allocation(cur, contact_id, agent_id, attempt_number):
    """Find the open allocation for this contact+agent+attempt."""
    cur.execute("""
        SELECT id, attempt_number FROM contact_allocations
        WHERE contact_id     = %s
          AND agent_id       = %s
          AND attempt_number = %s
          AND closed_at IS NULL
        LIMIT 1
    """, (contact_id, agent_id, attempt_number))
    return cur.fetchone()  # (id, attempt_number) or None


def get_phone_id(cur, contact_id, phone):
    cur.execute("""
        SELECT id FROM contact_phones
        WHERE contact_id = %s AND phone_number = %s LIMIT 1
    """, (contact_id, phone))
    row = cur.fetchone()
    return row[0] if row else None


def action_already_synced(cur, contact_id, agent_id, phone, called_at):
    """Prevent double-syncing the exact same call."""
    cur.execute("""
        SELECT 1 FROM call_actions
        WHERE contact_id        = %s
          AND agent_id          = %s
          AND phone_number_used = %s
          AND called_at         = %s
        LIMIT 1
    """, (contact_id, agent_id, phone, called_at))
    return bool(cur.fetchone())


# ── CORE: PROCESS ONE ROW ─────────────────────────────────────
def process_row(cur, row, agent_id, attempt_number, dry_run, stats):
    """
    Processes one filled row from an agent sub-sheet.
    Uses COL_NEW for New Contact sheet, COL_FU for FU1-FU5 sheets.
    Returns True if synced successfully.
    """
    COL = get_col(attempt_number)

    # ── Parse unique ID ──
    uid_raw   = row[COL["unique_id"]] if len(row) > COL["unique_id"] else ""
    source, source_id = parse_unique_id(uid_raw)
    if not source_id:
        stats["skipped_no_id"] += 1
        return False

    # ── Parse phone ──
    phone = parse_phone(
        row[COL["phone"]] if len(row) > COL["phone"] else ""
    )
    if not phone:
        stats["skipped_no_phone"] += 1
        return False

    # ── Require timestamp ──
    ts_raw  = row[COL["timestamp"]] if len(row) > COL["timestamp"] else ""
    called_at = parse_timestamp(ts_raw)
    if not called_at:
        stats["skipped_no_timestamp"] += 1
        return False

    # ── Require call status ──
    status_raw = row[COL["call_status"]] if len(row) > COL["call_status"] else ""
    if not status_raw.strip():
        stats["skipped_no_status"] += 1
        return False

    call_status    = norm_status(status_raw)
    current_state  = norm_state(
        row[COL["current_state"]] if len(row) > COL["current_state"] else ""
    )
    call_duration  = parse_duration(
        row[COL["call_duration"]] if len(row) > COL["call_duration"] else ""
    )
    remark         = (row[COL["remark"]]         if len(row) > COL["remark"]         else "") or None
    rec_link       = (row[COL["recording_link"]] if len(row) > COL["recording_link"] else "") or None
    transcript     = (row[COL["transcript_link"]]if len(row) > COL["transcript_link"]else "") or None
    dream_snap     = (row[COL["dream_snapshot"]] if len(row) > COL["dream_snapshot"] else "") or None

    # ── Look up contact ──
    contact_row = get_contact(cur, source, source_id)
    if not contact_row:
        logging.debug(f"Contact not in DB: {uid_raw}")
        stats["skipped_no_contact"] += 1
        return False
    contact_id, current_flag = contact_row

    # ── Look up open allocation ──
    alloc = get_open_allocation(cur, contact_id, agent_id, attempt_number)
    if not alloc:
        logging.debug(
            f"No open allocation: contact={contact_id} "
            f"agent={agent_id} attempt={attempt_number}"
        )
        stats["skipped_no_allocation"] += 1
        return False
    allocation_id = alloc[0]

    # ── Duplicate check ──
    if action_already_synced(cur, contact_id, agent_id, phone, called_at):
        stats["skipped_duplicate"] += 1
        return False

    if dry_run:
        logging.info(
            f"  [DRY RUN] Would sync: {uid_raw} | {phone} | "
            f"{call_status} | {current_state} | {called_at}"
        )
        stats["would_sync"] += 1
        return True

    # ── Insert call_action ──
    cur.execute("""
        INSERT INTO call_actions (
            contact_id, allocation_id, agent_id,
            phone_number_used, attempt_number,
            call_status, current_state,
            call_duration, remark, recording_link,
            transcript_link, dream_snapshot_link,
            called_at
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s
        )
    """, (
        contact_id, allocation_id, agent_id,
        phone, attempt_number,
        call_status, current_state,
        call_duration, remark, rec_link,
        transcript, dream_snap,
        called_at,
    ))
    stats["call_actions_inserted"] += 1

    # ── Mark phone invalid ──
    if call_status == "Invalid Number":
        phone_id = get_phone_id(cur, contact_id, phone)
        if phone_id:
            cur.execute("""
                UPDATE contact_phones
                SET is_invalid = TRUE, invalidated_at = %s
                WHERE id = %s AND is_invalid = FALSE
            """, (called_at, phone_id))
            if cur.rowcount:
                stats["phones_invalidated"] += 1

    # ── Update contact_flag ──
    new_flag = derive_flag(call_status, current_state)
    final_flag = best_flag(current_flag, new_flag)
    if final_flag != current_flag:
        cur.execute("""
            UPDATE contacts
            SET contact_flag = %s, flag_updated_at = NOW()
            WHERE id = %s
        """, (final_flag, contact_id))
        stats["flags_updated"] += 1

    # ── Handle allocation close / increment ──
    should_close    = False
    close_reason    = None
    next_attempt    = attempt_number + 1

    if call_status in FINAL_CLOSE_STATUSES:
        # DND / Referred → permanently close
        should_close = True
        close_reason = "completed"

    elif current_state == "Attempt Again after 3 months":
        # Max attempts exhausted (manually set by agent)
        should_close = True
        close_reason = "max_attempts"

    elif current_state in STOP_FOLLOWUP_STATES:
        # Explicit stop states from sheet rules.
        should_close = True
        close_reason = "completed"

    elif attempt_number >= 5:
        # FU5 reached → auto-close regardless of outcome
        should_close = True
        close_reason = "max_attempts"

    if should_close:
        cur.execute("""
            UPDATE contact_allocations
            SET closed_at    = %s,
                close_reason = %s
            WHERE id = %s
        """, (called_at, close_reason, allocation_id))
        stats["allocations_closed"] += 1
    else:
        # Increment attempt_number → contact will appear in FU(n+1) next allocation
        cur.execute("""
            UPDATE contact_allocations
            SET attempt_number = %s
            WHERE id = %s
        """, (next_attempt, allocation_id))
        stats["allocations_incremented"] += 1

    return True


# ── CORE: PROCESS ONE AGENT ───────────────────────────────────
def sync_agent(gc, conn, agent_id, agent_name, sheet_id,
               sync_date, dry_run, stats):
    """
    Reads all sub-sheets for one agent, syncs unsynced rows.
    """
    logging.info(f"\n  Agent: {agent_name} (id={agent_id})")

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        logging.error(f"  Cannot open sheet for {agent_name}: {e}")
        stats["agents_failed"] += 1
        return

    agent_stats = {
        "rows_read":      0,
        "rows_synced":    0,
        "rows_skipped":   0,
    }

    for subsheet_name, attempt_number in SUBSHEET_MAP.items():
        try:
            ws = sh.worksheet(subsheet_name)
        except gspread.WorksheetNotFound:
            logging.warning(f"  Sub-sheet '{subsheet_name}' not found — skipping")
            continue

        # Read all values (skip header row 1)
        all_values = ws.get_all_values()
        if len(all_values) < 2:
            continue   # empty sheet

        data_rows  = all_values[1:]   # skip header
        rows_to_mark_synced = []      # (row_index, col_N_value)
        COL = get_col(attempt_number)  # column map for this sub-sheet

        with conn.cursor() as cur:
            for row_idx, row in enumerate(data_rows, start=2):  # 1-indexed, row 1 = header

                # Skip already synced
                sync_col = row[COL["sync_status"]] if len(row) > COL["sync_status"] else ""
                if sync_col.strip().startswith("✓"):
                    continue

                # Skip rows with no timestamp (agent hasn't filled yet)
                ts_raw = row[COL["timestamp"]] if len(row) > COL["timestamp"] else ""
                if not ts_raw.strip():
                    continue

                # If sync_date specified, only sync rows from that date
                if sync_date:
                    ts = parse_timestamp(ts_raw)
                    if ts and ts.date() != sync_date:
                        continue

                agent_stats["rows_read"] += 1

                synced = process_row(
                    cur, row, agent_id, attempt_number,
                    dry_run, stats
                )

                if synced:
                    agent_stats["rows_synced"] += 1
                    rows_to_mark_synced.append(row_idx)
                else:
                    agent_stats["rows_skipped"] += 1

            if not dry_run:
                conn.commit()

        # ── Mark synced rows in sheet ──
        if rows_to_mark_synced and not dry_run:
            sync_marker = f"✓ Synced {datetime.now().strftime('%d/%m %H:%M')}"
            sheet_col = get_col(attempt_number)
            col_letter = chr(ord('A') + sheet_col["sync_status"])
            cell_updates = []
            for row_idx in rows_to_mark_synced:
                cell_updates.append({
                    "range": f"{col_letter}{row_idx}",
                    "values": [[sync_marker]]
                })
            if cell_updates:
                ws.batch_update(cell_updates)

        logging.info(
            f"  {subsheet_name}: read={agent_stats['rows_read']} "
            f"synced={agent_stats['rows_synced']} "
            f"skipped={agent_stats['rows_skipped']}"
        )

    stats["agents_synced"] += 1
    logging.info(
        f"  {agent_name} total — "
        f"synced: {agent_stats['rows_synced']}, "
        f"skipped: {agent_stats['rows_skipped']}"
    )


# ── COMPANY FLAG UPDATE ───────────────────────────────────────
COMPANY_FLAG_RANK = {
    "fresh":          0,
    "in_progress":    1,
    "dnd":            2,
    "not_interested": 3,
    "snapshot_sent":  4,
    "shared_story":   5,
    "referred":       6,
    "invalid_number": 7,
}

def update_company_flags(conn, dry_run):
    """
    After sync, roll up contact flags to company level.
    Each company gets the highest-priority flag across all its contacts.

    Only processes companies that have at least one contact whose
    flag_updated_at changed today (avoids full-table scan every night).
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH company_best AS (
                SELECT
                    c.company_id,
                    CASE
                        WHEN c.contact_flag = 'needs_followup' THEN 'in_progress'
                        ELSE c.contact_flag
                    END AS contact_flag,
                    -- Assign numeric rank so we can pick highest
                    CASE
                        WHEN c.contact_flag = 'invalid_number' THEN 7
                        WHEN c.contact_flag = 'referred'       THEN 6
                        WHEN c.contact_flag = 'shared_story'   THEN 5
                        WHEN c.contact_flag = 'snapshot_sent'  THEN 4
                        WHEN c.contact_flag = 'not_interested' THEN 3
                        WHEN c.contact_flag = 'dnd'            THEN 2
                        WHEN c.contact_flag = 'needs_followup' THEN 1
                        WHEN c.contact_flag = 'in_progress'    THEN 1
                        ELSE 0
                    END AS rank
                FROM contacts c
                WHERE c.company_id IS NOT NULL
                  AND c.flag_updated_at >= NOW() - INTERVAL '24 hours'
            ),
            ranked AS (
                SELECT
                    cb.company_id,
                    cb.contact_flag,
                    ROW_NUMBER() OVER (
                        PARTITION BY cb.company_id
                        ORDER BY cb.rank DESC
                    ) AS rn
                FROM company_best cb
            )
            UPDATE companies co
            SET company_flag    = r.contact_flag,
                flag_updated_at = NOW()
            FROM ranked r
            WHERE r.company_id = co.id
              AND r.rn = 1
              AND r.contact_flag != co.company_flag
        """)
        updated = cur.rowcount
        if not dry_run:
            conn.commit()
        logging.info(f"  Company flags updated: {updated}")
    return updated


# ── MAIN ──────────────────────────────────────────────────────
def run_sync(dry_run=False, sync_date=None, campaign='consulting'):
    log_file = setup_logging(dry_run)

    logging.info("=" * 60)
    logging.info("  CRM CALL ACTIONS SYNC")
    logging.info(f"  Date:     {date.today()}")
    logging.info(f"  Campaign: {campaign}")
    logging.info(f"  Mode:     {'DRY RUN — no writes' if dry_run else 'LIVE'}")
    if sync_date:
        logging.info(f"  Syncing:  {sync_date} only")
    logging.info("=" * 60)

    stats = {
        "agents_synced":           0,
        "agents_failed":           0,
        "call_actions_inserted":   0,
        "allocations_closed":      0,
        "allocations_incremented": 0,
        "phones_invalidated":      0,
        "flags_updated":           0,
        "company_flags_updated":   0,
        "skipped_no_id":           0,
        "skipped_no_phone":        0,
        "skipped_no_timestamp":    0,
        "skipped_no_status":       0,
        "skipped_no_contact":      0,
        "skipped_no_allocation":   0,
        "skipped_duplicate":       0,
        "would_sync":              0,
    }

    # Connect Google Sheets
    try:
        gc = get_sheets_client()
        logging.info("Google Sheets client connected")
    except Exception as e:
        logging.error(f"Google Sheets auth failed: {e}")
        sys.exit(1)

    # Connect DB
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        # Get active agents with sheet IDs for this campaign from agent_sheets
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.name, ags.sheet_id
                FROM agents a
                LEFT JOIN agent_sheets ags
                       ON ags.agent_id = a.id AND ags.campaign = %s
                WHERE a.status = 'active'
                  AND (a.team IS NULL OR a.team != 'mql')
                ORDER BY a.id
            """, (campaign,))
            active_agents = cur.fetchall()

        logging.info(f"Active agents in DB: {[a[1] for a in active_agents]}")

        for agent_id, agent_name, sheet_id in active_agents:
            if not sheet_id:
                logging.warning(
                    f"No sheet_id set for {agent_name} (id={agent_id}) campaign={campaign} "
                    f"— set it in the Agents page of the dashboard."
                )
                continue

            sync_agent(
                gc, conn, agent_id, agent_name, sheet_id,
                sync_date, dry_run, stats
            )

        # ── Roll up company flags ─────────────────────────────────
        logging.info("\nUpdating company flags from contact flags...")
        company_flags_updated = update_company_flags(conn, dry_run)
        stats["company_flags_updated"] = company_flags_updated

    except Exception as e:
        conn.rollback()
        logging.error(f"Sync failed: {e}", exc_info=True)
        raise
    finally:
        conn.close()

    # ── Summary ──────────────────────────────────────────────
    logging.info("\n" + "=" * 60)
    logging.info("  SYNC COMPLETE" + (" — DRY RUN" if dry_run else ""))
    logging.info("=" * 60)
    logging.info(f"  Agents synced:              {stats['agents_synced']}")
    logging.info(f"  Agents failed:              {stats['agents_failed']}")
    logging.info(f"  ─────────────────────────────────────────")
    if dry_run:
        logging.info(f"  Rows that WOULD sync:       {stats['would_sync']}")
    else:
        logging.info(f"  Call actions inserted:      {stats['call_actions_inserted']}")
        logging.info(f"  Allocations closed:         {stats['allocations_closed']}")
        logging.info(f"  Allocations incremented:    {stats['allocations_incremented']}")
        logging.info(f"  Phones invalidated:         {stats['phones_invalidated']}")
        logging.info(f"  Contact flags updated:      {stats['flags_updated']}")
        logging.info(f"  Company flags updated:      {stats['company_flags_updated']}")
    logging.info(f"  ─────────────────────────────────────────")
    logging.info(f"  Skipped (no ID):             {stats['skipped_no_id']}")
    logging.info(f"  Skipped (no phone):          {stats['skipped_no_phone']}")
    logging.info(f"  Skipped (no timestamp):      {stats['skipped_no_timestamp']}")
    logging.info(f"  Skipped (no status):         {stats['skipped_no_status']}")
    logging.info(f"  Skipped (contact not in DB): {stats['skipped_no_contact']}")
    logging.info(f"  Skipped (no open alloc):     {stats['skipped_no_allocation']}")
    logging.info(f"  Skipped (duplicate):         {stats['skipped_duplicate']}")
    logging.info(f"  Log: {log_file}")
    logging.info("=" * 60)


# ── ENTRYPOINT ────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync agent sheet outcomes to CRM database"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be synced without writing to DB or marking sheet"
    )
    parser.add_argument(
        "--date",
        help="Only sync rows from this date (YYYY-MM-DD). Default: all unsynced rows."
    )
    parser.add_argument(
        "--campaign", type=str, default="consulting",
        help="Campaign to sync (reads from that campaign's agent sheets). Default: consulting."
    )
    args = parser.parse_args()

    sync_date = None
    if args.date:
        try:
            sync_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
            sys.exit(1)

    run_sync(dry_run=args.dry_run, sync_date=sync_date, campaign=args.campaign)
