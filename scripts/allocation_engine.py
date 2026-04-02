"""
allocation_engine.py
====================
Runs every morning before agents start their shift.

What it does:
  1. Checks agent availability (who is present today)
  2. For each available agent:
       - Allocates ALL pending follow-ups (no cap):
           * Unresponded: allocated but never called (highest priority)
           * needs_followup: stalled contacts
           * Hot/Warm leads
           * Call-back/Rescheduled
           * All other in-progress
       - Allocates exactly kpi_dialed fresh contacts on top
  3. Inserts/updates contact_allocations
  4. Writes one Google Sheet tab per agent per sub-sheet
    (New Contact, FU1, FU2, FU3, FU4, FU5)
  5. Flags stalled contacts as needs_followup
  6. Resets is_on_leave = FALSE for all agents (previous day cleanup)

Run daily (e.g. Windows Task Scheduler at 2:00 PM before shift):
  python allocation_engine.py

Requirements:
  pip install psycopg2-binary python-dotenv gspread google-auth
"""

import os
import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import gspread
from google.oauth2.service_account import Credentials

from sheet_values_config import BD_CALL_STATUS_VALUES, BD_CURRENT_STATE_VALUES

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

# Path to Google Service Account credentials JSON
# Download from Google Cloud Console → Service Accounts → Keys
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", str(PROJECT_ROOT / "credentials.json"))

STALE_DAYS         = 7    # days without attempt → needs_followup
ALLOCATION_BUFFER  = 5    # extra fresh contacts on top of kpi_dialed (buffer for invalids/no-shows)
                          # e.g. kpi_dialed=65 → allocate 70 fresh total
# NOTE: all per-agent settings (kpi_dialed = fresh count, sheet_id) are stored in the agents
# table and read at runtime — nothing is hardcoded here.

# Sub-sheet names in agent's Google Sheet (must exist already)
SUBSHEET_NAMES = {
    0: "New Contact",
    1: "FU1",
    2: "FU2",
    3: "FU3",
    4: "FU4",
    5: "FU5",
}

SHEET_VALIDATION_END_ROW = 2000

# Only these synced outcomes should move to next BD follow-up sheet.
FOLLOWUP_PROGRESS_STATES = (
    "Attempt Again",
    "Rescheduled",
    "Interested",
    "Shared Story",
)

# New Contact sheet: no prior history, no context columns
HEADERS_NEW_CONTACT = [
    "Unique ID - Static",
    "Company",
    "Name",
    "Phone",
    "Title",
    "Call Duration",
    "Call Status",        # ← agent fills: Connected / Did not connect / etc.
    "Current State",
    "Remark",
    "Recording Link",
    "Transcript",
    "Dream Snapshot",
    "Timestamp",
    "Campaign",           # ← pre-filled (col 13); Sync Status written at col 14
]

# FU1-FU5 sheets: 3 extra pre-filled context columns after Title
# so agent sees exactly what happened last time before they dial
HEADERS_FOLLOWUP = [
    "Unique ID - Static",
    "Company",
    "Name",
    "Phone",
    "Title",
    "Last Status",        # ← pre-filled from DB
    "Last Remark",        # ← pre-filled from DB
    "Last Called",        # ← pre-filled from DB (DD/MM/YYYY)
    "Call Duration",
    "Call Status",        # ← agent fills: Connected / Did not connect / etc.
    "Current State",
    "Remark",
    "Recording Link",
    "Transcript",
    "Dream Snapshot",
    "Timestamp",
    "Campaign",           # ← pre-filled (col 16); Sync Status written at col 17
]

def get_headers_for_attempt(attempt_number):
    return HEADERS_NEW_CONTACT if attempt_number == 0 else HEADERS_FOLLOWUP


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging():
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = str(PROJECT_ROOT / "logs" / f"allocation_{ts}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ]
    )
    return log_file


# ── DATABASE ──────────────────────────────────────────────────
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


# ── STEP 1: Flag stalled contacts ────────────────────────────
def flag_stale_contacts(conn, dry_run=False):
    """
    Find contacts that:
    - Are in_progress (have been called at least once)
    - Have an active allocation
    - Last call_action was > STALE_DAYS ago
    - Haven't reached max attempts yet
    → Set contact_flag = 'needs_followup'
    These get priority in follow-up slots today.
    """
    stale_sql = f"""
        SELECT c.id
        FROM contacts c
        JOIN contact_allocations ca
            ON ca.contact_id = c.id AND ca.closed_at IS NULL
        JOIN call_actions a
            ON a.contact_id  = c.id
        WHERE c.contact_flag IN ('in_progress', 'fresh')
          AND ca.attempt_number < 5
        GROUP BY c.id
        HAVING MAX(a.called_at) < NOW() - INTERVAL '{STALE_DAYS} days'
    """
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(f"SELECT COUNT(*) FROM ({stale_sql}) sub")
            flagged = cur.fetchone()[0]
            logging.info(f"[DRY RUN] Would flag {flagged} contacts as needs_followup")
        else:
            cur.execute(f"""
                UPDATE contacts
                SET contact_flag    = 'needs_followup',
                    flag_updated_at = NOW()
                WHERE id IN ({stale_sql})
            """)
            flagged = cur.rowcount
            conn.commit()
            logging.info(f"Flagged {flagged} contacts as needs_followup")
    return flagged


# ── STEP 2: Reset previous day's leave ───────────────────────
def reset_leave_flags(conn, dry_run=False):
    """
    Reset is_on_leave = FALSE only for agents whose leave was set on a PREVIOUS day.
    Leave flags set TODAY (updated_at::date = CURRENT_DATE) are preserved so that
    marking an agent on leave in the dashboard before running allocation actually works.
    In dry_run mode: log how many would be reset but skip the UPDATE.
    """
    with conn.cursor() as cur:
        if dry_run:
            cur.execute("""
                SELECT COUNT(*) FROM agents
                WHERE is_on_leave = TRUE
                  AND updated_at::date < CURRENT_DATE
            """)
            reset = cur.fetchone()[0]
            logging.info(f"[DRY RUN] Would reset is_on_leave for {reset} agents from previous days (skipped)")
        else:
            cur.execute("""
                UPDATE agents
                SET is_on_leave = FALSE,
                    updated_at  = NOW()
                WHERE is_on_leave = TRUE
                  AND updated_at::date < CURRENT_DATE
            """)
            reset = cur.rowcount
            conn.commit()
            logging.info(f"Reset is_on_leave for {reset} agents (previous-day leave cleared)")


# ── STEP 3: Get available agents ─────────────────────────────
def get_available_agents(conn, campaign):
    """
    Returns list of agents who are active and NOT on leave today.
    Each row: {agent_id, name, kpi_dialed, shift_name, sheet_id}
    sheet_id comes from agent_sheets for the given campaign.
    kpi_dialed = number of FRESH contacts to allocate today.
    Follow-ups are ALL pending — no cap applied.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT a.id          AS agent_id,
                   a.name,
                   a.kpi_dialed,
                   a.shift_name,
                   ags.sheet_id
            FROM agents a
            LEFT JOIN agent_sheets ags
                   ON ags.agent_id = a.id AND ags.campaign = %s
            WHERE a.status      = 'active'
              AND a.is_on_leave = FALSE
              AND (a.team IS NULL OR a.team != 'mql')
            ORDER BY a.id
        """, (campaign,))
        agents = cur.fetchall()
        logging.info(f"Available agents today: {len(agents)}")
        for a in agents:
            logging.info(
                f"  → {a['name']} (shift: {a['shift_name']}, "
                f"fresh target: {a['kpi_dialed']} + buffer {ALLOCATION_BUFFER} = "
                f"{a['kpi_dialed'] + ALLOCATION_BUFFER} | FU: all pending)"
            )
        return agents


# ── STEP 4: Pick phone numbers for agent ─────────────────────

def get_fresh_phones(cur, agent_id, limit, campaign):
    """
    Fresh phone numbers for a given campaign:
    - contact_flag = 'fresh' OR 90-day-ready 'attempt_3_months'
    - phone not invalid
    - contact not actively allocated
    - for fresh contacts: agent has never been filled_by for this contact
    Returns list of dicts: {contact_id, phone_id, phone_number, ...}
    """
    cur.execute("""
        SELECT
            c.id            AS contact_id,
            p.id            AS phone_id,
            p.phone_number,
            p.rank          AS phone_rank,
            c.first_name,
            c.last_name,
            c.designation,
            c.source,
            c.source_id,
            co.name         AS company_name,
            co.raw_address
        FROM contacts c
        JOIN contact_phones p  ON p.contact_id = c.id
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE (
                c.contact_flag = 'fresh'
                OR (
                    c.contact_flag = 'attempt_3_months'
                    AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                        <= (CURRENT_DATE - INTERVAL '90 days')::date
                )
              )
          AND c.campaign     = %s
          AND p.is_invalid   = FALSE
          -- not currently allocated to anyone
          AND NOT EXISTS (
              SELECT 1 FROM contact_allocations a
              WHERE a.contact_id = c.id AND a.closed_at IS NULL
          )
          -- for fresh contacts avoid re-assigning same agent; 3-month-ready contacts
          -- are redistributed using normal daily allocation.
          AND (
              c.contact_flag = 'attempt_3_months'
              OR NOT EXISTS (
                  SELECT 1 FROM contact_allocations a
                  WHERE a.contact_id = c.id AND a.filled_by = %s
              )
          )
        ORDER BY c.id, p.rank
        LIMIT %s
    """, (campaign, agent_id, limit))
    return cur.fetchall()


def get_followup_phones(cur, agent_id, campaign):
    """
    Follow-up phone numbers for this agent (campaign-filtered).

    IMPORTANT:
    - Includes only contacts that have a synced call_action and latest current_state
      in FOLLOWUP_PROGRESS_STATES.
    - Excludes unresponded rows (no call_action yet), so unsynced sheet rows do not
      flood FU queues.
    """
    cur.execute("""
        WITH latest_action AS (
            SELECT DISTINCT ON (contact_id)
                contact_id,
                call_status,
                current_state,
                called_at
            FROM call_actions
            ORDER BY contact_id, called_at DESC
        ),
        priority_contacts AS (
            SELECT
                c.id            AS contact_id,
                ca.attempt_number,
                ca.id           AS allocation_id,
                CASE
                    WHEN c.contact_flag = 'needs_followup' THEN 1
                    WHEN la.current_state = 'Interested'   THEN 2
                    WHEN la.current_state = 'Shared Story' THEN 3
                    ELSE 4
                END             AS priority
            FROM contacts c
            JOIN contact_allocations ca
                ON ca.contact_id = c.id
               AND ca.closed_at IS NULL
               AND ca.agent_id  = %s
            JOIN latest_action la ON la.contact_id = c.id
            WHERE c.contact_flag NOT IN (
                'invalid_number', 'referred', 'language_issue',
                'shared_story',   'snapshot_sent'
            )
              AND c.campaign = %s
              AND la.current_state = ANY(%s::text[])
        )
        SELECT
            pc.contact_id,
            p.id            AS phone_id,
            p.phone_number,
            p.rank          AS phone_rank,
            pc.attempt_number,
            pc.allocation_id,
            pc.priority,
            c.first_name,
            c.last_name,
            c.designation,
            c.source,
            c.source_id,
            co.name         AS company_name,
            co.raw_address
        FROM priority_contacts pc
        JOIN contacts c        ON c.id  = pc.contact_id
        JOIN contact_phones p  ON p.contact_id = pc.contact_id
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE p.is_invalid = FALSE
        ORDER BY pc.priority, pc.contact_id, p.rank
    """, (agent_id, campaign, list(FOLLOWUP_PROGRESS_STATES)))
    return cur.fetchall()


def get_historical_phones(cur, agent_id, limit, campaign):
    """
    Historical (migrated) contacts for this agent+campaign that were handled,
    have no current open allocation, and still need follow-up.
    Used to reactivate pre-migration data into today's allocation.
    """
    cur.execute("""
        WITH agent_history AS (
            SELECT DISTINCT ON (c.id)
                c.id            AS contact_id,
                ca.attempt_number,
                CASE
                    WHEN c.contact_flag = 'needs_followup' THEN 1
                    WHEN c.contact_flag = 'in_progress'    THEN 2
                    ELSE 3
                END             AS priority
            FROM contacts c
            JOIN contact_allocations ca
                ON ca.contact_id = c.id
               AND ca.agent_id   = %s
               AND ca.closed_at  IS NOT NULL
            WHERE c.campaign = %s
              AND c.contact_flag NOT IN (
                'invalid_number', 'referred', 'language_issue',
                'shared_story',   'snapshot_sent', 'fresh',
                'not_interested', 'dnd', 'attempt_3_months'
            )
            AND NOT EXISTS (
                SELECT 1 FROM contact_allocations a2
                WHERE a2.contact_id = c.id AND a2.closed_at IS NULL
            )
            ORDER BY c.id, ca.attempt_number DESC
        )
        SELECT
            ah.contact_id,
            p.id            AS phone_id,
            p.phone_number,
            p.rank          AS phone_rank,
            ah.attempt_number,
            NULL::integer   AS allocation_id,
            ah.priority,
            c.first_name,
            c.last_name,
            c.designation,
            c.source,
            c.source_id,
            co.name         AS company_name,
            co.raw_address
        FROM agent_history ah
        JOIN contacts c        ON c.id  = ah.contact_id
        JOIN contact_phones p  ON p.contact_id = ah.contact_id
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE p.is_invalid = FALSE
        ORDER BY ah.priority, ah.contact_id, p.rank
        LIMIT %s
    """, (agent_id, campaign, limit))
    return cur.fetchall()


def pick_phones_for_agent(conn, agent_id, fresh_count, campaign):
    """
    Picks phone numbers for one agent (campaign-filtered).

    Follow-ups: only synced progression states (Attempt Again/Rescheduled/Interested/Shared Story).
    Fresh:      exactly fresh_count + ALLOCATION_BUFFER new contacts.

    Returns (fresh_rows, followup_rows)
    """
    fresh_needed = fresh_count + ALLOCATION_BUFFER

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # All pending follow-ups — no limit
        followup_rows  = list(get_followup_phones(cur, agent_id, campaign))
        followup_count = len(followup_rows)

        # Historical (migrated) contacts with closed allocations that still need FU
        hist_rows = get_historical_phones(cur, agent_id, 9999, campaign)
        if hist_rows:
            followup_rows  = followup_rows + list(hist_rows)
            followup_count = len(followup_rows)
            logging.info(f"  Agent {agent_id}: +{len(hist_rows)} historical contacts reactivated")

        fresh_rows = get_fresh_phones(cur, agent_id, fresh_needed, campaign)

        logging.info(
            f"  Agent {agent_id}: "
            f"{len(fresh_rows)} fresh (target {fresh_count} + buffer {ALLOCATION_BUFFER}) + "
            f"{followup_count} follow-ups "
            f"= {len(fresh_rows) + followup_count} total"
        )
    return fresh_rows, followup_rows


# ── STEP 5: Write allocations to DB ──────────────────────────

def upsert_allocation(cur, contact_id, agent_id, attempt_number, campaign):
    """
    Insert new allocation for fresh contacts (attempt_number = 0).
    For follow-up contacts, allocation already exists — just verify it's open.
    Returns allocation_id.
    """
    # Check if active allocation exists already
    cur.execute("""
        SELECT id, attempt_number FROM contact_allocations
        WHERE contact_id = %s AND closed_at IS NULL
    """, (contact_id,))
    row = cur.fetchone()

    if row:
        return row[0]   # use existing allocation_id

    # Insert new allocation (fresh contact, first time)
    cur.execute("""
        INSERT INTO contact_allocations
            (contact_id, agent_id, allocated_date, attempt_number, campaign)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (contact_id, agent_id, date.today(), attempt_number, campaign))
    return cur.fetchone()[0]


def write_allocations_to_db(conn, agent_id, fresh_rows, followup_rows, dry_run=False, campaign='consulting'):
    """
    Inserts contact_allocations for fresh contacts and reactivates historical ones.
    Follow-up rows with an existing open allocation need no insert.
    In dry_run mode: logs what would happen but writes nothing.
    """
    if dry_run:
        hist_count = sum(1 for r in followup_rows if r.get('allocation_id') is None)
        logging.info(
            f"  [DRY RUN] Would insert {len(fresh_rows)} fresh allocations "
            f"+ reactivate {hist_count} historical contacts"
        )
        return

    with conn.cursor() as cur:
        fresh_inserted = 0
        for row in fresh_rows:
            upsert_allocation(cur, row['contact_id'], agent_id, 0, campaign)
            # Update flag: fresh / 3-month-ready → in_progress
            cur.execute("""
                UPDATE contacts
                SET contact_flag    = 'in_progress',
                    flag_updated_at = NOW()
                WHERE id = %s AND contact_flag IN ('fresh', 'attempt_3_months')
            """, (row['contact_id'],))
            fresh_inserted += 1

        # Reactivate historical contacts (no current open allocation)
        hist_inserted = 0
        for row in followup_rows:
            if row.get('allocation_id') is None:
                last_attempt = row.get('attempt_number', 0)
                new_attempt  = min(last_attempt + 1, 5)
                upsert_allocation(cur, row['contact_id'], agent_id, new_attempt, campaign)
                hist_inserted += 1

        conn.commit()
        if hist_inserted:
            logging.info(f"  Reactivated {hist_inserted} historical contacts into open allocations")
        logging.info(f"  Inserted {fresh_inserted} new allocations")


# ── STEP 6: Build sheet rows ──────────────────────────────────

def build_unique_id(row):
    """Reconstruct display ID e.g. 'RR | 75863932'"""
    source_prefix = {
        'rocketreach': 'RR',
        'msme':        'MS',
        'pharma':      'PH',
        'manual':      'MN',
    }
    prefix = source_prefix.get(row['source'], 'RR')
    sid    = row['source_id'] or ''
    return f"{prefix} | {sid}" if sid else prefix


def get_last_call_context(conn, contact_id):
    """
    Fetches the most recent call_action for a contact.
    Returns (last_status, last_remark, last_called_date) as strings.
    Used to pre-fill context columns in FU sheets.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(current_state, call_status, ''),
                COALESCE(remark, ''),
                called_at
            FROM call_actions
            WHERE contact_id = %s
            ORDER BY called_at DESC
            LIMIT 1
        """, (contact_id,))
        row = cur.fetchone()
        if not row:
            return ('', '', '')
        status = row[0] or ''
        remark = row[1] or ''
        called = row[2].strftime('%d/%m/%Y') if row[2] else ''
        # Truncate remark to 80 chars to keep sheet readable
        if len(remark) > 80:
            remark = remark[:77] + '...'
        return (status, remark, called)


def build_sheet_row(row, attempt_number=0, last_context=None, campaign=''):
    """
    One sheet row per phone number.

    New Contact (attempt=0):
      Unique ID | Company | Name | Phone | Title |
      [agent fills] Duration | Category | State | Remark | ... | Timestamp |
      Campaign

    Follow-up (attempt 1-5):
      Unique ID | Company | Name | Phone | Title |
      [pre-filled] Last Status | Last Remark | Last Called |
      [agent fills] Duration | Category | State | Remark | ... | Timestamp |
      Campaign
    """
    base = [
        build_unique_id(row),
        row['company_name'] or '',
        f"{row['first_name'] or ''} {row['last_name'] or ''}".strip(),
        row['phone_number'],
        row['designation'] or '',
    ]
    agent_cols = ['', '', '', '', '', '', '', '']  # 8 agent-fill cols (Duration→Timestamp)

    if attempt_number == 0:
        # New Contact — 14 cols total (13 data + Campaign)
        return base + agent_cols[:8] + [campaign]
    else:
        # Follow-up — 17 cols total (5 + 3 context + 8 agent + Campaign)
        last_status, last_remark, last_called = last_context or ('', '', '')
        context_cols = [last_status, last_remark, last_called]
        return base + context_cols + agent_cols + [campaign]


def group_by_attempt(conn, fresh_rows, followup_rows, campaign=''):
    """
    Groups rows by attempt_number for sub-sheet routing.
    Fetches last call context for FU rows so agents see what happened before.
    Returns dict: {attempt_number: [rows]}
    """
    groups = {i: [] for i in range(6)}

    for row in fresh_rows:
        groups[0].append(build_sheet_row(row, attempt_number=0, campaign=campaign))

    for row in followup_rows:
        # attempt_number in DB already points to the NEXT FU sheet after sync.
        # So route directly to that attempt (do not increment again).
        next_attempt = min(max(int(row.get('attempt_number', 1)), 1), 5)
        # Fetch last call context for this contact
        last_context = get_last_call_context(conn, row['contact_id'])
        groups[next_attempt].append(
            build_sheet_row(row, attempt_number=next_attempt,
                            last_context=last_context, campaign=campaign)
        )

    return groups


# ── STEP 7: Write to Google Sheets ───────────────────────────

def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(
        GOOGLE_CREDS_FILE, scopes=scopes
    )
    return gspread.authorize(creds)


def _build_list_validation_rule(values: list[str]) -> dict:
    return {
        "condition": {
            "type": "ONE_OF_LIST",
            "values": [{"userEnteredValue": v} for v in values],
        },
        "strict": True,
        "showCustomUi": True,
    }


def _apply_dropdown_validations(sh, ws, validations: list[tuple[int, list[str]]]) -> None:
    """Apply dropdown validations to one worksheet.

    validations: list of (1-based column index, allowed values)
    """
    end_row_index = max(SHEET_VALIDATION_END_ROW, int(ws.row_count or SHEET_VALIDATION_END_ROW))
    requests = []
    for col_1based, values in validations:
        requests.append(
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,  # row 2
                        "endRowIndex": end_row_index,
                        "startColumnIndex": col_1based - 1,
                        "endColumnIndex": col_1based,
                    },
                    "rule": _build_list_validation_rule(values),
                }
            }
        )
    if requests:
        sh.batch_update({"requests": requests})


def _apply_bd_dropdowns_for_tab(sh, ws, attempt_num: int) -> None:
    """Set Call Status and Current State dropdowns for BD tabs."""
    if attempt_num == 0:
        call_status_col = 7
        current_state_col = 8
    else:
        call_status_col = 10
        current_state_col = 11

    _apply_dropdown_validations(
        sh,
        ws,
        [
            (call_status_col, BD_CALL_STATUS_VALUES),
            (current_state_col, BD_CURRENT_STATE_VALUES),
        ],
    )


BD_INSTRUCTIONS = [
    ["BD AGENT SHEET — FILLING GUIDE", ""],
    ["", ""],
    ["ABOUT YOUR SHEET", ""],
    ["New Contact tab", "Fresh contacts allocated today — first call ever from our team."],
    ["FU 1 – FU 5 tabs", "Follow-up contacts from previous calls. The last call's status, remark, and date are pre-filled so you can see what happened before dialling."],
    ["Sync timing", "Your entries sync overnight. New contacts appear in your sheet the next morning."],
    ["When a row syncs", "A row only syncs if the Timestamp column is filled. Always fill Timestamp after every call."],
    ["", ""],
    ["PRE-FILLED COLUMNS — DO NOT EDIT", ""],
    ["Unique ID – Static", "System identifier for this contact. Do not change."],
    ["Company", "Company the contact works at."],
    ["Name", "Contact's full name."],
    ["Phone", "The number to dial."],
    ["Title", "Contact's job title / designation."],
    ["Campaign", "Auto-filled — the dataset this contact belongs to."],
    ["[FU tabs only] Last Status", "The Current State recorded in the previous call."],
    ["[FU tabs only] Last Remark", "The note left from the previous call."],
    ["[FU tabs only] Last Called", "Date of the last call attempt for this contact."],
    ["", ""],
    ["COLUMNS YOU FILL", ""],
    ["Call Duration", "Length of the call in H:MM:SS format (e.g. 0:05:32). Enter 0:00:00 if the call did not connect."],
    ["Call Status", "How the call went — choose from the approved values listed below."],
    ["Current State", "What happened in this specific call — choose from the approved values listed below."],
    ["Remark", "A brief note about the conversation. Required when Current State = Shared Story."],
    ["Recording Link", "Paste the call recording URL. Mandatory when Current State = Shared Story."],
    ["Transcript", "Google Drive link to the call transcript (if available)."],
    ["Dream Snapshot", "Google Drive link to the dream snapshot document sent during this call."],
    ["Timestamp", "Date and time of the call. Format: DD/MM/YYYY or DD/MM/YYYY HH:MM. This field is required for the row to sync."],
    ["", ""],
    ["CALL STATUS VALUES", ""],
    ["Connected", "You spoke to the person."],
    ["Did not connect", "Call rang but no answer, went to voicemail, or was cut off."],
    ["Call back later", "Contact asked to be called again at a later time."],
    ["Do not Disturb", "Contact asked never to be called again. Permanently closes this record."],
    ["Invalid Number", "Number is wrong, disconnected, or not in use. Marks the number invalid so it won't appear again."],
    ["Referred", "Contact referred you to someone else. Closes this contact record."],
    ["", ""],
    ["CURRENT STATE VALUES", ""],
    ["Interested", "Contact showed interest — continue follow-up in the next session."],
    ["Rescheduled", "Contact asked to call again at a specific time or date."],
    ["Attempt Again", "Call did not progress — try again on the next allocated day."],
    ["Attempt Again after 3 months", "Contact unavailable for 3 months. Pauses all follow-up automatically."],
    ["Shared Story", "You shared our company success story with the contact.  Milestone ✅  Recording link is mandatory."],
    ["Snapshot Sent", "You sent the dream snapshot document to the contact.  Milestone ✅  Moves contact to MQL stage."],
    ["Not interested", "Contact explicitly declined. Closes this record."],
    ["Do not Disturb", "Contact refuses further calls. Closes this record permanently."],
    ["Allocate Again", "Pass this contact to another agent."],
    ["", ""],
    ["KEY RULES", ""],
    ["Timestamp is mandatory", "A row without a Timestamp will not be synced — your work will not be recorded."],
    ["One row = one contact", "Do not skip rows or crowd multiple contacts into one row."],
    ["Recording link with Shared Story", "Every Shared Story call must have a recording link. No exceptions."],
    ["The goal", "Move contacts to Shared Story or Snapshot Sent. This hands them over to the MQL team."],
    ["Sync Status column", "Auto-filled by the system with '✓ Synced' after nightly sync. Never edit this column."],
    ["FU 5 is the last attempt", "After FU5, a contact is automatically closed. Make every call count."],
]

INSTRUCTIONS_TAB = "Instructions"


def ensure_instructions_tab(sh, agent_name: str):
    """
    Create or refresh the Instructions tab in this agent's BD sheet.
    Skips creation if the tab already has the current content (checked by row count).
    Safe to call every run.
    """
    existing = [ws.title for ws in sh.worksheets()]
    expected_rows = len(BD_INSTRUCTIONS)

    if INSTRUCTIONS_TAB not in existing:
        ws = sh.add_worksheet(title=INSTRUCTIONS_TAB, rows=expected_rows + 5, cols=3)
        ws.update(range_name="A1", values=BD_INSTRUCTIONS)
        logging.info(f"  [{agent_name}] Created Instructions tab ({expected_rows} rows)")
    else:
        ws = sh.worksheet(INSTRUCTIONS_TAB)
        if ws.row_count < expected_rows or len(ws.col_values(1)) < expected_rows:
            ws.update(range_name="A1", values=BD_INSTRUCTIONS)
            logging.info(f"  [{agent_name}] Updated Instructions tab")


def ensure_sheet_tabs(sh, agent_name):
    """
    Auto-creates any missing sub-sheet tabs with the correct header.
    New Contact gets 13-col header. FU1-FU5 get 16-col header.
    Safe to call every run — skips tabs that already exist.
    """
    ensure_instructions_tab(sh, agent_name)

    existing_tabs = [ws.title for ws in sh.worksheets()]
    for attempt_num, tab_name in SUBSHEET_NAMES.items():
        headers = get_headers_for_attempt(attempt_num)
        if tab_name not in existing_tabs:
            ws = sh.add_worksheet(title=tab_name, rows=2000, cols=20)
            ws.update('A1', [headers])
            logging.info(f"  [{agent_name}] Created tab: {tab_name} ({len(headers)} cols)")
        else:
            ws = sh.worksheet(tab_name)
            existing_header = ws.row_values(1)
            if not existing_header or existing_header[0] != headers[0] or len(existing_header) != len(headers):
                ws.update('A1', [headers])
                logging.info(f"  [{agent_name}] Updated header: {tab_name}")

        try:
            _apply_bd_dropdowns_for_tab(sh, ws, attempt_num)
        except Exception as e:
            logging.warning(f"  [{agent_name}] Could not apply dropdowns on {tab_name}: {e}")


def write_to_agent_sheet(gc, spreadsheet_id, agent_name, groups):
    """
    APPEND MODE — never overwrites existing rows.

    For each sub-sheet:
      1. Auto-create tab if it doesn't exist
      2. Find the first empty row after the header
      3. Append new rows starting there
      4. Never touches rows already written (even if unfilled by agent)

    This means agents can keep working on yesterday's rows even after
    today's allocation runs — nothing gets deleted.
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        logging.error(f"Cannot open sheet for {agent_name}: {e}")
        return

    # Auto-create missing tabs
    ensure_sheet_tabs(sh, agent_name)

    total_written = 0
    for attempt_num, rows in groups.items():
        if not rows:
            continue

        tab_name = SUBSHEET_NAMES.get(attempt_num)
        if not tab_name:
            continue

        ws = sh.worksheet(tab_name)

        # Find first truly empty row (no Unique ID in col A)
        # Row 1 = header, data starts row 2
        col_a = ws.col_values(1)   # all values in col A (header + data)
        # col_a[0] = header, col_a[1]+ = data rows
        # Count rows that have content in col A (excluding header)
        data_rows_count = len([v for v in col_a[1:] if v.strip()])
        next_row = data_rows_count + 2   # +1 for header, +1 for next empty

        # Append rows starting at next_row
        start_cell = f"A{next_row}"
        ws.update(start_cell, rows)
        total_written += len(rows)
        logging.info(
            f"  [{agent_name}] {tab_name}: {len(rows)} rows appended "
            f"(starting row {next_row})"
        )

    logging.info(f"  [{agent_name}] Total rows appended: {total_written}")


# ── MAIN ──────────────────────────────────────────────────────

def run_allocation(dry_run=False, agent_id=None, fresh_count_override=None, campaign='consulting'):
    """
    agent_id:             if set, only process that one agent
    fresh_count_override: if set, use this fresh count instead of agent's kpi_dialed
    campaign:             which campaign's contacts to allocate (default: 'consulting')
    """
    log_file = setup_logging()

    logging.info("=" * 55)
    logging.info("  CRM ALLOCATION ENGINE — Starting" + (" [DRY RUN]" if dry_run else ""))
    logging.info(f"  Date:     {date.today()}")
    logging.info(f"  Campaign: {campaign}")
    logging.info("=" * 55)

    stats = {
        "agents_available":  0,
        "agents_skipped":    0,
        "fresh_total":       0,
        "followup_total":    0,
        "stale_flagged":     0,
    }

    conn = get_connection()

    try:
        # Step 1 — Flag stalled contacts
        logging.info("\n[1/5] Flagging stalled contacts...")
        stats["stale_flagged"] = flag_stale_contacts(conn, dry_run=dry_run)

        # Step 2 — Reset yesterday's leave
        logging.info("\n[2/5] Resetting leave flags from yesterday...")
        reset_leave_flags(conn, dry_run=dry_run)

        # Step 3 — Get available agents
        logging.info("\n[3/5] Checking agent availability...")
        agents = get_available_agents(conn, campaign)

        # Filter to single agent if specified
        if agent_id is not None:
            agents = [a for a in agents if a['agent_id'] == agent_id]
            if not agents:
                logging.warning(f"Agent id={agent_id} not found or not available today. Exiting.")
                return

        stats["agents_available"] = len(agents)

        if not agents:
            logging.warning("No agents available today. Exiting.")
            return

        # Step 4 — Connect to Google Sheets
        logging.info("\n[4/5] Connecting to Google Sheets...")
        try:
            gc = get_gspread_client()
            logging.info("Google Sheets connected OK")
        except Exception as e:
            logging.error(f"Google Sheets connection failed: {e}")
            logging.error("Allocation DB records will still be written.")
            gc = None

        # Step 5 — Process each agent
        logging.info("\n[5/5] Allocating contacts per agent...")
        for agent in agents:
            agent_id   = agent['agent_id']
            agent_name = agent['name']
            logging.info(f"\nProcessing: {agent_name} (id={agent_id})")

            # Pick phones: all pending FUs + fresh_count new contacts
            fresh = fresh_count_override if fresh_count_override is not None else agent['kpi_dialed']
            fresh_rows, followup_rows = pick_phones_for_agent(
                conn,
                agent_id,
                fresh_count = fresh,
                campaign    = campaign,
            )

            # Build sheet rows — FU rows include last status/remark/date
            groups = group_by_attempt(conn, fresh_rows, followup_rows, campaign=campaign)
            stats["fresh_total"]   += len(fresh_rows)
            stats["followup_total"] += len(followup_rows)

            # Write to DB
            write_allocations_to_db(conn, agent_id, fresh_rows, followup_rows,
                                    dry_run=dry_run, campaign=campaign)

            # Write to Google Sheet (skip in dry run)
            sheet_id = agent['sheet_id']
            if dry_run:
                logging.info(f"  [DRY RUN] Skipping Google Sheet write for {agent_name}")
            elif gc and sheet_id:
                write_to_agent_sheet(gc, sheet_id, agent_name, groups)
            else:
                logging.warning(
                    f"No sheet_id set for {agent_name} (agent_id={agent_id}). "
                    f"Set it in the Agents page of the dashboard."
                )

    finally:
        conn.close()

    # Summary
    logging.info("\n" + "=" * 55)
    logging.info("  ALLOCATION COMPLETE — SUMMARY")
    logging.info("=" * 55)
    logging.info(f"  Date:               {date.today()}")
    logging.info(f"  Agents available:   {stats['agents_available']}")
    logging.info(f"  Stale flagged:      {stats['stale_flagged']}")
    logging.info(f"  Fresh numbers:      {stats['fresh_total']}")
    logging.info(f"  Follow-up numbers:  {stats['followup_total']}")
    logging.info(f"  Total allocated:    {stats['fresh_total'] + stats['followup_total']}")
    logging.info(f"  Log saved to:       {log_file}")
    if dry_run:
        logging.info("  *** DRY RUN — no data was written to DB or Sheets ***")
    logging.info("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CRM Allocation Engine")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview only — no DB writes, no sheet pushes"
    )
    parser.add_argument(
        "--agent-id", type=int, default=None,
        help="Only allocate for this agent (by DB id). Omit to run for all available agents."
    )
    parser.add_argument(
        "--fresh-count", type=int, default=None,
        help="Override fresh contact count for this run (ignores agent's kpi_dialed)."
    )
    parser.add_argument(
        "--campaign", type=str, default="consulting",
        help="Campaign to allocate contacts for (default: consulting)."
    )
    args = parser.parse_args()
    run_allocation(
        dry_run=args.dry_run,
        agent_id=args.agent_id,
        fresh_count_override=args.fresh_count,
        campaign=args.campaign,
    )
