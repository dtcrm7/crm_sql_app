"""
mql_allocation_engine.py
========================
Allocates MQL-eligible contacts to MQL agents and writes to their Google Sheets.

MQL-eligible contacts:
    - Legacy gate: shared_story or snapshot_sent
    - Current gate: snapshot_sent
    - Reallocation gate: attempt_3_months after 90 days
    AND no active mql_allocation exists

Sheet structure per MQL agent:
  Tab "MQL FU 1-15"  → Contact info (12 cols) + FU1-FU15 blocks (11 cols each) = 177 cols
  Tab "MQL FU 16-30" → Contact info (12 cols) + FU16-FU30 blocks (11 cols each) = 177 cols

Contact info columns (pre-filled, A-L):
  Unique ID | Company Name | Person Name | Phone | Email |
  BD Agent | BD Call Date | BD Remark | BD Recording Link |
  Category | BD Transcript | Dream Snapshot

Each FU block (11 cols):
  MQL Category | Call Status | Current State | Call Duration |
  Remark | Recording Link | Transcript | Message Status |
  Timestamp | Follow-up Stage | Sync Status

Usage:
  python mql_allocation_engine.py --agent-id 5 --count 12 --campaign consulting
  python mql_allocation_engine.py --agent-id 5 --count 12 --dry-run
  python mql_allocation_engine.py --all-agents --campaign consulting
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

from sheet_values_config import (
    MQL_CALL_STATUS_VALUES,
    MQL_CURRENT_STATE_VALUES,
    MQL_LEAD_CATEGORY_VALUES,
    MQL_MESSAGE_STATUS_VALUES,
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

ALLOCATION_BUFFER = 2  # allocate this many extra to cover no-shows

# Funnel gate date:
# - Before this date: shared_story and snapshot_sent can enter MQL funnel.
# - On/after this date: only snapshot_sent enters MQL funnel.
SNAPSHOT_GATE_DATE = date(2026, 4, 1)

# ── SHEET STRUCTURE ───────────────────────────────────────────

MQL_TAB_1  = "MQL FU 1-15"    # FU 1-15
MQL_TAB_2  = "MQL FU 16-30"   # FU 16-30

CONTACT_COLS = 12  # columns A-L
FU_BLOCK_SIZE = 11  # 10 agent cols + 1 sync status

# Offsets within a FU block
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

SHEET_VALIDATION_END_ROW = 2000

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


def build_tab_headers(fu_start: int, fu_end: int) -> list[str]:
    """Build full header row for a tab covering fu_start..fu_end."""
    headers = list(CONTACT_HEADERS)
    for fu_num in range(fu_start, fu_end + 1):
        for col in FU_BLOCK_HEADERS:
            headers.append(f"FU{fu_num} — {col}")
    return headers


# ── LOGGING ───────────────────────────────────────────────────
def setup_logging(dry_run=False):
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix   = "_dryrun" if dry_run else ""
    log_file = str(PROJECT_ROOT / "logs" / f"mql_alloc_{ts}{suffix}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ]
    )
    return log_file


# ── DB ────────────────────────────────────────────────────────
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def build_unique_id(source: str, source_id: str) -> str:
    """
    Build display ID like 'RR | 6175204' or 'CC | 9851600232'.
    For story/manual contacts stored as 'CC-9851600232', reconstructs
    the original format by detecting the prefix inside source_id.
    """
    if source_id:
        # Story contacts: stored as BD-xxx, CC-xxx, AV-xxx, BW-xxx
        for pfx in ("BD", "CC", "AV", "BW"):
            if source_id.startswith(f"{pfx}-"):
                return f"{pfx} | {source_id[len(pfx)+1:]}"
        # ID-xxxxx format — keep as-is
        if source_id.startswith("ID-"):
            return source_id

    prefix_map = {
        "rocketreach": "RR",
        "msme":        "MS",
        "pharma":      "PH",
        "manual":      "MN",
    }
    prefix = prefix_map.get(source, "RR")
    return f"{prefix} | {source_id}" if source_id else prefix


def get_eligible_contacts(
    conn,
    campaign: str,
    count: int,
    agent_id: int,
    categories: list | None = None,
    contact_ids: list[int] | None = None,
) -> list:
    """
    Find MQL-eligible contacts for a campaign.
        Eligible =
            - Legacy (before SNAPSHOT_GATE_DATE): shared_story or snapshot_sent
            - Current (on/after SNAPSHOT_GATE_DATE): snapshot_sent only
            - Reallocation: attempt_3_months only after 90 days from flag_updated_at
               AND no active mql_allocation
               AND this agent hasn't worked this contact before
    Ordered by flag_updated_at DESC (most recent leads first).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT
                c.id            AS contact_id,
                c.source,
                c.source_id,
                c.first_name,
                c.last_name,
                c.campaign,
                c.bd_category,
                c.contact_flag,
                co.name         AS company_name,
                p.phone_number,
                pa.all_phones,
                e.email,
                -- BD context: live call_actions first, migrated history as fallback
                ag.name                                                 AS bd_agent_name,
                COALESCE(ca.called_at,      mca_bd.m_called_at)        AS bd_call_date,
                COALESCE(ca.current_state,  mca_bd.m_current_state)    AS bd_current_state,
                COALESCE(ca.remark,         mca_bd.m_remark)           AS bd_remark,
                COALESCE(ca.recording_link, mca_bd.m_recording_link)   AS bd_recording_link,
                COALESCE(ca.transcript_link,mca_bd.m_transcript)       AS bd_transcript,
                COALESCE(ca.dream_snapshot_link, mca_bd.m_snapshot)    AS bd_snapshot
            FROM contacts c
            LEFT JOIN companies co ON co.id = c.company_id
            LEFT JOIN LATERAL (
                SELECT id, phone_number
                FROM contact_phones
                                WHERE contact_id = c.id
                                    AND COALESCE(is_invalid, FALSE) = FALSE
                ORDER BY rank LIMIT 1
            ) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT array_agg(phone_number ORDER BY rank) AS all_phones
                FROM contact_phones
                                WHERE contact_id = c.id
                                    AND COALESCE(is_invalid, FALSE) = FALSE
            ) pa ON TRUE
            LEFT JOIN LATERAL (
                SELECT email
                FROM contact_emails
                WHERE contact_id = c.id
                ORDER BY rank LIMIT 1
            ) e ON TRUE
            LEFT JOIN LATERAL (
                -- BD context: prefer call_actions (live BD calls),
                -- fall back to mql_call_attempts bd_history (migrated BD records)
                SELECT agent_id, called_at, current_state,
                       remark, recording_link, transcript_link, dream_snapshot_link
                FROM call_actions
                WHERE contact_id = c.id
                ORDER BY called_at DESC LIMIT 1
            ) ca ON TRUE
            LEFT JOIN LATERAL (
                -- Fallback: migrated BD history stored in mql_call_attempts
                SELECT mca.agent_id      AS m_agent_id,
                       mca.called_at     AS m_called_at,
                       mca.current_state AS m_current_state,
                       mca.remark        AS m_remark,
                       mca.recording_link     AS m_recording_link,
                       mca.transcript_link    AS m_transcript,
                       mca.dream_snapshot_link AS m_snapshot
                FROM mql_call_attempts mca
                JOIN mql_allocations ma ON ma.id = mca.allocation_id
                WHERE mca.contact_id = c.id
                  AND ma.close_reason = 'bd_history'
                ORDER BY mca.called_at DESC LIMIT 1
            ) mca_bd ON TRUE
            LEFT JOIN agents ag ON ag.id = COALESCE(ca.agent_id, mca_bd.m_agent_id)
                        WHERE (
                                (
                                    COALESCE(c.flag_updated_at::date, DATE '1900-01-01') < %(gate_date)s
                                    AND c.contact_flag IN ('shared_story', 'snapshot_sent')
                                )
                                OR (
                                    COALESCE(c.flag_updated_at::date, CURRENT_DATE) >= %(gate_date)s
                                    AND c.contact_flag = 'snapshot_sent'
                                )
                                OR (
                                    %(contact_ids)s::int[] IS NOT NULL
                                    AND
                                    c.contact_flag = 'attempt_3_months'
                                    AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                                        <= (CURRENT_DATE - INTERVAL '90 days')::date
                                )
                                )
              AND c.campaign = %(campaign)s
              -- no active MQL allocation
              AND NOT EXISTS (
                  SELECT 1 FROM mql_allocations ma
                  WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
              )
              -- this MQL agent hasn't worked this contact before
              AND (
                  %(contact_ids)s::int[] IS NOT NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM mql_allocations ma2
                      WHERE ma2.contact_id = c.id AND ma2.agent_id = %(agent_id)s
                  )
              )
              AND (%(categories)s::text[] IS NULL OR c.bd_category = ANY(%(categories)s::text[]))
                            AND (%(contact_ids)s::int[] IS NULL OR c.id = ANY(%(contact_ids)s::int[]))
                            AND EXISTS (
                                    SELECT 1
                                    FROM contact_phones cpv
                                    WHERE cpv.contact_id = c.id
                                        AND COALESCE(cpv.is_invalid, FALSE) = FALSE
                            )
            ORDER BY c.flag_updated_at DESC NULLS LAST
            LIMIT %(limit)s
        """, {
            "campaign": campaign,
            "agent_id": agent_id,
            "limit": count,
            "categories": categories if categories else None,
                        "contact_ids": contact_ids if contact_ids else None,
            "gate_date": SNAPSHOT_GATE_DATE,
        })
        rows = cur.fetchall()
        logging.info(f"  Found {len(rows)} eligible contacts for agent_id={agent_id}")
        return rows


def get_fu16_contacts(conn, agent_id: int) -> list:
        """
        Find contacts whose active mql_allocation has reached FU15 (max follow_up_number >= 15)
        and are not yet closed. These need to be added to the FU16-30 tab.
        """
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("""
                        SELECT
                                c.id            AS contact_id,
                                c.source,
                                c.source_id,
                                c.first_name,
                                c.last_name,
                                c.campaign,
                                c.bd_category,
                                c.contact_flag,
                                co.name         AS company_name,
                                p.phone_number,
                                pa.all_phones,
                                e.email,
                                bd_ca.agent_name    AS bd_agent_name,
                                bd_ca.called_at     AS bd_call_date,
                                bd_ca.current_state AS bd_current_state,
                                bd_ca.remark        AS bd_remark,
                                bd_ca.recording_link AS bd_recording_link,
                                bd_ca.dream_snapshot_link AS bd_snapshot,
                                bd_ca.transcript_link AS bd_transcript,
                                ma.id           AS allocation_id
                        FROM mql_allocations ma
                        JOIN contacts c         ON c.id  = ma.contact_id
                        LEFT JOIN companies co  ON co.id = c.company_id
                        LEFT JOIN LATERAL (
                                SELECT phone_number
                                FROM contact_phones
                            WHERE contact_id = c.id
                              AND COALESCE(is_invalid, FALSE) = FALSE
                            ORDER BY rank
                                LIMIT 1
                        ) p ON TRUE
                        LEFT JOIN LATERAL (
                            SELECT array_agg(phone_number ORDER BY rank) AS all_phones
                            FROM contact_phones
                            WHERE contact_id = c.id
                              AND COALESCE(is_invalid, FALSE) = FALSE
                        ) pa ON TRUE
                        LEFT JOIN LATERAL (
                                SELECT email
                                FROM contact_emails
                                WHERE contact_id = c.id
                                    AND is_primary = TRUE
                                LIMIT 1
                        ) e ON TRUE
                        LEFT JOIN LATERAL (
                                SELECT a.name AS agent_name, ca.called_at,
                                             ca.current_state, ca.remark, ca.recording_link,
                                             ca.dream_snapshot_link, ca.transcript_link
                                FROM call_actions ca
                                JOIN agents a ON a.id = ca.agent_id
                                WHERE ca.contact_id = c.id
                                ORDER BY ca.called_at DESC LIMIT 1
                        ) bd_ca ON TRUE
                        WHERE ma.agent_id   = %s
                            AND ma.closed_at  IS NULL
                            AND EXISTS (
                                    SELECT 1 FROM mql_call_attempts mca
                                    WHERE mca.allocation_id = ma.id
                                        AND mca.follow_up_number >= 15
                            )
                """, (agent_id,))
                return cur.fetchall()


def _append_alt_phone_note(remark: str, all_phones) -> str:
    """Append non-primary phones at the end of BD remark as ALT: n1, n2."""
    phones = [str(p).strip() for p in (all_phones or []) if str(p).strip()]
    if len(phones) <= 1:
        return remark

    alt_note = f"ALT: {', '.join(phones[1:])}"
    if "ALT:" in (remark or ""):
        return remark
    if not remark:
        return alt_note
    return f"{remark} | {alt_note}"


def insert_mql_allocations(conn, contacts: list, agent_id: int,
                            campaign: str, dry_run: bool) -> list[int]:
    """
    Insert mql_allocations rows and update contact_flag → mql_in_progress.
    Returns list of allocation_ids.
    """
    if dry_run:
        logging.info(f"  [DRY RUN] Would insert {len(contacts)} mql_allocations")
        return list(range(len(contacts)))   # fake IDs for dry run

    alloc_ids = []
    with conn.cursor() as cur:
        for c in contacts:
            cur.execute("""
                INSERT INTO mql_allocations
                    (contact_id, agent_id, campaign, allocated_date, filled_by)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (c["contact_id"], agent_id, campaign, date.today(), agent_id))
            alloc_id = cur.fetchone()[0]
            alloc_ids.append(alloc_id)

            # Lock from BD reallocation
            cur.execute("""
                UPDATE contacts
                SET contact_flag    = 'mql_in_progress',
                    flag_updated_at = NOW()
                WHERE id = %s
                  AND contact_flag IN ('shared_story', 'snapshot_sent', 'attempt_3_months')
            """, (c["contact_id"],))

            # Pre-create mql_analysis row with BD context
            cur.execute("""
                INSERT INTO mql_analysis
                    (contact_id, allocation_id, agent_id,
                     bd_agent_name, bd_call_date, bd_current_state,
                     bd_remark, bd_recording_link, bd_snapshot_link,
                     outcome)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                ON CONFLICT DO NOTHING
            """, (
                c["contact_id"], alloc_id, agent_id,
                c["bd_agent_name"],
                c["bd_call_date"].date() if c["bd_call_date"] else None,
                c["bd_current_state"],
                c["bd_remark"],
                c["bd_recording_link"],
                c["bd_snapshot"],
            ))

    conn.commit()
    logging.info(f"  Inserted {len(alloc_ids)} mql_allocations, contacts locked to mql_in_progress")
    return alloc_ids


def build_contact_row(c) -> list:
    """
    Build the 12-column contact info row (pre-filled from BD data).
    Agent fill columns (FU1-FU30) are left blank — agent fills them.
    """
    bd_date = ""
    if c["bd_call_date"]:
        try:
            bd_date = c["bd_call_date"].strftime("%d/%m/%Y")
        except Exception:
            bd_date = str(c["bd_call_date"])[:10]

    remark = c["bd_remark"] or ""
    if len(remark) > 100:
        remark = remark[:97] + "..."

    # Keep one primary phone in column D and append remaining valid numbers
    # in BD Remark for quick switching by the MQL agent.
    remark = _append_alt_phone_note(remark, c.get("all_phones"))

    return [
        build_unique_id(c["source"] or "", c["source_id"] or ""),
        c["company_name"] or "",
        f"{c['first_name'] or ''} {c['last_name'] or ''}".strip(),
        c["phone_number"] or "",
        c["email"] or "",
        c["bd_agent_name"] or "",
        bd_date,
        remark,
        c["bd_recording_link"] or "",
        c["bd_category"] or c["campaign"] or "",
        c["bd_transcript"] or "",
        c["bd_snapshot"] or "",
    ]


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
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


def _apply_mql_dropdowns(sh, ws, fu_start: int, fu_end: int) -> None:
    """Apply dropdown validations to all FU blocks in one MQL worksheet."""
    end_row_index = max(SHEET_VALIDATION_END_ROW, int(ws.row_count or SHEET_VALIDATION_END_ROW))
    requests = []

    for fu_num in range(fu_start, fu_end + 1):
        local_idx = fu_num - fu_start
        block_start_col_0 = CONTACT_COLS + (local_idx * FU_BLOCK_SIZE)

        col_specs = [
            (block_start_col_0 + FU_LEAD_CATEGORY + 1, MQL_LEAD_CATEGORY_VALUES),
            (block_start_col_0 + FU_CALL_STATUS + 1, MQL_CALL_STATUS_VALUES),
            (block_start_col_0 + FU_CURRENT_STATE + 1, MQL_CURRENT_STATE_VALUES),
            (block_start_col_0 + FU_MESSAGE_STATUS + 1, MQL_MESSAGE_STATUS_VALUES),
        ]

        for col_1based, values in col_specs:
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


MQL_INSTRUCTIONS = [
    ["MQL AGENT SHEET — FILLING GUIDE", ""],
    ["", ""],
    ["ABOUT YOUR SHEET", ""],
    ["MQL FU 1-15 tab", "Contacts allocated to you for deep follow-up. Each contact occupies one full row. You fill your call outcomes in the FU blocks moving right across the row."],
    ["MQL FU 16-30 tab", "The same contact row is copied here automatically once a contact reaches FU15, so you can continue follow-up through FU16 to FU30 without losing any history."],
    ["Contact info (columns A-L)", "Pre-filled by the system. Shows the contact's details and the BD call history — everything the BD agent discovered before handing this contact to you."],
    ["FU blocks (columns M onwards)", "One block of 11 columns per follow-up attempt. You fill a new block for each call you make. Move right after every call — do not overwrite previous FU entries."],
    ["Sync timing", "Your entries are read every night. New contacts appear in your sheet the next morning."],
    ["", ""],
    ["PRE-FILLED COLUMNS (A–L) — DO NOT EDIT", ""],
    ["Unique ID (A)", "System identifier. Do not change."],
    ["Company Name (B)", "Company the contact works at."],
    ["Person Name (C)", "Contact's full name."],
    ["Phone (D)", "The number to dial. If you see an alt-phone note in BD Remark (col H), this is the primary number on file — confirm the correct one and update this column if needed."],
    ["Email (E)", "Contact's email address."],
    ["BD Agent (F)", "The BD agent who made the original call and handed this contact to MQL."],
    ["BD Call Date (G)", "Date of the last BD call."],
    ["BD Remark (H)", "Notes left by the BD agent. If the contact has multiple phone numbers on record, alternate numbers appear here — review and confirm the correct number in column D."],
    ["BD Recording Link (I)", "Link to the BD call recording."],
    ["Category (J)", "The source dataset this contact came from (e.g. 'clinics ds1', 'msme ds3')."],
    ["BD Transcript (K)", "Link to the BD call transcript."],
    ["Dream Snapshot (L)", "Link to the dream snapshot document the BD agent sent."],
    ["", ""],
    ["FU BLOCK COLUMNS — ONE BLOCK PER CALL (FILLED BY YOU)", ""],
    ["MQL Category", "How you rate this lead right now: Hot / Warm / Cold."],
    ["Call Status", "How the call went — choose from the approved values below."],
    ["Current State", "What happened in this call — choose from the approved values below. This is the most important field and drives all sync logic."],
    ["Call Duration", "Length of the call in H:MM:SS (e.g. 0:08:15). Enter 0:00:00 if the call did not connect."],
    ["Remark", "A brief note about the conversation."],
    ["Recording Link", "Link to the call recording."],
    ["Transcript", "Link to the call transcript (if available)."],
    ["Message Status", "Did you send a WhatsApp or message to this contact? Enter Yes or No."],
    ["Timestamp ⚠️", "Date and time of the call. Format: DD/MM/YYYY or DD/MM/YYYY HH:MM. This field is required — a row without a Timestamp will NOT sync."],
    ["Follow-up Stage", "Optional — note the stage of the follow-up conversation."],
    ["Sync Status", "Auto-filled by the system with '✓ Synced'. Never edit this column."],
    ["", ""],
    ["CALL STATUS VALUES", ""],
    ["Connected", "You spoke to the person."],
    ["Did not connect", "Call rang but no answer, went to voicemail, or was cut off."],
    ["Call back later", "Contact asked to be called again at a specific time."],
    ["Do not Disturb", "Contact does not want to be called. Closes this contact."],
    ["Invalid Number", "Number is wrong or disconnected."],
    ["", ""],
    ["CURRENT STATE VALUES — IN PROGRESS (keeps contact open)", ""],
    ["Escalate", "Contact needs senior team attention — flags for review without closing."],
    ["Attempt Again", "Call did not progress — retry at next opportunity."],
    ["Rescheduled", "Contact asked to call at a specific later time."],
    ["Respondent", "Contact is responsive but a decision has not been reached yet."],
    ["Dream Snapshot Confirmed", "Contact confirmed receipt and interest in the dream snapshot."],
    ["Allocate Again 3 months", "Contact unavailable — pause and revisit after 3 months."],
    ["Interested", "Contact is engaged and interested — continue follow-up."],
    ["Snapshot Sent", "You sent the dream snapshot in the MQL stage."],
    ["Snapshot Confirmed", "Contact confirmed the snapshot received."],
    ["Meeting Requested", "Contact asked for a meeting — coordinate scheduling."],
    ["", ""],
    ["CURRENT STATE VALUES — MEETING TRACK (keeps contact open, same agent continues)", ""],
    ["Meeting Scheduled", "A meeting date has been agreed. The system creates a meeting record and adds this contact to the shared Meetings sheet. You continue in this same sheet after the meeting."],
    ["Meeting Held", "Auto-filled in the next FU block by the system after the meeting sync runs — you do not type this manually."],
    ["Solution Sent", "You sent the solution proposal to the contact after the meeting."],
    ["", ""],
    ["CURRENT STATE VALUES — CLOSING OUTCOMES (closes this contact)", ""],
    ["Solution Picked / Picked Solution", "Contact accepted the solution. ✅ SQL Ready. Closes contact as Qualified."],
    ["Not interested", "Contact explicitly declined. Closes contact."],
    ["Do not Disturb", "Contact refuses all further contact. Closes contact."],
    ["Reffered", "Contact referred to someone else. Closes contact."],
    ["Irrelevant", "Contact is not a fit for the offering. Closes contact."],
    ["", ""],
    ["KEY RULES", ""],
    ["Timestamp is mandatory", "Without a Timestamp, the row will not sync and your work will not be recorded."],
    ["One row per contact, one block per call", "Do not overwrite earlier FU entries. Fill the next FU block for every new call."],
    ["Never edit pre-filled columns A–L", "These are populated by the system and used to match records. Editing them can cause sync failures."],
    ["Phone confirmation", "If BD Remark (col H) shows alternate phone numbers, call the right one and update column D with the confirmed number. The system will lock it as the primary number going forward."],
    ["Meeting track stays open", "When you log Meeting Scheduled, your allocation is NOT closed. You stay assigned to this contact and continue all follow-up in this same sheet."],
    ["FU 30 is the maximum", "After 30 follow-up attempts with no close outcome, the contact is automatically closed as stalled."],
    ["Sync Status", "The system writes '✓ Synced' here automatically. Never edit it — if it is already synced and you need to update a row, contact the admin."],
]

MQL_INSTRUCTIONS_TAB = "Instructions"


def ensure_mql_instructions_tab(sh, agent_name: str):
    """
    Create or refresh the Instructions tab in this agent's MQL sheet.
    Safe to call every run — only writes if the tab is missing or stale.
    """
    existing = [ws.title for ws in sh.worksheets()]
    expected_rows = len(MQL_INSTRUCTIONS)

    if MQL_INSTRUCTIONS_TAB not in existing:
        ws = sh.add_worksheet(title=MQL_INSTRUCTIONS_TAB, rows=expected_rows + 5, cols=3)
        ws.update(range_name="A1", values=MQL_INSTRUCTIONS)
        logging.info(f"  [{agent_name}] Created Instructions tab ({expected_rows} rows)")
    else:
        ws = sh.worksheet(MQL_INSTRUCTIONS_TAB)
        if len(ws.col_values(1)) < expected_rows:
            ws.update(range_name="A1", values=MQL_INSTRUCTIONS)
            logging.info(f"  [{agent_name}] Updated Instructions tab")


def ensure_mql_tabs(sh, agent_name: str):
    """
    Auto-create MQL tabs if they don't exist and write headers.
    Safe to call every run — skips tabs that already have the correct header.
    """
    ensure_mql_instructions_tab(sh, agent_name)

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
            logging.info(f"  [{agent_name}] Created tab: {tab_name} ({total_cols} cols)")
        else:
            ws = sh.worksheet(tab_name)
            existing_h = ws.row_values(1)
            if len(existing_h) != total_cols or existing_h[0] != headers[0]:
                ws.update(range_name="A1", values=[headers])
                logging.info(f"  [{agent_name}] Updated header: {tab_name}")

        try:
            _apply_mql_dropdowns(sh, ws, fu_start, fu_end)
        except Exception as e:
            logging.warning(f"  [{agent_name}] Could not apply dropdowns on {tab_name}: {e}")


def write_contacts_to_sheet(gc, sheet_id: str, agent_name: str,
                             contacts: list, dry_run: bool):
    """
    Appends contact rows to Tab "MQL FU 1-15".
    Tab "MQL FU 16-30" is created/ensured but left for the agent to scroll into
    once they reach FU16.
    Contact info (A-L) is pre-filled; FU blocks (M onwards) are blank.
    """
    if dry_run:
        logging.info(f"  [DRY RUN] Would write {len(contacts)} rows to {agent_name} sheet")
        return True

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        logging.error(f"  Cannot open sheet for {agent_name} (id={sheet_id}): {e}")
        return False

    try:
        ensure_mql_tabs(sh, agent_name)

        ws = sh.worksheet(MQL_TAB_1)

        # Find first empty row after header (never write before row 2)
        col_a = ws.col_values(1)
        filled = len([v for v in col_a[1:] if v.strip()])
        next_row = max(2, filled + 2)   # +1 header, +1 for next empty; min row 2

        # Build rows: 12 contact cols + 165 blank FU cols = 177 total
        total_cols = CONTACT_COLS + 15 * FU_BLOCK_SIZE
        rows_to_write = []
        for c in contacts:
            contact_row = build_contact_row(c)
            blank_fu    = [""] * (total_cols - CONTACT_COLS)
            rows_to_write.append(contact_row + blank_fu)

        ws.update(range_name=f"A{next_row}", values=rows_to_write)
        logging.info(
            f"  [{agent_name}] {len(rows_to_write)} contacts written to {MQL_TAB_1} "
            f"(starting row {next_row})"
        )
        return True
    except Exception as e:
        logging.error(f"  Sheet write failed for {agent_name}: {e}")
        return False


def write_fu16_to_sheet(gc, sheet_id: str, agent_name: str,
                         contacts: list, dry_run: bool):
    """
    Appends contacts that have reached FU15 to the MQL FU 16-30 tab,
    skipping any that are already present (by Unique ID match in col A).
    """
    if not contacts:
        return True
    if dry_run:
        logging.info(f"  [DRY RUN] Would write {len(contacts)} FU16+ contacts to {agent_name} FU16-30 tab")
        return True

    try:
        sh = gc.open_by_key(sheet_id)
        ensure_mql_tabs(sh, agent_name)
        ws = sh.worksheet(MQL_TAB_2)

        # Get existing Unique IDs to avoid duplicates
        existing_ids = set(ws.col_values(1)[1:])  # skip header

        rows_to_write = []
        for c in contacts:
            uid = build_unique_id(c["source"] or "", c["source_id"] or "")
            if uid in existing_ids:
                continue
            contact_row = build_contact_row(c)
            blank_fu    = [""] * (15 * FU_BLOCK_SIZE)  # FU16-FU30 blank cols
            rows_to_write.append(contact_row + blank_fu)

        if not rows_to_write:
            logging.info(f"  [{agent_name}] All FU16+ contacts already in {MQL_TAB_2}")
            return True

        col_a = ws.col_values(1)
        filled = len([v for v in col_a[1:] if v.strip()])
        next_row = max(2, filled + 2)
        ws.update(range_name=f"A{next_row}", values=rows_to_write)
        logging.info(
            f"  [{agent_name}] {len(rows_to_write)} contacts added to {MQL_TAB_2} "
            f"(starting row {next_row})"
        )
        return True
    except Exception as e:
        logging.error(f"  FU16+ sheet write failed for {agent_name}: {e}")
        return False


# ── AGENT LOOKUP ──────────────────────────────────────────────
def get_mql_agents(conn, campaign: str, agent_id: int | None = None) -> list:
    """Return MQL agents (team='mql') with their sheet_ids."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
            SELECT a.id AS agent_id, a.name, a.kpi_dialed, a.is_on_leave,
                   ags.sheet_id
            FROM agents a
            LEFT JOIN agent_sheets ags
                   ON ags.agent_id = a.id
                  AND ags.campaign = %(campaign)s
                  AND ags.sheet_type = 'calling'
            WHERE a.status  = 'active'
              AND a.team    = 'mql'
        """
        params = {"campaign": campaign}
        if agent_id is not None:
            sql += " AND a.id = %(agent_id)s"
            params["agent_id"] = agent_id
        sql += " ORDER BY a.name"
        cur.execute(sql, params)
        return cur.fetchall()


# ── MAIN ──────────────────────────────────────────────────────
def run_mql_allocation(
    agent_id:     int | None = None,
    count:        int | None = None,
    campaign:     str        = "consulting",
    dry_run:      bool       = False,
    all_agents:   bool       = False,
    categories:   list | None = None,
    contact_ids:  list[int] | None = None,
) -> tuple[bool, str]:
    """
    Main entry point. Called by CLI or dashboard.
    Returns (success: bool, log_output: str).
    """
    import io, logging as _logging

    # Capture output for dashboard display
    log_stream = io.StringIO()
    handler    = _logging.StreamHandler(log_stream)
    handler.setFormatter(_logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    root = _logging.getLogger()
    root.setLevel(_logging.DEBUG)   # must set before basicConfig or it stays at WARNING
    root.addHandler(handler)

    log_file = setup_logging(dry_run)

    logging.info("=" * 55)
    logging.info("  MQL ALLOCATION ENGINE" + (" [DRY RUN]" if dry_run else ""))
    logging.info(f"  Date:     {date.today()}")
    logging.info(f"  Campaign: {campaign}")
    logging.info(f"  Categories: {', '.join(categories) if categories else 'All'}")
    if contact_ids:
        logging.info(f"  Manual contacts: {len(contact_ids)}")
    logging.info("=" * 55)

    conn = get_connection()
    success = True

    try:
        agents = get_mql_agents(conn, campaign, agent_id if not all_agents else None)

        if not agents:
            logging.warning("No active MQL agents found (team='mql').")
            return False, log_stream.getvalue()

        logging.info(f"MQL agents found: {[a['name'] for a in agents]}")

        try:
            gc = get_gspread_client()
            logging.info("Google Sheets connected OK")
        except Exception as e:
            logging.error(f"Google Sheets connection failed: {e}")
            gc = None

        for agent in agents:
            a_id   = agent["agent_id"]
            a_name = agent["name"]
            a_kpi  = agent["kpi_dialed"] or 12
            if contact_ids and not all_agents:
                a_count = len(contact_ids)
            else:
                a_count = count if count is not None else a_kpi + ALLOCATION_BUFFER

            logging.info(f"\nProcessing MQL agent: {a_name} (id={a_id}, count={a_count})")

            if agent["is_on_leave"]:
                logging.info(f"  {a_name} is on leave — skipping")
                continue

            contacts = get_eligible_contacts(
                conn,
                campaign,
                a_count,
                a_id,
                categories,
                contact_ids,
            )
            if not contacts:
                logging.info(f"  No eligible contacts for {a_name}")
                continue

            insert_mql_allocations(conn, contacts, a_id, campaign, dry_run)

            sheet_id = agent["sheet_id"]
            if gc and sheet_id:
                sheet_ok = write_contacts_to_sheet(gc, sheet_id, a_name, contacts, dry_run)
                if not sheet_ok:
                    logging.error(
                        f"  SHEET WRITE FAILED for {a_name} — "
                        "contacts were saved to DB but NOT written to the agent's sheet."
                    )
                    success = False

                # ── Step 5: Move FU16+ contacts to FU16-30 tab ──
                fu16_contacts = get_fu16_contacts(conn, a_id)
                if fu16_contacts:
                    logging.info(f"  {len(fu16_contacts)} contacts at FU15+ to move to FU16-30 tab")
                    write_fu16_to_sheet(gc, sheet_id, a_name, fu16_contacts, dry_run)
                else:
                    logging.info(f"  No contacts at FU15+ yet")
            elif not sheet_id:
                logging.warning(
                    f"  No MQL sheet_id set for {a_name}. "
                    f"Set it in the Agents page of the dashboard."
                )
            else:
                logging.warning(f"  Google Sheets unavailable — skipping sheet write for {a_name}")
                success = False

    except Exception as e:
        logging.error(f"MQL allocation failed: {e}", exc_info=True)
        conn.rollback()
        success = False
    finally:
        conn.close()

    root.removeHandler(handler)
    logging.info(f"\nLog saved to: {log_file}")
    return success, log_stream.getvalue()


# ── CLI ────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQL Allocation Engine")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no DB writes, no sheet pushes")
    parser.add_argument("--agent-id", type=int, default=None,
                        help="Allocate for this specific MQL agent (by DB id)")
    parser.add_argument("--all-agents", action="store_true",
                        help="Allocate for all active MQL agents")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of contacts to allocate (default: agent's kpi_dialed)")
    parser.add_argument("--campaign", type=str, default="consulting",
                        help="Campaign to allocate from (default: consulting)")
    parser.add_argument("--categories", nargs="+", default=None,
                        help="Filter by bd_category values (space-separated). Default: all categories.")
    parser.add_argument("--contact-ids", nargs="+", type=int, default=None,
                        help="Manually allocate these contact IDs (space-separated)")
    args = parser.parse_args()

    if not args.agent_id and not args.all_agents:
        parser.error("Specify --agent-id <id> or --all-agents")
    if args.all_agents and args.contact_ids:
        parser.error("--contact-ids requires --agent-id (not --all-agents)")

    success, output = run_mql_allocation(
        agent_id   = args.agent_id,
        count      = args.count,
        campaign   = args.campaign,
        dry_run    = args.dry_run,
        all_agents = args.all_agents,
        categories = args.categories,
        contact_ids = args.contact_ids,
    )
    print(output)
    exit(0 if success else 1)
