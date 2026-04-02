"""
bd_story_import.py
==================
Import BD first-phase history from data/mql_t_d.csv into the BD pipeline.

Behavior implemented per user mapping:
1) Import all rows.
2) Use the first Date column when duplicate Date headers exist.
3) Use first agent name when Filled by has multiple names.
4) Auto-create missing agents.
5) Close historical allocations after migration.
6) Reallocate if contact is logged under a different agent.
7) Call status mapping:
   Picked -> Connected
   Busy -> Connected
   Attempt Again -> Did not connect
   Call Back -> Connected
8) Current state is forced to Shared Story for all rows.
9) Highest stage update writes contact_flag to shared_story.
10) Idempotent rerun: skip duplicate call_actions keys.
11) Enrich existing contacts with missing company, phones, emails.
12) Keep top 3 phones and top 3 emails.
13) Default is dry-run; use --apply to write.

Usage:
  python scripts/bd_story_import.py --file data/mql_t_d.csv
  python scripts/bd_story_import.py --file data/mql_t_d.csv --apply
"""

import argparse
import csv
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "crm_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

FALLBACK_DATE = datetime(2026, 1, 1, 0, 0, 0)


def clean_text(value):
    return (value or "").strip()


def normalize_spaces(value):
    return re.sub(r"\s+", " ", clean_text(value))


def _build_call_type_map():
    mapping = {
        "first call": 0,
        "first": 0,
    }
    for n in range(1, 31):
        mapping[f"follow-up {n}"] = n
        mapping[f"follow up {n}"] = n
        mapping[f"followup {n}"] = n
        mapping[f"follow-up{n}"] = n
        mapping[f"fu {n}"] = n
        mapping[f"fu{n}"] = n
    return mapping


CALL_TYPE_MAP = _build_call_type_map()

CALL_STATUS_MAP = {
    "connected": "Connected",
    "picked": "Connected",
    "picked up": "Connected",
    "busy": "Connected",
    "call back": "Connected",
    "callback": "Connected",
    "call back connected": "Connected",
    "call back later": "Connected",
    "rescheduled": "Connected",
    "reschedule": "Connected",
    "attempt again": "Did not connect",
    "did not connect": "Did not connect",
    "not picked": "Did not connect",
    "not picked up": "Did not connect",
    "no answer": "Did not connect",
    "do not disturb": "Do not Disturb",
    "dnd": "Do not Disturb",
    "invalid number": "Invalid Number",
    "wrong number": "Invalid Number",
    "referred": "Referred",
    "reffered": "Referred",
}


_contact_cache = {}
_agent_cache = {}
_table_col_cache = {}


def setup_logging(dry_run):
    os.makedirs(str(PROJECT_ROOT / "logs"), exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "_dryrun" if dry_run else ""
    log_file = str(PROJECT_ROOT / "logs" / f"bd_story_import_{ts}{suffix}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return log_file


def normalize_call_type(raw):
    if not raw:
        return ""
    value = raw.strip().lower()
    value = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\u00ad\ufe58\ufe63\uff0d]", "-", value)
    value = re.sub(r"\s+", " ", value)
    return value


def map_attempt_number(call_type_raw):
    normalized = normalize_call_type(call_type_raw)
    attempt = CALL_TYPE_MAP.get(normalized, 0)
    return min(max(attempt, 0), 5)


def normalize_call_status(raw_status):
    key = normalize_spaces(raw_status).lower()
    if not key:
        return "Connected", True
    if key in CALL_STATUS_MAP:
        return CALL_STATUS_MAP[key], False
    return "Connected", True


def normalize_current_state(_raw):
    return "Shared Story"


def parse_duration(raw):
    value = clean_text(raw)
    if not value or value == "0":
        return None
    value = value.lstrip(":")

    match = re.match(r"^(\d+):(\d{2}):(\d{2})$", value)
    if match:
        seconds = int(match.group(1)) * 3600 + int(match.group(2)) * 60 + int(match.group(3))
        return seconds if seconds > 0 else None

    try:
        fraction = float(value)
        if 0 < fraction < 1:
            seconds = round(fraction * 86400)
            return seconds if seconds > 0 else None
    except ValueError:
        pass

    return None


def parse_date(raw):
    value = clean_text(raw)
    if not value:
        return None
    fmts = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def parse_uid(uid_value):
    value = clean_text(uid_value)
    if not value:
        return None, None

    if "|" in value:
        parts = value.split("|", 1)
        prefix = clean_text(parts[0]).upper()
        raw_id = clean_text(parts[1])

        src_map = {
            "RR": "rocketreach",
            "MS": "msme",
            "PH": "pharma",
            "MN": "manual",
            "BD": "manual",
            "CC": "manual",
            "AV": "manual",
            "BW": "manual",
        }
        source = src_map.get(prefix, "manual")

        if raw_id and prefix not in ("RR", "MS", "PH", "MN"):
            source_id = f"{prefix}-{raw_id}"
        else:
            source_id = raw_id
        return source, source_id

    return "manual", value


def parse_phones(raw_value):
    value = clean_text(raw_value)
    if not value:
        return []

    phones = []
    for part in value.split(","):
        digits = re.sub(r"\D", "", part)
        if len(digits) >= 10:
            phone = digits[-10:]
            if phone not in phones:
                phones.append(phone)
    return phones


def parse_emails(raw_value):
    value = clean_text(raw_value)
    if not value:
        return []

    parts = re.split(r"[,;]", value)
    emails = []
    for part in parts:
        email = clean_text(part).lower()
        if "@" not in email:
            continue
        if email not in emails:
            emails.append(email)
    return emails


def split_name(person_name):
    value = clean_text(person_name)
    if not value:
        return None, None
    parts = value.split(None, 1)
    first = parts[0][:100]
    last = parts[1][:100] if len(parts) > 1 else None
    return first, last


def first_agent_name(raw):
    value = clean_text(raw)
    if not value:
        return "Unknown BD"
    return clean_text(value.split(",")[0]) or "Unknown BD"


def has_table_column(cur, table_name, column_name):
    key = (table_name, column_name)
    if key in _table_col_cache:
        return _table_col_cache[key]

    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    exists = cur.fetchone() is not None
    _table_col_cache[key] = exists
    return exists


def build_header_index(headers):
    index = {}
    for i, header in enumerate(headers):
        key = clean_text(header).lower()
        if key and key not in index:
            index[key] = i
    return index


def value_at(row, idx):
    if idx is None:
        return ""
    if idx < 0 or idx >= len(row):
        return ""
    return clean_text(row[idx])


def load_rows(csv_path):
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            return rows

        hidx = build_header_index(headers)

        for line_no, raw in enumerate(reader, start=2):
            if not any(clean_text(cell) for cell in raw):
                continue

            row = {
                "line_no": line_no,
                "Unique ID": value_at(raw, hidx.get("unique id")),
                "Call Type": value_at(raw, hidx.get("call type")),
                "Call Duration": value_at(raw, hidx.get("call duration")),
                "Call Status": value_at(raw, hidx.get("call status")),
                "Current State": value_at(raw, hidx.get("current state")),
                "Remark": value_at(raw, hidx.get("remark")),
                "Recording Link": value_at(raw, hidx.get("recording link")),
                "Date": value_at(raw, hidx.get("date")),
                "Filled by": value_at(raw, hidx.get("filled by")),
                "Category": value_at(raw, hidx.get("category")),
                "Phone No": value_at(raw, hidx.get("phone no")),
                "Company Name": value_at(raw, hidx.get("company name")),
                "Person Name": value_at(raw, hidx.get("person name")),
                "Email": value_at(raw, hidx.get("email")),
                "Transcript": value_at(raw, hidx.get("transcript")),
                "Dream Snapshot": value_at(raw, hidx.get("dream snapshot")),
                "Designation": value_at(raw, hidx.get("designation")),
                "Address": value_at(raw, hidx.get("address")),
            }
            rows.append(row)

    return rows


def get_contact_id(cur, source, source_id):
    key = (source, source_id)
    if key in _contact_cache:
        return _contact_cache[key]

    cur.execute(
        """
        SELECT id
        FROM contacts
        WHERE source = %s AND source_id = %s
        LIMIT 1
        """,
        (source, source_id),
    )
    found = cur.fetchone()
    if not found:
        cur.execute("SELECT id FROM contacts WHERE source_id = %s LIMIT 1", (source_id,))
        found = cur.fetchone()

    contact_id = found[0] if found else None
    _contact_cache[key] = contact_id
    return contact_id


def get_or_create_company(cur, company_name, address, can_write):
    name = clean_text(company_name)
    if not name:
        return None

    has_raw_address = has_table_column(cur, "companies", "raw_address")
    if can_write:
        if has_raw_address:
            cur.execute(
                """
                INSERT INTO companies (name, raw_address)
                VALUES (%s, NULLIF(%s, ''))
                ON CONFLICT (name_normalized) DO NOTHING
                RETURNING id
                """,
                (name, clean_text(address)),
            )
        else:
            cur.execute(
                """
                INSERT INTO companies (name)
                VALUES (%s)
                ON CONFLICT (name_normalized) DO NOTHING
                RETURNING id
                """,
                (name,),
            )
        inserted = cur.fetchone()
        if inserted:
            return inserted[0]

    cur.execute(
        "SELECT id FROM companies WHERE name_normalized = LOWER(TRIM(%s)) LIMIT 1",
        (name,),
    )
    found = cur.fetchone()
    company_id = found[0] if found else None

    if company_id and can_write and has_raw_address:
        addr = clean_text(address)
        if addr:
            cur.execute(
                """
                UPDATE companies
                SET raw_address = %s
                WHERE id = %s
                  AND (raw_address IS NULL OR raw_address = '')
                """,
                (addr, company_id),
            )

    return company_id


def add_contact_phones(cur, contact_id, phones):
    if not phones:
        return 0

    cur.execute(
        "SELECT phone_number, rank FROM contact_phones WHERE contact_id = %s ORDER BY rank",
        (contact_id,),
    )
    existing_rows = cur.fetchall()
    existing_numbers = {row[0] for row in existing_rows}
    used_ranks = {row[1] for row in existing_rows if row[1] is not None}

    inserted = 0
    for phone in phones[:3]:
        if phone in existing_numbers:
            continue

        rank = None
        for candidate in (1, 2, 3):
            if candidate not in used_ranks:
                rank = candidate
                break
        if rank is None:
            break

        cur.execute(
            """
            INSERT INTO contact_phones (contact_id, phone_number, rank)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (contact_id, phone, rank),
        )
        if cur.rowcount:
            inserted += 1
            existing_numbers.add(phone)
            used_ranks.add(rank)

    return inserted


def add_contact_emails(cur, contact_id, emails):
    if not emails:
        return 0

    cur.execute(
        "SELECT email, rank FROM contact_emails WHERE contact_id = %s ORDER BY rank",
        (contact_id,),
    )
    existing_rows = cur.fetchall()
    existing_emails = {row[0].lower() for row in existing_rows}
    used_ranks = {row[1] for row in existing_rows if row[1] is not None}

    inserted = 0
    for email in emails[:3]:
        if email.lower() in existing_emails:
            continue

        rank = 1
        while rank in used_ranks:
            rank += 1
        if rank > 3:
            break

        cur.execute(
            """
            INSERT INTO contact_emails (contact_id, email, rank)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (contact_id, email, rank),
        )
        if cur.rowcount:
            inserted += 1
            existing_emails.add(email.lower())
            used_ranks.add(rank)

    return inserted


def enrich_contact(cur, contact_id, company_id, first_name, last_name, designation, phones, emails):
    cur.execute(
        """
        UPDATE contacts
        SET company_id = COALESCE(company_id, %s),
            first_name = COALESCE(first_name, %s),
            last_name = COALESCE(last_name, %s),
            designation = COALESCE(designation, %s)
        WHERE id = %s
        """,
        (company_id, first_name, last_name, designation, contact_id),
    )
    phones_added = add_contact_phones(cur, contact_id, phones)
    emails_added = add_contact_emails(cur, contact_id, emails)
    return phones_added, emails_added


def get_or_create_contact(cur, source, source_id, row, can_write):
    existing_id = get_contact_id(cur, source, source_id)

    company_id = get_or_create_company(cur, row.get("Company Name"), row.get("Address"), can_write)
    first_name, last_name = split_name(row.get("Person Name"))
    designation = clean_text(row.get("Designation")) or None
    phones = parse_phones(row.get("Phone No"))
    emails = parse_emails(row.get("Email"))

    if existing_id:
        phones_added = emails_added = 0
        if can_write:
            phones_added, emails_added = enrich_contact(
                cur,
                existing_id,
                company_id,
                first_name,
                last_name,
                designation,
                phones,
                emails,
            )
        return existing_id, False, phones_added, emails_added

    if not can_write:
        return None, True, 0, 0

    has_bd_category = has_table_column(cur, "contacts", "bd_category")
    category = clean_text(row.get("Category")).lower()[:60] if clean_text(row.get("Category")) else "consulting"

    if has_bd_category:
        cur.execute(
            """
            INSERT INTO contacts (
                source, source_id, company_id,
                first_name, last_name, designation, bd_category
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL DO NOTHING
            RETURNING id
            """,
            (source, source_id, company_id, first_name, last_name, designation, category),
        )
    else:
        cur.execute(
            """
            INSERT INTO contacts (
                source, source_id, company_id,
                first_name, last_name, designation
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, source_id) WHERE source_id IS NOT NULL DO NOTHING
            RETURNING id
            """,
            (source, source_id, company_id, first_name, last_name, designation),
        )

    inserted = cur.fetchone()
    if inserted:
        contact_id = inserted[0]
    else:
        contact_id = get_contact_id(cur, source, source_id)

    if not contact_id:
        return None, False, 0, 0

    phones_added = add_contact_phones(cur, contact_id, phones)
    emails_added = add_contact_emails(cur, contact_id, emails)

    _contact_cache[(source, source_id)] = contact_id
    return contact_id, True, phones_added, emails_added


def get_or_create_agent_id(cur, agent_name, can_write):
    key = agent_name.lower()
    if key in _agent_cache:
        return _agent_cache[key], False

    cur.execute("SELECT id FROM agents WHERE LOWER(name) = LOWER(%s) LIMIT 1", (agent_name,))
    found = cur.fetchone()
    if found:
        agent_id = found[0]
        _agent_cache[key] = agent_id
        return agent_id, False

    if not can_write:
        return None, True

    has_team = has_table_column(cur, "agents", "team")
    if has_team:
        cur.execute(
            """
            INSERT INTO agents (name, team, status)
            VALUES (%s, 'bd', 'inactive')
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (agent_name,),
        )
    else:
        cur.execute(
            """
            INSERT INTO agents (name, status)
            VALUES (%s, 'inactive')
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (agent_name,),
        )
    inserted = cur.fetchone()

    if inserted:
        agent_id = inserted[0]
    else:
        cur.execute("SELECT id FROM agents WHERE LOWER(name) = LOWER(%s) LIMIT 1", (agent_name,))
        row = cur.fetchone()
        agent_id = row[0] if row else None

    _agent_cache[key] = agent_id
    return agent_id, True


def get_primary_phone(cur, contact_id):
    cur.execute(
        """
        SELECT phone_number
        FROM contact_phones
        WHERE contact_id = %s
          AND (is_invalid = FALSE OR is_invalid IS NULL)
        ORDER BY rank
        LIMIT 1
        """,
        (contact_id,),
    )
    found = cur.fetchone()
    return found[0] if found else None


def find_existing_call_action(cur, contact_id, agent_id, attempt_number, called_at, phone_used):
    cur.execute(
        """
        SELECT id, allocation_id
        FROM call_actions
        WHERE contact_id = %s
          AND agent_id = %s
          AND attempt_number = %s
          AND called_at = %s
          AND phone_number_used = %s
        ORDER BY id
        LIMIT 1
        """,
        (contact_id, agent_id, attempt_number, called_at, phone_used),
    )
    return cur.fetchone()


def update_existing_call_action(cur, action_id, allocation_id, payload):
    cur.execute(
        """
        UPDATE call_actions
        SET allocation_id = COALESCE(call_actions.allocation_id, %s),
            current_state = %s,
            call_duration = COALESCE(call_actions.call_duration, %s),
            remark = COALESCE(call_actions.remark, %s),
            recording_link = COALESCE(call_actions.recording_link, %s),
            transcript_link = COALESCE(call_actions.transcript_link, %s),
            dream_snapshot_link = COALESCE(call_actions.dream_snapshot_link, %s)
        WHERE id = %s
        """,
        (
            allocation_id,
            payload["current_state"],
            payload["call_duration"],
            payload["remark"],
            payload["recording_link"],
            payload["transcript_link"],
            payload["dream_snapshot_link"],
            action_id,
        ),
    )


def get_open_allocation(cur, contact_id):
    cur.execute(
        """
        SELECT id, agent_id
        FROM contact_allocations
        WHERE contact_id = %s
          AND closed_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        (contact_id,),
    )
    return cur.fetchone()


def create_allocation(cur, contact_id, agent_id, called_at, attempt_number):
    alloc_date = called_at.date()
    cur.execute(
        """
        INSERT INTO contact_allocations (
            contact_id, agent_id, allocated_date, attempt_number, filled_by
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (contact_id, agent_id, alloc_date, attempt_number, agent_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


def ensure_allocation(cur, contact_id, agent_id, called_at, attempt_number):
    open_alloc = get_open_allocation(cur, contact_id)
    if not open_alloc:
        alloc_id = create_allocation(cur, contact_id, agent_id, called_at, attempt_number)
        return alloc_id, "created"

    open_alloc_id, open_agent_id = open_alloc
    if open_agent_id != agent_id:
        cur.execute(
            """
            UPDATE contact_allocations
            SET closed_at = %s,
                close_reason = 'reallocated',
                filled_by = %s
            WHERE id = %s
              AND closed_at IS NULL
            """,
            (called_at, agent_id, open_alloc_id),
        )
        alloc_id = create_allocation(cur, contact_id, agent_id, called_at, attempt_number)
        return alloc_id, "reallocated"

    cur.execute(
        """
        UPDATE contact_allocations
        SET attempt_number = GREATEST(attempt_number, %s)
        WHERE id = %s
        """,
        (attempt_number, open_alloc_id),
    )
    return open_alloc_id, "reused"


def insert_call_action(cur, payload):
    cur.execute(
        """
        INSERT INTO call_actions (
            contact_id,
            allocation_id,
            agent_id,
            phone_number_used,
            attempt_number,
            call_status,
            current_state,
            call_duration,
            remark,
            recording_link,
            transcript_link,
            dream_snapshot_link,
            called_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            payload["contact_id"],
            payload["allocation_id"],
            payload["agent_id"],
            payload["phone_number_used"],
            payload["attempt_number"],
            payload["call_status"],
            payload["current_state"],
            payload["call_duration"],
            payload["remark"],
            payload["recording_link"],
            payload["transcript_link"],
            payload["dream_snapshot_link"],
            payload["called_at"],
        ),
    )


def close_historical_allocations(cur, contact_ids):
    if not contact_ids:
        return 0

    cur.execute(
        """
        WITH latest AS (
            SELECT
                ca.id AS allocation_id,
                MAX(a.called_at) AS max_called_at,
                (ARRAY_AGG(a.agent_id ORDER BY a.called_at DESC, a.id DESC))[1] AS last_agent_id
            FROM contact_allocations ca
            LEFT JOIN call_actions a ON a.allocation_id = ca.id
            WHERE ca.contact_id = ANY(%s)
              AND ca.closed_at IS NULL
            GROUP BY ca.id
        )
        UPDATE contact_allocations ca
        SET closed_at = COALESCE(latest.max_called_at, NOW()),
            close_reason = 'completed',
            filled_by = COALESCE(ca.filled_by, latest.last_agent_id)
        FROM latest
        WHERE ca.id = latest.allocation_id
          AND ca.closed_at IS NULL
        """,
        (contact_ids,),
    )
    return cur.rowcount


def migrate(csv_path, apply_changes):
    dry_run = not apply_changes
    log_file = setup_logging(dry_run)

    logging.info("=" * 72)
    logging.info("  BD STORY IMPORT%s", " [DRY RUN]" if dry_run else "")
    logging.info("  File: %s", csv_path)
    logging.info("=" * 72)

    rows = load_rows(csv_path)
    logging.info("Rows parsed from CSV: %s", len(rows))

    stats = {
        "rows_read": len(rows),
        "row_errors": 0,
        "no_id_rows": 0,
        "contacts_created": 0,
        "contacts_would_create": 0,
        "contacts_enriched": 0,
        "phones_added": 0,
        "emails_added": 0,
        "agents_created": 0,
        "agents_would_create": 0,
        "allocations_created": 0,
        "reallocations": 0,
        "attempts_inserted": 0,
        "attempts_existing": 0,
        "attempts_would_insert": 0,
        "status_fallback_to_connected": 0,
        "flags_updated": 0,
        "allocations_closed_completed": 0,
    }

    dry_contact_ids = {}
    dry_agent_ids = {}
    dry_open_alloc = {}
    dry_next_contact = -1
    dry_next_agent = -1
    dry_next_alloc = -1

    imported_contact_ids = set()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for idx, row in enumerate(rows):
                if idx > 0 and idx % 200 == 0:
                    logging.info("  %s/%s rows processed", idx, len(rows))
                    if apply_changes:
                        conn.commit()

                source, source_id = parse_uid(row.get("Unique ID"))
                if not source_id:
                    stats["no_id_rows"] += 1
                    continue

                agent_name = first_agent_name(row.get("Filled by"))
                attempt_number = map_attempt_number(row.get("Call Type"))
                call_status, used_fallback = normalize_call_status(row.get("Call Status"))
                if used_fallback:
                    stats["status_fallback_to_connected"] += 1

                current_state = normalize_current_state(row.get("Current State"))
                called_at = parse_date(row.get("Date")) or FALLBACK_DATE
                call_duration = parse_duration(row.get("Call Duration"))

                phones = parse_phones(row.get("Phone No"))
                emails = parse_emails(row.get("Email"))

                remark = clean_text(row.get("Remark")) or None
                recording_link = clean_text(row.get("Recording Link")) or None
                transcript_link = clean_text(row.get("Transcript")) or None
                dream_snapshot_link = clean_text(row.get("Dream Snapshot")) or None

                if dry_run:
                    contact_id = get_contact_id(cur, source, source_id)
                    if not contact_id:
                        ckey = (source, source_id)
                        if ckey not in dry_contact_ids:
                            dry_contact_ids[ckey] = dry_next_contact
                            dry_next_contact -= 1
                            stats["contacts_would_create"] += 1
                        contact_id = dry_contact_ids[ckey]

                    agent_id, _ = get_or_create_agent_id(cur, agent_name, can_write=False)
                    if not agent_id:
                        if agent_name not in dry_agent_ids:
                            dry_agent_ids[agent_name] = dry_next_agent
                            dry_next_agent -= 1
                            stats["agents_would_create"] += 1
                        agent_id = dry_agent_ids[agent_name]

                    phone_used = phones[0] if phones else (get_primary_phone(cur, contact_id) if contact_id > 0 else None)
                    if not phone_used:
                        phone_used = "0000000000"
                        note = "[No phone in CSV; placeholder used]"
                        remark = f"{remark} {note}".strip() if remark else note

                    existing = None
                    if contact_id > 0 and agent_id > 0:
                        existing = find_existing_call_action(
                            cur,
                            contact_id,
                            agent_id,
                            attempt_number,
                            called_at,
                            phone_used,
                        )

                    if existing:
                        stats["attempts_existing"] += 1
                        if contact_id > 0:
                            imported_contact_ids.add(contact_id)
                        continue

                    alloc_key = contact_id
                    open_alloc = dry_open_alloc.get(alloc_key)
                    if open_alloc is None:
                        open_alloc = get_open_allocation(cur, contact_id) if contact_id > 0 else None
                        if open_alloc:
                            dry_open_alloc[alloc_key] = {
                                "alloc_id": open_alloc[0],
                                "agent_id": open_alloc[1],
                            }
                        else:
                            dry_open_alloc[alloc_key] = None

                    state = dry_open_alloc.get(alloc_key)
                    if state is None:
                        dry_open_alloc[alloc_key] = {
                            "alloc_id": dry_next_alloc,
                            "agent_id": agent_id,
                        }
                        dry_next_alloc -= 1
                        stats["allocations_created"] += 1
                    else:
                        if state["agent_id"] != agent_id:
                            stats["reallocations"] += 1
                            stats["allocations_created"] += 1
                            dry_open_alloc[alloc_key] = {
                                "alloc_id": dry_next_alloc,
                                "agent_id": agent_id,
                            }
                            dry_next_alloc -= 1

                    stats["attempts_would_insert"] += 1
                    if contact_id > 0:
                        imported_contact_ids.add(contact_id)
                    continue

                cur.execute("SAVEPOINT sp_row")
                try:
                    contact_id, was_created, phones_added, emails_added = get_or_create_contact(
                        cur,
                        source,
                        source_id,
                        row,
                        can_write=True,
                    )
                    if not contact_id:
                        raise RuntimeError("Could not get or create contact")

                    if was_created:
                        stats["contacts_created"] += 1
                    else:
                        stats["contacts_enriched"] += 1
                    stats["phones_added"] += phones_added
                    stats["emails_added"] += emails_added

                    agent_id, agent_created = get_or_create_agent_id(cur, agent_name, can_write=True)
                    if not agent_id:
                        raise RuntimeError("Could not get or create agent")
                    if agent_created:
                        stats["agents_created"] += 1

                    phone_used = phones[0] if phones else get_primary_phone(cur, contact_id)
                    if not phone_used:
                        phone_used = "0000000000"
                        note = "[No phone in CSV; placeholder used]"
                        remark = f"{remark} {note}".strip() if remark else note

                    existing = find_existing_call_action(
                        cur,
                        contact_id,
                        agent_id,
                        attempt_number,
                        called_at,
                        phone_used,
                    )

                    if existing:
                        action_id, existing_alloc_id = existing
                        update_existing_call_action(
                            cur,
                            action_id,
                            existing_alloc_id,
                            {
                                "current_state": current_state,
                                "call_duration": call_duration,
                                "remark": remark,
                                "recording_link": recording_link,
                                "transcript_link": transcript_link,
                                "dream_snapshot_link": dream_snapshot_link,
                            },
                        )
                        stats["attempts_existing"] += 1
                        imported_contact_ids.add(contact_id)
                        cur.execute("RELEASE SAVEPOINT sp_row")
                        continue

                    allocation_id, alloc_action = ensure_allocation(
                        cur,
                        contact_id,
                        agent_id,
                        called_at,
                        attempt_number,
                    )
                    if not allocation_id:
                        raise RuntimeError("Could not ensure allocation")

                    if alloc_action == "created":
                        stats["allocations_created"] += 1
                    elif alloc_action == "reallocated":
                        stats["reallocations"] += 1
                        stats["allocations_created"] += 1

                    payload = {
                        "contact_id": contact_id,
                        "allocation_id": allocation_id,
                        "agent_id": agent_id,
                        "phone_number_used": phone_used,
                        "attempt_number": attempt_number,
                        "call_status": call_status,
                        "current_state": current_state,
                        "call_duration": call_duration,
                        "remark": remark,
                        "recording_link": recording_link,
                        "transcript_link": transcript_link,
                        "dream_snapshot_link": dream_snapshot_link,
                        "called_at": called_at,
                    }
                    insert_call_action(cur, payload)
                    stats["attempts_inserted"] += 1
                    imported_contact_ids.add(contact_id)

                    cur.execute("RELEASE SAVEPOINT sp_row")
                except Exception as row_err:
                    stats["row_errors"] += 1
                    logging.warning(
                        "  Row %s (%s): %s",
                        row.get("line_no"),
                        row.get("Unique ID"),
                        row_err,
                    )
                    cur.execute("ROLLBACK TO SAVEPOINT sp_row")
                    cur.execute("RELEASE SAVEPOINT sp_row")

            if apply_changes:
                conn.commit()

                if imported_contact_ids:
                    contact_id_list = sorted(imported_contact_ids)

                    cur.execute(
                        """
                        UPDATE contacts
                        SET contact_flag = 'shared_story',
                            flag_updated_at = NOW()
                        WHERE id = ANY(%s)
                          AND contact_flag IN (
                              'fresh',
                              'in_progress',
                              'needs_followup',
                              'snapshot_sent',
                              'not_interested',
                              'dnd',
                              'attempt_3_months',
                              'mql_in_progress',
                              'mql_qualified',
                              'mql_rejected'
                          )
                        """,
                        (contact_id_list,),
                    )
                    stats["flags_updated"] = cur.rowcount

                    stats["allocations_closed_completed"] = close_historical_allocations(cur, contact_id_list)

                conn.commit()
            else:
                # In dry run, estimate completed closures as number of open allocations
                # represented in simulated state.
                simulated_open = 0
                for state in dry_open_alloc.values():
                    if state is not None:
                        simulated_open += 1
                stats["allocations_closed_completed"] = simulated_open

    finally:
        conn.close()

    logging.info("\n%s", "=" * 72)
    logging.info("  BD STORY IMPORT COMPLETE%s", " (DRY RUN)" if dry_run else "")
    logging.info("%s", "=" * 72)
    logging.info("  Rows read:                     %s", stats["rows_read"])
    logging.info("  Rows with missing ID:          %s", stats["no_id_rows"])
    logging.info("  Row errors:                    %s", stats["row_errors"])
    logging.info("  Contacts created:              %s", stats["contacts_created"])
    logging.info("  Contacts would create:         %s", stats["contacts_would_create"])
    logging.info("  Contacts enriched:             %s", stats["contacts_enriched"])
    logging.info("  Phones added:                  %s", stats["phones_added"])
    logging.info("  Emails added:                  %s", stats["emails_added"])
    logging.info("  Agents created:                %s", stats["agents_created"])
    logging.info("  Agents would create:           %s", stats["agents_would_create"])
    logging.info("  Allocations created:           %s", stats["allocations_created"])
    logging.info("  Reallocations:                 %s", stats["reallocations"])
    logging.info("  Attempts inserted:             %s", stats["attempts_inserted"])
    logging.info("  Attempts existing (idempotent):%s", stats["attempts_existing"])
    logging.info("  Attempts would insert:         %s", stats["attempts_would_insert"])
    logging.info("  Status fallback to Connected:  %s", stats["status_fallback_to_connected"])
    logging.info("  Contact flags updated:         %s", stats["flags_updated"])
    logging.info("  Allocations closed completed:  %s", stats["allocations_closed_completed"])
    logging.info("  Log: %s", log_file)
    logging.info("%s", "=" * 72)

    return stats


def parse_args():
    parser = argparse.ArgumentParser(description="Import BD story history into CRM")
    parser.add_argument("--file", "-f", required=True, help="CSV file path")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply DB writes. Default mode is dry-run.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not os.path.exists(args.file):
        print(f"ERROR: File not found: {args.file}")
        sys.exit(1)

    migrate(args.file, apply_changes=args.apply)
