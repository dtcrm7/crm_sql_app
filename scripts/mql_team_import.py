"""
mql_team_import.py
==================
Normalize historical wide MQL team CSVs, import to DB, and backfill agent sheets.

Flow:
1. Read raw files in fixed owner order:
      Monika, Artuha, Dristhi, Suhana
2. Flatten wide follow-up columns into one-row-per-attempt records
3. Write audit files:
      data/mql_pro_from_team.csv
      data/mql_pro_from_team_rejected.csv
4. Optional DB import:
      - create/rotate mql_allocations while preserving multi-agent history
      - insert/update mql_call_attempts
      - update mql_analysis outcomes
      - apply close rules from user requirements
5. Optional Google Sheet backfill (overwrite FU blocks) via mql_pro_sheet_backfill

Usage:
  python scripts/mql_team_import.py --audit-only
  python scripts/mql_team_import.py --dry-run
  python scripts/mql_team_import.py --apply
    python scripts/mql_team_import.py --apply --rewrite-bd-remark-all
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv

# Reuse existing sheet writer so FU blocks are written exactly like prod.
import mql_pro_sheet_backfill as mql_sheet_backfill

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "MQL_team"
NORMALIZED_CSV = PROJECT_ROOT / "data" / "mql_pro_from_team.csv"
REJECTED_CSV = PROJECT_ROOT / "data" / "mql_pro_from_team_rejected.csv"

OWNER_FILE_ORDER: list[tuple[str, str]] = [
    ("monika_mql.csv", "Monika"),
    ("artuha_mql.csv", "Artuha"),
    ("dristhi_mql.csv", "Drishti"),
    ("suhana_mql.csv", "Suhana"),
]
OWNER_RANK = {owner: idx for idx, (_, owner) in enumerate(OWNER_FILE_ORDER)}
FILE_RANK = {name: idx for idx, (name, _) in enumerate(OWNER_FILE_ORDER)}

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "crm_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

CALL_STATUS_MAP = {
    "connected": "Connected",
    "did not connect": "Did not connect",
    "invalid number": "Invalid Number",
    "call back later": "Call back later",
    "do not disturb": "Do not Disturb",
    "referred": "Referred",
    "reffered": "Referred",
    "busy": "Did not connect",
    "not picked": "Did not connect",
    "picked": "Connected",
}

CURRENT_STATE_MAP = {
    "attempt again": "Attempt Again",
    "attempt again after 3 months": "Attempt Again after 3 months",
    "allocate again": "Allocate Again",
    "allocate again 3 months": "Allocate Again 3 months",
    "allocate again after 3 months": "Allocate Again 3 months",
    "shared story": "Shared Story",
    "not interested": "Not interested",
    "rescheduled": "Rescheduled",
    "interested": "Interested",
    "do not disturb": "Do not Disturb",
    "snapshot sent": "Snapshot Sent",
    "snapshot confirmed": "Snapshot Confirmed",
    "dream snapshot confirmed": "Dream Snapshot Confirmed",
    "meeting requested": "Meeting Requested",
    "meeting scheduled": "Meeting Scheduled",
    "meeting held": "Meeting Held",
    "solution sent": "Solution Sent",
    "solution picked": "Solution Picked",
    "escalate": "Escalate",
    "respondent": "Respondent",
    "picked solution": "Picked Solution",
    "reffered": "Reffered",
    "referred": "Reffered",
    "irrelevant": "Irrelevant",
}

EXACT_REJECT_CLOSE_REASONS = {
    "Not interested",
    "Irrelevant",
    "Do not Disturb",
    "Reffered",
}

QUALIFIED_STATES = {"Picked Solution", "Solution Picked"}

CSV_FIELDNAMES = [
    "Unique ID",
    "Lead Category",
    "Call Status",
    "Current State",
    "Call Duration",
    "Remark",
    "Recording Link",
    "Transcript",
    "Date",
    "Call Type",
    "Followups",
    "Follow-up Stage",
    "Assigned",
    "Category",
    "Phone No",
    "Company Name",
    "Person Name",
    "Email",
    "Dream Snapshot",
    "Source File",
    "Source Row",
]

DEFAULT_FALLBACK_DATE = datetime(2026, 1, 1)


@dataclass
class FUBlock:
    start: int
    end: int
    fu_number: int


def norm(text: str | None) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text).replace("\n", " ").strip().lower())


def pretty_datetime(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    for fmt in [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue

    # Handle excel-like datetime strings with trailing .0
    if raw.endswith(".0"):
        return parse_datetime(raw[:-2])

    return None


def parse_duration(raw: str | None) -> int | None:
    if not raw:
        return None
    s = str(raw).strip()
    if not s or s == "0":
        return None
    s = s.lstrip(":")

    m = re.match(r"^(\d+):(\d{2}):(\d{2})$", s)
    if m:
        secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
        return secs if secs > 0 else None

    try:
        frac = float(s)
        if 0 < frac < 1:
            secs = round(frac * 86400)
            return secs if secs > 0 else None
    except ValueError:
        pass

    return None


def parse_primary_phone(raw: str | None) -> str | None:
    """Return the first valid 10-digit phone from comma-separated values."""
    if not raw:
        return None

    parts = str(raw).split(",")
    for part in parts:
        digits = re.sub(r"\D", "", part)
        if len(digits) >= 10:
            return digits[-10:]

    digits = re.sub(r"\D", "", str(raw))
    if len(digits) >= 10:
        return digits[-10:]

    return None


def parse_fu_number(text: str | None) -> int | None:
    if not text:
        return None
    s = norm(text)
    if not s:
        return None

    # Common forms: Follow-up 3, follow up 3, fu3, fu 3, 3
    m = re.search(r"(?:follow[- ]?up|fu)\s*(\d{1,2})", s)
    if m:
        n = int(m.group(1))
        return n if 1 <= n <= 30 else None

    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 30 else None

    return None


def derive_call_status(current_state_raw: str) -> str:
    """Same behavior as mql_sync for state->status fallback."""
    s = norm(current_state_raw)
    if "invalid" in s:
        return "Invalid Number"
    if "do not disturb" in s:
        return "Do not Disturb"
    if "referred" in s or "reffered" in s:
        return "Referred"
    if s in ("call back later", "rescheduled"):
        return "Call back later"
    if s in (
        "attempt again",
        "attempt again after 3 months",
        "not interested",
        "shared story",
        "snapshot sent",
        "snapshot confirmed",
        "dream snapshot confirmed",
        "interested",
        "allocate again",
        "allocate again 3 months",
        "meeting requested",
        "meeting scheduled",
        "meeting held",
        "solution sent",
        "solution picked",
        "escalate",
        "respondent",
        "picked solution",
        "irrelevant",
    ):
        return "Connected"
    return "Did not connect"


def normalize_call_status(raw_status: str | None, current_state: str | None) -> str:
    s = (raw_status or "").strip()
    if s:
        mapped = CALL_STATUS_MAP.get(norm(s))
        return mapped if mapped else s
    if current_state:
        return derive_call_status(current_state)
    return "Did not connect"


def normalize_current_state(raw_state: str | None) -> str | None:
    s = (raw_state or "").strip()
    if not s:
        return None
    mapped = CURRENT_STATE_MAP.get(norm(s))
    return mapped if mapped else s


def classify_fu_col(label: str) -> str | None:
    t = norm(label)
    if "lead category" in t:
        return "lead_category"
    if t == "call status":
        return "call_status"
    if t == "current state":
        return "current_state"
    if t == "call duration":
        return "call_duration"
    if t.startswith("remark"):
        return "remark"
    if t.startswith("recording link"):
        return "recording_link"
    if t == "transcript":
        return "transcript"
    if t in ("timestamp", "date"):
        return "timestamp"
    if "follow-up stage" in t or t in ("call type", "calltype"):
        return "followup_stage"
    return None


def first_nonempty(row: list[str], indices: list[int]) -> str:
    for idx in indices:
        if idx < len(row):
            val = row[idx].strip()
            if val:
                return val
    return ""


def detect_fu_blocks(header_row_1: list[str], header_row_2: list[str]) -> list[FUBlock]:
    n = max(len(header_row_1), len(header_row_2))
    starts: list[tuple[int, int]] = []
    prev_num = 0

    for idx in range(n):
        h1 = norm(header_row_1[idx] if idx < len(header_row_1) else "")
        if not ("follow-up" in h1 or "follow up" in h1 or "followup" in h1):
            continue

        fu_num = parse_fu_number(h1)
        if fu_num is None:
            fu_num = prev_num + 1 if prev_num else 1

        curr_h2 = norm(header_row_2[idx] if idx < len(header_row_2) else "")
        prev_h2 = norm(header_row_2[idx - 1] if idx - 1 >= 0 and idx - 1 < len(header_row_2) else "")
        if "lead category" in prev_h2 and "lead category" not in curr_h2:
            start = idx - 1
        else:
            start = idx

        starts.append((max(0, start), fu_num))
        prev_num = fu_num

    # Remove duplicate starts while keeping first FU number seen.
    dedup: dict[int, int] = {}
    for start, fu_num in sorted(starts, key=lambda x: x[0]):
        if start not in dedup:
            dedup[start] = fu_num

    ordered = [(s, dedup[s]) for s in sorted(dedup)]

    # Some files include one trailing unlabeled FU block.
    if ordered:
        last_start, last_fu = ordered[-1]
        tail_from = last_start + 8
        for idx in range(tail_from, len(header_row_2)):
            lbl = norm(header_row_2[idx])
            if lbl not in ("call status", "lead category"):
                continue
            window = [norm(c) for c in header_row_2[idx: idx + 10]]
            if "current state" in window and ("timestamp" in window or "date" in window):
                ordered.append((idx, last_fu + 1))
                break

    blocks: list[FUBlock] = []
    for i, (start, fu_num) in enumerate(ordered):
        end = ordered[i + 1][0] if i + 1 < len(ordered) else len(header_row_2)
        if end <= start:
            continue
        blocks.append(FUBlock(start=start, end=end, fu_number=fu_num))

    return blocks


def parse_uid(uid: str) -> tuple[str, str | None]:
    """
    Converts display ID to (source, source_id) used in contacts table lookup.
    """
    raw = (uid or "").strip()
    if not raw:
        return "rocketreach", None

    if "|" in raw:
        prefix, source_id = raw.split("|", 1)
        pfx = prefix.strip().upper()
        sid = source_id.strip()

        source_map = {
            "RR": "rocketreach",
            "MS": "msme",
            "PH": "pharma",
            "MN": "manual",
            "BD": "manual",
            "CC": "manual",
            "AV": "manual",
            "BW": "manual",
        }
        source = source_map.get(pfx, "manual")

        if pfx not in ("RR", "MS", "PH", "MN") and sid:
            sid = f"{pfx}-{sid}"

        return source, sid

    if raw.startswith("ID-"):
        return "manual", raw

    return "manual", raw


def setup_logging(mode: str) -> Path:
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"mql_team_import_{mode}_{ts}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    return log_path


def parse_file(
    file_path: Path,
    owner_name: str,
    global_state: dict[str, Any],
    stats: dict[str, Any],
    rejects: list[dict[str, str]],
) -> list[dict[str, Any]]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))

    if len(rows) < 3:
        rejects.append(
            {
                "source_file": file_path.name,
                "source_row": "",
                "unique_id": "",
                "reason": "File has no data rows",
            }
        )
        return []

    header_row_1 = rows[0]
    header_row_2 = rows[1]
    data_rows = rows[2:]

    labels = [norm(c) for c in header_row_2]
    label_positions: defaultdict[str, list[int]] = defaultdict(list)
    for idx, lbl in enumerate(labels):
        if lbl:
            label_positions[lbl].append(idx)

    uid_cols: list[int] = []
    for key in ("unique id - dynamic", "unique id - new", "unique id - static", "unique id"):
        uid_cols.extend(label_positions.get(key, []))
    # Keep deterministic index order
    uid_cols = sorted(set(uid_cols))

    base_cols = {
        "phone": sorted(label_positions.get("phone no", [])),
        "company": sorted(label_positions.get("company name", [])),
        "person": sorted(label_positions.get("person name", [])),
        "email": sorted(label_positions.get("email", [])),
        "base_date": sorted(label_positions.get("date", [])),
        "category": sorted(label_positions.get("category", [])),
        "snapshot": sorted(label_positions.get("dream snapshot", [])),
    }

    blocks = detect_fu_blocks(header_row_1, header_row_2)
    if not blocks:
        rejects.append(
            {
                "source_file": file_path.name,
                "source_row": "",
                "unique_id": "",
                "reason": "No follow-up blocks detected",
            }
        )
        return []

    out_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()

    for row_idx, row in enumerate(data_rows, start=3):
        uid = first_nonempty(row, uid_cols)
        if not uid:
            rejects.append(
                {
                    "source_file": file_path.name,
                    "source_row": str(row_idx),
                    "unique_id": "",
                    "reason": "Missing Unique ID",
                }
            )
            continue

        phone = first_nonempty(row, base_cols["phone"])
        company = first_nonempty(row, base_cols["company"])
        person = first_nonempty(row, base_cols["person"])
        email = first_nonempty(row, base_cols["email"])
        campaign = first_nonempty(row, base_cols["category"]) or "consulting"
        snapshot = first_nonempty(row, base_cols["snapshot"])
        row_base_dt = parse_datetime(first_nonempty(row, base_cols["base_date"]))

        last_uid_dt = global_state["last_date_by_uid"].get(uid)
        last_uid_fu = global_state["last_fu_by_uid"].get(uid, 0)

        row_attempts = 0
        for block in blocks:
            col_map: dict[str, int] = {}
            for col_idx in range(block.start, min(block.end, len(header_row_2))):
                key = classify_fu_col(header_row_2[col_idx])
                if key and key not in col_map:
                    col_map[key] = col_idx

            def cell(name: str) -> str:
                idx = col_map.get(name)
                return row[idx].strip() if idx is not None and idx < len(row) else ""

            lead_category = cell("lead_category")
            raw_call_status = cell("call_status")
            raw_current_state = cell("current_state")
            call_duration = cell("call_duration")
            remark = cell("remark")
            rec_link = cell("recording_link")
            transcript = cell("transcript")
            ts_raw = cell("timestamp")
            fu_stage_raw = cell("followup_stage")

            has_activity = any(
                [
                    lead_category,
                    raw_call_status,
                    raw_current_state,
                    call_duration,
                    remark,
                    rec_link,
                    transcript,
                    ts_raw,
                    fu_stage_raw,
                ]
            )
            if not has_activity:
                continue

            if not ts_raw:
                # Keep processing when FU timestamp is blank using documented fallback chain:
                # last UID date -> row base date -> global last seen date -> default date.
                called_at = (
                    last_uid_dt
                    or row_base_dt
                    or global_state.get("last_seen_date")
                    or DEFAULT_FALLBACK_DATE
                )
                stats["rows_missing_timestamp_backfilled"] += 1
            else:
                called_at = parse_datetime(ts_raw)
                if not called_at:
                    stats["rows_stopped_invalid_timestamp"] += 1
                    rejects.append(
                        {
                            "source_file": file_path.name,
                            "source_row": str(row_idx),
                            "unique_id": uid,
                            "reason": f"Invalid timestamp in FU block: '{ts_raw}'",
                        }
                    )
                    break

            if called_at.weekday() == 6:
                called_at = called_at + timedelta(days=2)
                stats["sunday_shifted"] += 1

            fu_num = parse_fu_number(fu_stage_raw)
            if fu_num is None:
                fu_num = block.fu_number if block.fu_number else (last_uid_fu + 1 if last_uid_fu else 1)

            if fu_num < 1:
                fu_num = last_uid_fu + 1 if last_uid_fu else 1

            current_state = normalize_current_state(raw_current_state)
            call_status = normalize_call_status(raw_call_status, current_state)

            key = (
                uid,
                owner_name,
                fu_num,
                called_at,
                current_state or "",
                call_status,
                lead_category,
                remark,
                rec_link,
                transcript,
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)

            out_rows.append(
                {
                    "Unique ID": uid,
                    "Lead Category": lead_category,
                    "Call Status": call_status,
                    "Current State": current_state or "",
                    "Call Duration": call_duration,
                    "Remark": remark,
                    "Recording Link": rec_link,
                    "Transcript": transcript,
                    "Date": pretty_datetime(called_at),
                    "Call Type": f"Follow-up {fu_num}",
                    "Followups": f"Follow-up {fu_num}",
                    "Follow-up Stage": f"Follow-up {fu_num}",
                    "Assigned": owner_name,
                    "Category": campaign,
                    "Phone No": phone,
                    "Company Name": company,
                    "Person Name": person,
                    "Email": email,
                    "Dream Snapshot": snapshot,
                    "Source File": file_path.name,
                    "Source Row": row_idx,
                    "_called_at": called_at,
                    "_follow_up_number": fu_num,
                }
            )

            row_attempts += 1
            last_uid_dt = called_at
            last_uid_fu = max(last_uid_fu, fu_num)
            global_state["last_seen_date"] = called_at
        if row_attempts > 0:
            global_state["last_date_by_uid"][uid] = last_uid_dt
            global_state["last_fu_by_uid"][uid] = last_uid_fu

    stats["rows_per_owner"][owner_name] += len(out_rows)
    return out_rows


def write_csv(rows: list[dict[str, Any]], file_path: Path, fieldnames: list[str]) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def build_reject_rows(rejects: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "Source File": r.get("source_file", ""),
            "Source Row": r.get("source_row", ""),
            "Unique ID": r.get("unique_id", ""),
            "Reason": r.get("reason", ""),
        }
        for r in rejects
    ]


def resolve_agents(conn, agent_names: set[str]) -> dict[str, int]:
    result: dict[str, int] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM agents")
        all_agents = cur.fetchall()

        by_norm = {norm(name): aid for aid, name in all_agents}
        for name in agent_names:
            key = norm(name)
            aid = by_norm.get(key)
            if aid is None:
                cur.execute(
                    """
                    INSERT INTO agents (name, team, status)
                    VALUES (%s, 'mql', 'active')
                    RETURNING id
                    """,
                    (name,),
                )
                aid = cur.fetchone()[0]
                logging.info(f"Auto-created MQL agent: {name} (id={aid})")
            result[name] = aid

    return result


def ensure_close_reason_constraint(conn) -> None:
    allowed = [
        "qualified",
        "rejected",
        "stalled",
        "reallocated",
        "escalated",
        "bd_history",
        "Not interested",
        "Irrelevant",
        "Do not Disturb",
        "Reffered",
    ]
    allowed_sql = ", ".join(f"'{v}'" for v in allowed)

    with conn.cursor() as cur:
        cur.execute(
            """
            ALTER TABLE mql_allocations
            ALTER COLUMN close_reason TYPE VARCHAR(40)
            """
        )
        cur.execute(
            """
            DO $$
            DECLARE
                r record;
            BEGIN
                FOR r IN
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'mql_allocations'::regclass
                      AND contype = 'c'
                      AND pg_get_constraintdef(oid) ILIKE '%close_reason%'
                LOOP
                    EXECUTE format('ALTER TABLE mql_allocations DROP CONSTRAINT IF EXISTS %I', r.conname);
                END LOOP;
            END $$;
            """
        )
        cur.execute(
            f"""
            ALTER TABLE mql_allocations
            ADD CONSTRAINT mql_allocations_close_reason_check
            CHECK (close_reason IN ({allowed_sql}))
            """
        )


def resolve_contact_id(cur, uid: str, cache: dict[str, int | None]) -> int | None:
    if uid in cache:
        return cache[uid]

    source, source_id = parse_uid(uid)
    if not source_id:
        cache[uid] = None
        return None

    cur.execute(
        """
        SELECT id FROM contacts
        WHERE source = %s AND source_id = %s
        LIMIT 1
        """,
        (source, source_id),
    )
    row = cur.fetchone()
    if not row:
        cur.execute("SELECT id FROM contacts WHERE source_id = %s LIMIT 1", (source_id,))
        row = cur.fetchone()

    contact_id = row[0] if row else None
    cache[uid] = contact_id
    return contact_id


def get_or_create_analysis(cur, allocation_id: int, contact_id: int, agent_id: int) -> None:
    cur.execute("SELECT id FROM mql_analysis WHERE allocation_id = %s LIMIT 1", (allocation_id,))
    if cur.fetchone():
        return

    cur.execute(
        """
        INSERT INTO mql_analysis (contact_id, allocation_id, agent_id, outcome)
        VALUES (%s, %s, %s, 'pending')
        """,
        (contact_id, allocation_id, agent_id),
    )


def create_allocation(cur, contact_id: int, agent_id: int, campaign: str, called_at: datetime) -> int:
    cur.execute(
        """
        INSERT INTO mql_allocations
            (contact_id, agent_id, campaign, allocated_date, filled_by, allocated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (contact_id, agent_id, campaign, called_at.date(), agent_id, called_at),
    )
    return cur.fetchone()[0]


def get_open_allocation_map(cur) -> dict[int, tuple[int, int]]:
    cur.execute(
        """
        SELECT contact_id, id, agent_id
        FROM mql_allocations
        WHERE closed_at IS NULL
        """
    )
    return {contact_id: (alloc_id, agent_id) for contact_id, alloc_id, agent_id in cur.fetchall()}


def update_analysis_outcome(
    cur,
    allocation_id: int,
    outcome: str,
    reason: str | None,
    called_at: datetime,
    follow_up_number: int,
    lead_category: str,
) -> None:
    cur.execute(
        """
        UPDATE mql_analysis
        SET outcome = %s,
            outcome_reason = %s,
            outcome_date = %s,
            last_follow_up = GREATEST(COALESCE(last_follow_up, 0), %s),
            lead_category = COALESCE(NULLIF(%s, ''), lead_category),
            updated_at = NOW()
        WHERE allocation_id = %s
        """,
        (outcome, reason, called_at, follow_up_number, lead_category or None, allocation_id),
    )


def upsert_attempt(
    cur,
    contact_id: int,
    allocation_id: int,
    agent_id: int,
    row: dict[str, Any],
) -> str:
    called_at = row["_called_at"]
    follow_up = row["_follow_up_number"]
    duration = parse_duration(row.get("Call Duration"))

    phone_used = parse_primary_phone(row.get("Phone No"))

    cur.execute(
        """
        INSERT INTO mql_call_attempts (
            contact_id,
            allocation_id,
            agent_id,
            follow_up_number,
            phone_number_used,
            lead_category,
            call_status,
            current_state,
            call_duration,
            remark,
            recording_link,
            transcript_link,
            called_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s
        )
        ON CONFLICT (contact_id, agent_id, follow_up_number, called_at)
        DO UPDATE SET
            allocation_id = EXCLUDED.allocation_id,
            phone_number_used = EXCLUDED.phone_number_used,
            lead_category = EXCLUDED.lead_category,
            call_status = EXCLUDED.call_status,
            current_state = EXCLUDED.current_state,
            call_duration = EXCLUDED.call_duration,
            remark = EXCLUDED.remark,
            recording_link = EXCLUDED.recording_link,
            transcript_link = EXCLUDED.transcript_link
        RETURNING (xmax = 0)
        """,
        (
            contact_id,
            allocation_id,
            agent_id,
            follow_up,
            phone_used,
            (row.get("Lead Category") or "").strip() or None,
            (row.get("Call Status") or "").strip() or None,
            (row.get("Current State") or "").strip() or None,
            duration,
            (row.get("Remark") or "").strip() or None,
            (row.get("Recording Link") or "").strip() or None,
            (row.get("Transcript") or "").strip() or None,
            called_at,
        ),
    )
    inserted = cur.fetchone()[0]
    return "inserted" if inserted else "updated"


def maybe_update_contact_flag(
    cur,
    contact_id: int,
    new_flag: str,
    called_at: datetime,
    flag_cache: dict[int, str],
) -> bool:
    current = flag_cache.get(contact_id)
    if current is None:
        cur.execute("SELECT contact_flag FROM contacts WHERE id = %s", (contact_id,))
        row = cur.fetchone()
        current = row[0] if row else ""
        flag_cache[contact_id] = current

    if new_flag == current:
        return False

    if new_flag == "mql_in_progress":
        if current not in {
            "fresh",
            "in_progress",
            "needs_followup",
            "shared_story",
            "snapshot_sent",
            "mql_in_progress",
        }:
            return False

    cur.execute(
        """
        UPDATE contacts
        SET contact_flag = %s,
            flag_updated_at = %s
        WHERE id = %s
        """,
        (new_flag, called_at, contact_id),
    )
    flag_cache[contact_id] = new_flag
    return True


def apply_db_import(rows: list[dict[str, Any]], dry_run: bool, campaign_default: str) -> dict[str, int]:
    stats = {
        "rows_total": len(rows),
        "rows_missing_contact": 0,
        "allocations_created": 0,
        "allocations_reallocated": 0,
        "allocations_closed": 0,
        "attempts_inserted": 0,
        "attempts_updated": 0,
        "flags_updated": 0,
    }

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            ensure_close_reason_constraint(conn)

            agent_map = resolve_agents(conn, {r["Assigned"] for r in rows})
            contact_cache: dict[str, int | None] = {}
            flag_cache: dict[int, str] = {}

            for row in rows:
                row["_agent_id"] = agent_map[row["Assigned"]]
                cid = resolve_contact_id(cur, row["Unique ID"], contact_cache)
                row["_contact_id"] = cid
                if cid is None:
                    stats["rows_missing_contact"] += 1

            # Process rows in deterministic timeline order.
            rows_sorted = sorted(
                [r for r in rows if r.get("_contact_id")],
                key=lambda r: (
                    r["_contact_id"],
                    r["_called_at"],
                    OWNER_RANK.get(r["Assigned"], 999),
                    FILE_RANK.get(r.get("Source File", ""), 999),
                    int(r.get("Source Row", 0)),
                    r["_follow_up_number"],
                ),
            )

            open_alloc = get_open_allocation_map(cur)

            for row in rows_sorted:
                contact_id = row["_contact_id"]
                agent_id = row["_agent_id"]
                called_at = row["_called_at"]
                follow_up = row["_follow_up_number"]
                lead_category = (row.get("Lead Category") or "").strip()
                current_state = (row.get("Current State") or "").strip()
                campaign = (row.get("Category") or "").strip() or campaign_default

                # If this exact attempt exists, preserve its original allocation mapping.
                cur.execute(
                    """
                    SELECT allocation_id
                    FROM mql_call_attempts
                    WHERE contact_id = %s
                      AND agent_id = %s
                      AND follow_up_number = %s
                      AND called_at = %s
                    LIMIT 1
                    """,
                    (contact_id, agent_id, follow_up, called_at),
                )
                existing_attempt = cur.fetchone()

                if existing_attempt:
                    alloc_id = existing_attempt[0]
                else:
                    current_open = open_alloc.get(contact_id)
                    if current_open and current_open[1] != agent_id:
                        prev_alloc_id, _ = current_open
                        cur.execute(
                            """
                            UPDATE mql_allocations
                            SET closed_at = %s,
                                close_reason = 'reallocated'
                            WHERE id = %s AND closed_at IS NULL
                            """,
                            (called_at, prev_alloc_id),
                        )
                        if cur.rowcount:
                            stats["allocations_reallocated"] += 1
                            update_analysis_outcome(
                                cur,
                                prev_alloc_id,
                                outcome="pending",
                                reason="reallocated",
                                called_at=called_at,
                                follow_up_number=follow_up,
                                lead_category=lead_category,
                            )
                        open_alloc.pop(contact_id, None)

                    current_open = open_alloc.get(contact_id)
                    if current_open and current_open[1] == agent_id:
                        alloc_id = current_open[0]
                    else:
                        alloc_id = create_allocation(cur, contact_id, agent_id, campaign, called_at)
                        get_or_create_analysis(cur, alloc_id, contact_id, agent_id)
                        open_alloc[contact_id] = (alloc_id, agent_id)
                        stats["allocations_created"] += 1

                get_or_create_analysis(cur, alloc_id, contact_id, agent_id)

                result = upsert_attempt(cur, contact_id, alloc_id, agent_id, row)
                if result == "inserted":
                    stats["attempts_inserted"] += 1
                else:
                    stats["attempts_updated"] += 1

                close_reason = None
                outcome = "pending"
                outcome_reason = None
                target_flag = "mql_in_progress"

                if current_state in EXACT_REJECT_CLOSE_REASONS:
                    close_reason = current_state
                    outcome = "back_to_bd"
                    target_flag = "mql_rejected"
                elif current_state == "Escalate":
                    close_reason = "escalated"
                    outcome = "back_to_bd"
                    outcome_reason = "escalated"
                    target_flag = "mql_rejected"
                elif current_state in QUALIFIED_STATES:
                    close_reason = "qualified"
                    outcome = "sql"
                    target_flag = "mql_qualified"
                elif current_state == "Allocate Again 3 months":
                    close_reason = "reallocated"
                    outcome = "pending"
                    outcome_reason = "Allocate again 3 months"
                    target_flag = "attempt_3_months"
                elif current_state == "Meeting Scheduled":
                    target_flag = "meeting_in_progress"
                elif current_state == "Dream Snapshot Confirmed":
                    target_flag = "mql_in_progress"

                if maybe_update_contact_flag(cur, contact_id, target_flag, called_at, flag_cache):
                    stats["flags_updated"] += 1

                if close_reason:
                    cur.execute(
                        """
                        UPDATE mql_allocations
                        SET closed_at = %s,
                            close_reason = %s
                        WHERE id = %s AND closed_at IS NULL
                        """,
                        (called_at, close_reason, alloc_id),
                    )
                    if cur.rowcount:
                        stats["allocations_closed"] += 1
                        open_alloc.pop(contact_id, None)

                    update_analysis_outcome(
                        cur,
                        alloc_id,
                        outcome=outcome,
                        reason=outcome_reason,
                        called_at=called_at,
                        follow_up_number=follow_up,
                        lead_category=lead_category,
                    )

            if dry_run:
                conn.rollback()
                logging.info("DB changes rolled back (dry-run).")
            else:
                conn.commit()

        return stats

    finally:
        conn.close()


def run_sheet_backfill(csv_path: Path, dry_run: bool) -> tuple[int, int]:
    return mql_sheet_backfill.backfill_mql_sheets(
        csv_path=csv_path,
        dry_run=dry_run,
        full_rewrite=True,
    )


def run_bd_remark_rewrite(dry_run: bool, campaign: str) -> tuple[bool, str]:
    """Reuse mql_sync full BD Remark ALT rewrite mode for MQL agent sheets."""
    import mql_sync

    return mql_sync.run_mql_sync(
        dry_run=dry_run,
        campaign=campaign,
        rewrite_bd_remark_all=True,
    )


def summarize_audit(rows: list[dict[str, Any]], stats: dict[str, Any], rejects: list[dict[str, str]]) -> None:
    by_owner = Counter(r["Assigned"] for r in rows)
    by_state = Counter((r.get("Current State") or "<blank>") for r in rows)

    logging.info("\nAudit summary")
    logging.info("-" * 64)
    logging.info(f"Normalized attempt rows: {len(rows)}")
    logging.info(f"Rejected rows:           {len(rejects)}")
    logging.info(f"Rows backfilled (no ts): {stats['rows_missing_timestamp_backfilled']}")
    logging.info(f"Rows stopped (bad ts):  {stats['rows_stopped_invalid_timestamp']}")
    logging.info(f"Sunday shifts (+2d):    {stats['sunday_shifted']}")

    logging.info("Rows by owner:")
    for owner in [x[1] for x in OWNER_FILE_ORDER]:
        logging.info(f"  {owner}: {by_owner.get(owner, 0)}")

    logging.info("Top current states:")
    for state, count in by_state.most_common(12):
        logging.info(f"  {state}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize MQL_team CSVs and import to DB + sheets"
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--audit-only", action="store_true", help="Only generate audit CSV files")
    mode.add_argument("--dry-run", action="store_true", help="Audit + DB dry-run + sheet dry-run")
    mode.add_argument("--apply", action="store_true", help="Audit + DB apply + sheet apply")
    parser.add_argument("--campaign", default="consulting", help="Fallback campaign value")
    parser.add_argument(
        "--rewrite-bd-remark-all",
        action="store_true",
        help="After import/backfill, run full BD Remark ALT rewrite (same as mql_sync).",
    )
    parser.add_argument(
        "--rewrite-last-remark-all",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    mode_name = "audit" if args.audit_only else ("dryrun" if args.dry_run else "apply")
    log_path = setup_logging(mode_name)

    logging.info("=" * 72)
    logging.info("MQL TEAM IMPORT")
    logging.info(f"Mode: {mode_name}")
    logging.info(f"Raw dir: {RAW_DIR}")
    logging.info("=" * 72)

    global_state: dict[str, Any] = {
        "last_date_by_uid": {},
        "last_fu_by_uid": {},
        "last_seen_date": None,
    }
    stats: dict[str, Any] = {
        "rows_per_owner": Counter(),
        "rows_missing_timestamp_backfilled": 0,
        "rows_stopped_invalid_timestamp": 0,
        "sunday_shifted": 0,
    }
    rejects: list[dict[str, str]] = []
    normalized_rows: list[dict[str, Any]] = []

    for file_name, owner in OWNER_FILE_ORDER:
        fp = RAW_DIR / file_name
        if not fp.exists():
            rejects.append(
                {
                    "source_file": file_name,
                    "source_row": "",
                    "unique_id": "",
                    "reason": "Expected owner file not found",
                }
            )
            continue

        parsed = parse_file(fp, owner, global_state, stats, rejects)
        normalized_rows.extend(parsed)
        logging.info(f"Parsed {file_name}: {len(parsed)} attempts")

    write_csv(normalized_rows, NORMALIZED_CSV, CSV_FIELDNAMES)
    reject_rows = build_reject_rows(rejects)
    write_csv(reject_rows, REJECTED_CSV, ["Source File", "Source Row", "Unique ID", "Reason"])

    summarize_audit(normalized_rows, stats, rejects)
    logging.info(f"Audit CSV written:   {NORMALIZED_CSV}")
    logging.info(f"Rejected CSV written:{REJECTED_CSV}")

    if args.audit_only:
        logging.info(f"Done. Log: {log_path}")
        return

    db_stats = apply_db_import(normalized_rows, dry_run=args.dry_run, campaign_default=args.campaign)

    logging.info("\nDB import summary")
    logging.info("-" * 64)
    for key, value in db_stats.items():
        logging.info(f"{key}: {value}")

    processed, written = run_sheet_backfill(NORMALIZED_CSV, dry_run=args.dry_run)
    logging.info("\nSheet backfill summary")
    logging.info("-" * 64)
    logging.info(f"Rows processed: {processed}")
    logging.info(f"Rows written:   {written}")

    run_rewrite = args.rewrite_bd_remark_all or args.rewrite_last_remark_all
    if run_rewrite:
        logging.info("\nBD Remark ALT rewrite via mql_sync")
        logging.info("-" * 64)
        success, rewrite_output = run_bd_remark_rewrite(
            dry_run=args.dry_run,
            campaign=args.campaign,
        )
        if not success:
            raise RuntimeError("BD Remark ALT rewrite failed")
        logging.info("BD Remark ALT rewrite completed")

    logging.info(f"\nDone. Log: {log_path}")


if __name__ == "__main__":
    main()
