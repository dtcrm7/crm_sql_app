"""
marketing_sheet_export.py
==========================
Export MQL contacts to Google Sheet for marketing/newsletter campaigns.

Three tabs:
    Tab 1 "All MQLs": Full MQL lifecycle contacts (for newsletters)
                 - Contacts in shared_story/snapshot_sent/mql_in_progress/mql_qualified/mql_rejected
                 - Includes both allocated and unallocated contacts
         - Use for: Email newsletters, product announcements, seasonal outreach
  
  Tab 2 "Interested MQLs": BD team contacts marked as interested
         - Contacts from BD team who showed interest
         - Use for: Newsletter campaigns for interested prospects
  
  Tab 3 "Rejected MQLs": Contacts marked Not Interested / Do Not Disturb
         - Use for: Careful/limited marketing (email only for DND)

All tabs have identical format:
  Unique ID | Name | Title | Company | Email | Phone | Category | Allocated | Status

Usage:
  python marketing_sheet_export.py --dry-run
  python marketing_sheet_export.py --apply

Schedule: Daily 04:00 AM
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import gspread
import psycopg2
import psycopg2.extras
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

DEFAULT_SHEET_ID = os.getenv("MARKETING_SHEET_ID", None)

MARKETING_TAB_ALL = "All MQLs"
MARKETING_TAB_INTERESTED = "Interested MQLs"
MARKETING_TAB_REJECTED = "Rejected MQLs"
MARKETING_TAB_METADATA = "Metadata"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("marketing.export")


# ── DATABASE ──────────────────────────────────────────────────
def get_connection():
    """Connect to PostgreSQL with explicit password handling."""
    try:
        conn_params = {
            "host": DB_CONFIG["host"],
            "port": int(DB_CONFIG["port"]),
            "database": DB_CONFIG["dbname"],
            "user": DB_CONFIG["user"],
        }
        
        if DB_CONFIG["password"] and DB_CONFIG["password"].strip():
            conn_params["password"] = DB_CONFIG["password"]
        
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False
        return conn
        
    except psycopg2.OperationalError as e:
        error_msg = str(e).lower()
        if "password" in error_msg or "authentication" in error_msg:
            log.error(f"✗ Password authentication failed")
            log.error(f"  Check .env: DB_PASSWORD={'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '[empty]'}")
            log.error(f"  Connection: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        else:
            log.error(f"Database connection failed: {e}")
        sys.exit(1)
    except (psycopg2.Error, ValueError) as e:
        log.error(f"Failed to connect to database: {e}")
        sys.exit(1)


def build_unique_id(source: str, source_id: str) -> str:
    """Build display ID like 'RR | 6175204'."""
    if source_id:
        for pfx in ("BD", "CC", "AV", "BW"):
            if str(source_id).startswith(f"{pfx}-"):
                return f"{pfx} | {str(source_id)[len(pfx)+1:]}"
        if str(source_id).startswith("ID-"):
            return source_id
    
    prefix_map = {
        "rocketreach": "RR",
        "msme": "MS",
        "pharma": "PH",
        "manual": "MN",
    }
    prefix = prefix_map.get(source, "RR")
    return f"{prefix} | {source_id}" if source_id else prefix


def fetch_active_mql_contacts(conn) -> list:
    """Fetch full marketing-eligible MQL contacts.

    Includes:
      - Contacts marked with contact_flag in
        ('shared_story', 'snapshot_sent', 'mql_in_progress', 'mql_qualified', 'mql_rejected')
      - Both allocated (to agents) and unallocated
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute("""
                SELECT
                    c.source, c.source_id, c.first_name, c.last_name,
                    c.designation AS title, co.name AS company_name,
                    c.bd_category AS category,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM mql_allocations mx
                            WHERE mx.contact_id = c.id
                        ) THEN 'Yes'
                        ELSE 'No'
                    END AS allocated,
                    CASE
                        WHEN c.contact_flag = 'snapshot_sent' THEN 'Snapshot Sent'
                        WHEN c.contact_flag = 'shared_story' THEN 'Shared Story'
                        WHEN c.contact_flag = 'mql_in_progress' THEN 'MQL In Progress'
                        WHEN c.contact_flag = 'mql_qualified' THEN 'MQL Qualified'
                        WHEN c.contact_flag = 'mql_rejected' THEN 'MQL Rejected'
                        ELSE 'MQL'
                    END AS status,
                    p.phone_number AS phone, e.email AS email
                FROM contacts c
                LEFT JOIN companies co ON co.id = c.company_id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, phone_number FROM contact_phones WHERE is_invalid = FALSE ORDER BY contact_id, rank) p ON p.contact_id = c.id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, email FROM contact_emails ORDER BY contact_id, rank) e ON e.contact_id = c.id
                WHERE c.contact_flag IN ('shared_story', 'snapshot_sent', 'mql_in_progress', 'mql_qualified', 'mql_rejected')
                ORDER BY c.flag_updated_at DESC
            """)

            result = [dict(row) for row in cur.fetchall()]
            log.info(f"Fetched {len(result)} all-MQL contacts")
            return result
        except psycopg2.Error as e:
            log.error(f"Query error (all-MQL contacts): {e}")
            return []


def fetch_interested_mql_contacts(conn) -> list:
    """Fetch all contacts marked as interested in call attempts.
    
    Includes:
      - Contacts where mql_call_attempts.current_state contains 'Interested'
      - Shows allocation status
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute("""
                SELECT
                    c.source, c.source_id, c.first_name, c.last_name,
                    c.designation AS title, co.name AS company_name,
                    c.bd_category AS category,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM mql_allocations mx
                            WHERE mx.contact_id = c.id
                        ) THEN 'Yes'
                        ELSE 'No'
                    END AS allocated,
                    'Interested' AS status,
                    p.phone_number AS phone, e.email AS email
                FROM contacts c
                LEFT JOIN companies co ON co.id = c.company_id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, phone_number FROM contact_phones WHERE is_invalid = FALSE ORDER BY contact_id, rank) p ON p.contact_id = c.id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, email FROM contact_emails ORDER BY contact_id, rank) e ON e.contact_id = c.id
                JOIN LATERAL (
                    SELECT mca.current_state, mca.called_at
                    FROM mql_call_attempts mca
                    WHERE mca.contact_id = c.id
                      AND mca.current_state = 'Interested'
                    ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                    LIMIT 1
                ) mca ON TRUE
                ORDER BY c.flag_updated_at DESC
            """)
            
            result = [dict(row) for row in cur.fetchall()]
            log.info(f"Fetched {len(result)} interested contacts")
            return result
        except psycopg2.Error as e:
            log.error(f"Query error (interested contacts): {e}")
            return []


def fetch_rejected_mql_contacts(conn) -> list:
    """Fetch all contacts marked as Not Interested or Do Not Disturb."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute("""
                SELECT
                    c.source, c.source_id, c.first_name, c.last_name,
                    c.designation AS title, co.name AS company_name,
                    c.bd_category AS category,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM mql_allocations mx
                            WHERE mx.contact_id = c.id
                        ) THEN 'Yes'
                        ELSE 'No'
                    END AS allocated,
                    mca.current_state AS status,
                    p.phone_number AS phone, e.email AS email
                FROM contacts c
                LEFT JOIN companies co ON co.id = c.company_id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, phone_number FROM contact_phones WHERE is_invalid = FALSE ORDER BY contact_id, rank) p ON p.contact_id = c.id
                LEFT JOIN (SELECT DISTINCT ON (contact_id) contact_id, email FROM contact_emails ORDER BY contact_id, rank) e ON e.contact_id = c.id
                JOIN LATERAL (
                    SELECT mca.current_state, mca.called_at
                    FROM mql_call_attempts mca
                    WHERE mca.contact_id = c.id
                      AND mca.current_state IN ('Not interested', 'Do not Disturb', 'Referred', 'Irrelevant')
                    ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                    LIMIT 1
                ) mca ON TRUE
                ORDER BY mca.called_at DESC
            """)
            
            result = [dict(row) for row in cur.fetchall()]
            log.info(f"Fetched {len(result)} rejected contacts")
            return result
        except psycopg2.Error as e:
            log.error(f"Query error (rejected contacts): {e}")
            return []


def format_row(contact: dict) -> list:
    """Format contact dict into sheet row (no source, remark, or date columns)."""
    name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
    
    return [
        build_unique_id(contact.get("source", ""), contact.get("source_id", "")),
        name,
        contact.get("title", "") or "",
        contact.get("company_name", "") or "",
        contact.get("email", "") or "",
        contact.get("phone", "") or "",
        contact.get("category", "") or "",
        contact.get("allocated", "No") or "No",
        contact.get("status", "") or "",
    ]


# ── GOOGLE SHEETS ─────────────────────────────────────────────
def get_gspread_client():
    """Get authenticated gspread client."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def ensure_tabs(sh):
    """Ensure all marketing tabs exist with headers."""
    existing = [ws.title for ws in sh.worksheets()]
    
    headers = [
        "Unique ID", "Name", "Title", "Company", "Email", "Phone", "Category",
        "Allocated", "Status"
    ]
    
    # Ensure data tabs exist with headers
    for tab_name in [MARKETING_TAB_ALL, MARKETING_TAB_INTERESTED, MARKETING_TAB_REJECTED]:
        if tab_name not in existing:
            log.info(f"  Creating tab: {tab_name}")
            try:
                ws = sh.add_worksheet(title=tab_name, rows=1, cols=15)
                log.info(f"    ✓ Worksheet created (initial 1 row)")
                # Write headers first
                ws.update(range_name="A1", values=[headers])
                log.info(f"    ✓ Headers written")
                # NOW expand by appending 4999 empty rows
                ws.append_rows([[""] * len(headers) for _ in range(4999)])
                log.info(f"    ✓ Expanded to 5000 rows")
            except Exception as e:
                log.error(f"    ✗ Failed to create tab {tab_name}: {e}")
        else:
            log.info(f"  Tab exists: {tab_name}")
            ws = sh.worksheet(tab_name)
            # Check if worksheet needs expansion
            try:
                current_rows = ws.row_count
                if current_rows < 5000:
                    log.info(f"    Current rows: {current_rows}, expanding to 5000...")
                    ws.append_rows([[""] * len(headers) for _ in range(5000 - current_rows)])
                    log.info(f"    ✓ Expanded to 5000 rows")
                else:
                    log.info(f"    Already has {current_rows} rows")
            except Exception as e:
                log.warning(f"    Could not expand rows: {e}")
            current_headers = ws.row_values(1)
            if current_headers != headers:
                log.info(f"    Updating headers (old: {len(current_headers)}, new: {len(headers)})")
                ws.update(range_name="A1", values=[headers])
                log.info(f"    ✓ Headers updated")
    
    # Ensure metadata tab exists
    if MARKETING_TAB_METADATA not in existing:
        log.info(f"  Creating tab: {MARKETING_TAB_METADATA}")
        try:
            ws = sh.add_worksheet(title=MARKETING_TAB_METADATA, rows=1, cols=5)
            ws.update(range_name="A1", values=[["Metric", "Value"]])
            ws.append_rows([[""] * 2 for _ in range(99)])
            log.info(f"    ✓ Metadata tab created and expanded")
        except Exception as e:
            log.error(f"    ✗ Failed to create metadata tab: {e}")
    else:
        log.info(f"  Tab exists: {MARKETING_TAB_METADATA}")


def write_to_sheet(gc, sheet_id: str, active: list, interested: list, rejected: list, dry_run: bool = False) -> bool:
    """Write MQL contacts to Google Sheet (all three tabs)."""
    total = len(active) + len(interested) + len(rejected)
    
    if total == 0:
        log.info("No contacts to export")
        return True
    
    if dry_run:
        log.info(f"\n  [DRY RUN] Would export:")
        log.info(f"    - {len(active)} active MQL contacts → {MARKETING_TAB_ALL}")
        log.info(f"    - {len(interested)} interested MQL contacts → {MARKETING_TAB_INTERESTED}")
        log.info(f"    - {len(rejected)} rejected MQL contacts → {MARKETING_TAB_REJECTED}")
        log.info(f"    Total: {total} contacts\n")
        return True
    
    try:
        sh = gc.open_by_key(sheet_id)
        log.info(f"✓ Opened sheet: {sheet_id}")
    except gspread.exceptions.SpreadsheetNotFound:
        log.error(f"✗ Sheet {sheet_id} not found")
        return False
    except Exception as e:
        log.error(f"✗ Cannot open sheet: {e}")
        return False
    
    try:
        log.info(f"\nEnsuring tabs exist...")
        ensure_tabs(sh)
        
        # Write All MQLs tab
        log.info(f"\nWriting {MARKETING_TAB_ALL} tab...")
        if active:
            ws = sh.worksheet(MARKETING_TAB_ALL)
            rows = [format_row(c) for c in active]
            # Clear all rows except header (row 1)
            try:
                if ws.row_count > 1:
                    ws.delete_rows(2, ws.row_count)
                    log.info(f"    Cleared old data rows")
            except Exception as e:
                log.warning(f"    Could not clear rows: {e}")
            # Append new data rows
            ws.append_rows(rows)
            log.info(f"  ✓ Wrote {len(active)} active MQL contacts")
        else:
            log.info(f"  ℹ No active MQL contacts to write")
        
        # Write Interested MQLs tab
        log.info(f"\nWriting {MARKETING_TAB_INTERESTED} tab...")
        if interested:
            ws = sh.worksheet(MARKETING_TAB_INTERESTED)
            rows = [format_row(c) for c in interested]
            try:
                if ws.row_count > 1:
                    ws.delete_rows(2, ws.row_count)
                    log.info(f"    Cleared old data rows")
            except Exception as e:
                log.warning(f"    Could not clear rows: {e}")
            ws.append_rows(rows)
            log.info(f"  ✓ Wrote {len(interested)} interested MQL contacts")
        else:
            log.info(f"  ℹ No interested MQL contacts to write")
        
        # Write Rejected MQLs tab
        log.info(f"\nWriting {MARKETING_TAB_REJECTED} tab...")
        if rejected:
            ws = sh.worksheet(MARKETING_TAB_REJECTED)
            rows = [format_row(c) for c in rejected]
            try:
                if ws.row_count > 1:
                    ws.delete_rows(2, ws.row_count)
                    log.info(f"    Cleared old data rows")
            except Exception as e:
                log.warning(f"    Could not clear rows: {e}")
            ws.append_rows(rows)
            log.info(f"  ✓ Wrote {len(rejected)} rejected MQL contacts")
        else:
            log.info(f"  ℹ No rejected MQL contacts to write")
        
        # Update Metadata tab
        log.info(f"\nUpdating {MARKETING_TAB_METADATA} tab...")
        ws = sh.worksheet(MARKETING_TAB_METADATA)
        metadata = [
            ["Last Updated", datetime.now().isoformat()],
            ["All MQLs", len(active)],
            ["Interested MQLs", len(interested)],
            ["Rejected MQLs", len(rejected)],
            ["Total", total],
        ]
        try:
            if ws.row_count > 1:
                ws.delete_rows(2, ws.row_count)
        except Exception as e:
            log.warning(f"    Could not clear metadata rows: {e}")
        ws.append_rows(metadata)
        log.info(f"  ✓ Metadata updated")
        
        return True
    
    except Exception as e:
        log.error(f"✗ Failed to write to sheet: {e}")
        import traceback
        log.error(f"  Traceback: {traceback.format_exc()}")
        return False


def run_export(sheet_id: str = None, dry_run: bool = False) -> bool:
    """Run marketing sheet export."""
    if not sheet_id:
        sheet_id = DEFAULT_SHEET_ID
        if not sheet_id:
            log.error("No sheet ID. Use --sheet-id or set MARKETING_SHEET_ID env var")
            return False
    
    log.info(f"\n{'=' * 70}")
    log.info(f"Starting marketing sheet export...")
    log.info(f"Sheet ID: {sheet_id}")
    log.info(f"{'=' * 70}")
    
    if dry_run:
        log.info("[DRY RUN] No sheets will be modified")
    
    conn = get_connection()
    
    try:
        log.info(f"\nFetching data from database...")
        active = fetch_active_mql_contacts(conn)
        interested = fetch_interested_mql_contacts(conn)
        rejected = fetch_rejected_mql_contacts(conn)
        
        if not active and not interested and not rejected:
            log.warning("No MQL contacts found")
            return True
        
        log.info(f"Total contacts fetched: {len(active) + len(interested) + len(rejected)}")
        
        gc = get_gspread_client()
        success = write_to_sheet(gc, sheet_id, active, interested, rejected, dry_run)
        
        log.info(f"\n{'=' * 70}")
        if success:
            log.info(f"✓ Export complete: {len(active) + len(interested) + len(rejected)} total contacts")
        else:
            log.error(f"✗ Export failed")
        log.info(f"{'=' * 70}\n")
        
        return success
    
    except Exception as e:
        log.error(f"✗ Export failed: {e}")
        import traceback
        log.error(f"  Traceback: {traceback.format_exc()}")
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Export MQL contacts to marketing Google Sheet (active + interested + rejected)"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying")
    parser.add_argument("--apply", action="store_true", help="Execute export")
    parser.add_argument("--sheet-id", type=str, help="Google Sheet ID")
    
    args = parser.parse_args()
    
    if not args.dry_run and not args.apply:
        log.error("Must specify --dry-run or --apply")
        parser.print_help()
        sys.exit(1)
    
    success = run_export(sheet_id=args.sheet_id, dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
