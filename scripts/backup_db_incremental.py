"""
backup_db_incremental.py
========================
Creates weekly incremental backups by appending only new/changed rows to cumulative CSV files.

One file per table. Each file includes:
- Header row (column names)
- Data rows
- Watermark row at end of each weekly batch (timestamp when backup was run)

Watermark format:
    TABLE: <table_name>, UPDATED_AT: <timestamp>, ROWS_APPENDED: <count>, BATCH_ID: <uuid>

This allows recovery to pinpoint exactly when data was added and allows resuming
from a specific watermark if needed.

WORKS WITH:
- Local PostgreSQL
- DigitalOcean Managed PostgreSQL
- Supabase PostgreSQL
- Any PostgreSQL instance (update DB_HOST, DB_PORT, DB_USER, DB_PASSWORD in .env)

Usage:
  python backup_db_incremental.py                    # backup to default folder
  python backup_db_incremental.py --output D:/Backups/incremental
  python backup_db_incremental.py --dry-run
  python backup_db_incremental.py --tables contacts mql_allocations

Scheduled via Windows Task Scheduler (see setup_backup_incremental.bat).

Dependencies: PostgreSQL psycopg2 driver

TROUBLESHOOTING:
- Password error: Ensure .env has DB_PASSWORD set
- Connection refused: Check DB_HOST and DB_PORT match your PostgreSQL instance
- SSL errors (Supabase/DigitalOcean): May need to add sslmode='require' to connection
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── LOAD ENV FIRST ────────────────────────────────────────────
load_dotenv(PROJECT_ROOT / ".env")

# ── DATABASE SETTINGS ─────────────────────────────────────────
INCREMENTAL_BACKUP_DIR = PROJECT_ROOT / "backups" / "incremental"
DEFAULT_TABLES = [
    "companies",
    "contacts",
    "contact_emails",
    "contact_phones",
    "agents",
    "mql_allocations",
    "mql_analysis",
    "mql_call_attempts",
    "call_actions",
    "agent_sheets",  # if exists
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("backup.incremental")


# ── CONFIG ────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "crm_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}


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
            log.error(f"  Check .env: DB_PASSWORD={'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '[empty]'}")
            log.error(f"  Connection attempt: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
        else:
            log.error(f"Database connection failed: {e}")
        sys.exit(1)
    except (psycopg2.Error, ValueError) as e:
        log.error(f"Failed to connect to database: {e}")
        sys.exit(1)


def read_watermark(backup_file: Path) -> datetime | None:
    """
    Read the last watermark from a backup file.
    Watermark is the last line after the data: TABLE: ..., UPDATED_AT: <ISO timestamp>, ...
    Returns None if file doesn't exist or has no watermark yet.
    """
    if not backup_file.exists():
        return None

    try:
        with open(backup_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            if len(lines) < 2:
                return None

            # Last line should be watermark (starts with "TABLE:")
            last_line = lines[-1].strip()
            if last_line.startswith("TABLE:"):
                # Parse: TABLE: contacts, UPDATED_AT: 2026-03-30T15:30:45.123456, ROWS_APPENDED: 5, BATCH_ID: ...
                parts = dict(item.split(": ", 1) for item in last_line.split(", "))
                if "UPDATED_AT" in parts:
                    ts_str = parts["UPDATED_AT"]
                    return datetime.fromisoformat(ts_str)
        return None
    except Exception as e:
        log.warning(f"Could not read watermark from {backup_file.name}: {e}")
        return None


def get_table_columns(conn, table_name: str) -> list[str]:
    """Fetch column names for a table."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            cur.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            return [row["column_name"] for row in cur.fetchall()]
        except psycopg2.Error as e:
            log.error(f"Could not fetch columns for {table_name}: {e}")
            return []


def table_exists(conn, table_name: str) -> bool:
    """Check if a table exists."""
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
                """,
                (table_name,),
            )
            return cur.fetchone() is not None
        except psycopg2.Error:
            return False


def fetch_new_rows(
    conn,
    table_name: str,
    since_timestamp: datetime | None,
    columns: list[str],
) -> tuple[int, list]:
    """
    Fetch rows created or updated since the given timestamp.
    Assumes all tables have created_at and updated_at columns.

    Returns: (row_count, list_of_rows_as_lists)
    """
    if since_timestamp is None:
        # First time: fetch all rows
        where_clause = ""
        params = (table_name,)
    else:
        # Incremental: fetch rows created or updated after watermark
        where_clause = f"WHERE (created_at > %s OR updated_at > %s)"
        params = (table_name, since_timestamp, since_timestamp)

    column_list = ", ".join(columns)

    query = f"SELECT {column_list} FROM {table_name} {where_clause} ORDER BY created_at ASC"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        try:
            if since_timestamp is None:
                cur.execute(query)
            else:
                cur.execute(query, (since_timestamp, since_timestamp))

            rows = cur.fetchall()
            data_rows = []
            for row in rows:
                data_row = [row[col] if row[col] is not None else "" for col in columns]
                data_rows.append(data_row)

            return len(data_rows), data_rows
        except psycopg2.Error as e:
            log.error(f"Query error for {table_name}: {e}")
            return 0, []


def append_backup(
    backup_file: Path,
    columns: list[str],
    data_rows: list,
    table_name: str,
    dry_run: bool = False,
) -> int:
    """
    Append data rows and watermark to CSV backup file.
    If file doesn't exist, create it with headers first.

    Returns: number of rows written (excluding watermark)
    """
    if dry_run:
        log.info(f"  [DRY RUN] Would append {len(data_rows)} rows to {backup_file.name}")
        return len(data_rows)

    if not data_rows:
        log.info(f"  No new rows for {table_name}")
        return 0

    try:
        file_is_new = not backup_file.exists()

        with open(backup_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Write header if new file
            if file_is_new:
                writer.writerow(columns)
                log.info(f"  Created {backup_file.name} (header row)")

            # Write data rows
            writer.writerows(data_rows)

            # Write watermark: TABLE: ..., UPDATED_AT: ..., ROWS_APPENDED: ..., BATCH_ID: ...
            now = datetime.now().isoformat()
            batch_id = str(uuid4())
            watermark = f"TABLE: {table_name}, UPDATED_AT: {now}, ROWS_APPENDED: {len(data_rows)}, BATCH_ID: {batch_id}"
            writer.writerow([watermark])

        log.info(
            f"  {backup_file.name}: Appended {len(data_rows)} rows "
            f"(batch {batch_id[:8]}...)"
        )
        return len(data_rows)

    except Exception as e:
        log.error(f"Failed to write to {backup_file}: {e}")
        return 0


def backup_table(
    conn,
    table_name: str,
    backup_dir: Path,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Backup a single table incrementally.

    Returns: (rows_appended, time_since_last_backup_seconds)
    """
    if not table_exists(conn, table_name):
        log.warning(f"Table {table_name} does not exist, skipping")
        return 0, 0

    log.info(f"Processing {table_name}...")

    # Prepare backup file
    backup_file = backup_dir / f"{table_name}.csv"

    # Get columns
    columns = get_table_columns(conn, table_name)
    if not columns:
        log.error(f"Could not fetch columns for {table_name}")
        return 0, 0

    # Read existing watermark to find cutoff timestamp
    watermark = read_watermark(backup_file)

    # Fetch new rows
    row_count, data_rows = fetch_new_rows(conn, table_name, watermark, columns)

    # Append to file
    if not dry_run:
        append_backup(backup_file, columns, data_rows, table_name, dry_run=False)
    else:
        append_backup(backup_file, columns, data_rows, table_name, dry_run=True)

    # Calculate time since last backup
    seconds_since = 0
    if watermark:
        delta = datetime.now() - watermark
        seconds_since = int(delta.total_seconds())

    return row_count, seconds_since


def run_backup(
    tables: list[str] | None = None,
    backup_dir: Path | None = None,
    dry_run: bool = False,
) -> bool:
    """
    Run incremental backup for specified tables.

    Args:
        tables: List of table names to backup. If None, uses DEFAULT_TABLES.
        backup_dir: Directory to write backup files to.
        dry_run: If True, log what would happen without writing files.

    Returns: True if all succeeded, False otherwise
    """
    if backup_dir is None:
        backup_dir = INCREMENTAL_BACKUP_DIR

    if tables is None:
        tables = DEFAULT_TABLES

    # Ensure backup directory exists
    backup_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Backup directory: {backup_dir}")

    if dry_run:
        log.info("=" * 70)
        log.info("DRY RUN MODE — No files will be modified")
        log.info("=" * 70)

    # Connect to DB
    conn = get_connection()
    try:
        total_rows = 0
        total_tables = 0
        batch_id = str(uuid4())[:8]

        log.info(f"\n{'=' * 70}")
        log.info(f"Starting incremental backup (batch: {batch_id})...")
        log.info(f"{'=' * 70}\n")

        for table_name in tables:
            rows_added, time_since = backup_table(conn, table_name, backup_dir, dry_run)
            if rows_added > 0 or not backup_dir.joinpath(f"{table_name}.csv").exists():
                total_tables += 1
                total_rows += rows_added

                if time_since > 0:
                    hours_since = time_since / 3600
                    log.info(f"  ✓ {table_name}: {rows_added} rows added ({hours_since:.1f}h since last)")
                else:
                    log.info(f"  ✓ {table_name}: {rows_added} rows added (first backup)")

        log.info(f"\n{'=' * 70}")
        log.info(f"Backup complete: {total_rows} rows across {total_tables} tables")
        log.info(f"{'=' * 70}\n")

        return True

    except Exception as e:
        log.error(f"Backup failed: {e}")
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Create incremental weekly backups of CRM database"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=INCREMENTAL_BACKUP_DIR,
        help=f"Output directory (default: {INCREMENTAL_BACKUP_DIR})",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        help=f"Specific tables to backup (default: {', '.join(DEFAULT_TABLES)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be backed up without writing files",
    )

    args = parser.parse_args()

    success = run_backup(
        tables=args.tables,
        backup_dir=args.output,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
