"""
backup_db.py
============
Creates a timestamped pg_dump backup of crm_db.
Keeps the last N backups, deletes older ones automatically.

Usage:
  python backup_db.py                    # backup to default folder
  python backup_db.py --keep 10          # keep last 10 backups
  python backup_db.py --output D:/Backups/crm

Scheduled via Windows Task Scheduler (see setup_backup.bat).

Requirements: pg_dump must be on PATH (installed with PostgreSQL)
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT  = Path(__file__).resolve().parent.parent
BACKUP_DIR    = PROJECT_ROOT / "backups"
DEFAULT_KEEP  = 7   # keep last 7 backups

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger("crm.backup")


def verify_pg_dump() -> str:
    """Confirm pg_dump is available. Returns its path."""
    path = shutil.which("pg_dump")
    if path:
        return path

    # Common Windows PostgreSQL install paths
    common_paths = [
        r"C:\Program Files\PostgreSQL\18\bin\pg_dump.exe",
        r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe",
        r"C:\Program Files\PostgreSQL\16\bin\pg_dump.exe",
        r"C:\Program Files\PostgreSQL\15\bin\pg_dump.exe",
        r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
    ]
    for p in common_paths:
        if Path(p).exists():
            return p

    log.error(
        "pg_dump not found. Add PostgreSQL bin to PATH or set PGDUMP_PATH in .env"
    )
    sys.exit(1)


def rotate_backups(backup_dir: Path, keep: int) -> None:
    """Delete oldest backup files, keeping only `keep` most recent."""
    files = sorted(backup_dir.glob("crm_db_*.sql"), key=lambda f: f.stat().st_mtime)
    to_delete = files[:-keep] if len(files) > keep else []
    for f in to_delete:
        f.unlink()
        log.info(f"  Deleted old backup: {f.name}")


def run_backup(output_dir: Path, keep: int) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    pg_dump = verify_pg_dump()

    db_host = os.getenv("DB_HOST",     "localhost")
    db_port = os.getenv("DB_PORT",     "5432")
    db_name = os.getenv("DB_NAME",     "crm_db")
    db_user = os.getenv("DB_USER",     "postgres")
    db_pass = os.getenv("DB_PASSWORD", "")

    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = output_dir / f"crm_db_{ts}.sql"

    log.info("=" * 55)
    log.info(f"  CRM DATABASE BACKUP")
    log.info(f"  DB:      {db_name} on {db_host}:{db_port}")
    log.info(f"  Output:  {backup_file}")
    log.info(f"  Keeping: last {keep} backups")
    log.info("=" * 55)

    env = os.environ.copy()
    env["PGPASSWORD"] = db_pass   # pg_dump reads this to skip password prompt

    cmd = [
        pg_dump,
        f"--host={db_host}",
        f"--port={db_port}",
        f"--username={db_user}",
        f"--dbname={db_name}",
        "--format=plain",          # plain SQL — human-readable, easy to restore
        "--no-password",
        f"--file={backup_file}",
    ]

    log.info("Running pg_dump...")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    if result.returncode != 0:
        log.error(f"pg_dump failed (exit {result.returncode}):")
        log.error(result.stderr)
        sys.exit(1)

    size_mb = backup_file.stat().st_size / (1024 * 1024)
    log.info(f"  Backup complete: {backup_file.name}  ({size_mb:.2f} MB)")

    # Rotate old backups
    rotate_backups(output_dir, keep)

    remaining = sorted(output_dir.glob("crm_db_*.sql"))
    log.info(f"  Backups retained ({len(remaining)}):")
    for f in remaining:
        mb = f.stat().st_size / (1024 * 1024)
        log.info(f"    {f.name}  ({mb:.2f} MB)")

    log.info("=" * 55)
    return backup_file


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backup crm_db via pg_dump")
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=str(BACKUP_DIR),
        help=f"Backup directory (default: {BACKUP_DIR})",
    )
    parser.add_argument(
        "--keep", "-k",
        type=int,
        default=DEFAULT_KEEP,
        help=f"Number of backups to retain (default: {DEFAULT_KEEP})",
    )
    args = parser.parse_args()

    run_backup(Path(args.output), args.keep)
