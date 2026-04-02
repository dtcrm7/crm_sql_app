"""Wrappers to run allocation and sync scripts from the dashboard."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

DASHBOARD_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR   = DASHBOARD_DIR.parent / "scripts"
ALLOCATION_SCRIPT = SCRIPTS_DIR / "allocation_engine.py"
SYNC_SCRIPT       = SCRIPTS_DIR / "call_actions_sync.py"


def _run_command(command: list[str]) -> Tuple[bool, str]:
    """Run command and return (success, combined_output)."""
    try:
        result = subprocess.run(
            command,
            cwd=SCRIPTS_DIR,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=1200,
            check=False,
        )
    except Exception as exc:
        return False, f"Execution failed: {exc}"

    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode == 0, output.strip() or "(No output)"


def run_allocation(
    dry_run: bool = False,
    agent_id: Optional[int] = None,
    fresh_count: Optional[int] = None,
    campaign: str = "consulting",
) -> Tuple[bool, str]:
    """Run allocation_engine.py and return success flag with console output."""
    if not ALLOCATION_SCRIPT.exists():
        return False, f"Script not found: {ALLOCATION_SCRIPT}"

    cmd = [sys.executable, str(ALLOCATION_SCRIPT)]
    if dry_run:
        cmd.append("--dry-run")
    if agent_id is not None:
        cmd.extend(["--agent-id", str(agent_id)])
    if fresh_count is not None:
        cmd.extend(["--fresh-count", str(fresh_count)])
    cmd.extend(["--campaign", campaign])

    return _run_command(cmd)


def run_sync(
    dry_run: bool = False,
    date_str: Optional[str] = None,
    campaign: str = "consulting",
) -> Tuple[bool, str]:
    """Run call_actions_sync.py and return success flag with console output."""
    if not SYNC_SCRIPT.exists():
        return False, f"Script not found: {SYNC_SCRIPT}"

    cmd = [sys.executable, str(SYNC_SCRIPT)]
    if dry_run:
        cmd.append("--dry-run")
    if date_str:
        cmd.extend(["--date", date_str])
    cmd.extend(["--campaign", campaign])

    return _run_command(cmd)
