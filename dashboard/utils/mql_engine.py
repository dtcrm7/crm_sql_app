"""Wrappers to run MQL allocation and sync scripts from the dashboard."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

DASHBOARD_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR   = DASHBOARD_DIR.parent / "scripts"

MQL_ALLOCATION_SCRIPT = SCRIPTS_DIR / "mql_allocation_engine.py"
MQL_SYNC_SCRIPT       = SCRIPTS_DIR / "mql_sync.py"


def _run_command(command: list[str]) -> Tuple[bool, str]:
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


def run_mql_allocation(
    agent_id:   Optional[int]        = None,
    count:      Optional[int]        = None,
    campaign:   str                  = "consulting",
    dry_run:    bool                 = False,
    all_agents: bool                 = False,
    categories: Optional[list]       = None,
    contact_ids: Optional[list[int]] = None,
) -> Tuple[bool, str]:
    if not MQL_ALLOCATION_SCRIPT.exists():
        return False, f"Script not found: {MQL_ALLOCATION_SCRIPT}"

    cmd = [sys.executable, str(MQL_ALLOCATION_SCRIPT)]
    if dry_run:
        cmd.append("--dry-run")
    if all_agents:
        cmd.append("--all-agents")
    elif agent_id is not None:
        cmd.extend(["--agent-id", str(agent_id)])
    if count is not None:
        cmd.extend(["--count", str(count)])
    cmd.extend(["--campaign", campaign])
    if categories:
        cmd.extend(["--categories"] + categories)
    if contact_ids:
        cmd.extend(["--contact-ids"] + [str(cid) for cid in contact_ids])

    return _run_command(cmd)


def run_mql_sync(
    dry_run:  bool           = False,
    date_str: Optional[str]  = None,
    campaign: str            = "consulting",
) -> Tuple[bool, str]:
    if not MQL_SYNC_SCRIPT.exists():
        return False, f"Script not found: {MQL_SYNC_SCRIPT}"

    cmd = [sys.executable, str(MQL_SYNC_SCRIPT)]
    if dry_run:
        cmd.append("--dry-run")
    if date_str:
        cmd.extend(["--date", date_str])
    cmd.extend(["--campaign", campaign])

    return _run_command(cmd)
