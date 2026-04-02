# MQL Import Detailed Runbook

This runbook documents the full MQL historical import pipeline implemented by `scripts/mql_team_import.py`.

## Scope

The script performs four stages:
1. Parse wide MQL team CSV files in fixed owner order.
2. Normalize them into one row per follow-up attempt.
3. Import into DB (`mql_allocations`, `mql_call_attempts`, `mql_analysis`, contact flags).
4. Backfill FU blocks into MQL Google Sheets.

## Input Files and Owner Order

Source directory:
- `data/MQL_team/`

Expected files and processing order:
1. `monika_mql.csv`
2. `artuha_mql.csv`
3. `dristhi_mql.csv`
4. `suhana_mql.csv`

This order is intentional and used to preserve deterministic ownership chronology.

## Output Files

Generated on every run:
- `data/mql_pro_from_team.csv` (normalized attempts)
- `data/mql_pro_from_team_rejected.csv` (rejected rows with reason)

Logs:
- `logs/mql_team_import_audit_*.log`
- `logs/mql_team_import_dryrun_*.log`
- `logs/mql_team_import_apply_*.log`

## Run Modes

### 1) Audit only

```bash
python scripts/mql_team_import.py --audit-only
```

Use when:
- You want normalization + reject report only.
- You do not want DB or sheet writes.

### 2) Dry run

```bash
python scripts/mql_team_import.py --dry-run
```

Use when:
- You want full simulation.
- DB writes are rolled back.
- Sheet writes run in dry-run mode.

### 3) Apply

```bash
python scripts/mql_team_import.py --apply --rewrite-bd-remark-all
```

Use when:
- You want DB commits and sheet updates.
- You want BD Remark ALT segments refreshed in MQL sheets after backfill.

Optional campaign override:

```bash
python scripts/mql_team_import.py --apply --campaign consulting --rewrite-bd-remark-all
```

## Normalization Rules

### Wide-to-flat conversion

- Each FU block becomes one normalized attempt row.
- Empty FU blocks are skipped.
- Duplicate attempts within the same parse key are de-duplicated.

### UID and contact lookup

- UID is read from first available unique-id field.
- Prefix-style IDs (like `RR|...`, `BD|...`) are normalized to DB lookup format.
- Missing UID rows are rejected into `mql_pro_from_team_rejected.csv`.

### Follow-up number

- FU number is read from follow-up stage when available.
- If missing, fallback uses FU block number.
- If still missing, fallback uses prior known FU for that UID + 1.

### Date handling

- Attempt timestamp uses FU timestamp if available.
- If missing, date is backfilled from:
  1. last date for same UID
  2. row base date
  3. global last seen date
  4. default fallback date
- If derived date lands on Sunday, shift by +2 days.

### Status and state normalization

- Call status is normalized via mapping table.
- Current state is normalized to canonical values.
- If call status is missing, derive from current state.

## Database Import Rules

### Allocation lifecycle

- Keeps multi-agent history.
- If a contact is currently open under another agent, old allocation is closed as `reallocated`.
- New allocation is created for new owner when required.
- Existing exact attempts keep original allocation mapping.

### Close behavior

The following states close allocation immediately with exact reasons:
- `Not interested`
- `Irrelevant`
- `Do not Disturb`
- `Reffered`

Additional close rules:
- `Escalate` -> close reason `escalated`
- Qualified states (`Picked Solution`, `Solution Picked`) -> close reason `qualified`
- `Allocate Again 3 months` -> close reason `reallocated`

States that remain open:
- `Dream Snapshot Confirmed`
- `Meeting Scheduled`

### Contact flags

Flags are updated by state outcome, including:
- `mql_in_progress`
- `meeting_in_progress`
- `mql_rejected`
- `mql_qualified`
- `attempt_3_months`

`attempt_3_months` is treated as a temporary hold bucket and becomes eligible for reallocation after 90 days.

## Sheet Backfill Behavior

The import calls `scripts/mql_pro_sheet_backfill.py` internals to write FU history to MQL sheets.

Behavior:
- Overwrite mode for FU blocks.
- Throttle + retry handling for transient Google API issues.
- Supports per-agent reruns if needed.

Manual per-agent rerun example:

```bash
python scripts/mql_pro_sheet_backfill.py --apply --agent-id 19 --file data/mql_pro_from_team.csv
```

## Validation Checklist

Run these checks after apply.

```sql
SELECT COUNT(*) AS attempts FROM mql_call_attempts;
SELECT COUNT(*) AS allocations FROM mql_allocations;
SELECT COUNT(*) AS analyses FROM mql_analysis;

SELECT a.name, COUNT(*) AS attempts
FROM mql_call_attempts m
JOIN agents a ON a.id = m.agent_id
GROUP BY a.name
ORDER BY attempts DESC;

SELECT close_reason, COUNT(*)
FROM mql_allocations
GROUP BY close_reason
ORDER BY COUNT(*) DESC;
```

## Recommended Execution Sequence

1. `python scripts/mql_team_import.py --audit-only`
2. Inspect `data/mql_pro_from_team.csv` and rejects file.
3. `python scripts/mql_team_import.py --dry-run`
4. Review dry-run log summary.
5. `python scripts/mql_team_import.py --apply`
6. Run validation SQL and spot-check sheets.

## Troubleshooting

- If rows are missing in sheet after apply, rerun backfill by agent.
- If API quota errors occur, rerun after cooldown.
- If UID lookup fails, inspect UID prefix and contact `source/source_id` consistency.
