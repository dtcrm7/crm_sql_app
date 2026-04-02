# Historical Import Flow

This document defines the operational sequence for rebuilding historical CRM data.

## 1) Reset Database Data

Run `sql/reset_data.sql` first.

Purpose:
- Clears contacts, companies, phones, emails, allocations, meetings, and call history.
- Keeps the `agents` table intact.

Run with psql:

```bash
psql -h <host> -U <user> -d <db_name> -f sql/reset_data.sql
```

Expected result:
- Tables are truncated and identities reset.
- Final status row confirms reset completion.

## 2) Import BD Story History

Run `scripts/bd_story_import.py` using `data/mql_t_d.csv`.

Recommended run order:

```bash
python scripts/bd_story_import.py --file data/mql_t_d.csv
python scripts/bd_story_import.py --file data/mql_t_d.csv --apply
```

Behavior highlights:
- Supports dry-run by default.
- Auto-creates missing agents.
- Closes historical allocations and handles reallocation when owner changes.
- Forces state to `Shared Story` for this migration stage.
- Idempotent for duplicate keys.

Expected outputs:
- Logs in `logs/bd_story_import_*.log`.

## 3) Add Campaign Support

Run `sql/03_add_campaign.sql` after BD import.

Purpose:
- Creates `campaigns` table (if missing).
- Adds `contacts.campaign` with default `consulting`.
- Seeds default campaign and adds index for performance.

Run with psql:

```bash
psql -h <host> -U <user> -d <db_name> -f sql/03_add_campaign.sql
```

## 4) Import MQL Team History

Use the detailed MQL runbook:
- `docs/MQL_IMPORT_DETAILED.md`

Minimum sequence:

```bash
python scripts/mql_team_import.py --audit-only
python scripts/mql_team_import.py --dry-run
python scripts/mql_team_import.py --apply --rewrite-bd-remark-all
```

This step normalizes wide MQL team files, imports attempts/allocations/analysis, and backfills agent sheets.
The `--rewrite-bd-remark-all` option also refreshes BD Remark ALT numbers in all MQL FU tabs.

## Recommended Post-Run Checks

1. Validate record counts in `mql_call_attempts`, `mql_allocations`, and `mql_analysis`.
2. Spot-check agent sheet writes in Google Sheets.
3. In Pipeline, validate that **True MQL** counts unique contacts with at least one MQL call in `Dream Snapshot Confirmed`.
4. In Block accumulation, validate:
	- `Do not Disturb` and `Invalid Number` are permanent buckets.
	- `Allocate Again after 90 days` appears as temporary buckets separately for BD and MQL.
5. Review latest migration logs in `logs/`.
