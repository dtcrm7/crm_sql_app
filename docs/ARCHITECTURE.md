# B2B CRM — System Architecture
> Phase: 2C — BD + MQL + Meetings pipeline live
> DB: PostgreSQL (`crm_db`) | Dashboard: Streamlit | Sync: Python scripts via Google Sheets API
> Last updated: April 2026

---

## 1. What This System Does

A two-stage B2B outreach CRM:

1. **BD stage** — BD agents cold-call contacts from uploaded datasets. Contacts who receive the company story and dream snapshot are promoted to MQL.
2. **MQL stage** — MQL agents do deep follow-up (up to 30 attempts) to qualify contacts into SQL-ready. Meetings can be scheduled mid-pipeline; the same agent continues through to Solution Picked.

Data lives in PostgreSQL. Agents work in Google Sheets. Python scripts sync between sheets and DB nightly. The Streamlit dashboard gives managers full visibility and control.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GOOGLE SHEETS (Agent-facing)                     │
│                                                                         │
│  BD Agent Sheets (one per BD agent)                                     │
│  Tabs: Instructions | New Contact | FU1 | FU2 | FU3 | FU4 | FU5        │
│                                                                         │
│  MQL Agent Sheets (one per MQL agent)                                   │
│  Tabs: Instructions | MQL FU 1-15 | MQL FU 16-30                       │
│                                                                         │
│  Shared Meetings Sheet (one sheet, all agents)                          │
│  Tabs: Meetings | Meeting Context | Meeting Context Normalized           │
└──────────────┬─────────────────────────────────┬───────────────────────┘
               │ gspread API                      │ gspread API
       ┌───────▼──────────────┐        ┌──────────▼──────────────┐
       │  BD Scripts           │        │  MQL Scripts             │
       │  ─────────────────── │        │  ─────────────────────── │
       │  call_actions_sync.py │        │  mql_sync.py  (1:00 AM)  │
       │  (1:00 AM)            │        │  meeting_sync.py(1:30 AM)│
       │  allocation_engine.py │        │  mql_allocation_         │
       │  (2:00 AM)            │        │  engine.py  (2:30 AM)    │
       └───────┬───────────────┘        └──────────┬───────────────┘
               │ psycopg2                           │ psycopg2
               ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          PostgreSQL — crm_db                            │
│                                                                         │
│  CORE             companies · contacts · contact_emails · contact_phones│
│  BD PIPELINE      agents · agent_sheets · contact_allocations           │
│                   call_actions                                          │
│  MQL PIPELINE     mql_allocations · mql_analysis · mql_call_attempts   │
│  MEETINGS         meetings · meeting_call_attempts                      │
│  SYSTEM           dashboard_users · action_log · campaigns              │
│                   reallocation_campaigns · reallocation_campaign_contacts│
└─────────────────────────────────────────────────────────────────────────┘
               ▲
  ┌────────────┴──────────────┐
  │  Streamlit Dashboard      │
  │  dashboard/app.py         │
  │  13 pages                 │
  └───────────────────────────┘
```

---

## 3. Contact Lifecycle — Full Pipeline

```
CSV Upload
    │
    ▼
contact_flag = 'fresh'
    │
    │  BD agent dials → call_actions_sync.py reads sheet → DB updated
    ▼
contact_flag = 'in_progress'
    │
    ├──→ 'needs_followup'       stalled >7 days — gets FU priority next day
    ├──→ 'not_interested'       retry allowed after 2 months
    ├──→ 'dnd'                  retry allowed after 3 months
    ├──→ 'attempt_3_months'     all 6 BD attempts used
    ├──→ 'invalid_number'       permanent block — bad number
    ├──→ 'referred'             permanent block — referred out
    ├──→ 'language_issue'       permanent block
    │
    ├──→ 'shared_story' ─────────────────────────────────────────────┐
    └──→ 'snapshot_sent' ────────────────────────────────────────────┤
                                                                     │
                                            MQL-eligible pool (unallocated)
                                                                     │
                                     mql_allocation_engine.py assigns
                                     to an MQL agent → writes to sheet
                                                                     │
                                                                     ▼
                                          contact_flag = 'mql_in_progress'
                                          MQL agent follows up (FU1–FU30)
                                                                     │
                        ┌────────────────────────────────────────────┤
                        │                            │               │
                        ▼                            ▼               ▼
              Path A — Direct              Path B — Meeting    Escalate
                        │                            │               │
              Agent logs Solution Sent     "Meeting Scheduled"  close_reason
              → Solution Picked            → contact_flag =     = 'escalated'
                        │                 'meeting_in_progress'  → Escalation
                        │                  Same agent continues  queue in
                        │                  in same MQL sheet     dashboard
                        │                            │
                        │                  Meeting held
                        │                  → "Meeting Held"
                        │                  auto-filled by
                        │                  meeting_sync.py
                        │                            │
                        │                  Agent logs Solution Sent
                        │                  → Solution Picked
                        │                            │
                        └────────────────────────────┘
                                          │
                                          ▼
                              contact_flag = 'mql_qualified'
                              SQL Ready — handed to sales team
```

---

## 4. Database Schema

### Phase 1 — Core tables (`sql/01_schema_phase1.sql`)

| Table | Key columns / notes |
|---|---|
| `companies` | `name_normalized` (generated stored column) used for deduplication. `company_flag` set by `call_actions_sync.py`. |
| `contacts` | `source` + `source_id` = unique key. `contact_flag` drives lifecycle. `campaign = 'consulting'` (business filter). `bd_category` = original CSV Category (e.g. `'clinics ds1'`). |
| `contact_emails` | `rank=1` is primary. |
| `contact_phones` | Up to 3 per contact. `rank=1` is primary. `is_invalid` marks bad numbers. `invalidated_at` records when. |

### Phase 2A — BD pipeline (`sql/02_schema_phase2a.sql`)

| Table | Key columns / notes |
|---|---|
| `agents` | `team = 'bd' / 'mql' / 'sales'`. `kpi_dialed` = daily target. `fresh_pct` + `followup_pct` = 100. `is_on_leave` resets nightly. |
| `agent_sheets` | Sheet ID per agent per campaign per `sheet_type` (`'calling'` / `'meetings'`). UNIQUE on `(agent_id, campaign, sheet_type)`. |
| `contact_allocations` | One active row per contact. `attempt_number` 0–5 (0 = New Contact). `close_reason`: `completed` / `reallocated` / `max_attempts`. |
| `call_actions` | One row per phone number per BD call. `call_status`, `current_state`, `remark`, `recording_link`, `transcript_link`, `dream_snapshot_link`. |
| `agent_leave_log` | Permanent leave history. `is_on_leave` on `agents` = today only (resets daily). |

Also adds to existing tables:
- `contacts.contact_flag` + `flag_updated_at`
- `contact_phones.is_invalid` + `invalidated_at`
- `companies.company_flag` + `flag_updated_at`

Views added: `agent_availability_today`, `contacts_allocation_ready`, `agent_daily_kpi`, `contacts_needing_followup`.

### Phase 2B — MQL pipeline (`sql/05_mql_migration.sql`)

| Table | Key columns / notes |
|---|---|
| `mql_allocations` | One active per contact at a time. `close_reason`: `qualified` / `rejected` / `stalled` / `reallocated` / `escalated` / `bd_history`. |
| `mql_analysis` | One row per allocation. Pre-filled with BD context (`bd_agent_name`, `bd_call_date`, `bd_current_state`, `bd_remark`, `bd_recording_link`, `bd_snapshot_link`). `outcome`: `sql` / `back_to_bd` / `disqualified` / `pending`. `outcome_reason` = text note (e.g. `reassigned_from_escalation:42`). |
| `mql_call_attempts` | One row per FU block filled by agent. `follow_up_number` 0–30. `phone_uncertain = TRUE` = multi-phone contact, correct number needs confirmation. `dream_snapshot_link` also stored here. |

Also adds: `agents.team` + extends `contacts.contact_flag` CHECK.

### Phase 2C — Meetings (`sql/08_meetings_schema.sql`)

| Table | Key columns / notes |
|---|---|
| `meetings` | One row per scheduled meeting. `status`: `scheduled → pushed → held → solution_sent → solution_picked / rescheduled / lost`. `scheduled_at_fu` = MQL FU# where booked. `held_at_fu` = meeting sheet FU# where held was logged. `sheet_row_index` = row position in the shared meetings sheet. |
| `meeting_call_attempts` | One row per FU in the meeting sheet (up to FU10). Tracks Solution Sent / Solution Picked in the meeting track. |

Also extends `contacts.contact_flag` CHECK to include `meeting_in_progress`.
Also updates `contacts_allocation_ready` view to exclude all MQL-locked flags from BD pool.

### Support tables

| Table | File | Description |
|---|---|---|
| `campaigns` | `03_add_campaign.sql` | Campaign reference. Seeded with `'consulting'`. |
| `dashboard_users` | `06_rbac_auth.sql` | Login credentials + role (`admin` / `user`). |
| `action_log` | `06_rbac_auth.sql` | Audit trail of admin actions. |
| `reallocation_campaigns` | `07_reallocation_campaigns.sql` | Saved BD re-allocation filter sets. |
| `reallocation_campaign_contacts` | `07_reallocation_campaigns.sql` | Contact snapshot per re-allocation. |

### Schema patch files (run after the numbered sequence if column is missing)

| File | What it adds |
|---|---|
| `sql/10_add_bd_category.sql` | `contacts.bd_category VARCHAR(100)` + index. Stores original CSV Category for segmentation. |
| `sql/add_escalated_close_reason.sql` | Patches `mql_allocations.close_reason` CHECK constraint to include `'escalated'`. Safe to re-run. |
| `sql/agent_sheets_schema.sql` | Standalone creation of `agent_sheets` table (run after `02`, before `08` if skipped by `04`). |

---

## 5. Google Sheets Structure

### BD Agent Sheets

One sheet per BD agent. All tabs auto-created/verified by `allocation_engine.py` on every run.

| Tab | Content |
|---|---|
| `Instructions` | Auto-generated guide: all columns explained, every valid Call Status and Current State, key rules. Refreshed if stale. |
| `New Contact` | Fresh contacts — first ever call. 14 columns. |
| `FU1` – `FU5` | Follow-up contacts. Same structure + 3 extra pre-filled context columns: `Last Status`, `Last Remark`, `Last Called`. |

**New Contact headers:**
`Unique ID - Static | Company | Name | Phone | Title | Call Duration | Call Status | Current State | Remark | Recording Link | Transcript | Dream Snapshot | Timestamp | Campaign`

**FU1–FU5 extra pre-filled cols (after Title):**
`Last Status | Last Remark | Last Called` — pulled from DB so agent sees what happened on the previous call before dialling.

**BD Call Status values:**
`Connected` · `Did not connect` · `Call back later` · `Do not Disturb` · `Invalid Number` · `Referred`

**BD Current State values:**
`Interested` · `Rescheduled` · `Attempt Again` · `Attempt Again after 3 months` · `Allocate Again` · `Shared Story` · `Snapshot Sent` · `Not interested` · `Do not Disturb`

---

### MQL Agent Sheets

One sheet per MQL agent. All tabs auto-created/verified by `mql_allocation_engine.py` on every run.

| Tab | Content |
|---|---|
| `Instructions` | Auto-generated guide: pre-filled columns explained, FU block columns explained, all valid values, meeting track rules, phone confirmation rules. Refreshed if stale. |
| `MQL FU 1-15` | 177 cols = 12 contact info cols (A–L) + FU1–FU15 (11 cols × 15). |
| `MQL FU 16-30` | Same structure. Contacts copied here automatically when they reach FU15. |

**Contact info columns (A–L, pre-filled):**

| Col | Header | Source |
|---|---|---|
| A | Unique ID | `RR\|source_id` / `CC\|source_id` / etc. |
| B | Company Name | `companies.name` |
| C | Person Name | `first_name + last_name` |
| D | Phone | `contact_phones rank=1` (if multi-phone: alt phones noted in col H) |
| E | Email | `contact_emails rank=1` |
| F | BD Agent | last `call_action.agent` |
| G | BD Call Date | last `call_action.called_at` |
| H | BD Remark | last `call_action.remark` + alt phone note if applicable |
| I | BD Recording Link | last `call_action.recording_link` |
| J | Category | `contacts.bd_category` |
| K | BD Transcript | last `call_action.transcript_link` |
| L | Dream Snapshot | last `call_action.dream_snapshot_link` |

**Each FU block (11 cols, agent fills):**
`MQL Category | Call Status | Current State | Call Duration | Remark | Recording Link | Transcript | Message Status | Timestamp | Follow-up Stage | Sync Status`

**MQL Current State → outcome:**

| Current State | Closes Allocation? | contact_flag after |
|---|---|---|
| Escalate | **Yes → escalated** | `mql_rejected` → goes to Escalation Queue |
| Attempt Again / Rescheduled / Respondent / Interested | No | `mql_in_progress` |
| Snapshot Sent / Snapshot Confirmed / Dream Snapshot Confirmed | No | `mql_in_progress` |
| Meeting Requested / Allocate Again 3 months | No | `mql_in_progress` |
| **Meeting Scheduled** | **No** — allocation stays open | **`meeting_in_progress`** |
| Meeting Held | No | `meeting_in_progress` |
| Solution Sent | No | `meeting_in_progress` |
| **Solution Picked / Picked Solution** | **Yes → qualified** | **`mql_qualified`** ✅ |
| Not interested / Do not Disturb / Reffered / Irrelevant | Yes → rejected | `mql_rejected` |
| *(FU30 reached without close)* | Yes → stalled | unchanged |

---

### Shared Meetings Sheet

One sheet shared by all agents and campaigns. ID stored in `.env` as `MEETING_SHEET_ID`.

**Tab: `Meetings`** (18 columns, A–R)

| Cols | Who fills | Content |
|---|---|---|
| A–L | `mql_sync.py` | Meeting ID · Company · Person · Phone · Email · MQL Agent · Campaign · Scheduled Date · MQL FU# · BD Remark · BD Snapshot Link · Last MQL Remark |
| M–Q | MQL Agent | Meeting Date · Duration (min) · Problems Identified · Solution Proposed · Solution Link |
| R | `meeting_sync.py` | Sync Status — `✓ Synced DD/MM HH:MM` |

**Tab: `Meeting Context`** (auto-created by `mql_sync.py`)
Wide format — one row per meeting. Columns: `Meeting ID | Unique ID | Company | Person | Campaign | MQL Agent | Meeting Scheduled Date | Shared Story Date | Shared Story Recording Link | Shared Story Transcript | FU1 Date | FU1 Recording Link | FU1 Transcript | ... | FU30 Date | FU30 Recording Link | FU30 Transcript`
Used as a research reference — shows the full BD story + every MQL FU side by side.

**Tab: `Meeting Context Normalized`** (auto-created by `mql_sync.py`)
Long format — one row per FU per meeting. Columns: `Meeting ID | Unique ID | Company | Person | Campaign | MQL Agent | Meeting Scheduled Date | Stage | FU Number | Date | Recording Link | Transcript`
Used for analysis / reporting on the full call timeline.

---

## 6. Scripts Reference

### BD scripts

| Script | When | What it does |
|---|---|---|
| `scripts/bd_story_import.py` | One-time | Imports historical BD story data from `data/mql_t_d.csv`. Dry-run by default (`--apply` to commit). Auto-creates missing agents, handles reallocation when owner changes. Idempotent. |
| `scripts/call_actions_sync.py` | ~1:00 AM | Reads BD agent sheets → inserts `call_actions` → updates `contact_flag` + `company_flag` → marks phones invalid → closes allocations. |
| `scripts/allocation_engine.py` | ~2:00 AM | Flags stalled contacts → resets leave → writes New Contact + FU1–FU5 to agent sheets → auto-creates Instructions tab. |

### MQL scripts

| Script | When | What it does |
|---|---|---|
| `scripts/mql_team_import.py` | One-time | Full MQL historical import pipeline. Parses wide MQL team CSV files → normalizes to one row per FU → imports to DB → backfills MQL Google Sheets. See `docs/MQL_IMPORT_DETAILED.md`. |
| `scripts/mql_pro_sheet_backfill.py` | On-demand | Writes normalized FU history back to MQL agent sheets. Used internally by `mql_team_import.py`; can be re-run per agent with `--agent-id`. |
| `scripts/mql_sync.py` | ~1:00 AM | Reads MQL agent sheets → inserts `mql_call_attempts` → updates `contact_flag` → on "Meeting Scheduled": creates `meetings` DB row + appends to Meetings tab + upserts Meeting Context and Meeting Context Normalized tabs. |
| `scripts/meeting_sync.py` | ~1:30 AM | Reads Meetings tab → updates `meetings` table → auto-fills FU(n+1) in agent's MQL sheet with "Meeting Held" + conclusion. |
| `scripts/mql_allocation_engine.py` | ~2:30 AM | Queries `shared_story`/`snapshot_sent` contacts → inserts `mql_allocations` + `mql_analysis` → writes contact info to MQL sheets → moves FU15+ contacts to FU16-30 tab → auto-creates Instructions tab. Supports `--contact-ids` for manual/exact allocation. |

### Utility scripts

| Script | Purpose |
|---|---|
| `scripts/backup_db.py` | Full `pg_dump` backup to `backups/` folder. Keeps the N most recent. |
| `scripts/backup_db_incremental.py` | Weekly incremental backup — appends only new/changed rows to per-table CSV files with watermarks. Works with local and managed PostgreSQL. Scheduled via `setup_backup_incremental.bat`. |
| `scripts/marketing_sheet_export.py` | Exports MQL contacts to a Google Sheet for marketing/newsletter campaigns. Three tabs: All MQLs, Interested MQLs, Rejected MQLs. |
| `scripts/sheet_values_config.py` | Centralized dropdown values and normalization maps for all sheet columns. Update here once to keep sheet UI and sync parsing aligned. |
| `scripts/create_admin_user.py` | Create / reset / deactivate dashboard users. |
| `scripts/ai_query_shell.py` | CLI natural-language SQL (Gemini / Claude). |

---

## 7. Dashboard Pages

| Page | File | Stage | Role |
|---|---|---|---|
| Home | `0_Home.py` | BD + MQL | All |
| Pipeline | `1_Pipeline.py` | BD + MQL | All |
| Contacts | `2_Contacts.py` | BD | All |
| Agents | `3_Agents.py` | BD | Admin |
| Allocation | `4_Allocation.py` | BD | Admin |
| Alloc Manager | `5_Allocation_Manager.py` | BD | Admin |
| Dashboard | `6_Dashboard.py` | BD | All |
| Upload | `7_Upload.py` | BD | Admin |
| AI Query | `8_AI_Query.py` | BD + MQL | All |
| Reallocation | `9_Reallocation.py` | BD | Admin |
| MQL Allocation | `10_MQL_Allocation.py` | MQL | Admin |
| MQL Manager | `11_MQL_Manager.py` | MQL | Admin |
| MQL Dashboard | `12_MQL_Dashboard.py` | MQL | All |

**Roles:** `admin` = full access. `user` = read-only (Home, Pipeline, Contacts, Dashboard, AI Query, MQL Dashboard).

### Notable dashboard features by page

**`10_MQL_Allocation.py`**
- Escalation alert banner — warns if any escalated contacts are pending reassignment.
- Pool health metrics (Shared Story / Snapshot Sent / Unallocated / In Progress / Qualified / Rejected).
- Unallocated contacts table with category filter.
- **Manual allocation** — multiselect exact contacts from the filtered table → allocate to a specific agent with preview + confirm. Uses `contact_ids` parameter in `run_mql_allocation()`.
- Standard allocation (by count + optional category filter) with dry run / live run.
- MQL sync (sheets → DB) with date filter, dry run / live run.
- Meeting sync (meetings sheet → DB + write-back to MQL sheets), dry run / live run.
- Delete test data by date.

**`11_MQL_Manager.py`**
- Allocation summary (active / qualified / rejected / stalled / **escalated** / reallocated per agent).
- **Escalation queue** — lists all escalated contacts waiting for admin reassignment, grouped by escalating agent. Shows remark that triggered the escalation.
- **Manual reassignment** — select an escalated contact + target MQL agent → creates new `mql_allocations` row + `mql_analysis` with merged context (BD + last MQL) + writes contact row directly to target agent's `MQL FU 1-15` sheet.
- Browse allocations with filters (status / agent / date range).
- Delete allocations (single or bulk by date, with cascade).
- Close / reallocate / SQL Ready marking.

**`9_Reallocation.py`**
- Filter-based re-allocation campaigns: pick `call_status` + `current_state` → preview matching pool → save snapshot → distribute across BD agents.
- Only contacts not in terminal flags and without active allocation are eligible.

---

## 8. Dashboard Utilities (`dashboard/utils/`)

Shared helper modules imported by all dashboard pages.

| Module | What it provides |
|---|---|
| `db.py` | PostgreSQL connection pool for Streamlit. `get_conn()`, `query_df()`, `execute()`, `execute_many()`. All queries logged with full traceback on error. |
| `auth.py` | Cookie-based session auth. `is_logged_in()`, `get_user()`, `get_role()`, `is_admin()`, `log_action()`. Wraps `streamlit-authenticator`. |
| `engine.py` | Wrappers to trigger BD allocation and sync scripts from the dashboard. `run_allocation()`, `run_sync()`, `_run_command()`. |
| `mql_engine.py` | Wrappers to trigger MQL allocation and sync scripts from the dashboard. `run_mql_allocation()`, `run_mql_sync()`. |
| `sheets.py` | Google Sheets utilities. `get_gspread_client()`, `delete_contact_from_sheet()`. Resolves `credentials.json` via env var or project root. |
| `errors.py` | Shared error handling. `log_and_show()` logs full traceback to console and renders `st.error` with collapsible details in UI. `log_and_warn()` for non-fatal warnings. |
| `campaign.py` | Campaign selector widget. `get_campaign()` renders a sidebar selector and returns the active campaign string used by all pages. |

---

## 9. Escalation Flow

When an MQL agent logs `Current State = Escalate`:

1. `mql_sync.py` closes the allocation with `close_reason = 'escalated'` and `outcome = 'back_to_bd'`.
2. Contact appears in the **Escalation Queue** on `11_MQL_Manager.py`.
3. Admin selects the contact + target MQL agent → clicks **Reassign escalated contact**.
4. System:
   - Creates a new `mql_allocations` row for the target agent.
   - Creates `mql_analysis` row with merged BD + last MQL context in the remark field.
   - Sets `contact_flag = 'mql_in_progress'`.
   - Writes the contact row to the target agent's `MQL FU 1-15` sheet tab (starts fresh from FU1).

---

## 10. Daily Run Order

```
01:00 AM  call_actions_sync.py       BD: sheet outcomes → DB
01:00 AM  mql_sync.py                MQL: FU outcomes → DB + meeting rows/sheets
01:30 AM  meeting_sync.py            Meetings: sheet → DB + Meeting Held written back to MQL sheets
02:00 AM  allocation_engine.py       BD: today's contacts → BD agent sheets
02:30 AM  mql_allocation_engine.py   MQL: today's contacts → MQL agent sheets
03:00 PM  Agents start their shift
```

Manual run commands:
```bash
python scripts/call_actions_sync.py --campaign consulting
python scripts/mql_sync.py --campaign consulting
python scripts/meeting_sync.py --campaign consulting
python scripts/allocation_engine.py --campaign consulting
python scripts/mql_allocation_engine.py --all-agents --campaign consulting
```

---

## 11. Historical Data — One-Time Import Flow

See `docs/HISTORICAL_IMPORT_FLOW.md` for the full operational sequence. Summary:

```
Step 1 — Reset DB data (if rebuilding from scratch)
    psql -f sql/reset_data.sql

Step 2 — Import BD story history
    python scripts/bd_story_import.py --file data/mql_t_d.csv   # dry-run
    python scripts/bd_story_import.py --file data/mql_t_d.csv --apply

Step 3 — Add campaign support
    psql -f sql/03_add_campaign.sql

Step 4 — Import MQL team history
    python scripts/mql_team_import.py --audit-only
    python scripts/mql_team_import.py --dry-run
    python scripts/mql_team_import.py --apply --rewrite-bd-remark-all

Step 5 — Run MQL allocation engine
    python scripts/mql_allocation_engine.py --all-agents --campaign consulting
```

---

## 12. Key Business Rules

| Rule | Detail |
|---|---|
| `contacts.campaign` | Always `'consulting'`. Never set to the CSV Category value. Used as the business filter in all queries. |
| `contacts.bd_category` | Original CSV Category column (e.g. `'clinics ds1'`). Stored for segmentation. |
| MQL snapshot gate | `SNAPSHOT_GATE_DATE = 2026-04-01`. Before this date: `shared_story` and `snapshot_sent` are both MQL-eligible. On/after this date: only `snapshot_sent` is eligible. Internal rule — not exposed to agents. |
| MQL exclusivity | One MQL agent per contact at a time. An agent cannot be re-allocated a contact they have worked before. |
| Meeting track stays open | "Meeting Scheduled" does NOT close the MQL allocation. Same agent continues in same sheet through to Solution Picked. |
| SQL flag | `mql_qualified` is set only on "Solution Picked" or "Picked Solution" — never at any earlier stage. |
| Max BD attempts | FU5 is the last BD follow-up. Auto-closed after that. |
| Max MQL attempts | FU30 is the last MQL follow-up. Auto-closed as stalled. |
| Sync trigger | A row only syncs if the Timestamp column is filled. |
| Idempotent syncs | Re-running any sync skips rows already marked `✓ Synced`. |
| Phone uncertainty | Contacts with multiple phones get `phone_uncertain=TRUE`. MQL agent must confirm correct number and update col D. `mql_sync.py` then locks it as primary and marks others invalid. |
| bd_history allocations | Allocations imported via `mql_team_import.py` have `close_reason='bd_history'`. Excluded from all live metrics and KPI counts. |

---

## 13. Environment Variables (`.env`)

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crm_db
DB_USER=postgres
DB_PASSWORD=<your_password>

GOOGLE_CREDS_FILE=credentials.json

MEETING_SHEET_ID=<google_sheet_id_of_shared_meetings_sheet>
COOKIE_SECRET=<long_random_string_for_session_security>

AI_PROVIDER=gemini
GEMINI_API_KEY=<your_gemini_key>
ANTHROPIC_API_KEY=<your_anthropic_key>
```

---

## 14. Known Column Renames (March 2026)

| Old Header | New Header | Location |
|---|---|---|
| Lead Category | Call Status | BD sheets (New Contact + FU1–FU5) |
| Lead Category | MQL Category | MQL sheets (FU blocks col 0) |

Rows in agent sheets with old headers are skipped by sync until re-filled.

---

## 15. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| "Unable to load agents" | `team` column missing | `ALTER TABLE agents ADD COLUMN IF NOT EXISTS team VARCHAR(20) DEFAULT 'bd'` |
| MQL agents appearing in BD allocation | `team` not set to `'mql'` | `UPDATE agents SET team='mql' WHERE name='...'` |
| MQL sheet writes to row 1 | `next_row` calculation | Fixed: `next_row = max(2, filled + 2)` |
| Email blank in MQL sheet | `contact_emails` not joined | Fixed in `mql_allocation_engine.py` |
| Sync timestamp parse fail | Wrong date format | Accepts `DD/MM/YYYY` and `DD/MM/YYYY HH:MM:SS` |
| `cursor already closed` error | Reusing cursor after `with` block exits | Open new `with conn.cursor() as cur2:` block |
| `meeting_sync: Meetings tab not found` | First run before any meetings | Normal — tab auto-created by `mql_sync` on first "Meeting Scheduled" |
| `Meeting Context` tab not updating | `mql_sync` only writes on "Meeting Scheduled" detection | Re-run `mql_sync` after ensuring the FU row timestamp is filled |
| Save button shows nothing | `st.success` + `st.rerun` conflict | Fixed: replaced with `st.toast` + `st.rerun` |
| `phone_uncertain` column missing | Schema predates column | `ALTER TABLE mql_call_attempts ADD COLUMN IF NOT EXISTS phone_uncertain BOOLEAN NOT NULL DEFAULT FALSE` |
| `mql_migration.py`: agent not found | Name mismatch in CSV vs DB | Script auto-inserts as inactive MQL agent — verify name spelling |
| Login page loops on refresh | `COOKIE_SECRET` not set | Add `COOKIE_SECRET=...` to `.env` |
| `column "connection_rate" does not exist` | Stale view | Re-run `09_reporting_views.sql` |
| Escalation queue shows contacts already reassigned | Active allocation exists | Normal — escalation queue filters these out automatically |
| Manual allocation writes wrong row | `next_row` from sheet count | Fixed: uses `max(2, filled + 2)` |
