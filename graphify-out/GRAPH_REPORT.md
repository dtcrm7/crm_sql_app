# Graph Report - .  (2026-04-11)

## Corpus Check
- 44 files · ~74,469 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 659 nodes · 983 edges · 51 communities detected
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 28 edges (avg confidence: 0.89)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `migrate()` - 23 edges
2. `PostgreSQL DB Schema (crm_db)` - 20 edges
3. `process_fu_block()` - 19 edges
4. `process_row()` - 16 edges
5. `main()` - 15 edges
6. `clean_text()` - 15 edges
7. `run_allocation()` - 12 edges
8. `get_or_create_contact()` - 12 edges
9. `backfill_mql_sheets()` - 12 edges
10. `apply_db_import()` - 12 edges

## Surprising Connections (you probably didn't know these)
- `Managed PostgreSQL Options (Supabase, Neon, Railway)` --references--> `PostgreSQL Database (crm_db)`  [INFERRED]
  docs/SETUP_GUIDE.md → README.md
- `PyArrow ArrowTypeError (int in object column)` --references--> `Streamlit Dashboard (13 pages)`  [INFERRED]
  streamlitlog.txt → README.md
- `Streamlit Community Cloud Deployment Option` --references--> `Streamlit Dashboard (13 pages)`  [INFERRED]
  docs/SETUP_GUIDE.md → README.md
- `Streamlit use_container_width Deprecation Warning` --references--> `Streamlit Dashboard (13 pages)`  [INFERRED]
  streamlitlog.txt → README.md
- `B2B CRM SQL System` --references--> `gspread (Python package)`  [EXTRACTED]
  README.md → requirements.txt

## Hyperedges (group relationships)
- **Nightly Sync Pipeline (BD + MQL + Meetings + Allocation)** — readme_call_actions_sync, readme_mql_sync, readme_meeting_sync, readme_allocation_engine, readme_mql_allocation_engine, readme_postgresql, readme_google_sheets [EXTRACTED 1.00]
- **Three-Tier CRM Architecture (Sheets + Python + DB + Dashboard)** — readme_google_sheets, readme_postgresql, readme_streamlit_dashboard, req_gspread, req_psycopg2, req_streamlit [EXTRACTED 1.00]
- **MQL Pipeline Database Tables** — arch_mql_allocations_table, arch_mql_analysis_table, arch_mql_call_attempts_table, arch_meetings_table, arch_meeting_call_attempts_table [EXTRACTED 1.00]
- **Full Historical Import Sequence** — hist_reset_data_sql, hist_bd_story_import, arch_campaigns_table, hist_mql_team_import, mql_import_mql_pro_sheet_backfill [EXTRACTED 1.00]
- **Core Database Tables (Phase 1)** — arch_companies_table, arch_contacts_table, arch_contact_emails_table, arch_contact_phones_table [EXTRACTED 1.00]
- **BD Pipeline Database Tables (Phase 2A)** — arch_agents_table, arch_agent_sheets_table, arch_contact_allocations_table, arch_call_actions_table [EXTRACTED 1.00]
- **MQL Team Import Four Stages** — mql_import_source_files, mql_import_normalized_output, arch_mql_allocations_table, arch_mql_agent_sheets [EXTRACTED 1.00]

## Communities

### Community 0 - "Database Schema & CRM Tables"
Cohesion: 0.04
Nodes (73): action_log table, agent_sheets table, agents table, BD Agent Sheets (Google Sheets structure), call_actions table, campaigns table, companies table, contact_allocations table (+65 more)

### Community 1 - "MQL Sync Pipeline"
Cohesion: 0.07
Nodes (48): append_to_meeting_sheet(), attempt_already_synced(), _build_remark_with_alt(), build_unique_id(), col_index_to_letter(), derive_call_status(), _format_remark_with_alt(), fu_col_offset() (+40 more)

### Community 2 - "BD Allocation Engine"
Cohesion: 0.07
Nodes (45): _apply_bd_dropdowns_for_tab(), _apply_dropdown_validations(), _build_list_validation_rule(), build_sheet_row(), build_unique_id(), ensure_instructions_tab(), ensure_sheet_tabs(), flag_stale_contacts() (+37 more)

### Community 3 - "MQL Team Import"
Cohesion: 0.1
Nodes (37): apply_db_import(), build_reject_rows(), classify_fu_col(), create_allocation(), derive_call_status(), detect_fu_blocks(), ensure_close_reason_constraint(), ensure_contact_flag_constraint() (+29 more)

### Community 4 - "BD Story Historical Import"
Cohesion: 0.13
Nodes (35): add_contact_emails(), add_contact_phones(), build_header_index(), clean_text(), close_historical_allocations(), create_allocation(), enrich_contact(), ensure_allocation() (+27 more)

### Community 5 - "MQL Sheet Backfill"
Cohesion: 0.08
Nodes (35): backfill_mql_sheets(), build_contact_row_from_csv(), build_contact_row_from_db(), build_fu_row(), build_tab_headers(), call_with_retry(), ensure_mql_tabs(), find_allocation() (+27 more)

### Community 6 - "Call Actions Sync"
Cohesion: 0.09
Nodes (31): action_already_synced(), best_flag(), derive_flag(), get_col(), get_contact(), get_open_allocation(), get_phone_id(), get_sheets_client() (+23 more)

### Community 7 - "MQL Allocation Engine"
Cohesion: 0.09
Nodes (33): _append_alt_phone_note(), _apply_mql_dropdowns(), build_contact_row(), _build_list_validation_rule(), build_tab_headers(), build_unique_id(), ensure_mql_instructions_tab(), ensure_mql_tabs() (+25 more)

### Community 8 - "AI Query Shell CLI"
Cohesion: 0.18
Nodes (24): ask_ai(), c(), err(), execute_sql(), fetch_schema(), get_db_connection(), load_config(), main() (+16 more)

### Community 9 - "Dashboard Auth & RBAC"
Cohesion: 0.14
Nodes (23): _fetch_user(), _get_controller(), get_role(), get_user(), init_cookie_auth(), is_admin(), is_logged_in(), log_action() (+15 more)

### Community 10 - "Marketing Sheet Export"
Cohesion: 0.13
Nodes (22): build_unique_id(), ensure_tabs(), fetch_active_mql_contacts(), fetch_interested_mql_contacts(), fetch_rejected_mql_contacts(), format_row(), get_connection(), get_gspread_client() (+14 more)

### Community 11 - "MQL Dashboard Page"
Cohesion: 0.15
Nodes (12): _apply_attempt_filters(), _build_connection_efficiency(), _canonical_filter_value(), _compute_agent_summary(), _flag_call_status(), _flag_current_state(), _non_blank_values(), pages/12_MQL_Dashboard.py — MQL Performance Dashboard  Designed to mirror the (+4 more)

### Community 12 - "Unified Performance Dashboard"
Cohesion: 0.14
Nodes (17): _avg_duration(), _compute_agent_summary(), _compute_daily_agg(), _flag_call_status(), _flag_current_state(), _load_agents(), _load_daily_raw(), _load_period_raw() (+9 more)

### Community 13 - "Incremental DB Backup"
Cohesion: 0.15
Nodes (18): append_backup(), backup_table(), fetch_new_rows(), get_connection(), get_table_columns(), main(), backup_db_incremental.py ======================== Creates weekly incremental b, Read the last watermark from a backup file.     Watermark is the last line afte (+10 more)

### Community 14 - "AI Query Dashboard Page"
Cohesion: 0.12
Nodes (8): 8_AI_Query.py — AI Query page for the CRM Dashboard Ask plain-English questions, Remove -- line comments and /* block comments */ so they can't hide DDL., Returns (success, error_message). error_message is '' on success., _save_and_reload(), save_config(), _strip_sql_comments(), test_key(), validate_sql()

### Community 15 - "Database Connection Helpers"
Cohesion: 0.18
Nodes (15): execute(), execute_many(), get_conn(), _get_db_config(), _load_env(), _persistent_conn(), query_df(), Database helpers for Streamlit dashboard pages.  All functions log the full Py (+7 more)

### Community 16 - "Pipeline Funnel Dashboard"
Cohesion: 0.13
Nodes (11): _load_bd_stats(), _load_combined_activity(), _load_contact_stats(), _load_fu_breakdown(), _load_mql_stats(), pages/1_Pipeline.py — End-to-End Pipeline Funnel CEO / Board view: every stage, Per-agent BD rows. TODAY totals derived in pandas by summing the column., Per-agent MQL rows. TODAY totals derived in pandas. (+3 more)

### Community 17 - "Meeting Sync"
Cohesion: 0.27
Nodes (13): _build_mql_tab_headers(), col_index_to_letter(), _ensure_meetings_tab(), _ensure_mql_tab(), fu_col_offset(), get_sheets_client(), parse_date(), meeting_sync.py =============== Reads the shared meetings Google Sheet and syn (+5 more)

### Community 18 - "CSV Upload Page"
Cohesion: 0.22
Nodes (5): parse_source_id(), pages/7_Upload.py — Master Data Upload  Upload a cleaned CSV of new contacts i, Extract numeric ID: 'RR | 62078057' → '62078057', or return raw., Split comma/semicolon-separated cell into up to max_items non-empty strings., split_multi()

### Community 19 - "Script Runner Engine"
Cohesion: 0.32
Nodes (7): Wrappers to run allocation and sync scripts from the dashboard., Run command and return (success, combined_output)., Run allocation_engine.py and return success flag with console output., Run call_actions_sync.py and return success flag with console output., run_allocation(), _run_command(), run_sync()

### Community 20 - "Google Sheets Utilities"
Cohesion: 0.32
Nodes (6): delete_contact_from_sheet(), _find_creds_file(), get_gspread_client(), Google Sheets utilities for the Streamlit dashboard.  Used by the Allocation M, Locate credentials.json — env var first, then project root/credentials.json., Search all 6 tabs of an agent's sheet for a row whose column-A value     matche

### Community 21 - "User Admin CLI"
Cohesion: 0.46
Nodes (7): create_user(), deactivate_user(), get_conn(), hash_password(), list_users(), create_admin_user.py ==================== CLI tool to create or reset dashboar, reset_password()

### Community 22 - "App Navigation Controller"
Cohesion: 0.29
Nodes (1): app.py — Navigation Controller Run with: streamlit run app.py

### Community 23 - "MQL Manager Page"
Cohesion: 0.33
Nodes (3): _build_tab_headers(), _ensure_mql_tab(), pages/11_MQL_Manager.py — MQL Allocation Manager - Summary: active/closed alloc

### Community 24 - "Reallocation Campaigns"
Cohesion: 0.33
Nodes (4): build_filter_sql(), 9_Reallocation.py — Filter-Based Re-Allocation Campaigns  Workflow:   1. Buil, Build the SELECT SQL + params dict from a filters dict.     Returns (sql, param, run_preview()

### Community 25 - "Full DB Backup"
Cohesion: 0.38
Nodes (6): backup_db.py ============ Creates a timestamped pg_dump backup of crm_db. Kee, Confirm pg_dump is available. Returns its path., Delete oldest backup files, keeping only `keep` most recent., rotate_backups(), run_backup(), verify_pg_dump()

### Community 26 - "Shared Error Handling"
Cohesion: 0.33
Nodes (5): log_and_show(), log_and_warn(), Shared error-handling utilities for the CRM Streamlit dashboard.  Usage in any, Log full traceback to console; show st.error + collapsible details in UI., Log full traceback to console; show st.warning + collapsible details in UI.

### Community 27 - "Business Rules & Rationale"
Cohesion: 0.33
Nodes (6): Key Business Rules, Idempotent Syncs Rule, Meeting Track Stays Open Rule, MQL Exclusivity Rule (one agent per contact), MQL Snapshot Gate Date (2026-04-01), SQL Flag (mql_qualified) Set Only on Solution Picked

### Community 28 - "MQL Script Wrappers"
Cohesion: 0.6
Nodes (4): Wrappers to run MQL allocation and sync scripts from the dashboard., _run_command(), run_mql_allocation(), run_mql_sync()

### Community 29 - "Campaign Selector Widget"
Cohesion: 0.5
Nodes (3): get_campaign(), Campaign selector utility for the Streamlit dashboard.  Every page calls `get_, Render the campaign selector in the sidebar.     Returns the currently selected

### Community 30 - "AI & LLM Integration Layer"
Cohesion: 0.5
Nodes (4): ai_query_shell.py (CLI NL-to-SQL), AI Query Layer (Gemini / Claude), anthropic (Python package), google-generativeai (Python package)

### Community 31 - "One-Time Import Scripts"
Cohesion: 0.67
Nodes (3): etl.py (CSV ingestion script), mql_migration.py (one-time MQL import), Rationale: Never Use etl.py for MQL Data

### Community 32 - "Singleton Utilities A"
Cohesion: 1.0
Nodes (1): pages/0_Home.py — CRM Home BD view  : today's dialling stats, agent cards, pool

### Community 33 - "Singleton Utilities B"
Cohesion: 1.0
Nodes (1): pages/10_MQL_Allocation.py — MQL Allocation - View MQL-eligible contacts (share

### Community 34 - "Singleton Utilities C"
Cohesion: 1.0
Nodes (1): pages/2_Contacts.py — Contact Search & History - Multi-filter search: text, fla

### Community 35 - "Singleton Utilities D"
Cohesion: 1.0
Nodes (1): pages/3_Agents.py — Agent Management Tabs:   1. Today      — presence toggle f

### Community 36 - "Singleton Utilities E"
Cohesion: 1.0
Nodes (1): pages/4_Allocation.py — Daily Allocation - Mark agents on leave - Per-agent: s

### Community 37 - "Singleton Utilities F"
Cohesion: 1.0
Nodes (1): pages/5_Allocation_Manager.py — Allocation Visibility & Management  Sections:

### Community 38 - "Singleton Utilities G"
Cohesion: 1.0
Nodes (1): Dashboard utility package.

### Community 39 - "Isolated Node A"
Cohesion: 1.0
Nodes (1): Contact Lifecycle Pipeline

### Community 40 - "Isolated Node B"
Cohesion: 1.0
Nodes (1): streamlit (Python package)

### Community 41 - "Isolated Node C"
Cohesion: 1.0
Nodes (1): pandas (Python package)

### Community 42 - "Isolated Node D"
Cohesion: 1.0
Nodes (1): python-dotenv (Python package)

### Community 43 - "Isolated Node E"
Cohesion: 1.0
Nodes (1): extra-streamlit-components (Python package)

### Community 44 - "Isolated Node F"
Cohesion: 1.0
Nodes (1): tabulate (Python package)

### Community 45 - "Isolated Node G"
Cohesion: 1.0
Nodes (1): streamlit (dashboard dependency)

### Community 46 - "Isolated Node H"
Cohesion: 1.0
Nodes (1): psycopg2-binary (dashboard dependency)

### Community 47 - "Isolated Node I"
Cohesion: 1.0
Nodes (1): gspread (dashboard dependency)

### Community 48 - "Isolated Node J"
Cohesion: 1.0
Nodes (1): migrate_old_data.py (historical BD import)

### Community 49 - "Isolated Node K"
Cohesion: 1.0
Nodes (1): backup_db.py

### Community 50 - "Isolated Node L"
Cohesion: 1.0
Nodes (1): create_admin_user.py

## Knowledge Gaps
- **222 isolated node(s):** `app.py — Navigation Controller Run with: streamlit run app.py`, `pages/0_Home.py — CRM Home BD view  : today's dialling stats, agent cards, pool`, `pages/10_MQL_Allocation.py — MQL Allocation - View MQL-eligible contacts (share`, `pages/11_MQL_Manager.py — MQL Allocation Manager - Summary: active/closed alloc`, `pages/12_MQL_Dashboard.py — MQL Performance Dashboard  Designed to mirror the` (+217 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Singleton Utilities A`** (2 nodes): `0_Home.py`, `pages/0_Home.py — CRM Home BD view  : today's dialling stats, agent cards, pool`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities B`** (2 nodes): `10_MQL_Allocation.py`, `pages/10_MQL_Allocation.py — MQL Allocation - View MQL-eligible contacts (share`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities C`** (2 nodes): `2_Contacts.py`, `pages/2_Contacts.py — Contact Search & History - Multi-filter search: text, fla`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities D`** (2 nodes): `3_Agents.py`, `pages/3_Agents.py — Agent Management Tabs:   1. Today      — presence toggle f`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities E`** (2 nodes): `4_Allocation.py`, `pages/4_Allocation.py — Daily Allocation - Mark agents on leave - Per-agent: s`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities F`** (2 nodes): `5_Allocation_Manager.py`, `pages/5_Allocation_Manager.py — Allocation Visibility & Management  Sections:`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Singleton Utilities G`** (2 nodes): `__init__.py`, `Dashboard utility package.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node A`** (1 nodes): `Contact Lifecycle Pipeline`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node B`** (1 nodes): `streamlit (Python package)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node C`** (1 nodes): `pandas (Python package)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node D`** (1 nodes): `python-dotenv (Python package)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node E`** (1 nodes): `extra-streamlit-components (Python package)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node F`** (1 nodes): `tabulate (Python package)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node G`** (1 nodes): `streamlit (dashboard dependency)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node H`** (1 nodes): `psycopg2-binary (dashboard dependency)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node I`** (1 nodes): `gspread (dashboard dependency)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node J`** (1 nodes): `migrate_old_data.py (historical BD import)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node K`** (1 nodes): `backup_db.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Isolated Node L`** (1 nodes): `create_admin_user.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What connects `app.py — Navigation Controller Run with: streamlit run app.py`, `pages/0_Home.py — CRM Home BD view  : today's dialling stats, agent cards, pool`, `pages/10_MQL_Allocation.py — MQL Allocation - View MQL-eligible contacts (share` to the rest of the system?**
  _222 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Database Schema & CRM Tables` be split into smaller, more focused modules?**
  _Cohesion score 0.04 - nodes in this community are weakly interconnected._
- **Should `MQL Sync Pipeline` be split into smaller, more focused modules?**
  _Cohesion score 0.07 - nodes in this community are weakly interconnected._
- **Should `BD Allocation Engine` be split into smaller, more focused modules?**
  _Cohesion score 0.07 - nodes in this community are weakly interconnected._
- **Should `MQL Team Import` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._
- **Should `BD Story Historical Import` be split into smaller, more focused modules?**
  _Cohesion score 0.13 - nodes in this community are weakly interconnected._
- **Should `MQL Sheet Backfill` be split into smaller, more focused modules?**
  _Cohesion score 0.08 - nodes in this community are weakly interconnected._