# B2B CRM SQL

A two-stage B2B outreach CRM built on PostgreSQL, Google Sheets, and Streamlit. BD agents cold-call contacts from uploaded datasets; contacts who receive the company story are promoted to MQL, where MQL agents do deep follow-up (up to 30 attempts) to qualify contacts into SQL-ready. Meetings can be scheduled mid-pipeline.

Agents work in Google Sheets. Python scripts sync between sheets and the database nightly. A Streamlit dashboard gives managers full visibility and control.

## Architecture

```
Google Sheets (BD + MQL + Meetings)
        │  gspread API
        ▼
Python sync scripts (nightly cron)
        │  psycopg2
        ▼
PostgreSQL (crm_db)
        ▲
Streamlit Dashboard (13 pages)
```

## Tech Stack

- **Database:** PostgreSQL 15+
- **Dashboard:** Streamlit (multi-page app)
- **Sync layer:** Python scripts via Google Sheets API (gspread)
- **AI query:** Google Gemini / Anthropic Claude (natural-language SQL)
- **Auth:** bcrypt password hashing with cookie-based sessions
- **Charts:** Plotly, Altair

## Project Structure

```
├── dashboard/              Streamlit app
│   ├── app.py              Navigation controller
│   ├── pages/              13 dashboard pages (BD + MQL + Meetings)
│   └── utils/              DB, auth, engine, sheets helpers
├── scripts/                Nightly sync & utility scripts
│   ├── call_actions_sync.py    BD sheet → DB
│   ├── mql_sync.py             MQL sheet → DB + meeting creation
│   ├── meeting_sync.py         Meetings sheet → DB + write-back
│   ├── allocation_engine.py    BD contact allocation → sheets
│   ├── mql_allocation_engine.py MQL allocation → sheets
│   ├── backup_db.py            Database backup
│   ├── create_admin_user.py    User management
│   └── ai_query_shell.py       CLI natural-language SQL
├── sql/                    Schema migration files (run in order)
│   ├── 01_schema_phase1.sql    Core tables
│   ├── 02_schema_phase2a.sql   BD pipeline
│   ├── 03_add_campaign.sql     Campaigns
│   ├── 04_migration_campaign_v2.sql  Agent sheets
│   ├── 05_mql_migration.sql    MQL pipeline
│   ├── 06_rbac_auth.sql        Auth & audit
│   ├── 07_reallocation_campaigns.sql
│   ├── 08_meetings_schema.sql  Meetings pipeline
│   └── 09_reporting_views.sql  Views & indexes
├── docs/                   Detailed documentation
│   ├── ARCHITECTURE.md     Full system architecture
│   ├── SETUP_GUIDE.md      Local & cloud deployment guide
│   ├── HISTORICAL_IMPORT_FLOW.md
│   └── MQL_IMPORT_DETAILED.md
├── schema--data_model/     Data flow diagrams (XML)
├── data/                   Sample/import CSV files
├── backups/                Database backups
├── logs/                   Script run logs
├── credentials.json        Google service account (not in git)
├── .env                    Environment config (not in git)
└── requirements.txt        Python dependencies
```

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Google service account with Sheets API enabled

### Setup

```bash
# 1. Create virtual environment
python -m venv crm_etl
source crm_etl/bin/activate   # Linux/Mac
crm_etl\Scripts\activate      # Windows
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your DB credentials, Google Sheet IDs, and API keys

# 3. Create database and run schema
# In pgAdmin or psql:
#   CREATE DATABASE crm_db;
# Then run sql/01 through sql/09 in order

# 4. Create admin user
python scripts/create_admin_user.py --username admin --name "Your Name" --role admin

# 5. Run the dashboard
cd dashboard
streamlit run app.py
```

Open `http://localhost:8501` and log in.

### Environment Variables

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crm_db
DB_USER=postgres
DB_PASSWORD=<password>
GOOGLE_CREDS_FILE=credentials.json
MEETING_SHEET_ID=<google_sheet_id>
COOKIE_SECRET=<random_string>
AI_PROVIDER=gemini
GEMINI_API_KEY=<key>
ANTHROPIC_API_KEY=<key>
```

## Nightly Sync Schedule

| Time     | Script                     | Purpose                              |
|----------|----------------------------|--------------------------------------|
| 1:00 AM  | `call_actions_sync.py`     | BD sheet outcomes → DB               |
| 1:00 AM  | `mql_sync.py`              | MQL FU outcomes → DB + meetings      |
| 1:30 AM  | `meeting_sync.py`          | Meetings sheet → DB + write-back     |
| 2:00 AM  | `allocation_engine.py`     | Allocate contacts → BD agent sheets  |
| 2:30 AM  | `mql_allocation_engine.py` | Allocate contacts → MQL agent sheets |

## Contact Lifecycle

```
CSV Upload → fresh → BD calls (FU1-FU5) → shared_story / snapshot_sent
    → MQL follow-up (FU1-FU30) → Meeting Scheduled → Solution Picked → mql_qualified (SQL Ready)
```

## Dashboard Pages

| Page             | Stage    | Access |
|------------------|----------|--------|
| Home             | BD + MQL | All    |
| Pipeline         | BD + MQL | All    |
| Contacts         | BD       | All    |
| Agents           | BD       | Admin  |
| Allocation       | BD       | Admin  |
| Alloc Manager    | BD       | Admin  |
| Dashboard        | BD       | All    |
| Upload           | BD       | Admin  |
| AI Query         | BD + MQL | All    |
| Reallocation     | BD       | Admin  |
| MQL Allocation   | MQL      | Admin  |
| MQL Manager      | MQL      | Admin  |
| MQL Dashboard    | MQL      | All    |

## Documentation

See the [docs/](docs/) folder for detailed guides:

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — Full system architecture, schema details, business rules
- [SETUP_GUIDE.md](docs/SETUP_GUIDE.md) — Local and cloud deployment instructions
- [HISTORICAL_IMPORT_FLOW.md](docs/HISTORICAL_IMPORT_FLOW.md) — One-time historical data import
- [MQL_IMPORT_DETAILED.md](docs/MQL_IMPORT_DETAILED.md) — MQL migration details
