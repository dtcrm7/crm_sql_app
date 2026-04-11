# B2B CRM — Setup & Deployment Guide
> Covers: fresh local install, database creation, historical data import, and cloud deployment.
> Last updated: April 2026

---

## Part A — Local Setup (Windows, PostgreSQL + Streamlit)

Use this when setting up on a new development PC or a local Windows machine.

---

### A1. Prerequisites

Install these before anything else:

1. **Python 3.11+** — [python.org](https://python.org)
2. **PostgreSQL 15+** — [postgresql.org](https://postgresql.org/download/windows)
   - During install: set the `postgres` user password — you will need it.
   - Keep the default port: `5432`.
3. **pgAdmin 4** — installed with PostgreSQL, or separately at [pgadmin.org](https://pgadmin.org)
4. **Git** (optional, for cloning) — [git-scm.com](https://git-scm.com)

---

### A2. Get the Project

Copy the project folder to your machine (e.g. `D:\Projects\crm_sql_app`).

If using Git:
```bash
git clone <repo-url> "D:\Projects\crm_sql_app"
```

---

### A3. Create the Python Virtual Environment

```bash
cd "D:\Projects\crm_sql_app"
python -m venv crm_etl
crm_etl\Scripts\activate
pip install -r requirements.txt
```

For the dashboard only (lighter install):
```bash
pip install -r dashboard/requirements_dashboard.txt
```

---

### A4. Configure Environment Variables

Copy the example env file and fill it in:
```bash
copy .env.example .env
```

Open `.env` and set:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crm_db
DB_USER=postgres
DB_PASSWORD=<your_postgres_password>

GOOGLE_CREDS_FILE=credentials.json
MEETING_SHEET_ID=<your_shared_meetings_google_sheet_id>
COOKIE_SECRET=<generate_any_long_random_string>

AI_PROVIDER=gemini
GEMINI_API_KEY=<your_gemini_api_key>
ANTHROPIC_API_KEY=<your_anthropic_api_key>
```

Place the Google service account `credentials.json` file in the project root.

---

### A5. Create the Database

Open pgAdmin → connect to your local PostgreSQL server → open the Query Tool and run:

```sql
CREATE DATABASE crm_db;
```

Then switch the connection to `crm_db` before running the schema files below.

---

### A6. Run SQL Schema Files — In Exact Order

Open each file in pgAdmin (File → Open → navigate to `sql/`) and run with F5. Do not skip steps or swap order.

| Step | File | What it creates |
|---|---|---|
| 1 | `sql/01_schema_phase1.sql` | `companies`, `contacts`, `contact_emails`, `contact_phones` |
| 2 | `sql/02_schema_phase2a.sql` | `agents`, `contact_allocations`, `call_actions` + adds `contact_flag`, `is_invalid`, `company_flag` |
| 3 | `sql/03_add_campaign.sql` | `campaigns` table + adds `campaign` column to contacts, seeds `'consulting'` |
| 4 | `sql/04_migration_campaign_v2.sql` | `agent_sheets` table + migrates any existing `agents.sheet_id` values |
| 5 | `sql/05_mql_migration.sql` | `mql_allocations`, `mql_analysis`, `mql_call_attempts` + adds `agents.team` |
| 6 | `sql/06_rbac_auth.sql` | `dashboard_users`, `action_log` (dashboard login) |
| 7 | `sql/07_reallocation_campaigns.sql` | `reallocation_campaigns`, `reallocation_campaign_contacts` |
| 8 | `sql/08_meetings_schema.sql` | `meetings`, `meeting_call_attempts` + extends `agent_sheets` with `sheet_type` |
| 9 | `sql/09_reporting_views.sql` | All reporting views + performance indexes |
| 10 | `sql/10_add_bd_category.sql` | `contacts.bd_category` column + index |

All files use `IF NOT EXISTS` — safe to re-run without breaking anything.

**Patch files** (run if the column is missing on an existing install):

| File | When needed |
|---|---|
| `sql/add_escalated_close_reason.sql` | If `mql_allocations.close_reason` CHECK doesn't include `'escalated'` |
| `sql/agent_sheets_schema.sql` | If `agent_sheets` table is missing and step 4 didn't create it |

---

### A7. Post-Schema Setup

**Create your first admin user:**
```bash
python scripts/create_admin_user.py --username admin --name "Your Name" --role admin
```

**Set MQL agents** (run in pgAdmin after creating agent records):
```sql
UPDATE agents SET team = 'mql' WHERE name IN ('Agent Name 1', 'Agent Name 2');
```

**Set Google Sheet IDs for agents:**
Go to Dashboard → Agents → select each agent → paste their Google Sheet ID.
Get the Sheet ID from the sheet URL: `https://docs.google.com/spreadsheets/d/SHEET_ID/edit`

---

### A8. Upload Initial Contact Data

Via dashboard:
```
Dashboard → Upload → select your CSV → Upload
```

Required CSV columns: `source`, `source_id`, `first_name`, `company_name`, `phone1`
Optional: `last_name`, `designation`, `email1`, `phone2`, `phone3`, `website`, `industry`

---

### A9. Import Historical Data (One-Time)

If you have existing BD story and MQL call history, import it before running the allocation engine. See `docs/HISTORICAL_IMPORT_FLOW.md` for the full sequence.

Quick reference:
```bash
# BD story history
python scripts/bd_story_import.py --file data/mql_t_d.csv          # dry-run
python scripts/bd_story_import.py --file data/mql_t_d.csv --apply

# Then run: sql/03_add_campaign.sql in pgAdmin

# MQL team history
python scripts/mql_team_import.py --audit-only
python scripts/mql_team_import.py --dry-run
python scripts/mql_team_import.py --apply --rewrite-bd-remark-all
```

---

### A10. Run the Dashboard

```bash
cd dashboard
streamlit run app.py
```

Open in browser: `http://localhost:8501`
Log in with the admin credentials created in A7.

---

### A11. Set Up Automatic Backup (Optional)

**Full backup** (pg_dump, timestamped):
```bash
python scripts/backup_db.py
```

Scheduled full backup every 5 days at 3 AM — run once as Administrator:
```
scripts\setup_backup.bat
```

**Incremental backup** (appends only new/changed rows to CSV files with watermarks):
```bash
python scripts/backup_db_incremental.py
python scripts/backup_db_incremental.py --output D:/Backups/incremental
python scripts/backup_db_incremental.py --dry-run
python scripts/backup_db_incremental.py --tables contacts mql_allocations
```

Verify scheduled task registered:
```bash
schtasks /query /tn "CRM_DB_Backup" /fo LIST
```

---

## Part B — Cloud / Online PostgreSQL Deployment

Use this when moving to a hosted server — either a cloud VM (e.g. a free-tier DigitalOcean or AWS EC2 instance running PostgreSQL + Streamlit), or a managed PostgreSQL service (e.g. Supabase, Neon, Railway, Render) with the Python scripts running separately.

---

### B1. Choose Your Hosting Approach

**Option 1 — Full cloud VM (PostgreSQL + Streamlit on same machine)**
Best for: full control, no service limits, cron jobs for nightly scripts.
Free options: Oracle Cloud Always Free (2 ARM instances), Google Cloud e2-micro (always free in some regions).

**Option 2 — Managed PostgreSQL + local or separate script runner**
Best for: zero DB maintenance, easy scaling.
Free options: Supabase (500 MB free), Neon (3 GB free), Railway ($5/month credit free tier).
Scripts still run on your local machine or a separate lightweight VM.

---

### B2. Option 1 — Cloud VM Full Stack

#### B2.1 Provision the VM

- Create a VM with at least 1 GB RAM, 10 GB disk, Ubuntu 22.04.
- Open firewall ports: `22` (SSH), `5432` (PostgreSQL — restrict to your IP), `8501` (Streamlit).

#### B2.2 Install dependencies on the VM

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv postgresql postgresql-contrib git
```

#### B2.3 Configure PostgreSQL

```bash
sudo -u postgres psql
```
```sql
CREATE DATABASE crm_db;
CREATE USER crm_user WITH PASSWORD 'strong_password_here';
GRANT ALL PRIVILEGES ON DATABASE crm_db TO crm_user;
\q
```

To allow connections from your local machine (for pgAdmin):
```bash
sudo nano /etc/postgresql/15/main/pg_hba.conf
```
Add (replace `YOUR_IP`):
```
host  crm_db  crm_user  YOUR_IP/32  md5
```
```bash
sudo nano /etc/postgresql/15/main/postgresql.conf
```
Change:
```
listen_addresses = '*'
```
```bash
sudo systemctl restart postgresql
```

#### B2.4 Clone the project and configure

```bash
git clone <repo-url> /home/ubuntu/crm
cd /home/ubuntu/crm
python3 -m venv crm_etl
source crm_etl/bin/activate
pip install -r requirements.txt
```

Create `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=crm_db
DB_USER=crm_user
DB_PASSWORD=strong_password_here
GOOGLE_CREDS_FILE=/home/ubuntu/crm/credentials.json
MEETING_SHEET_ID=<your_sheet_id>
COOKIE_SECRET=<long_random_string>
GEMINI_API_KEY=<key>
ANTHROPIC_API_KEY=<key>
```

Upload `credentials.json` via SCP:
```bash
scp credentials.json ubuntu@YOUR_VM_IP:/home/ubuntu/crm/credentials.json
```

#### B2.5 Run the SQL schema (steps 1–10 from A6)

Connect pgAdmin to the remote server (use the VM's public IP, port 5432) and run the 10 SQL files in order.

#### B2.6 Run the Streamlit dashboard as a service

Create `/etc/systemd/system/crm-dashboard.service`:
```ini
[Unit]
Description=CRM Streamlit Dashboard
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/crm/dashboard
ExecStart=/home/ubuntu/crm/crm_etl/bin/streamlit run app.py --server.port 8501 --server.address 0.0.0.0
Restart=always
EnvironmentFile=/home/ubuntu/crm/.env

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable crm-dashboard
sudo systemctl start crm-dashboard
sudo systemctl status crm-dashboard
```

Dashboard is now accessible at `http://YOUR_VM_IP:8501`.

#### B2.7 Schedule nightly scripts with cron

```bash
crontab -e
```

Add:
```cron
# BD sync: 1:00 AM
0 1 * * * cd /home/ubuntu/crm && source crm_etl/bin/activate && python scripts/call_actions_sync.py --campaign consulting >> logs/cron_bd_sync.log 2>&1

# MQL sync: 1:00 AM (runs in parallel with BD sync)
0 1 * * * cd /home/ubuntu/crm && source crm_etl/bin/activate && python scripts/mql_sync.py --campaign consulting >> logs/cron_mql_sync.log 2>&1

# Meeting sync: 1:30 AM
30 1 * * * cd /home/ubuntu/crm && source crm_etl/bin/activate && python scripts/meeting_sync.py --campaign consulting >> logs/cron_meeting_sync.log 2>&1

# BD allocation: 2:00 AM
0 2 * * * cd /home/ubuntu/crm && source crm_etl/bin/activate && python scripts/allocation_engine.py --campaign consulting >> logs/cron_bd_alloc.log 2>&1

# MQL allocation: 2:30 AM
30 2 * * * cd /home/ubuntu/crm && source crm_etl/bin/activate && python scripts/mql_allocation_engine.py --all-agents --campaign consulting >> logs/cron_mql_alloc.log 2>&1
```

---

### B3. Option 2 — Managed PostgreSQL + Local Scripts

Use this if you want a hosted database but run scripts from your local machine or a lightweight server.

#### B3.1 Create a managed PostgreSQL instance

Recommended free options:

| Provider | Free Tier | Notes |
|---|---|---|
| **Supabase** | 500 MB, 2 projects | Has a full dashboard UI — easiest to start |
| **Neon** | 3 GB, serverless | Scales to zero when idle — cheapest |
| **Railway** | $5 credit/month | Predictable, easy setup |
| **Render** | 90 days free then $7/month | Good for full-stack later |

Steps (Supabase example):
1. Go to [supabase.com](https://supabase.com) → New project → set password → choose region closest to your team.
2. Go to Project Settings → Database → copy the **Connection String** (URI format).
3. Note the host, port (usually `5432` or `6543` for pooler), database name, user, and password.

#### B3.2 Update your `.env`

```
DB_HOST=<your_supabase_or_neon_host>
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=<your_managed_db_password>
```

For Supabase with connection pooling (recommended for scripts):
```
DB_HOST=<project>.pooler.supabase.com
DB_PORT=6543
```

#### B3.3 Run the SQL schema files

Use your managed provider's SQL editor (Supabase has one built in) or connect pgAdmin to the remote host.

Run the 10 schema files from A6 in the same order.

#### B3.4 Run scripts locally

Scripts connect to the remote DB via `.env` — no other changes needed:
```bash
python scripts/call_actions_sync.py --campaign consulting
python scripts/mql_allocation_engine.py --all-agents --campaign consulting
```

For nightly scheduling on Windows (local machine), use Task Scheduler:
```
scripts\setup_backup.bat  ← (adapt with cron-style commands for each script)
```
Or set up a lightweight cron on a free Oracle Cloud VM and point it to your managed DB.

#### B3.5 Deploy the Streamlit dashboard

Option A — **Streamlit Community Cloud** (free, easiest):
1. Push your project to a GitHub repo (keep `.env` out of git — use Streamlit's Secrets instead).
2. Go to [share.streamlit.io](https://share.streamlit.io) → New app → connect repo → set `dashboard/app.py` as entry point.
3. Add all `.env` values under **Advanced settings → Secrets** in TOML format:
   ```toml
   DB_HOST = "your_host"
   DB_PASSWORD = "your_password"
   COOKIE_SECRET = "your_secret"
   ```
4. Deploy. Dashboard is live at `https://your-app.streamlit.app`.

Option B — **Render** (free tier, more control):
1. Create a Render Web Service → connect GitHub repo.
2. Set Build Command: `pip install -r dashboard/requirements_dashboard.txt`
3. Set Start Command: `streamlit run dashboard/app.py --server.port $PORT --server.address 0.0.0.0`
4. Add environment variables in Render dashboard.

---

### B4. SSL / HTTPS for the Dashboard (Optional but Recommended)

If hosting on a cloud VM, put Nginx in front of Streamlit:

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
```

Create `/etc/nginx/sites-available/crm`:
```nginx
server {
    server_name your-domain.com;
    location / {
        proxy_pass http://localhost:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/crm /etc/nginx/sites-enabled/
sudo certbot --nginx -d your-domain.com
sudo systemctl reload nginx
```

---

### B5. Post-Deployment Checklist

- [ ] All 10 SQL schema files run successfully
- [ ] `dashboard_users` table exists and admin user created
- [ ] All MQL agents have `team='mql'` set in DB
- [ ] All agent Google Sheet IDs set in Dashboard → Agents
- [ ] `MEETING_SHEET_ID` set in `.env` / secrets
- [ ] `COOKIE_SECRET` set (required for login to work)
- [ ] `credentials.json` on server and path correct in `.env`
- [ ] Test `call_actions_sync.py --dry-run` — should connect and log agents
- [ ] Test `mql_allocation_engine.py --agent-id X --count 1 --dry-run` — should find eligible contacts
- [ ] Dashboard login works at your URL
- [ ] Nightly cron jobs registered and verified (check logs next morning)

---

## Part C — Upgrading an Existing Install

If you already have some tables and need to add missing pieces, all schema files are idempotent — safe to re-run.

| Missing component | Run |
|---|---|
| `meetings` / `meeting_call_attempts` tables | Step 8 then Step 9 |
| `agent_sheets.sheet_type` column | Step 8 (uses `ADD COLUMN IF NOT EXISTS`) |
| `reallocation_campaigns` | Step 7 |
| `dashboard_users` / `action_log` | Step 6 |
| `mql_*` tables | Step 5 |
| `agent_sheets` (whole table) | Step 4 |
| `campaigns` table | Step 3 |
| `contacts.bd_category` column | Step 10 |
| All views outdated | Step 9 (views use `CREATE OR REPLACE`) |
| `mql_allocations.close_reason` missing `escalated` | Run `sql/add_escalated_close_reason.sql` |
| `phone_uncertain` column missing | `ALTER TABLE mql_call_attempts ADD COLUMN IF NOT EXISTS phone_uncertain BOOLEAN NOT NULL DEFAULT FALSE` |

---

## Part D — Common Errors Reference

| Error | Cause | Fix |
|---|---|---|
| `relation "campaigns" does not exist` | Step 3 not run | Run `03_add_campaign.sql` |
| `relation "agent_sheets" does not exist` | Step 4 not run | Run `04_migration_campaign_v2.sql` |
| `column "sheet_type" does not exist` | Step 8 not run | Run `08_meetings_schema.sql` |
| `relation "meetings" does not exist` | Step 8 not run | Run `08_meetings_schema.sql` |
| `relation "mql_allocations" does not exist` | Step 5 not run | Run `05_mql_migration.sql` |
| `relation "dashboard_users" does not exist` | Step 6 not run | Run `06_rbac_auth.sql` |
| `column "team" does not exist` on agents | Step 5 not run | Run `05_mql_migration.sql` |
| `column "bd_category" does not exist` | Step 10 not run | Run `10_add_bd_category.sql` |
| `column "connection_rate" does not exist` | Old view cached | Re-run `09_reporting_views.sql` |
| Login page loops on refresh | `COOKIE_SECRET` not set | Add `COOKIE_SECRET=...` to `.env` |
| `MEETING_SHEET_ID not set` | `.env` missing the key | Add `MEETING_SHEET_ID=` to `.env` |
| `mql_call_attempts.phone_uncertain` missing | Schema predates column | `ALTER TABLE mql_call_attempts ADD COLUMN IF NOT EXISTS phone_uncertain BOOLEAN NOT NULL DEFAULT FALSE` |
| `mql_team_import.py`: agent not found | Name mismatch in CSV vs DB | Script auto-inserts as inactive MQL agent; verify spelling |
| `ws.update()` DeprecationWarning | gspread positional args | Use `ws.update(range_name="A1", values=[...])` |
| `cursor already closed` | Reusing cursor after `with` block | Open a new `with conn.cursor() as cur2:` block |
