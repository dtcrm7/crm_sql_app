-- ============================================================
-- B2B CRM — Phase 2A Schema (Final)
-- Run AFTER schema.sql (Phase 1 tables must exist first)
--
-- Tables:
--   • agents               (TABLE 5)
--   • contact_allocations  (TABLE 6)
--   • call_actions         (TABLE 7)
--
-- Plus:
--   • contact_flag + flag_updated_at added to contacts
--   • is_invalid added to contact_phones
--
-- Allocation logic (enforced by ETL, not DB):
--   120 rows/agent/day = 120 phone numbers
--   Fresh  = 60 rows (50%, fixed floor)
--   Follow-up = up to 60 rows (50% cap)
--     if follow-ups >= 60  → take 60
--     if follow-ups 36–59  → take all, fill rest fresh
--     if follow-ups < 36   → take all, fill rest fresh
--
-- call_actions: one row per phone number per attempt
--   = one dialed call toward 120 KPI
--
-- Attempt structure per agent per contact:
--   attempt_number = 0  → New Contact (first ever call)
--   attempt_number = 1  → FU1
--   attempt_number = 2  → FU2
--   attempt_number = 3  → FU3
--   attempt_number = 4  → FU4
--   attempt_number = 5  → FU5 (max, then idle)
-- ============================================================


-- ── STEP 0: Extend Phase 1 tables ────────────────────────────

-- Add contact_flag to contacts
-- Updated by ETL after every call_action inserted
-- Priority: invalid_number > referred > language_issue
--         > shared_story > snapshot_sent
--         > not_interested > dnd > attempt_3_months
--         > needs_followup > in_progress > fresh
ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS contact_flag VARCHAR(20)
        NOT NULL DEFAULT 'fresh'
        CHECK (contact_flag IN (
            'fresh',             -- never called
            'in_progress',       -- being worked
            'needs_followup',    -- has attempts but stalled > 7 days
            'shared_story',      -- ✅ success milestone
            'snapshot_sent',     -- ✅ dream snapshot sent
            'not_interested',    -- ⏳ retry after 2 months
            'dnd',               -- ⏳ retry after 3 months
            'attempt_3_months',  -- ⏳ all 6 attempts exhausted
            'invalid_number',    -- 🔒 permanent block
            'referred',          -- 🔒 permanent block
            'language_issue'     -- 🔒 permanent block
        ));

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS flag_updated_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_contacts_flag
    ON contacts(contact_flag);


-- Add is_invalid to contact_phones
-- Set TRUE when agent logs 'Invalid Number' for that specific number
-- ETL skips invalid numbers when building agent sheet
ALTER TABLE contact_phones
    ADD COLUMN IF NOT EXISTS is_invalid BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE contact_phones
    ADD COLUMN IF NOT EXISTS invalidated_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_phones_invalid
    ON contact_phones(is_invalid)
    WHERE is_invalid = TRUE;


-- Add company_flag to companies table
-- Derived from the flags of all contacts at that company.
-- Highest-priority contact flag across the company wins.
-- Updated by call_actions_sync after each sync run.
--
-- Priority (same order as contact_flag):
--   invalid_number  → all contacts at company are invalid
--   referred        → referred out at company level
--   shared_story    → at least one contact reached story stage
--   snapshot_sent   → at least one snapshot sent
--   not_interested  → no positive signals, rejections only
--   dnd             → everyone said DND
--   in_progress     → at least one contact being worked
--   fresh           → no contacts have been called yet
ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS company_flag VARCHAR(20) NOT NULL DEFAULT 'fresh'
        CHECK (company_flag IN (
            'fresh',
            'in_progress',
            'not_interested',
            'dnd',
            'snapshot_sent',
            'shared_story',
            'referred',
            'invalid_number'
        ));

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS flag_updated_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_companies_flag
    ON companies(company_flag);


-- ── TABLE 5: agents ───────────────────────────────────────────
-- One row per BD agent.
-- is_on_leave: manager flips TRUE on leave day, ETL resets FALSE next morning.
-- kpi_dialed: daily target phone numbers (e.g. 120).
-- kpi_connected: daily target connected calls (varies by shift).
-- fresh_pct: % of kpi_dialed that must be fresh contacts (default 50%).
-- followup_pct: % of kpi_dialed that can be follow-ups (default 50%).
--   Engine calculates: fresh_count = round(kpi_dialed * fresh_pct / 100)
--                      followup_cap = round(kpi_dialed * followup_pct / 100)
--   fresh_pct + followup_pct must = 100 (CHECK constraint).
--   Changing these per agent allows different split rules per BD person.
CREATE TABLE IF NOT EXISTS agents (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    email           VARCHAR(255),
    phone           VARCHAR(20),
    joining_date    DATE,
    shift_name      VARCHAR(50),        -- e.g. 'Afternoon', '3pm-9pm'
    shift_start     TIME,
    shift_end       TIME,
    kpi_dialed      INT NOT NULL DEFAULT 120,   -- daily target: phone numbers dialed
    kpi_connected   INT,                         -- daily target: connected calls
    fresh_pct       NUMERIC(5,2) NOT NULL DEFAULT 50.00
                    CHECK (fresh_pct BETWEEN 0 AND 100),
    followup_pct    NUMERIC(5,2) NOT NULL DEFAULT 50.00
                    CHECK (followup_pct BETWEEN 0 AND 100),
    is_on_leave     BOOLEAN NOT NULL DEFAULT FALSE,
    status          VARCHAR(20) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive')),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Ensure the two percentages always add up to 100
    CHECK (fresh_pct + followup_pct = 100)
);

CREATE INDEX IF NOT EXISTS idx_agents_status   ON agents(status);
CREATE INDEX IF NOT EXISTS idx_agents_leave    ON agents(is_on_leave);

-- ── ALTER for existing installs: add fresh_pct + followup_pct ─
-- Run these if agents table already exists without these columns:
ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS fresh_pct    NUMERIC(5,2) NOT NULL DEFAULT 50.00;
ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS followup_pct NUMERIC(5,2) NOT NULL DEFAULT 50.00;

-- ── ALTER: add sheet_id column ────────────────────────────────
-- Stores the Google Spreadsheet ID for each agent.
-- Replaces the hardcoded AGENT_SHEET_IDS dict in scripts.
-- Get from sheet URL: https://docs.google.com/spreadsheets/d/SHEET_ID/edit
ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS sheet_id VARCHAR(200);


-- ── TABLE 6: agent_leave_log ──────────────────────────────────
-- Permanent history of leave days per agent.
-- Populated by ETL whenever is_on_leave is set TRUE.
-- is_on_leave on agents = today's status (resets daily)
-- agent_leave_log = full history for reporting
CREATE TABLE IF NOT EXISTS agent_leave_log (
    id           SERIAL PRIMARY KEY,
    agent_id     INT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    leave_date   DATE NOT NULL,
    hours_taken  NUMERIC(3,1) NOT NULL DEFAULT 8.0
                 CHECK (hours_taken > 0 AND hours_taken <= 8),
    leave_type   VARCHAR(20) DEFAULT 'planned'
                 CHECK (leave_type IN ('planned', 'sick', 'personal')),
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (agent_id, leave_date)
);

CREATE INDEX IF NOT EXISTS idx_leave_log_agent ON agent_leave_log(agent_id);
CREATE INDEX IF NOT EXISTS idx_leave_log_date  ON agent_leave_log(leave_date);


-- ── TABLE 7: contact_allocations ─────────────────────────────
-- One ACTIVE row per contact at any time (closed_at IS NULL = active).
-- Tracks which agent owns this contact and how many attempts made.
--
-- attempt_number: 0 = New Contact, 1-5 = FU1-FU5
--   New agent always restarts at 0.
--   Incremented by ETL when all phones logged 'Did not connect'.
--
-- filled_by: agent who logged the final outcome.
--   ETL uses this to exclude them from future allocations of contact.
--   Not enforced at DB level — enforced in ETL logic.
--
-- close_reason:
--   completed    → real outcome logged (Connected, Shared Story, etc.)
--   reallocated  → CRM lab manually moved to different agent
--   max_attempts → all 6 attempts exhausted, contact now idle
CREATE TABLE IF NOT EXISTS contact_allocations (
    id               SERIAL PRIMARY KEY,
    contact_id       INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    agent_id         INT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    allocated_date   DATE NOT NULL DEFAULT CURRENT_DATE,
    attempt_number   SMALLINT NOT NULL DEFAULT 0
                     CHECK (attempt_number BETWEEN 0 AND 5),
    allocated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    closed_at        TIMESTAMP,
    close_reason     VARCHAR(20)
                     CHECK (close_reason IN (
                         'completed',
                         'reallocated',
                         'max_attempts'
                     )),
    filled_by        INT REFERENCES agents(id) ON DELETE SET NULL
);

-- One active allocation per contact at any time
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_allocation
    ON contact_allocations(contact_id)
    WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_alloc_contact  ON contact_allocations(contact_id);
CREATE INDEX IF NOT EXISTS idx_alloc_agent    ON contact_allocations(agent_id);
CREATE INDEX IF NOT EXISTS idx_alloc_date     ON contact_allocations(allocated_date);
CREATE INDEX IF NOT EXISTS idx_alloc_active   ON contact_allocations(closed_at)
    WHERE closed_at IS NULL;


-- ── TABLE 8: call_actions ─────────────────────────────────────
-- One row per phone number per attempt.
-- = one dialed call toward agent's 120 KPI.
--
-- phone_number_used: which specific number was dialed.
--   If marked Invalid Number → ETL sets contact_phones.is_invalid = TRUE.
--   Invalid numbers never appear in future agent sheets.
--
-- attempt_number: matches contact_allocations.attempt_number.
--   0 = New Contact, 1 = FU1 ... 5 = FU5.
--
-- lead_category: retained for historical migration data only (pre-2026).
--   BD sheets no longer write this column — it is always NULL for new rows.
--   MQL tables have their own lead_category column (mql_contacts).
--
-- current_state: not constrained — evolves over time.
--   Old: Shared Story, Attempt Again, Rescheduled...
--   New: Call Later, Story Shared, Snapshot sent, S5/Budget...
--
-- recording_link: mandatory when current_state = 'Shared Story'.
--   Enforced by ETL validation, not DB constraint.
CREATE TABLE IF NOT EXISTS call_actions (
    id                   SERIAL PRIMARY KEY,
    contact_id           INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    allocation_id        INT REFERENCES contact_allocations(id) ON DELETE SET NULL,
    agent_id             INT NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    phone_number_used    VARCHAR(20) NOT NULL,
    attempt_number       SMALLINT NOT NULL DEFAULT 0
                         CHECK (attempt_number BETWEEN 0 AND 5),
    call_status          VARCHAR(30) NOT NULL
                         CHECK (call_status IN (
                             'Connected',
                             'Did not connect',
                             'Do not Disturb',
                             'Call back later',
                             'Invalid Number',
                             'Referred'
                         )),
    lead_category        VARCHAR(20)
                         CHECK (lead_category IN (
                             'Cold', 'Warm', 'Hot', 'Dont Call'
                         ) OR lead_category IS NULL),
    current_state        VARCHAR(60),
    call_duration        INT,                -- seconds (HH:MM:SS converted), NULL if not logged
    remark               TEXT,
    recording_link       TEXT,              -- mandatory when Shared Story
    transcript_link      TEXT,              -- Google Drive link
    dream_snapshot_link  TEXT,              -- Google Drive link
    called_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_actions_contact    ON call_actions(contact_id);
CREATE INDEX IF NOT EXISTS idx_actions_agent      ON call_actions(agent_id);
CREATE INDEX IF NOT EXISTS idx_actions_called_at  ON call_actions(called_at);
CREATE INDEX IF NOT EXISTS idx_actions_status     ON call_actions(call_status);
CREATE INDEX IF NOT EXISTS idx_actions_attempt    ON call_actions(attempt_number);


-- ══════════════════════════════════════════════════════════════
-- HELPER VIEWS
-- ══════════════════════════════════════════════════════════════

-- agent_availability_today
-- Run every morning before allocation.
-- Shows each active agent with their calculated fresh + FU targets.
CREATE OR REPLACE VIEW agent_availability_today AS
SELECT
    id                                                      AS agent_id,
    name,
    shift_name,
    shift_start,
    shift_end,
    kpi_dialed,
    kpi_connected,
    fresh_pct,
    followup_pct,
    is_on_leave,
    CASE WHEN is_on_leave THEN 0 ELSE kpi_dialed END        AS numbers_to_assign,
    -- Calculated targets (what the engine will actually use)
    CASE WHEN is_on_leave THEN 0
         ELSE ROUND(kpi_dialed * fresh_pct    / 100)
    END                                                     AS fresh_target,
    CASE WHEN is_on_leave THEN 0
         ELSE ROUND(kpi_dialed * followup_pct / 100)
    END                                                     AS followup_target
FROM agents
WHERE status = 'active';


-- contacts_allocation_ready
-- Phone-number-level view — one row per valid phone number per contact.
-- This is what ETL reads to build the 120-row agent sheet.
-- Excludes:
--   • permanently blocked contacts
--   • timed blocks still within window
--   • contacts already actively allocated
--   • invalid phone numbers
CREATE OR REPLACE VIEW contacts_allocation_ready AS
SELECT
    c.id            AS contact_id,
    c.contact_flag,
    c.first_name,
    c.last_name,
    co.name         AS company_name,
    co.raw_address,
    p.id            AS phone_id,
    p.phone_number,
    p.rank          AS phone_rank
FROM contacts c
JOIN contact_phones p  ON p.contact_id = c.id
LEFT JOIN companies co ON co.id = c.company_id
WHERE
    -- exclude permanent blocks
    c.contact_flag NOT IN ('invalid_number', 'referred', 'language_issue')
    -- exclude active timed blocks
    AND NOT (
        c.contact_flag = 'not_interested'
        AND c.flag_updated_at > NOW() - INTERVAL '2 months'
    )
    AND NOT (
        c.contact_flag = 'dnd'
        AND c.flag_updated_at > NOW() - INTERVAL '3 months'
    )
    AND NOT (
        c.contact_flag = 'attempt_3_months'
        AND c.flag_updated_at > NOW() - INTERVAL '3 months'
    )
    -- exclude contacts already actively allocated
    AND NOT EXISTS (
        SELECT 1 FROM contact_allocations a
        WHERE a.contact_id = c.id
          AND a.closed_at IS NULL
    )
    -- exclude invalid phone numbers
    AND p.is_invalid = FALSE;


-- agent_daily_kpi
-- KPI tracking per agent per day.
-- dialed_count: rows in call_actions (one per phone number = one dialed call).
-- connected_count: where call_status = 'Connected'.
CREATE OR REPLACE VIEW agent_daily_kpi AS
SELECT
    ca.agent_id,
    a.name                              AS agent_name,
    DATE(ca.called_at)                  AS call_date,
    COUNT(*)                            AS dialed_count,
    COUNT(*) FILTER (
        WHERE ca.call_status = 'Connected'
    )                                   AS connected_count,
    a.kpi_dialed                        AS target_dialed,
    a.kpi_connected                     AS target_connected,
    ROUND(COUNT(*) * 100.0
        / NULLIF(a.kpi_dialed, 0), 1)   AS dialed_pct
FROM call_actions ca
JOIN agents a ON a.id = ca.agent_id
GROUP BY ca.agent_id, a.name, DATE(ca.called_at),
         a.kpi_dialed, a.kpi_connected;


-- contacts_needing_followup
-- Contacts stalled for > 7 days with no final outcome.
-- ETL reads this daily to flag needs_followup and
-- prioritise these in the follow-up 60 allocation slots.
CREATE OR REPLACE VIEW contacts_needing_followup AS
SELECT
    c.id            AS contact_id,
    c.first_name,
    c.last_name,
    c.contact_flag,
    ca.attempt_number,
    ca.agent_id,
    MAX(a.called_at) AS last_attempt_at,
    NOW() - MAX(a.called_at) AS days_since_last_attempt
FROM contacts c
JOIN contact_allocations ca ON ca.contact_id = c.id AND ca.closed_at IS NULL
JOIN call_actions a         ON a.contact_id  = c.id
WHERE
    c.contact_flag  = 'in_progress'
    AND ca.attempt_number < 5
    AND a.call_status NOT IN ('Connected', 'Referred')
GROUP BY c.id, c.first_name, c.last_name, c.contact_flag,
         ca.attempt_number, ca.agent_id
HAVING MAX(a.called_at) < NOW() - INTERVAL '7 days';
