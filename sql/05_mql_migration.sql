-- ============================================================
-- B2B CRM — Phase 2B: MQL Stage Migration
-- Run AFTER 02_schema_phase2a.sql
--
-- Changes:
--   1. Add team column to agents
--   2. Extend contacts.contact_flag CHECK constraint
--   3. Create mql_allocations table
--   4. Create mql_analysis table (outcome tracking)
--   5. Create mql_call_attempts table (per-FU call log)
--   6. Indexes
-- ============================================================

-- ── 1. agents.team ───────────────────────────────────────────
ALTER TABLE agents
    ADD COLUMN IF NOT EXISTS team VARCHAR(20) NOT NULL DEFAULT 'bd'
    CHECK (team IN ('bd', 'mql', 'sales'));

-- After running: set MQL agents manually:
--   UPDATE agents SET team = 'mql' WHERE name IN ('Agent A', 'Agent B');


-- ── 2. Extend contacts.contact_flag ──────────────────────────
-- Drop old constraint (name may vary — check with \d contacts if this fails)
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_contact_flag_check;

ALTER TABLE contacts ADD CONSTRAINT contacts_contact_flag_check
    CHECK (contact_flag IN (
        'fresh',
        'in_progress',
        'needs_followup',
        'shared_story',
        'snapshot_sent',
        'mql_in_progress',    -- NEW: MQL agent currently working this contact
        'mql_qualified',      -- NEW: Confirmed SQL — ready for sales team
        'mql_rejected',       -- NEW: Did not pass MQL — back to BD or out
        'not_interested',
        'dnd',
        'attempt_3_months',
        'invalid_number',
        'referred',
        'language_issue'
    ));


-- ── 3. mql_allocations ───────────────────────────────────────
-- Tracks which MQL agent is assigned which contact.
-- Separate from contact_allocations — different KPI + rules.
CREATE TABLE IF NOT EXISTS mql_allocations (
    id              SERIAL PRIMARY KEY,
    contact_id      INT NOT NULL REFERENCES contacts(id),
    agent_id        INT NOT NULL REFERENCES agents(id),
    campaign        VARCHAR(60) NOT NULL DEFAULT 'consulting',
    allocated_date  DATE NOT NULL DEFAULT CURRENT_DATE,
    allocated_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMP,
    close_reason    VARCHAR(40)
                    CHECK (close_reason IN (
                        'qualified',
                        'rejected',
                        'stalled',
                        'reallocated',
                        'escalated',
                        'bd_history',
                        'Not interested',
                        'Irrelevant',
                        'Do not Disturb',
                        'Reffered'
                    )),
    filled_by       INT REFERENCES agents(id)
);

-- Existing deployments: upgrade close_reason check safely (non-destructive)
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'mql_allocations'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%close_reason%'
    LOOP
        EXECUTE format('ALTER TABLE mql_allocations DROP CONSTRAINT IF EXISTS %I', r.conname);
    END LOOP;

    ALTER TABLE mql_allocations
        ALTER COLUMN close_reason TYPE VARCHAR(40);

    ALTER TABLE mql_allocations
        ADD CONSTRAINT mql_allocations_close_reason_check
        CHECK (close_reason IN (
            'qualified',
            'rejected',
            'stalled',
            'reallocated',
            'escalated',
            'bd_history',
            'Not interested',
            'Irrelevant',
            'Do not Disturb',
            'Reffered'
        ));
END $$;

-- One active MQL allocation per contact at a time
CREATE UNIQUE INDEX IF NOT EXISTS uq_mql_one_active_per_contact
    ON mql_allocations(contact_id)
    WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mql_alloc_agent   ON mql_allocations(agent_id);
CREATE INDEX IF NOT EXISTS idx_mql_alloc_date    ON mql_allocations(allocated_date);
CREATE INDEX IF NOT EXISTS idx_mql_alloc_contact ON mql_allocations(contact_id);
CREATE INDEX IF NOT EXISTS idx_mql_alloc_campaign ON mql_allocations(campaign);


-- ── 4. mql_analysis ──────────────────────────────────────────
-- One row per MQL contact allocation.
-- Pre-filled with BD context; outcome filled when agent completes work.
CREATE TABLE IF NOT EXISTS mql_analysis (
    id                  SERIAL PRIMARY KEY,
    contact_id          INT NOT NULL REFERENCES contacts(id),
    allocation_id       INT NOT NULL REFERENCES mql_allocations(id),
    agent_id            INT NOT NULL REFERENCES agents(id),

    -- Pre-filled from BD stage
    bd_agent_name       VARCHAR(100),
    bd_call_date        DATE,
    bd_current_state    VARCHAR(60),
    bd_remark           TEXT,
    bd_recording_link   TEXT,
    bd_snapshot_link    TEXT,

    -- Outcome (written by mql_sync.py when agent closes a contact)
    last_follow_up      INT,                -- highest FU number reached (1-30)
    lead_category       VARCHAR(20),        -- final lead category from last FU
    outcome             VARCHAR(20)
                        CHECK (outcome IN ('sql', 'back_to_bd', 'disqualified', 'pending')),
    outcome_reason      TEXT,
    outcome_date        TIMESTAMP,

    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mql_analysis_alloc   ON mql_analysis(allocation_id);
CREATE INDEX IF NOT EXISTS idx_mql_analysis_contact ON mql_analysis(contact_id);
CREATE INDEX IF NOT EXISTS idx_mql_analysis_agent   ON mql_analysis(agent_id);


-- ── 5. mql_call_attempts ─────────────────────────────────────
-- One row per follow-up attempt (FU1-FU30) per contact.
-- Written by mql_sync.py when reading MQL agent sheets.
CREATE TABLE IF NOT EXISTS mql_call_attempts (
    id                  SERIAL PRIMARY KEY,
    contact_id          INT NOT NULL REFERENCES contacts(id),
    allocation_id       INT NOT NULL REFERENCES mql_allocations(id),
    agent_id            INT NOT NULL REFERENCES agents(id),
    follow_up_number    INT NOT NULL CHECK (follow_up_number BETWEEN 0 AND 30),  -- 0 = First Call

    phone_number_used   VARCHAR(20),
    phone_uncertain     BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE = legacy multi-phone, team must confirm correct number
    call_status         VARCHAR(30),
    lead_category       VARCHAR(20),
    current_state       VARCHAR(60),
    call_duration       INT,               -- seconds
    remark              TEXT,
    recording_link      TEXT,
    transcript_link     TEXT,
    dream_snapshot_link TEXT,
    message_status      BOOLEAN DEFAULT FALSE,

    called_at           TIMESTAMP,
    synced_at           TIMESTAMP NOT NULL DEFAULT NOW()
);

-- For existing deployments where table was already created:
ALTER TABLE mql_call_attempts
    ADD COLUMN IF NOT EXISTS phone_uncertain     BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE mql_call_attempts
    ADD COLUMN IF NOT EXISTS dream_snapshot_link TEXT;
-- Widen follow_up_number to allow 0 (First Call):
ALTER TABLE mql_call_attempts
    DROP CONSTRAINT IF EXISTS mql_call_attempts_follow_up_number_check;
ALTER TABLE mql_call_attempts
    ADD  CONSTRAINT mql_call_attempts_follow_up_number_check
    CHECK (follow_up_number BETWEEN 0 AND 30);

-- Prevent double-syncing the same FU attempt
CREATE UNIQUE INDEX IF NOT EXISTS uq_mql_attempt
    ON mql_call_attempts(contact_id, agent_id, follow_up_number, called_at);

CREATE INDEX IF NOT EXISTS idx_mql_attempt_alloc   ON mql_call_attempts(allocation_id);
CREATE INDEX IF NOT EXISTS idx_mql_attempt_contact ON mql_call_attempts(contact_id);
CREATE INDEX IF NOT EXISTS idx_mql_attempt_agent   ON mql_call_attempts(agent_id);
CREATE INDEX IF NOT EXISTS idx_mql_attempt_fu      ON mql_call_attempts(follow_up_number);


-- ── 6. BD pool exclusion reminder ────────────────────────────
-- IMPORTANT: Update your BD allocation engine / contacts_allocation_ready view
-- to exclude MQL-locked contacts:
--
--   AND c.contact_flag NOT IN ('mql_in_progress', 'mql_qualified', 'mql_rejected')
--
-- This prevents BD agents from getting contacts that are already in MQL pipeline.
