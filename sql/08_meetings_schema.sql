-- ============================================================
-- B2B CRM — Phase 2C: Meetings & SQL Pipeline
-- Run AFTER 05_mql_migration.sql
--
-- ── TWO PATHS TO SQL ─────────────────────────────────────────
--
--   Path A — Direct solution in MQL calling sheet:
--     MQL FU1…FUn → "Solution Sent" → FU → "Solution Picked" → SQL
--     All tracked in mql_call_attempts. No meeting sheet involved.
--
--   Path B — Meeting track (separate meeting sheet, same agent):
--     MQL FU → "Meeting Scheduled"
--       → contact pushed to agent's MEETING sheet (by meeting_push script)
--       → Meeting held → conclusion + solution filled in meeting sheet
--       → FU1, FU2… in meeting sheet (solution sent, solution picked)
--       → "Solution Picked" → SQL
--     Tracked in meeting_call_attempts.
--
-- ── MEETING SHEET LAYOUT ─────────────────────────────────────
--
--   Contact info block (A–M, 13 cols) — pre-filled by push script:
--     A  Unique ID
--     B  Company Name
--     C  Person Name
--     D  Phone
--     E  Email
--     F  MQL Agent            (who scheduled the meeting)
--     G  Scheduled Date       (date "Meeting Scheduled" was logged in MQL sheet)
--     H  Campaign
--     I  BD Remark            (context from BD stage)
--     J  BD Snapshot Link
--     K  Last MQL Remark      (last remark before meeting was scheduled)
--     L  BD Recording Link
--     M  MQL FU # at Scheduling (which FU it was booked at)
--
--   Meeting section (N–R, 5 cols) — filled by agent during/after meeting:
--     N  Meeting Date         (actual date held)
--     O  Meeting Duration     (minutes)
--     P  Problems Identified  (what problems the contact shared)
--     Q  Solution Proposed    (which solution was presented)
--     R  Solution Link        (link to solution document)
--
--   FU blocks (col S onwards, 8 cols × max 10 FUs = 80 cols):
--     For each FU block (FU1 … FU10):
--       +0  Call Status
--       +1  Current State   (Solution Sent, Solution Picked, Rescheduled, etc.)
--       +2  Call Duration
--       +3  Remark
--       +4  Recording Link
--       +5  Timestamp
--       +6  Message Status  (was solution resent via message?)
--       +7  Sync Status     (written by meeting_sync.py: "✓ Synced DD/MM HH:MM")
--
--   Total cols = 13 + 5 + 10×8 = 98
--
-- ── CURRENT_STATE values in meeting sheet FU blocks ──────────
--   "Meeting Held"       — agent had the meeting, conclusion/solution filled
--   "Rescheduled"        — meeting moved to new date
--   "Solution Sent"      — solution document sent post-meeting
--   "Solution Picked"    — contact accepted → SQL (closes meeting allocation)
--   "Not interested"     — declined after seeing solution → rejected
--   "Follow-up"          — general follow-up, no decision yet
--   "Call back later"    — contact asked to be called later
-- ============================================================


-- ── 0. Constants for meeting_push / meeting_sync scripts ─────
-- MEETING_CONTACT_COLS = 13  (A–M)
-- MEETING_SECTION_COLS = 5   (N–R)
-- MEETING_HEADER_COLS  = 18  (A–R, before FU blocks)
-- MEETING_FU_BLOCK_SIZE = 8
-- MEETING_MAX_FU        = 10


-- ── 1. Extend contacts.contact_flag ──────────────────────────
ALTER TABLE contacts DROP CONSTRAINT IF EXISTS contacts_contact_flag_check;

ALTER TABLE contacts ADD CONSTRAINT contacts_contact_flag_check
    CHECK (contact_flag IN (
        -- BD flags
        'fresh', 'in_progress', 'needs_followup',
        'shared_story', 'snapshot_sent',
        'not_interested', 'dnd', 'attempt_3_months',
        'invalid_number', 'referred', 'language_issue',
        -- MQL flags
        'mql_in_progress',
        'meeting_in_progress',  -- meeting scheduled, now tracked in meeting sheet
        'mql_qualified',        -- SQL: solution picked (either path)
        'mql_rejected'
    ));


-- ── 2. Add sheet_type to agent_sheets ────────────────────────
-- Allows one agent to have both a calling sheet and a meeting sheet
-- for the same campaign.
--
-- sheet_type:
--   'calling'  — BD or MQL calling sheet (existing rows, default)
--   'meetings' — meeting sheet for MQL agents (new)
--
-- Old UNIQUE (agent_id, campaign) → new UNIQUE (agent_id, campaign, sheet_type)
ALTER TABLE agent_sheets
    ADD COLUMN IF NOT EXISTS sheet_type VARCHAR(20) NOT NULL DEFAULT 'calling'
    CHECK (sheet_type IN ('calling', 'meetings'));

-- Drop old unique constraint and add new one
ALTER TABLE agent_sheets
    DROP CONSTRAINT IF EXISTS agent_sheets_agent_id_campaign_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_sheets_type
    ON agent_sheets(agent_id, campaign, sheet_type);


-- ── 3. meetings (metadata) ────────────────────────────────────
-- One row per meeting.
-- Created by mql_sync when "Meeting Scheduled" appears in MQL sheet.
-- Updated by meeting_sync as later states arrive in meeting sheet.
CREATE TABLE IF NOT EXISTS meetings (
    id                  SERIAL PRIMARY KEY,
    contact_id          INT NOT NULL REFERENCES contacts(id),
    mql_allocation_id   INT REFERENCES mql_allocations(id),
    mql_agent_id        INT REFERENCES agents(id),
    campaign            VARCHAR(60) NOT NULL DEFAULT 'consulting',

    status              VARCHAR(20) NOT NULL DEFAULT 'scheduled'
                        CHECK (status IN (
                            'scheduled',       -- booked in MQL sheet, not yet pushed to meeting sheet
                            'pushed',          -- contact row written to meeting sheet
                            'held',            -- meeting happened (conclusion filled in sheet)
                            'solution_sent',   -- solution sent post-meeting
                            'solution_picked', -- solution accepted → SQL
                            'rescheduled',     -- meeting date moved
                            'lost'             -- declined at any stage
                        )),

    scheduled_date      DATE,
    scheduled_at_fu     SMALLINT,       -- MQL FU# where "Meeting Scheduled" was logged
    held_at             TIMESTAMPTZ,
    held_at_fu          SMALLINT,       -- meeting sheet FU# where "Meeting Held" logged
    conclusion          TEXT,           -- problems identified (from meeting sheet col P)
    solution_proposed   TEXT,           -- which solution (from meeting sheet col Q)
    solution_link       TEXT,           -- solution document (from meeting sheet col R)
    solution_sent_at    TIMESTAMPTZ,
    solution_picked_at  TIMESTAMPTZ,

    -- Row index in meeting sheet (so sync can mark ✓ Synced)
    sheet_row_index     INT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_meeting_per_mql_alloc
    ON meetings(mql_allocation_id)
    WHERE status != 'lost';

CREATE INDEX IF NOT EXISTS idx_meetings_contact   ON meetings(contact_id);
CREATE INDEX IF NOT EXISTS idx_meetings_agent     ON meetings(mql_agent_id);
CREATE INDEX IF NOT EXISTS idx_meetings_campaign  ON meetings(campaign);
CREATE INDEX IF NOT EXISTS idx_meetings_status    ON meetings(status);
CREATE INDEX IF NOT EXISTS idx_meetings_date      ON meetings(scheduled_date);


-- ── 4. meeting_call_attempts ──────────────────────────────────
-- One row per FU in the meeting sheet (FU1–FU10).
-- Written by meeting_sync.py when reading agent meeting sheets.
-- current_state values: see header comments above.
CREATE TABLE IF NOT EXISTS meeting_call_attempts (
    id                  SERIAL PRIMARY KEY,
    meeting_id          INT NOT NULL REFERENCES meetings(id),
    contact_id          INT NOT NULL REFERENCES contacts(id),
    agent_id            INT REFERENCES agents(id),
    follow_up_number    SMALLINT NOT NULL
                        CHECK (follow_up_number BETWEEN 1 AND 10),

    phone_number_used   VARCHAR(20),
    call_status         VARCHAR(30),
    current_state       VARCHAR(60),
    call_duration       INT,            -- seconds
    remark              TEXT,
    recording_link      TEXT,
    message_status      BOOLEAN NOT NULL DEFAULT FALSE,

    called_at           TIMESTAMPTZ NOT NULL,
    synced_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (meeting_id, follow_up_number, called_at)
);

CREATE INDEX IF NOT EXISTS idx_mtg_att_meeting  ON meeting_call_attempts(meeting_id);
CREATE INDEX IF NOT EXISTS idx_mtg_att_contact  ON meeting_call_attempts(contact_id);
CREATE INDEX IF NOT EXISTS idx_mtg_att_agent    ON meeting_call_attempts(agent_id);
CREATE INDEX IF NOT EXISTS idx_mtg_att_fu       ON meeting_call_attempts(follow_up_number);


-- ── 5. Update contacts_allocation_ready view ─────────────────
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
    c.contact_flag NOT IN (
        'invalid_number', 'referred', 'language_issue',
        'mql_in_progress', 'meeting_in_progress',
        'mql_qualified', 'mql_rejected'
    )
    AND NOT (c.contact_flag = 'not_interested'
             AND c.flag_updated_at > NOW() - INTERVAL '2 months')
    AND NOT (c.contact_flag = 'dnd'
             AND c.flag_updated_at > NOW() - INTERVAL '3 months')
    AND NOT (c.contact_flag = 'attempt_3_months'
             AND c.flag_updated_at > NOW() - INTERVAL '3 months')
    AND NOT EXISTS (
        SELECT 1 FROM contact_allocations a
        WHERE a.contact_id = c.id AND a.closed_at IS NULL
    )
    AND p.is_invalid = FALSE;


-- ── 6. After running, set meeting sheet IDs in Agents page ────
-- Agents page → Settings → select MQL agent
-- → "Meeting Sheet ID" section → paste Sheet ID → Save
--
-- Or directly in DB:
--   INSERT INTO agent_sheets (agent_id, campaign, sheet_type, sheet_id)
--   VALUES (<mql_agent_id>, 'consulting', 'meetings', '<SHEET_ID>')
--   ON CONFLICT (agent_id, campaign, sheet_type)
--   DO UPDATE SET sheet_id = EXCLUDED.sheet_id;
