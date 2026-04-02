-- ============================================================
-- Campaign v2 Migration
-- Run AFTER 02_schema_phase2a.sql and 03_add_campaign.sql
--
-- Changes:
--   1. Create agent_sheets table  (agent ↔ campaign ↔ sheet_id)
--   2. Migrate existing agents.sheet_id into agent_sheets
--   3. Add campaign column to contact_allocations
-- ============================================================


-- ── 1. agent_sheets ───────────────────────────────────────────
-- Replaces the single sheet_id column on agents.
-- One row per (agent, campaign) — agent can have a sheet per campaign.
CREATE TABLE IF NOT EXISTS agent_sheets (
    id          SERIAL PRIMARY KEY,
    agent_id    INT          NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    campaign    VARCHAR(50)  NOT NULL,
    sheet_id    VARCHAR(200),
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW(),

    UNIQUE (agent_id, campaign)
);

CREATE INDEX IF NOT EXISTS idx_agent_sheets_agent    ON agent_sheets(agent_id);
CREATE INDEX IF NOT EXISTS idx_agent_sheets_campaign ON agent_sheets(campaign);


-- ── 2. Migrate existing sheet_id from agents → agent_sheets ───
-- Only copies rows where sheet_id is set.
-- Safe to re-run — ON CONFLICT DO NOTHING.
INSERT INTO agent_sheets (agent_id, campaign, sheet_id)
SELECT id, 'consulting', sheet_id
FROM agents
WHERE sheet_id IS NOT NULL
ON CONFLICT (agent_id, campaign) DO NOTHING;


-- ── 3. campaign column on contact_allocations ─────────────────
-- Records which campaign this allocation belongs to.
-- Default 'consulting' covers all existing rows.
ALTER TABLE contact_allocations
    ADD COLUMN IF NOT EXISTS campaign VARCHAR(50) NOT NULL DEFAULT 'consulting';

CREATE INDEX IF NOT EXISTS idx_alloc_campaign
    ON contact_allocations(campaign);


-- ── Verification ──────────────────────────────────────────────
-- Run these after migration to confirm:
--
--   SELECT * FROM agent_sheets ORDER BY agent_id, campaign;
--
--   SELECT campaign, COUNT(*) FROM contact_allocations GROUP BY campaign;
