-- ============================================================
-- Migration: Re-allocation Campaign Tables
-- Run in pgAdmin before using the Reallocation page.
--
-- Tables added:
--   • reallocation_campaigns         — stores each campaign (name + filters)
--   • reallocation_campaign_contacts — stores the filtered contact list
--
-- Flow:
--   1. Manager builds filters in the dashboard UI.
--   2. Preview runs the filter SQL — count shown, sample rows shown.
--   3. "Save campaign" inserts into reallocation_campaigns and
--      populates reallocation_campaign_contacts (snapshot of matching contacts).
--   4. "Allocate" distributes contacts across chosen agents via
--      contact_allocations, then marks campaign status = 'allocated'.
-- ============================================================


-- ── TABLE: reallocation_campaigns ────────────────────────────
-- One row per re-allocation campaign.
-- filters: JSONB snapshot of what the manager configured.
--   {
--     "campaign":              "consulting",
--     "call_statuses":         ["Connected"],
--     "lead_categories":       ["Warm", "Hot"],
--     "contact_flags_exclude": ["shared_story","snapshot_sent",
--                               "invalid_number","referred","language_issue"],
--     "date_from":             null,
--     "date_to":               null
--   }
CREATE TABLE IF NOT EXISTS reallocation_campaigns (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    filters         JSONB NOT NULL DEFAULT '{}',
    status          VARCHAR(20)  NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft', 'verified', 'allocated', 'closed')),
    contact_count   INT,             -- set when campaign is saved after preview
    allocated_at    TIMESTAMP,       -- set when contacts are actually allocated
    created_by      VARCHAR(100),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rcampaigns_status
    ON reallocation_campaigns(status);


-- ── TABLE: reallocation_campaign_contacts ────────────────────
-- Snapshot of which contacts are in this campaign.
-- agent_id + allocated_at set when the "Allocate" action runs.
CREATE TABLE IF NOT EXISTS reallocation_campaign_contacts (
    id              SERIAL PRIMARY KEY,
    campaign_id     INT  NOT NULL REFERENCES reallocation_campaigns(id)
                         ON DELETE CASCADE,
    contact_id      INT  NOT NULL REFERENCES contacts(id)
                         ON DELETE CASCADE,
    agent_id        INT  REFERENCES agents(id) ON DELETE SET NULL,
    allocated_at    TIMESTAMP,

    UNIQUE (campaign_id, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_rcc_campaign ON reallocation_campaign_contacts(campaign_id);
CREATE INDEX IF NOT EXISTS idx_rcc_contact  ON reallocation_campaign_contacts(contact_id);


-- ── Verify ───────────────────────────────────────────────────
SELECT 'reallocation_campaigns'         AS tbl, COUNT(*) FROM reallocation_campaigns
UNION ALL
SELECT 'reallocation_campaign_contacts' AS tbl, COUNT(*) FROM reallocation_campaign_contacts;
