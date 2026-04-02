-- ============================================================
--  agent_sheets  —  per-agent, per-campaign Google Sheet IDs
--  Run ORDER: after 02_schema_phase2a.sql, before 08_meetings_schema.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS agent_sheets (
    id          SERIAL      PRIMARY KEY,
    agent_id    INT         NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    campaign    VARCHAR(60) NOT NULL,
    sheet_id    VARCHAR(200),
    sheet_type  VARCHAR(20) NOT NULL DEFAULT 'calling'
                    CHECK (sheet_type IN ('calling', 'meetings')),
    updated_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_agent_sheets_type UNIQUE (agent_id, campaign, sheet_type)
);

CREATE INDEX IF NOT EXISTS idx_agent_sheets_agent
    ON agent_sheets (agent_id);

COMMENT ON TABLE  agent_sheets                IS 'Google Sheet IDs used by each agent for each campaign';
COMMENT ON COLUMN agent_sheets.sheet_type     IS 'calling = BD/MQL calling sheet; meetings = shared meeting tracking sheet';
COMMENT ON COLUMN agent_sheets.campaign       IS 'Must match campaigns.name';
