-- ============================================================
-- Migration: Add campaign support
-- Run once in pgAdmin before restarting the dashboard.
-- ============================================================

-- 1. Campaign reference table
CREATE TABLE IF NOT EXISTS campaigns (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(50)  UNIQUE NOT NULL,
    description TEXT,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Seed initial campaign
INSERT INTO campaigns (name, description)
VALUES ('consulting', 'B2B consulting outreach — primary campaign')
ON CONFLICT (name) DO NOTHING;

-- 2. Add campaign column to contacts
ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS campaign VARCHAR(50) NOT NULL DEFAULT 'consulting';

-- 3. Tag all existing contacts as the default campaign
UPDATE contacts SET campaign = 'consulting' WHERE campaign = 'consulting';

-- 4. Add FK constraint (optional but enforces referential integrity)
-- ALTER TABLE contacts
--     ADD CONSTRAINT fk_contacts_campaign
--     FOREIGN KEY (campaign) REFERENCES campaigns(name)
--     ON UPDATE CASCADE;
-- (Commented out by default — uncomment after seeding all campaign names you need)

-- 5. Index for performance (campaign appears in nearly every WHERE clause)
CREATE INDEX IF NOT EXISTS idx_contacts_campaign ON contacts(campaign);

-- 6. Verify
SELECT
    campaign,
    COUNT(*) AS contacts
FROM contacts
GROUP BY campaign
ORDER BY campaign;
