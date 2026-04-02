-- ============================================================
-- Reset all CRM data (keeps table structure + agents intact)
-- Run in psql or pgAdmin before a fresh ETL test.
-- ============================================================

-- Dependent tables first (FK order)
TRUNCATE TABLE mql_call_attempts  RESTART IDENTITY CASCADE;
TRUNCATE TABLE mql_analysis       RESTART IDENTITY CASCADE;
TRUNCATE TABLE meetings           RESTART IDENTITY CASCADE;
TRUNCATE TABLE mql_allocations    RESTART IDENTITY CASCADE;
TRUNCATE TABLE call_actions       RESTART IDENTITY CASCADE;
TRUNCATE TABLE contact_allocations RESTART IDENTITY CASCADE;
TRUNCATE TABLE agent_leave_log    RESTART IDENTITY CASCADE;
TRUNCATE TABLE contact_emails     RESTART IDENTITY CASCADE;
TRUNCATE TABLE contact_phones     RESTART IDENTITY CASCADE;
TRUNCATE TABLE contacts           RESTART IDENTITY CASCADE;
TRUNCATE TABLE companies          RESTART IDENTITY CASCADE;

-- agents is intentionally NOT truncated — kept for re-use.
-- To also wipe agents, add:
--   TRUNCATE TABLE agents RESTART IDENTITY CASCADE;

SELECT 'Reset complete. Contacts, companies, phones, emails, allocations, meetings, call history all cleared.' AS status;
