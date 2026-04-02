-- 06_rbac_auth.sql
-- Dashboard user auth + audit log
--
-- HOW TO RUN (pick one):
--   psql:    \i sql/06_rbac_auth.sql
--   pgAdmin: Open Query Tool → paste this file → Execute (F5)
--   CLI:     psql -U postgres -d crm_db -f sql/06_rbac_auth.sql
--
-- Safe to re-run — all statements use IF NOT EXISTS
-- ----------------------------------------------------------------

-- ── Dashboard users ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dashboard_users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(50)  UNIQUE NOT NULL,
    name          VARCHAR(100) NOT NULL,
    password_hash TEXT         NOT NULL,        -- bcrypt hash
    role          VARCHAR(20)  NOT NULL DEFAULT 'user'
                  CHECK (role IN ('admin', 'user')),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP    NOT NULL DEFAULT NOW(),
    last_login    TIMESTAMP
);

-- ── Action audit log ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS action_log (
    id         SERIAL PRIMARY KEY,
    username   VARCHAR(50)  NOT NULL,
    role       VARCHAR(20)  NOT NULL,
    action     VARCHAR(100) NOT NULL,   -- e.g. 'allocation_run', 'contact_delete'
    details    TEXT,                    -- JSON or free text
    logged_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_action_log_username ON action_log (username);
CREATE INDEX IF NOT EXISTS idx_action_log_logged_at ON action_log (logged_at DESC);

-- ── Verify ────────────────────────────────────────────────────
SELECT 'dashboard_users created' AS status
WHERE EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'dashboard_users'
);
SELECT 'action_log created' AS status
WHERE EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_name = 'action_log'
);
