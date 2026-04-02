-- ============================================================
-- B2B CRM — Reporting Views + Query Optimisation Indexes
-- Run once (or re-run anytime — all statements are idempotent).
-- Views auto-refresh on every query.
-- ============================================================

-- ── PERFORMANCE INDEXES ───────────────────────────────────────
-- These complement the base indexes in 01_schema_phase1.sql, 02_schema_phase2a.sql, and 05_mql_migration.sql.
-- Cover the columns most frequently hit by dashboard queries.

-- call_actions: composite for daily KPI dashboard (agent + date)
CREATE INDEX IF NOT EXISTS idx_actions_agent_date
    ON call_actions(agent_id, DATE(called_at));

-- call_actions: composite for funnel queries (contact + status)
CREATE INDEX IF NOT EXISTS idx_actions_contact_status
    ON call_actions(contact_id, call_status);

-- call_actions: current_state text searches (v_success_funnel uses ILIKE)
CREATE INDEX IF NOT EXISTS idx_actions_current_state
    ON call_actions(current_state);

-- contacts: flag + campaign composite (most allocation / pool queries filter on both)
CREATE INDEX IF NOT EXISTS idx_contacts_flag_campaign
    ON contacts(contact_flag, campaign);

-- contacts: flag_updated_at for stale-contact detection and MQL ordering
CREATE INDEX IF NOT EXISTS idx_contacts_flag_updated
    ON contacts(flag_updated_at);

-- mql_allocations: closed_at partial index — active allocations only (most common lookup)
CREATE INDEX IF NOT EXISTS idx_mql_alloc_active
    ON mql_allocations(contact_id, agent_id)
    WHERE closed_at IS NULL;

-- mql_call_attempts: FU number range queries
CREATE INDEX IF NOT EXISTS idx_mql_attempt_contact_fu
    ON mql_call_attempts(contact_id, follow_up_number);

-- mql_call_attempts: date-based syncing
CREATE INDEX IF NOT EXISTS idx_mql_attempt_called_at
    ON mql_call_attempts(called_at);

-- meetings: status filter used by v_meeting_pipeline + v_sql_pipeline
CREATE INDEX IF NOT EXISTS idx_meetings_status
    ON meetings(status);

CREATE INDEX IF NOT EXISTS idx_meetings_contact
    ON meetings(contact_id);

-- contact_allocations: active allocation lookup (already has partial idx but add composite)
CREATE INDEX IF NOT EXISTS idx_alloc_contact_active
    ON contact_allocations(contact_id, closed_at)
    WHERE closed_at IS NULL;


-- ── VIEW 1: Daily agent KPI summary ──────────────────────────
-- Per agent per day: dialled, connected, DND, invalid, referred, etc.
-- Uses idx_actions_agent_date for O(log n) date filter.
CREATE OR REPLACE VIEW v_agent_daily_kpi AS
SELECT
    a.id                                    AS agent_id,
    a.name                                  AS agent_name,
    DATE(ca.called_at)                      AS call_date,
    COUNT(*)                                AS total_dialled,
    a.kpi_dialed                            AS kpi_target,
    a.kpi_dialed + 5                        AS allocated_target,
    ROUND(COUNT(*) * 100.0 / NULLIF(a.kpi_dialed, 0), 1) AS kpi_pct,

    COUNT(*) FILTER (WHERE ca.call_status = 'Connected')         AS connected,
    COUNT(*) FILTER (WHERE ca.call_status = 'Did not connect')   AS did_not_connect,
    COUNT(*) FILTER (WHERE ca.call_status = 'Do not Disturb')    AS dnd,
    COUNT(*) FILTER (WHERE ca.call_status = 'Invalid Number')    AS invalid,
    COUNT(*) FILTER (WHERE ca.call_status = 'Referred')          AS referred,
    COUNT(*) FILTER (WHERE ca.call_status = 'Call back later')   AS call_back_later,

    ROUND(
        COUNT(*) FILTER (WHERE ca.call_status = 'Connected') * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                       AS connection_rate_pct,

    ROUND(AVG(ca.call_duration) FILTER (
        WHERE ca.call_duration IS NOT NULL AND ca.call_duration > 0
    ) / 60.0, 1)                            AS avg_call_duration_mins

FROM call_actions ca
JOIN agents a ON a.id = ca.agent_id
GROUP BY a.id, a.name, DATE(ca.called_at), a.kpi_dialed;


-- ── VIEW 2: Contact pipeline state ───────────────────────────
-- Current distribution of contact_flag across the whole pool.
-- Uses idx_contacts_flag for the GROUP BY.
CREATE OR REPLACE VIEW v_contact_pipeline AS
SELECT
    contact_flag,
    COUNT(*)                                AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct,
    COUNT(*) FILTER (WHERE
        contact_flag NOT IN ('invalid_number', 'referred', 'language_issue')
        AND NOT (contact_flag = 'not_interested'   AND flag_updated_at > NOW() - INTERVAL '2 months')
        AND NOT (contact_flag = 'dnd'              AND flag_updated_at > NOW() - INTERVAL '3 months')
        AND NOT (contact_flag = 'attempt_3_months' AND flag_updated_at > NOW() - INTERVAL '3 months')
    )                                       AS callable
FROM contacts
GROUP BY contact_flag
ORDER BY
    CASE contact_flag
        WHEN 'invalid_number'   THEN 10
        WHEN 'referred'         THEN 9
        WHEN 'language_issue'   THEN 8
        WHEN 'shared_story'     THEN 7
        WHEN 'snapshot_sent'    THEN 6
        WHEN 'not_interested'   THEN 5
        WHEN 'dnd'              THEN 4
        WHEN 'attempt_3_months' THEN 3
        WHEN 'needs_followup'   THEN 2
        WHEN 'in_progress'      THEN 1
        ELSE 0
    END DESC;


-- ── VIEW 3: Success funnel ────────────────────────────────────
-- Story/snapshot pipeline — counts per BD agent.
-- Replaces ILIKE '%shared story%' with direct equality on current_state
-- for index use (idx_actions_current_state).
CREATE OR REPLACE VIEW v_success_funnel AS
SELECT
    a.name                                  AS agent_name,
    COUNT(*) FILTER (
        WHERE ca.current_state IN ('Shared Story', 'Story Shared', 'shared story')
    )                                       AS shared_story_calls,
    COUNT(*) FILTER (
        WHERE ca.current_state IN ('Snapshot Sent', 'Dream Snapshot Sent', 'snapshot sent')
    )                                       AS snapshot_calls,
    COUNT(*) FILTER (
        WHERE ca.current_state = 'Interested'
    )                                       AS interested_calls,
    COUNT(*) FILTER (
        WHERE ca.call_status = 'Connected'
    )                                       AS connected_calls,
    COUNT(*) FILTER (
        WHERE ca.current_state = 'Interested'
    )                                       AS interested_leads
FROM call_actions ca
JOIN agents a ON a.id = ca.agent_id
GROUP BY a.name
ORDER BY shared_story_calls DESC;


-- ── VIEW 4: Latest remark per contact ────────────────────────
-- Most recent remark per contact. Uses LATERAL + idx_actions_contact.
CREATE OR REPLACE VIEW v_latest_remarks AS
SELECT
    c.id                                    AS contact_id,
    co.name                                 AS company,
    TRIM(COALESCE(c.first_name, '') || ' ' || COALESCE(c.last_name, '')) AS contact_name,
    c.contact_flag,
    ca.current_state                        AS last_status,
    ca.remark                               AS last_remark,
    ca.called_at                            AS last_called_at,
    a.name                                  AS last_agent,
    ca.attempt_number                       AS last_attempt
FROM contacts c
JOIN companies co ON co.id = c.company_id
JOIN LATERAL (
    SELECT *
    FROM call_actions
    WHERE contact_id = c.id
    ORDER BY called_at DESC
    LIMIT 1
) ca ON TRUE
JOIN agents a ON a.id = ca.agent_id
WHERE c.contact_flag NOT IN ('fresh');


-- ── VIEW 5: Agent weekly summary ─────────────────────────────
-- Rolling 7-day window. Uses idx_actions_agent_date for date range.
CREATE OR REPLACE VIEW v_agent_weekly AS
SELECT
    a.name                                  AS agent_name,
    COUNT(*)                                AS total_dialled,
    COUNT(*) FILTER (WHERE ca.call_status = 'Connected')    AS connected,
    COUNT(*) FILTER (
        WHERE ca.current_state IN ('Shared Story', 'Story Shared')
    )                                       AS shared_story,
    COUNT(*) FILTER (
        WHERE ca.current_state IN ('Snapshot Sent', 'Dream Snapshot Sent')
    )                                       AS snapshots,
    COUNT(*) FILTER (
        WHERE ca.current_state = 'Interested'
    )                                       AS interested_leads,
    ROUND(
        COUNT(*) FILTER (WHERE ca.call_status = 'Connected') * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                       AS connection_rate_pct,
    MIN(DATE(ca.called_at))                 AS from_date,
    MAX(DATE(ca.called_at))                 AS to_date
FROM call_actions ca
JOIN agents a ON a.id = ca.agent_id
WHERE ca.called_at >= NOW() - INTERVAL '7 days'
GROUP BY a.name
ORDER BY total_dialled DESC;


-- ── VIEW 6: Company pipeline ──────────────────────────────────
-- Company-level flag distribution. Grouped by company — no subqueries.
CREATE OR REPLACE VIEW v_company_pipeline AS
SELECT
    co.id                                   AS company_id,
    co.name                                 AS company_name,
    co.company_flag,
    COUNT(c.id)                             AS total_contacts,
    COUNT(c.id) FILTER (
        WHERE c.contact_flag = 'fresh'
    )                                       AS fresh_contacts,
    COUNT(c.id) FILTER (
        WHERE c.contact_flag = 'in_progress'
    )                                       AS in_progress,
    COUNT(c.id) FILTER (
        WHERE c.contact_flag IN ('shared_story', 'snapshot_sent')
    )                                       AS success_contacts,
    COUNT(c.id) FILTER (
        WHERE c.contact_flag IN ('invalid_number', 'referred', 'language_issue')
    )                                       AS permanently_blocked,
    COUNT(c.id) FILTER (
        WHERE c.contact_flag IN ('dnd', 'not_interested', 'attempt_3_months')
    )                                       AS timed_blocked
FROM companies co
LEFT JOIN contacts c ON c.company_id = co.id
GROUP BY co.id, co.name, co.company_flag
ORDER BY success_contacts DESC, total_contacts DESC;


-- ── VIEW 7: Callable pool size ────────────────────────────────
-- How many unique phones can be allocated right now.
-- Uses idx_contacts_flag_campaign + idx_phones_invalid partial index.
CREATE OR REPLACE VIEW v_callable_pool AS
SELECT
    COUNT(DISTINCT c.id)                    AS callable_contacts,
    COUNT(p.id)                             AS callable_phones,
    (SELECT COUNT(*) FROM contacts)         AS total_contacts,
    ROUND(
        COUNT(DISTINCT c.id) * 100.0
        / NULLIF((SELECT COUNT(*) FROM contacts), 0), 1
    )                                       AS callable_pct
FROM contacts c
JOIN contact_phones p ON p.contact_id = c.id
WHERE
    c.contact_flag NOT IN (
        'invalid_number', 'referred', 'language_issue',
        'mql_in_progress', 'mql_qualified', 'mql_rejected', 'meeting_in_progress'
    )
    AND NOT (c.contact_flag = 'not_interested'   AND c.flag_updated_at > NOW() - INTERVAL '2 months')
    AND NOT (c.contact_flag = 'dnd'              AND c.flag_updated_at > NOW() - INTERVAL '3 months')
    AND NOT (c.contact_flag = 'attempt_3_months' AND c.flag_updated_at > NOW() - INTERVAL '3 months')
    AND NOT EXISTS (
        SELECT 1 FROM contact_allocations ca
        WHERE ca.contact_id = c.id AND ca.closed_at IS NULL
    )
    AND p.is_invalid = FALSE;


-- ── VIEW 8: Meeting pipeline ──────────────────────────────────
-- All meetings with status, FU depth, and conclusion.
-- Scalar subqueries on meeting_call_attempts replaced with a single
-- lateral join to avoid two separate seq-scans per meeting row.
CREATE OR REPLACE VIEW v_meeting_pipeline AS
SELECT
    m.id                                    AS meeting_id,
    c.id                                    AS contact_id,
    co.name                                 AS company,
    TRIM(COALESCE(c.first_name,'') || ' ' || COALESCE(c.last_name,'')) AS person,
    c.contact_flag,
    m.campaign,
    m.status                                AS meeting_status,
    m.scheduled_date,
    m.held_at::date                         AS held_date,
    a.name                                  AS mql_agent,
    m.conclusion,
    m.solution_proposed,
    m.solution_link,
    m.solution_sent_at::date                AS solution_sent_date,
    m.solution_picked_at::date              AS solution_picked_date,
    m.scheduled_at_fu                       AS mql_fu_when_scheduled,
    COALESCE(mfu.max_fu, 0)                 AS meeting_fu_reached,
    COALESCE(mfu.last_state, '—')           AS last_state,
    m.created_at::date                      AS created_date
FROM meetings m
JOIN contacts c        ON c.id = m.contact_id
LEFT JOIN companies co ON co.id = c.company_id
LEFT JOIN agents a     ON a.id = m.mql_agent_id
LEFT JOIN LATERAL (
    SELECT
        MAX(follow_up_number)   AS max_fu,
        (
            SELECT current_state
            FROM meeting_call_attempts
            WHERE meeting_id = m.id
            ORDER BY follow_up_number DESC
            LIMIT 1
        )                       AS last_state
    FROM meeting_call_attempts
    WHERE meeting_id = m.id
) mfu ON TRUE
ORDER BY m.created_at DESC;


-- ── VIEW 9: SQL pipeline summary (both paths) ─────────────────
-- MQL Direct vs Meeting Track split. Uses idx_meetings_contact.
CREATE OR REPLACE VIEW v_sql_pipeline AS
SELECT
    'MQL Direct'                            AS path,
    COUNT(*)                                AS total_sql
FROM contacts c
WHERE c.contact_flag = 'mql_qualified'
  AND NOT EXISTS (
      SELECT 1 FROM meetings m
      WHERE m.contact_id = c.id AND m.status = 'solution_picked'
  )
UNION ALL
SELECT
    'Meeting Track'                         AS path,
    COUNT(*)                                AS total_sql
FROM contacts c
WHERE c.contact_flag = 'mql_qualified'
  AND EXISTS (
      SELECT 1 FROM meetings m
      WHERE m.contact_id = c.id AND m.status = 'solution_picked'
  );


-- ============================================================
-- QUICK QUERIES — copy-paste in pgAdmin anytime
-- ============================================================

-- Today's dialled vs KPI per agent:
-- SELECT agent_name, total_dialled, kpi_target, kpi_pct, connected, connection_rate_pct
-- FROM v_agent_daily_kpi WHERE call_date = CURRENT_DATE;

-- Current pool health:
-- SELECT * FROM v_contact_pipeline;

-- Callable pool remaining:
-- SELECT * FROM v_callable_pool;

-- Stories and snapshots this week:
-- SELECT * FROM v_success_funnel;

-- Company-level pipeline:
-- SELECT * FROM v_company_pipeline WHERE success_contacts > 0 ORDER BY success_contacts DESC;

-- Last remark for a specific contact:
-- SELECT * FROM v_latest_remarks WHERE contact_id = <id>;

-- All open meetings:
-- SELECT * FROM v_meeting_pipeline WHERE meeting_status NOT IN ('solution_picked', 'lost');

-- SQL ready contacts by path:
-- SELECT * FROM v_sql_pipeline;
