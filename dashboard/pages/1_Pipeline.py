"""
pages/1_Pipeline.py — End-to-End Pipeline Funnel
CEO / Board view: every stage from raw contact → SQL Ready.

Query strategy — 5 DB round-trips total (down from 10):
  _load_contact_stats  → all contact-flag counts (funnel, pool, blocks, velocity)
  _load_bd_stats       → BD per-agent rows + today's totals
  _load_mql_stats      → MQL per-agent rows + today's MQL totals
    _load_fu_breakdown   → BD call depth by FU stage
    _load_bd_activity    → BD call activity trends by period
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import date
from utils.db import query_df
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
st.title("Pipeline Health")
st.caption(f"Campaign: **{campaign.title()}** — full end-to-end funnel from first contact to SQL")

with st.sidebar:
    st.markdown("---")
    st.markdown("**BD activity date filter**")
    bd_date_from = st.date_input("From", value=date(2026, 1, 1), key="bd_dash_from")
    bd_date_to   = st.date_input("To",   value=date.today(),     key="bd_dash_to")


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 1 — All contact-flag stats in one round-trip
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10, show_spinner=False)
def _load_contact_stats(campaign: str) -> pd.DataFrame:
    """
    Single query covering: funnel counts, pool health, MQL pipeline,
    block accumulation, velocity, and pool runway estimate.
    """
    return query_df("""
        WITH bd_counts AS (
            SELECT
                contact_id,
                COUNT(*)                                            AS total_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Connected')   AS connected_attempts,
                COUNT(*) FILTER (WHERE current_state = 'Interested') AS interested_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Invalid Number') AS invalid_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Do not Disturb') AS dnd_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Not interested', 'Not Interested')) AS not_int_attempts,
                COUNT(*) FILTER (WHERE call_status = 'Referred' OR current_state IN ('Referred', 'Reffered')) AS referred_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months')) AS realloc_3m_attempts,
                COUNT(*) FILTER (WHERE current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months') AND called_at <= NOW() - INTERVAL '90 days') AS realloc_3m_ready_attempts,
                MIN(called_at)                                      AS first_called_at,
                MIN(called_at) FILTER (WHERE current_state = 'Shared Story') AS first_story_at,
                MIN(called_at) FILTER (WHERE current_state IN ('Snapshot Sent', 'Dream Snapshot Sent')) AS first_snapshot_at
            FROM call_actions
            GROUP BY contact_id
        ),
        mql_counts AS (
            SELECT
                mca.contact_id,
                COUNT(*) FILTER (WHERE mca.call_status = 'Invalid Number') AS invalid_attempts,
                COUNT(*) FILTER (WHERE mca.call_status = 'Do not Disturb') AS dnd_attempts,
                COUNT(*) FILTER (WHERE mca.current_state = 'Not interested') AS not_int_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Referred', 'Reffered')) AS referred_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months')) AS realloc_3m_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Attempt Again after 3 months', 'Allocate Again 3 months') AND mca.called_at <= NOW() - INTERVAL '90 days') AS realloc_3m_ready_attempts,
                COUNT(*) FILTER (WHERE mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')) AS confirmed_snapshot_attempts
            FROM mql_call_attempts mca
            JOIN mql_allocations ma ON ma.id = mca.allocation_id
            JOIN agents am ON am.id = mca.agent_id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason IS DISTINCT FROM 'bd_history'
              AND am.team = 'mql'
            GROUP BY mca.contact_id
        )
        SELECT
            -- ── Funnel (Cumulative: Ever Reached) ──────────────────────────
            COUNT(DISTINCT c.id)                                            AS total,
            COUNT(DISTINCT c.id) FILTER (WHERE bc.total_attempts > 0)       AS reached,
            COUNT(DISTINCT c.id) FILTER (WHERE bc.connected_attempts > 0)   AS connected,
            COUNT(DISTINCT c.id) FILTER (WHERE bc.interested_attempts > 0)  AS interested,
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.first_story_at IS NOT NULL 
                   OR c.contact_flag IN ('shared_story', 'snapshot_sent', 'bd_qualified', 'mql_qualified', 'meeting_in_progress', 'mql_rejected')
            )                                                               AS shared_story,
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.first_snapshot_at IS NOT NULL
                   OR c.contact_flag IN ('snapshot_sent', 'bd_qualified', 'mql_qualified', 'meeting_in_progress', 'mql_rejected')
                   OR EXISTS (SELECT 1 FROM mql_allocations ma WHERE ma.contact_id = c.id)
            )                                                               AS snapshot_sent,
            COUNT(DISTINCT c.id) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM mql_allocations ma 
                    WHERE ma.contact_id = c.id 
                      AND ma.close_reason IS DISTINCT FROM 'bd_history'
                )
            )                                                               AS mql_active,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'mql_qualified'
            )                                                               AS sql_ready,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mc.confirmed_snapshot_attempts > 0
            )                                                               AS true_mql,

            -- ── Pool health ───────────────────────────────────────────────
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'fresh'
            )                                                               AS fresh,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag IN ('in_progress','needs_followup')
            )                                                               AS in_progress_cnt,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag IN ('fresh','needs_followup','in_progress')
                  AND NOT EXISTS (
                      SELECT 1 FROM contact_allocations a
                      WHERE a.contact_id = c.id AND a.closed_at IS NULL
                  )
                  AND EXISTS (
                      SELECT 1 FROM contact_phones p
                      WHERE p.contact_id = c.id AND p.is_invalid = FALSE
                  )
            )                                                               AS callable,

            -- ── MQL pipeline ──────────────────────────────────────────────
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag IN ('shared_story','snapshot_sent')
                  AND NOT EXISTS (
                      SELECT 1 FROM mql_allocations ma
                      WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
                  )
            )                                                               AS mql_pool_unallocated,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'mql_in_progress'
            )                                                               AS mql_in_progress,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'mql_rejected'
            )                                                               AS mql_rejected,

            -- ── Block accumulation (historical) ───────────────────────────
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.invalid_attempts > 0 OR mc.invalid_attempts > 0
            )                                                               AS blk_invalid,
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.dnd_attempts > 0 OR mc.dnd_attempts > 0
            )                                                               AS blk_dnd,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'not_interested'
                   OR bc.not_int_attempts > 0
                   OR mc.not_int_attempts > 0
            )                                                               AS blk_not_int,
            COUNT(DISTINCT c.id) FILTER (
                WHERE c.contact_flag = 'referred'
                   OR bc.referred_attempts > 0
                   OR mc.referred_attempts > 0
            )                                                               AS blk_referred,
            COUNT(DISTINCT c.id) FILTER (WHERE c.contact_flag = 'language_issue')   AS blk_language,
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.realloc_3m_attempts > 0
            )                                                               AS blk_bd_3months_all,
            COUNT(DISTINCT c.id) FILTER (
                WHERE bc.realloc_3m_ready_attempts > 0
            )                                                               AS blk_bd_3months_ready,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mc.realloc_3m_attempts > 0
            )                                                               AS blk_mql_3months_all,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mc.realloc_3m_ready_attempts > 0
            )                                                               AS blk_mql_3months_ready,

            -- ── Velocity: avg days from first BD call → first Shared Story ─
            ROUND(AVG(
                CASE
                    WHEN bc.first_called_at IS NOT NULL
                     AND bc.first_story_at IS NOT NULL
                     AND bc.first_story_at >= bc.first_called_at
                    THEN EXTRACT(EPOCH FROM (bc.first_story_at - bc.first_called_at)) / 86400
                END
            ), 1)                                                           AS avg_days_to_story,

            -- ── End-to-end cycle: first BD call → first final MQL close ───
            ROUND(AVG(
                CASE
                    WHEN bc.first_called_at IS NOT NULL
                     AND mql_close.first_mql_closed_at IS NOT NULL
                     AND mql_close.first_mql_closed_at >= bc.first_called_at
                    THEN EXTRACT(EPOCH FROM (mql_close.first_mql_closed_at - bc.first_called_at)) / 86400
                END
            ), 1)                                                           AS avg_days_total_e2e,

            -- ── Pool runway: avg fresh contacts allocated/day last 7 days ─
            (
                SELECT ROUND(COUNT(*)::numeric / 7, 1)
                FROM contact_allocations ca2
                JOIN contacts c2 ON c2.id = ca2.contact_id
                WHERE ca2.allocated_date >= CURRENT_DATE - 7
                  AND c2.campaign = %(campaign)s
                  AND c2.contact_flag = 'fresh'
            )                                                               AS avg_daily_fresh
        FROM contacts c
        LEFT JOIN bd_counts bc ON bc.contact_id = c.id
        LEFT JOIN mql_counts mc ON mc.contact_id = c.id
        LEFT JOIN LATERAL (
            SELECT MIN(ma.closed_at) AS first_mql_closed_at
            FROM mql_allocations ma
            JOIN agents am2 ON am2.id = ma.agent_id
            WHERE ma.contact_id = c.id
              AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.closed_at IS NOT NULL
              AND ma.close_reason IN ('qualified', 'rejected', 'stalled')
              AND am2.team = 'mql'
        ) mql_close ON TRUE
        WHERE c.campaign = %(campaign)s
    """, params={"campaign": campaign, "campaign_like": f"{campaign} %"})


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 2 — BD agents: all-time funnel + today's numbers in one round-trip
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def _load_bd_stats(campaign: str) -> pd.DataFrame:
    """Per-agent BD rows. TODAY totals derived in pandas by summing the column."""
    return query_df("""
        SELECT
            a.name                                                          AS agent,
            a.status                                                        AS agent_status,
            a.shift_name,
            a.kpi_dialed,
            a.is_on_leave,
            -- All-time per agent
            COUNT(DISTINCT ca.contact_id)                                  AS contacts_called,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.call_status = 'Connected'
            )                                                               AS connected_all,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state = 'Interested'
            )                                                               AS interested_all,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state = 'Shared Story'
            )                                                               AS shared_story_all,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state IN ('Snapshot Sent','Dream Snapshot Sent')
            )                                                               AS snapshot_all,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state IN ('Meeting Requested','Meeting Scheduled')
            )                                                               AS meeting_all,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state = 'Meeting Scheduled'
            )                                                               AS meeting_sched_all,
            ROUND(
                COUNT(DISTINCT ca.contact_id) FILTER (
                    WHERE ca.current_state = 'Shared Story'
                ) * 100.0 / NULLIF(
                    COUNT(DISTINCT ca.contact_id) FILTER (
                        WHERE ca.call_status = 'Connected'
                    ), 0
                ), 1
            )                                                               AS story_conv_pct,
            -- Today
            COUNT(ca.id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
            )                                                               AS dialled_today,
            COUNT(ca.id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
                  AND ca.call_status = 'Connected'
            )                                                               AS connected_today,
                        COUNT(ca.id) FILTER (
                                WHERE DATE(ca.called_at) = CURRENT_DATE
                                    AND ca.call_status = 'Connected'
                                                AND COALESCE(ca.attempt_number, 0) <= 1
                        )                                                               AS fresh_connect_today,
                        COUNT(ca.id) FILTER (
                                WHERE DATE(ca.called_at) = CURRENT_DATE
                                                AND COALESCE(ca.attempt_number, 0) > 1
                        )                                                               AS followups_today,
                        COUNT(ca.id) FILTER (
                                WHERE DATE(ca.called_at) = CURRENT_DATE
                                    AND ca.call_status = 'Connected'
                                                AND COALESCE(ca.attempt_number, 0) > 1
                        )                                                               AS followup_connect_today,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
                  AND ca.current_state IN ('Shared Story')
            )                                                               AS stories_today,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
                AND ca.current_state IN ('Snapshot Sent','Dream Snapshot Sent')
            )                                                               AS snapshots_today,
            COUNT(ca.id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
                  AND ca.call_status = 'Do not Disturb'
            )                                                               AS dnd_today,
            COUNT(ca.id) FILTER (
                WHERE DATE(ca.called_at) = CURRENT_DATE
                  AND ca.call_status = 'Invalid Number'
            )                                                               AS invalid_today
        FROM agents a
        LEFT JOIN (
            SELECT ca2.agent_id, ca2.contact_id,
                   ca2.call_status, ca2.current_state, ca2.called_at, ca2.id,
                   ca2.attempt_number
            FROM call_actions ca2
            JOIN contacts c2 ON c2.id = ca2.contact_id
            WHERE (c2.campaign = %(campaign)s OR c2.campaign ILIKE %(campaign_like)s)
        ) ca ON ca.agent_id = a.id
                WHERE (a.team IS NULL OR LOWER(a.team) = 'bd')
                GROUP BY a.name, a.status, a.shift_name, a.kpi_dialed, a.is_on_leave
        ORDER BY shared_story_all DESC
    """, params={"campaign": campaign, "campaign_like": f"{campaign} %"})


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 3 — MQL agents: all-time + today's numbers in one round-trip
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def _load_mql_stats(campaign: str) -> pd.DataFrame:
    """Per-agent MQL rows. TODAY totals derived in pandas."""
    return query_df("""
        WITH alloc AS (
            SELECT
                ma.agent_id,
                COUNT(DISTINCT ma.contact_id)                                  AS total_allocated,
                COUNT(DISTINCT ma.contact_id) FILTER (
                    WHERE ma.closed_at IS NULL
                )                                                               AS active,
                COUNT(DISTINCT ma.contact_id) FILTER (
                    WHERE ma.close_reason = 'qualified'
                )                                                               AS qualified,
                COUNT(DISTINCT ma.contact_id) FILTER (
                    WHERE ma.close_reason = 'rejected'
                )                                                               AS rejected,
                COUNT(DISTINCT ma.contact_id) FILTER (
                    WHERE ma.close_reason = 'stalled'
                )                                                               AS stalled,
                ROUND(
                    COUNT(DISTINCT ma.contact_id) FILTER (
                        WHERE ma.close_reason = 'qualified'
                    ) * 100.0 / NULLIF(
                        COUNT(DISTINCT ma.contact_id) FILTER (
                            WHERE ma.close_reason IN ('qualified', 'rejected', 'stalled')
                        ), 0
                    ), 1
                )                                                               AS qual_rate_pct,
                ROUND(AVG(
                    CASE WHEN ma.closed_at IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (ma.closed_at - ma.allocated_at)) / 86400
                    END
                ), 1)                                                           AS avg_days_to_close,
                ROUND(AVG(
                    EXTRACT(EPOCH FROM (ma.closed_at - ma.allocated_at)) / 86400
                ) FILTER (WHERE ma.closed_at IS NOT NULL), 1)                  AS avg_days_mql_close
            FROM mql_allocations ma
                        JOIN agents am ON am.id = ma.agent_id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason IS DISTINCT FROM 'bd_history'
                            AND am.team = 'mql'
            GROUP BY ma.agent_id
        ),
        attempts_today AS (
            SELECT
                mca.agent_id,
                COUNT(mca.id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                )                                                               AS fu_today,
                COUNT(DISTINCT mca.contact_id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                      AND mca.call_status = 'Connected'
                )                                                               AS connected_today,
                COUNT(mca.id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                      AND mca.call_status = 'Connected'
                      AND COALESCE(mca.follow_up_number, 1) <= 1
                )                                                               AS fresh_connect_today,
                COUNT(mca.id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                      AND COALESCE(mca.follow_up_number, 1) > 1
                )                                                               AS followups_today,
                COUNT(mca.id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                      AND mca.call_status = 'Connected'
                      AND COALESCE(mca.follow_up_number, 1) > 1
                )                                                               AS followup_connect_today,
                COUNT(DISTINCT mca.contact_id) FILTER (
                    WHERE DATE(mca.called_at) = CURRENT_DATE
                      AND mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')
                )                                                               AS true_mql_today
            FROM mql_call_attempts mca
            JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        JOIN agents am ON am.id = mca.agent_id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason IS DISTINCT FROM 'bd_history'
                            AND am.team = 'mql'
            GROUP BY mca.agent_id
        ),
                true_mql_by_agent AS (
            SELECT
                                mca.agent_id,
                                COUNT(DISTINCT mca.contact_id) AS true_mql
                        FROM mql_call_attempts mca
                        JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        JOIN agents am ON am.id = mca.agent_id
                        WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                            AND ma.close_reason IS DISTINCT FROM 'bd_history'
                            AND am.team = 'mql'
                            AND mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')
                        GROUP BY mca.agent_id
        ),
        agent_scope AS (
            SELECT agent_id FROM alloc
            UNION
            SELECT agent_id FROM attempts_today
            UNION
                        SELECT agent_id FROM true_mql_by_agent
        )
        SELECT
            a.name                                                          AS agent,
                        a.status                                                        AS agent_status,
            a.shift_name,
            a.kpi_dialed,
            a.is_on_leave,
            COALESCE(alloc.total_allocated, 0)                              AS total_allocated,
            COALESCE(alloc.active, 0)                                       AS active,
                        COALESCE(true_mql_by_agent.true_mql, 0)                         AS true_mql,
            COALESCE(alloc.qualified, 0)                                    AS qualified,
            COALESCE(alloc.rejected, 0)                                     AS rejected,
            COALESCE(alloc.stalled, 0)                                      AS stalled,
            alloc.qual_rate_pct,
            alloc.avg_days_to_close,
            COALESCE(attempts_today.fu_today, 0)                            AS fu_today,
            COALESCE(attempts_today.connected_today, 0)                     AS connected_today,
            COALESCE(attempts_today.fresh_connect_today, 0)                 AS fresh_connect_today,
            COALESCE(attempts_today.followups_today, 0)                     AS followups_today,
            COALESCE(attempts_today.followup_connect_today, 0)              AS followup_connect_today,
            COALESCE(attempts_today.true_mql_today, 0)                      AS true_mql_today,
            alloc.avg_days_mql_close
        FROM agent_scope s
        JOIN agents a ON a.id = s.agent_id
        LEFT JOIN alloc ON alloc.agent_id = a.id
        LEFT JOIN attempts_today ON attempts_today.agent_id = a.id
        LEFT JOIN true_mql_by_agent ON true_mql_by_agent.agent_id = a.id
        ORDER BY COALESCE(alloc.qualified, 0) DESC,
             COALESCE(true_mql_by_agent.true_mql, 0) DESC,
                 a.name
    """, params={"campaign": campaign, "campaign_like": f"{campaign} %"})


# ─────────────────────────────────────────────────────────────────────────────
# QUERY 4 — BD Follow-up depth breakdown
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60, show_spinner=False)
def _load_fu_breakdown(campaign: str) -> pd.DataFrame:
    """
    Count BD contacts by their current follow-up stage (max attempt_number).
    Excludes contacts already in MQL pipeline or permanently blocked.
    attempt_number: 0 = New Contact (first call), 1-5 = FU1-FU5.
    """
    return query_df("""
        SELECT
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt IS NULL
            )                                   AS fresh_never_called,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 0
            )                                   AS new_contact,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 1
            )                                   AS fu1,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 2
            )                                   AS fu2,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 3
            )                                   AS fu3,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 4
            )                                   AS fu4,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt = 5
            )                                   AS fu5,
            COUNT(DISTINCT c.id) FILTER (
                WHERE mx.max_attempt >= 1
            )                                   AS total_followups,
            COUNT(DISTINCT c.id)                AS total_bd
        FROM contacts c
        LEFT JOIN (
            SELECT contact_id, MAX(attempt_number) AS max_attempt
            FROM call_actions
            GROUP BY contact_id
        ) mx ON mx.contact_id = c.id
        WHERE c.campaign = %(campaign)s
          AND c.contact_flag NOT IN (
              'mql_in_progress', 'mql_qualified', 'mql_rejected',
              'meeting_in_progress',
              'invalid_number', 'referred', 'language_issue'
          )
    """, params={"campaign": campaign})


@st.cache_data(ttl=120, show_spinner=False)
def _load_bd_activity(campaign: str, df: str, dt: str, trunc: str) -> pd.DataFrame:
    return query_df(f"""
        SELECT
            DATE_TRUNC('{trunc}', ca.called_at)::date          AS period,
            COUNT(ca.id)                                        AS total_calls,
            COUNT(ca.id) FILTER (WHERE ca.call_status = 'Connected')          AS connected,
            COUNT(ca.id) FILTER (WHERE ca.call_status = 'Did not connect')    AS dnc,
            COUNT(ca.id) FILTER (WHERE ca.call_status = 'Not interested')     AS not_interested,
            COUNT(ca.id) FILTER (WHERE ca.call_status = 'Do not Disturb')     AS dnd,
            COUNT(DISTINCT ca.contact_id) FILTER (
                WHERE ca.current_state = 'Shared Story'
            )                                                   AS stories
        FROM call_actions ca
        JOIN contacts c ON c.id = ca.contact_id
        WHERE c.campaign = %(campaign)s
          AND ca.called_at::date BETWEEN %(df)s AND %(dt)s
        GROUP BY DATE_TRUNC('{trunc}', ca.called_at)::date
        ORDER BY period
    """, params={"campaign": campaign, "df": df, "dt": dt})


def _load_combined_activity(campaign: str, df: str, dt: str, trunc: str) -> pd.DataFrame:
    """Load BD + MQL metrics including True MQL and Not Interested."""
    return query_df(f"""
        WITH bd_metrics AS (
            SELECT
                DATE_TRUNC('{trunc}', ca.called_at)::date          AS period,
                COUNT(ca.id) FILTER (WHERE ca.call_status = 'Connected')          AS bd_connected,
                COUNT(ca.id) FILTER (WHERE ca.call_status = 'Did not connect')    AS bd_dnc,
                COUNT(ca.id) FILTER (WHERE ca.call_status = 'Not interested')     AS bd_not_interested,
                COUNT(ca.id) FILTER (WHERE ca.call_status = 'Do not Disturb')     AS bd_dnd,
                COUNT(DISTINCT ca.contact_id) FILTER (
                    WHERE ca.current_state = 'Shared Story'
                )                                                   AS bd_stories,
                COUNT(DISTINCT ca.contact_id) FILTER (
                    WHERE ca.dream_snapshot_link IS NOT NULL AND ca.dream_snapshot_link != ''
                )                                                   AS bd_snapshots
            FROM call_actions ca
            JOIN contacts c ON c.id = ca.contact_id
            WHERE c.campaign = %(campaign)s
              AND ca.called_at::date BETWEEN %(df)s AND %(dt)s
            GROUP BY DATE_TRUNC('{trunc}', ca.called_at)::date
        ),
        mql_metrics AS (
            SELECT
                DATE_TRUNC('{trunc}', mca.called_at)::date          AS period,
                COUNT(DISTINCT mca.contact_id) FILTER (
                                        WHERE mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')
                      AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_prefix)s)
                      AND ma.close_reason IS DISTINCT FROM 'bd_history'
                )                                                   AS true_mql,
                COUNT(DISTINCT mca.contact_id) FILTER (
                    WHERE mca.current_state = 'Not interested'
                      AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_prefix)s)
                      AND ma.close_reason IS DISTINCT FROM 'bd_history'
                )                                                   AS mql_not_interested
            FROM mql_call_attempts mca
            JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        JOIN agents am ON am.id = mca.agent_id
            WHERE mca.called_at::date BETWEEN %(df)s AND %(dt)s
                            AND am.team = 'mql'
            GROUP BY DATE_TRUNC('{trunc}', mca.called_at)::date
        )
        SELECT
            COALESCE(b.period, m.period) AS period,
            COALESCE(b.bd_connected, 0) AS connected,
            COALESCE(b.bd_dnc, 0) AS dnc,
            COALESCE(b.bd_not_interested, 0) + COALESCE(m.mql_not_interested, 0) AS not_interested,
            COALESCE(b.bd_dnd, 0) AS dnd,
            COALESCE(b.bd_stories, 0) AS stories,
            COALESCE(b.bd_snapshots, 0) AS snapshots,
            COALESCE(m.true_mql, 0) AS true_mql
        FROM bd_metrics b
        FULL OUTER JOIN mql_metrics m ON b.period = m.period
        ORDER BY period
    """, params={
        "campaign": campaign,
        "campaign_prefix": f"{campaign}%",
        "df": df,
        "dt": dt
    })


# ─────────────────────────────────────────────────────────────────────────────
# LOAD ALL DATA
# ─────────────────────────────────────────────────────────────────────────────
cs       = None   # contact stats row
bd_df    = None   # BD agent rows
mql_df   = None   # MQL agent rows
fu_df    = None   # follow-up depth breakdown

try:
    _cs = _load_contact_stats(campaign)
    if not _cs.empty:
        cs = _cs.iloc[0]
except Exception as e:
    log_and_show("contact stats", e)

try:
    bd_df = _load_bd_stats(campaign)
except Exception as e:
    log_and_show("BD stats", e)

try:
    mql_df = _load_mql_stats(campaign)
except Exception as e:
    log_and_show("MQL stats", e)

try:
    _fu = _load_fu_breakdown(campaign)
    if not _fu.empty:
        fu_df = _fu.iloc[0]
except Exception as e:
    log_and_warn("FU breakdown", e)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — END-TO-END FUNNEL WATERFALL
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("End-to-end funnel")
st.caption("Cumulative contacts that have reached each milestone at least once.")

if cs is not None:
    total        = int(cs["total"])
    reached      = int(cs["reached"])
    connected    = int(cs["connected"])
    interested   = int(cs["interested"])
    shared_story = int(cs["shared_story"])
    snapshot     = int(cs["snapshot_sent"])
    mql_active   = int(cs["mql_active"])
    true_mql     = int(cs["true_mql"])
    sql_ready    = int(cs["sql_ready"])

    def pct(num, den):
        return f"{round(num * 100 / den, 1)}%" if den > 0 else "—"

    st.markdown("**BD stage**")
    r1 = st.columns(5)
    r1[0].metric("Total contacts", total)
    r1[1].metric("Reached",        reached,      delta=pct(reached,      total),      delta_color="off")
    r1[2].metric("Connected",      connected,    delta=pct(connected,    reached),    delta_color="off")
    r1[3].metric("Interested",     interested,   delta=pct(interested,   connected),  delta_color="off")
    r1[4].metric("Shared Story",   shared_story, delta=pct(shared_story, connected),  delta_color="off")

    st.markdown("**Handoff & MQL stage**")
    r2 = st.columns(5)
    r2[0].metric("Snapshot Sent",  snapshot,   delta=pct(snapshot,   shared_story), delta_color="off")
    r2[1].metric("MQL Active",     mql_active, delta=pct(mql_active, snapshot),     delta_color="off")
    r2[2].metric("True MQL",       true_mql,   delta=pct(true_mql,   mql_active),   delta_color="off")
    r2[3].metric("SQL Ready",      sql_ready,  delta=pct(sql_ready,  mql_active),   delta_color="off")
    r2[4].metric("BD → SQL rate",  pct(sql_ready, total),                           delta_color="off")

    # Altair horizontal funnel chart — stages in correct order, no alphabetical sort
    stages = ["Reached","Connected","Interested","Shared Story","Snapshot Sent","MQL Active","SQL Ready"]
    counts = [reached,  connected,  interested,  shared_story,  snapshot,       mql_active,  sql_ready]
    colors = ["#4e8df5","#3d7fe0","#2d71cb","#1a9e6e","#17b87e","#14d18e","#10eb9e"]

    chart_df = pd.DataFrame({
        "Stage": stages,
        "Count": counts,
        "Color": colors,
        "Order": list(range(len(stages))),
        "Label": [f"{c:,}  ({pct(c, total)})" for c in counts],
    })

    bars = alt.Chart(chart_df).mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
        y   = alt.Y("Stage:N", sort=stages, axis=alt.Axis(labelFontSize=13), title=None),
        x   = alt.X("Count:Q", axis=alt.Axis(format=",", labelFontSize=11), title="Contacts"),
        color = alt.Color("Color:N", scale=None, legend=None),
        tooltip = [
            alt.Tooltip("Stage:N",  title="Stage"),
            alt.Tooltip("Count:Q",  title="Contacts", format=","),
            alt.Tooltip("Label:N",  title="% of total"),
        ],
    )
    labels = alt.Chart(chart_df).mark_text(align="left", dx=6, fontSize=12, color="#555").encode(
        y     = alt.Y("Stage:N", sort=stages),
        x     = alt.X("Count:Q"),
        text  = alt.Text("Label:N"),
    )
    st.altair_chart((bars + labels).properties(height=240), use_container_width=True)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — PER-AGENT MEMBER SCOPE (shared for BD + MQL tables)
# ─────────────────────────────────────────────────────────────────────────────
agent_scope_mode = st.radio(
    "Per-agent member scope",
    ["All time", "Active members"],
    index=0,
    horizontal=True,
    key="pipeline_agent_scope_mode_v2",
    help="Applies only to BD funnel per agent and MQL pipeline per agent sections.",
)


def _filter_member_scope(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.empty or "agent_status" not in df.columns:
        return df
    if agent_scope_mode == "Active members":
        return df[df["agent_status"].fillna("").str.lower() == "active"].copy()
    return df.copy()


bd_agent_df = _filter_member_scope(bd_df)
mql_agent_df = _filter_member_scope(mql_df)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — BD FUNNEL PER AGENT  (from bd_df — no extra query)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("BD funnel — per agent")
st.caption(f"Lifetime milestones reached per BD agent. Scope: {agent_scope_mode}.")

if bd_agent_df is not None and not bd_agent_df.empty:
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Total called",  int(bd_agent_df["contacts_called"].sum()))
    t2.metric("Connected",     int(bd_agent_df["connected_all"].sum()))
    t3.metric("Interested",    int(bd_agent_df["interested_all"].sum()))
    t4.metric("Shared Story",  int(bd_agent_df["shared_story_all"].sum()))
    t5.metric("Snapshot",      int(bd_agent_df["snapshot_all"].sum()))

    st.dataframe(
        bd_agent_df[[
            "agent","contacts_called","connected_all","interested_all",
            "shared_story_all","snapshot_all","meeting_all","meeting_sched_all","story_conv_pct"
        ]].rename(columns={
            "agent":             "Agent",
            "contacts_called":   "Called",
            "connected_all":     "Connected",
            "interested_all":    "Interested",
            "shared_story_all":  "Shared Story",
            "snapshot_all":      "Snapshot",
            "meeting_all":       "Meeting (all)",
            "meeting_sched_all": "Meeting Sched",
            "story_conv_pct":    "Story Conv %",
        }),
        use_container_width=True, hide_index=True,
    )
else:
    st.info("No BD call data yet.")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — MQL PIPELINE  (from mql_df + cs — no extra query)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("MQL pipeline")
st.caption(f"MQL pool health and per-agent outcomes. Scope: {agent_scope_mode}.")
st.caption(
    "Stalled = contact reached FU30 without qualifying or being rejected. "
    "Qualification rate = Qualified / (Qualified + Rejected + Stalled)."
)

if cs is not None:
    mp1, mp2, mp3, mp4, mp5 = st.columns(5)
    mp1.metric("Pool (unallocated)",  int(cs["mql_pool_unallocated"]),
               help="Ready to assign to MQL agents")
    mp2.metric("MQL in progress",    int(cs["mql_in_progress"]))
    mp3.metric("True MQL",            int(cs["true_mql"]))
    mp4.metric("SQL Ready",           int(cs["sql_ready"]))
    mp5.metric("Rejected",            int(cs["mql_rejected"]))

if mql_agent_df is not None and not mql_agent_df.empty:
    total_closed = int(mql_agent_df["qualified"].sum()) + int(mql_agent_df["rejected"].sum())
    if total_closed > 0:
        conv = round(int(mql_agent_df["qualified"].sum()) * 100 / total_closed, 1)
        st.caption(f"MQL → SQL conversion rate: **{conv}%** of closed allocations qualified")

    st.dataframe(
        mql_agent_df[[
            "agent","total_allocated","active","true_mql","qualified",
            "rejected","stalled","qual_rate_pct","avg_days_to_close"
        ]].rename(columns={
            "agent":            "MQL Agent",
            "total_allocated":  "Total Allocated",
            "active":           "Active",
            "true_mql":         "True MQL",
            "qualified":        "Qualified (SQL)",
            "rejected":         "Rejected",
            "stalled":          "Stalled",
            "qual_rate_pct":    "Qual Rate %",
            "avg_days_to_close":"Avg Days to Close",
        }),
        use_container_width=True, hide_index=True,
    )
else:
    st.info("No MQL agent data yet.")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — POOL HEALTH & RUNWAY  (from cs — no extra query)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Pool health & runway")
st.caption("How long will the contact pool last at the current allocation rate?")
st.caption("Pool runway = unallocated/fresh callable contacts divided by current daily allocation rate.")

if cs is not None:
    callable_c  = int(cs["callable"])
    fresh_c     = int(cs["fresh"])
    total_c     = int(cs["total"])
    daily_rate  = float(cs["avg_daily_fresh"] or 0)
    callable_pct= round(callable_c * 100 / total_c, 1) if total_c > 0 else 0
    runway_days = round(fresh_c / daily_rate) if daily_rate > 0 else None

    ph1, ph2, ph3, ph4 = st.columns(4)
    ph1.metric("Callable contacts",    callable_c,
               delta=f"{callable_pct}% of total", delta_color="off")
    ph2.metric("Fresh (never called)", fresh_c)
    ph3.metric("Avg daily fresh used", f"{daily_rate}/day",
               help="Fresh contacts used per day (last 7 days)")
    ph4.metric("Pool runway",
               f"{runway_days} days" if runway_days else "N/A",
               help="Days of fresh contacts remaining at current rate",
               delta_color="off")

    if callable_pct < 30:
        st.error("Pool below 30%. Import new contacts urgently.")
    elif callable_pct < 50:
        st.warning("Pool below 50%. Plan a data import this month.")
    elif runway_days is not None and runway_days < 14:
        st.warning(f"Only {runway_days} days of fresh contacts at current rate.")
    else:
        st.success("Pool is healthy.")

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — BD CALL DEPTH  (from fu_df)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("BD call depth")
st.caption(
    "How many contacts are at each follow-up stage. "
    "Excludes MQL pipeline and permanently blocked contacts."
)

if fu_df is not None:
    fresh_nc = int(fu_df["fresh_never_called"])
    nc       = int(fu_df["new_contact"])
    f1       = int(fu_df["fu1"])
    f2       = int(fu_df["fu2"])
    f3       = int(fu_df["fu3"])
    f4       = int(fu_df["fu4"])
    f5       = int(fu_df["fu5"])
    total_fu = int(fu_df["total_followups"])
    total_bd = int(fu_df["total_bd"])

    # ── Metric cards ──────────────────────────────────────────
    cd1, cd2, cd3, cd4, cd5, cd6, cd7, cd8 = st.columns(8)
    cd1.metric("Fresh",         fresh_nc,
               help="Never called — no call_actions recorded")
    cd2.metric("New Contact",   nc,
               help="Called once as a new contact (attempt 0)")
    cd3.metric("FU 1",  f1)
    cd4.metric("FU 2",  f2)
    cd5.metric("FU 3",  f3)
    cd6.metric("FU 4",  f4)
    cd7.metric("FU 5",  f5)
    cd8.metric("All Follow-ups", total_fu,
               help="Total contacts in FU1–FU5 (combined)")

    # ── Bar chart ─────────────────────────────────────────────
    depth_data = pd.DataFrame([
        {"Stage": "Fresh",        "Contacts": fresh_nc, "Order": 0},
        {"Stage": "New Contact",  "Contacts": nc,       "Order": 1},
        {"Stage": "FU 1",         "Contacts": f1,       "Order": 2},
        {"Stage": "FU 2",         "Contacts": f2,       "Order": 3},
        {"Stage": "FU 3",         "Contacts": f3,       "Order": 4},
        {"Stage": "FU 4",         "Contacts": f4,       "Order": 5},
        {"Stage": "FU 5",         "Contacts": f5,       "Order": 6},
    ])

    stage_order = depth_data["Stage"].tolist()
    bar = (
        alt.Chart(depth_data)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y     = alt.Y("Stage:N", sort=stage_order, title=None,
                          axis=alt.Axis(labelFontSize=13)),
            x     = alt.X("Contacts:Q",
                          axis=alt.Axis(format=",", labelFontSize=11),
                          title="Contacts"),
            color = alt.condition(
                alt.datum["Stage"] == "Fresh",
                alt.value("#4e8df5"),
                alt.value("#22c55e"),
            ),
            tooltip=[
                alt.Tooltip("Stage:N",    title="Stage"),
                alt.Tooltip("Contacts:Q", title="Contacts", format=","),
            ],
        )
        .properties(height=220)
    )
    labels = (
        alt.Chart(depth_data)
        .mark_text(align="left", dx=6, fontSize=12, color="#555")
        .encode(
            y    = alt.Y("Stage:N", sort=stage_order),
            x    = alt.X("Contacts:Q"),
            text = alt.Text("Contacts:Q", format=","),
        )
    )
    st.altair_chart((bar + labels), use_container_width=True)

    fu_pct = round(total_fu * 100 / total_bd, 1) if total_bd > 0 else 0
    st.caption(
        f"Total BD contacts tracked: **{total_bd:,}** — "
        f"Fresh: **{fresh_nc:,}** · "
        f"Follow-ups pending: **{total_fu:,}** ({fu_pct}%)"
    )
else:
    st.info("No call depth data yet.")

st.markdown("---")
st.markdown("**Call activity over time (BD + MQL)**")

# New: Toggle for BD vs MQL view
view_mode = st.radio(
    "View",
    ["All (BD + MQL)", "BD only", "MQL only"],
    horizontal=True,
    key="activity_view_mode",
    help="Filter activity by source: All = combined metrics, BD = call_actions, MQL = mql_call_attempts"
)

gran_bd = st.radio("Granularity", ["Daily", "Weekly", "Monthly"], horizontal=True, key="bd_activity_gran")
trunc_map = {"Daily": "day", "Weekly": "week", "Monthly": "month"}
trunc_bd  = trunc_map[gran_bd]

try:
    act_df = _load_combined_activity(campaign, str(bd_date_from), str(bd_date_to), trunc_bd)
    if act_df.empty:
        st.info("No call data in this date range.")
    else:
        # Filter columns based on view mode
        if view_mode == "BD only":
            # BD metrics: connected, dnc, dnd, stories, snapshots (but not true_mql or not_interested from MQL)
            value_vars = ["connected", "dnc", "dnd", "stories", "snapshots"]
            metric_map = {
                "connected":     "Connected",
                "dnc":           "Did not connect",
                "dnd":           "Do not Disturb",
                "stories":       "Shared Story",
                "snapshots":     "Snapshot Sent",
            }
            chart_colors = ["#22c55e", "#6366f1", "#ef4444", "#10eb9e", "#06b6d4"]
        elif view_mode == "MQL only":
            # MQL metrics: true_mql and not_interested (from MQL)
            value_vars = ["true_mql", "not_interested"]
            metric_map = {
                "true_mql":      "True MQL",
                "not_interested":"Not interested",
            }
            chart_colors = ["#a855f7", "#f59e0b"]
        else:  # "All (BD + MQL)"
            # All metrics
            value_vars = ["connected", "dnc", "not_interested", "dnd", "stories", "true_mql"]
            metric_map = {
                "connected":     "Connected",
                "dnc":           "Did not connect",
                "not_interested":"Not interested",
                "dnd":           "Do not Disturb",
                "stories":       "Shared Story",
                "true_mql":      "True MQL",
            }
            chart_colors = ["#22c55e", "#6366f1", "#f59e0b", "#ef4444", "#10eb9e", "#a855f7"]
        
        melted = act_df.melt(
            id_vars="period",
            value_vars=value_vars,
            var_name="metric", value_name="count"
        )
        melted["metric"] = melted["metric"].map(metric_map)
        
        st.altair_chart(
            alt.Chart(melted).mark_line(point=True).encode(
                x=alt.X("period:T", title=gran_bd),
                y=alt.Y("count:Q", title="Contacts/Calls"),
                color=alt.Color("metric:N",
                    scale=alt.Scale(
                        domain=list(metric_map.values()),
                        range=chart_colors
                    ),
                    legend=alt.Legend(orient="bottom")),
                tooltip=[alt.Tooltip("period:T"), alt.Tooltip("metric:N"), alt.Tooltip("count:Q")],
            ).properties(height=260),
            use_container_width=True
        )
        
        # Add legend explaining metrics
        with st.expander("Metric definitions"):
            st.markdown("""
            **BD Metrics:**
            - **Connected**: Call answered by contact
            - **Did not connect**: Phone not reachable
            - **Shared Story**: Contact agreed to share their story
            - **Do not Disturb**: Contact said "do not call"
            
            **MQL Metrics:**
            - **True MQL**: Unique contact with at least one MQL call marked Dream Snapshot Confirmed or Snapshot Confirmed
            - **Not interested**: MQL contact expressed no interest
            """)
except Exception as e:
    log_and_warn("Call activity chart", e)

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 8 — VELOCITY  (from cs + mql_df — no extra query)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Pipeline velocity")
st.caption("Average time for a contact to move through each stage transition.")
st.caption(
    "BD velocity = avg days from first call to Shared Story. "
    "MQL velocity = avg days from allocation to close. "
    "Total cycle is shown in two ways: stage-sum and end-to-end."
)

if cs is not None:
    avg_bd = float(cs["avg_days_to_story"]) if pd.notna(cs["avg_days_to_story"]) else None
    avg_mql = None
    if mql_df is not None and not mql_df.empty:
        avg_mql_raw = mql_df["avg_days_mql_close"].mean()
        avg_mql = float(avg_mql_raw) if pd.notna(avg_mql_raw) else None
    avg_e2e = float(cs["avg_days_total_e2e"]) if pd.notna(cs["avg_days_total_e2e"]) else None

    total_stage_sum = (avg_bd + avg_mql) if (avg_bd is not None and avg_mql is not None) else None

    v1, v2, v3, v4, v5 = st.columns(5)
    v1.metric("BD: Avg days to Shared Story",
              f"{round(avg_bd, 1)} days" if avg_bd is not None else "—")
    v2.metric("MQL: Avg days to close",
              f"{round(avg_mql, 1)} days" if avg_mql is not None else "—")
    v3.metric("Pool runway",
              f"{runway_days} days" if (cs is not None and daily_rate > 0 and runway_days) else "—",
              help="At current daily rate")
    v4.metric(
        "Total cycle (BD+MQL)",
        f"{round(total_stage_sum, 1)} days" if total_stage_sum is not None else "—",
        help="Stage-sum method: BD average + MQL average",
    )
    v5.metric(
        "Total cycle (End-to-end)",
        f"{round(avg_e2e, 1)} days" if avg_e2e is not None else "—",
        help="Per-contact method: first BD call to first final MQL close",
    )

    with st.expander("Cycle-time logic", expanded=False):
        st.markdown(
            "- **Stage-sum (BD+MQL):** add the two stage averages.  \n"
            "  Formula: `AVG(BD first call → Shared Story) + AVG(MQL allocation → close)`  \n"
            "- **End-to-end:** average per-contact full journey.  \n"
            "  Formula: `AVG(first BD call → first final MQL close)`  \n"
            "- These values can differ because stage-sum combines independent stage averages, while end-to-end uses the same contact journey."
        )

st.divider()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 9 — BLOCK ACCUMULATION  (from cs — no extra query)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Block accumulation")
st.caption(
    "Historical contacts that were marked blocked at least once. "
    "DND/Invalid are treated as permanent; Allocate Again is a temporary 90-day hold."
)

if cs is not None:
    permanent_data = {
        "Invalid Number": int(cs["blk_invalid"]),
        "Do not Disturb": int(cs["blk_dnd"]),
        "Not interested": int(cs["blk_not_int"]),
        "Referred": int(cs["blk_referred"]),
        "Language issue": int(cs["blk_language"]),
    }
    temporary_data = {
        "Allocate Again after 90 days (BD)": {
            "contacts": int(cs["blk_bd_3months_all"]),
            "ready": int(cs["blk_bd_3months_ready"]),
        },
        "Allocate Again after 90 days (MQL)": {
            "contacts": int(cs["blk_mql_3months_all"]),
            "ready": int(cs["blk_mql_3months_ready"]),
        },
    }

    total_blocked  = sum(permanent_data.values()) + sum(v["contacts"] for v in temporary_data.values())
    total_contacts = int(cs["total"])
    blocked_pct    = round(total_blocked * 100 / total_contacts, 1) if total_contacts else 0

    if total_blocked > 0:
        bl1, bl2, bl3 = st.columns(3)
        bl1.metric("Total blocked contacts", total_blocked)
        bl2.metric("% of total pool",        f"{blocked_pct}%")
        bl3.metric("DND + Invalid", permanent_data["Do not Disturb"] + permanent_data["Invalid Number"])

        block_rows = []
        for label, value in permanent_data.items():
            if value <= 0:
                continue
            block_rows.append(
                {
                    "Bucket": label,
                    "Type": "Permanent",
                    "Contacts": value,
                    "Ready?": "No",
                    "% of blocked": round(value * 100 / total_blocked, 1) if total_blocked else 0,
                }
            )

        for label, payload in temporary_data.items():
            value = payload["contacts"]
            if value <= 0:
                continue
            ready_str = "Yes" if payload["ready"] > 0 else "No"
            block_rows.append(
                {
                    "Bucket": label,
                    "Type": "Temporary (90-day hold)",
                    "Contacts": value,
                    "Ready?": ready_str,
                    "% of blocked": round(value * 100 / total_blocked, 1) if total_blocked else 0,
                }
            )

        blocks_df = pd.DataFrame(block_rows).sort_values("Contacts", ascending=False)
        st.dataframe(blocks_df, use_container_width=True, hide_index=True)
    else:
        st.info("No blocked contacts yet.")


st.caption(f'Page last updated: {date.today().isoformat()} 22:15')
