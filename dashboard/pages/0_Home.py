"""
pages/0_Home.py — CRM Home
BD view  : today's dialling stats, agent cards, pool snapshot, recent wins.
MQL view : today's FU activity, agent cards, pipeline snapshot, recent SQL.
"""

import streamlit as st
import pandas as pd
from datetime import date
from utils.db import query_df
from utils.errors import log_and_warn

campaign = st.session_state.get("campaign", "consulting")
stage    = st.session_state.get("stage_select", "BD")
is_mql   = "MQL" in stage

# ══════════════════════════════════════════════════════════════════════════════
#  BD HOME
# ══════════════════════════════════════════════════════════════════════════════
if not is_mql:
    st.title("DT Consulting CRM")
    st.caption(f"Campaign: **{campaign.title()}** &nbsp;·&nbsp; {date.today().strftime('%A, %d %B %Y')}")
    st.divider()

    # ── TODAY AT A GLANCE ────────────────────────────────────────────────────
    st.subheader("Today at a glance")
    try:
        today_df = query_df("""
            SELECT
                COUNT(ca.id)                                                                    AS dialled,
                COALESCE(SUM(CASE WHEN ca.call_status = 'Connected'      THEN 1 ELSE 0 END),0) AS connected,
                COALESCE(SUM(CASE WHEN ca.call_status = 'Connected' AND COALESCE(ca.attempt_number, 0) <= 1 THEN 1 ELSE 0 END),0) AS fresh_connect,
                COALESCE(SUM(CASE WHEN COALESCE(ca.attempt_number, 0) > 1 THEN 1 ELSE 0 END),0) AS followups,
                COALESCE(SUM(CASE WHEN ca.call_status = 'Connected' AND COALESCE(ca.attempt_number, 0) > 1 THEN 1 ELSE 0 END),0) AS followup_connect,
                COALESCE(SUM(CASE WHEN ca.call_status = 'Do not Disturb' THEN 1 ELSE 0 END),0) AS dnd,
                COALESCE(SUM(CASE WHEN ca.call_status = 'Invalid Number' THEN 1 ELSE 0 END),0) AS invalid,
                COUNT(DISTINCT ca.contact_id) FILTER (
                    WHERE ca.current_state = 'Shared Story'
                )                                                                               AS stories_today,
                COUNT(DISTINCT ca.contact_id) FILTER (
                    WHERE ca.current_state IN ('Snapshot Sent','Dream Snapshot Sent')
                )                                                                               AS snapshots_today,
                COALESCE((
                    SELECT SUM(kpi_dialed) FROM agents
                    WHERE status = 'active' AND is_on_leave = FALSE
                      AND (team = 'bd' OR team IS NULL)
                ), 0)                                                                           AS target
            FROM call_actions ca
            JOIN contacts c ON c.id = ca.contact_id
            WHERE DATE(ca.called_at) = CURRENT_DATE
              AND c.campaign = %(campaign)s
        """, params={"campaign": campaign})

        r         = today_df.iloc[0] if not today_df.empty else None
        dialled   = int(r["dialled"])       if r is not None else 0
        connected = int(r["connected"])     if r is not None else 0
        fresh_connect = int(r["fresh_connect"]) if r is not None else 0
        followups = int(r["followups"]) if r is not None else 0
        followup_connect = int(r["followup_connect"]) if r is not None else 0
        dnd       = int(r["dnd"])           if r is not None else 0
        invalid   = int(r["invalid"])       if r is not None else 0
        stories   = int(r["stories_today"]) if r is not None else 0
        snapshots = int(r["snapshots_today"]) if r is not None else 0
        target    = int(r["target"])        if r is not None else 0
        conn_rate = round(connected * 100 / dialled, 1) if dialled > 0 else 0.0
        kpi_pct   = round(dialled * 100 / target, 1)   if target > 0  else 0.0

        g1, g2, g3, g4, g5, g6 = st.columns(6)
        g1.metric("Dialed", dialled, f"Target: {target}")
        g2.metric("Fresh connect (FU1)", fresh_connect)
        g3.metric("Follow-ups (FU2+)", followups)
        g4.metric("Follow-up connect (FU2+)", followup_connect)
        g5.metric("Conn %", f"{conn_rate}%")
        g6.metric("KPI", f"{kpi_pct}%")

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Stories today", stories)
        s2.metric("Dream Snapshot Sent", snapshots)
        s3.metric("DND", dnd)
        s4.metric("Invalid", invalid)

        with st.expander("Metric definitions", expanded=False):
            st.markdown(
                "- Fresh connect (FU1): Connected calls where follow-up number is FU1.  \n"
                "- Follow-up connect (FU2+): Connected calls from FU2 onward.  \n"
                "- Follow-ups (FU2+): Calls from FU2 onward.  \n"
                "- Conn %: Connected / Dialed."
            )
    except Exception as e:
        log_and_warn("BD today stats", e)

    st.divider()

    # ── BD AGENTS TODAY ──────────────────────────────────────────────────────
    st.subheader("BD agents today")
    try:
        agents_df = query_df("""
            SELECT
                a.name,
                a.shift_name,
                a.kpi_dialed,
                a.is_on_leave,
                COALESCE(k.total_dialled, 0)    AS dialled,
                COALESCE(k.connected, 0)        AS connected,
                COALESCE(k.connection_rate_pct, 0)  AS conn_rate,
                COALESCE(k.kpi_pct, 0)          AS kpi_pct,
                                COUNT(ca.id) FILTER (
                                        WHERE DATE(ca.called_at) = CURRENT_DATE
                                            AND ca.call_status = 'Connected'
                                            AND COALESCE(ca.attempt_number, 0) <= 1
                                )                               AS fresh_connect_today,
                                COUNT(ca.id) FILTER (
                                        WHERE DATE(ca.called_at) = CURRENT_DATE
                                            AND COALESCE(ca.attempt_number, 0) > 1
                                )                               AS followups_today,
                                COUNT(ca.id) FILTER (
                                        WHERE DATE(ca.called_at) = CURRENT_DATE
                                            AND ca.call_status = 'Connected'
                                            AND COALESCE(ca.attempt_number, 0) > 1
                                )                               AS followup_connect_today,
                -- Stories & snapshots today
                COUNT(ca.id) FILTER (
                    WHERE DATE(ca.called_at) = CURRENT_DATE
                      AND ca.current_state = 'Shared Story'
                )                               AS stories_today,
                COUNT(ca.id) FILTER (
                    WHERE DATE(ca.called_at) = CURRENT_DATE
                        AND ca.current_state IN ('Snapshot Sent','Dream Snapshot Sent')
                )                               AS snapshots_today
            FROM agents a
            LEFT JOIN v_agent_daily_kpi k
                   ON k.agent_id = a.id AND k.call_date = CURRENT_DATE
            LEFT JOIN call_actions ca ON ca.agent_id = a.id
            WHERE a.status = 'active'
              AND (a.team = 'bd' OR a.team IS NULL)
            GROUP BY a.id, a.name, a.shift_name, a.kpi_dialed, a.is_on_leave,
                     k.total_dialled, k.connected, k.connection_rate_pct, k.kpi_pct
            ORDER BY dialled DESC, a.name
        """)

        if agents_df.empty:
            st.info("No active BD agents.")
        else:
            for _, row in agents_df.iterrows():
                on_leave = bool(row["is_on_leave"])
                badge    = "On Leave" if on_leave else "Present"
                with st.container(border=True):
                    hc1, hc2, hc3, hc4, hc5, hc6, hc7 = st.columns([3, 1, 1, 1, 1, 1, 1])
                    hc1.markdown(f"**{row['name']}** &nbsp;&nbsp; {badge}")
                    hc2.metric("Dialled",   int(row["dialled"]),
                               f"/ {row['kpi_dialed']}")
                    hc3.metric("Fresh connect", int(row["fresh_connect_today"]))
                    hc4.metric("Follow-ups", int(row["followups_today"]))
                    hc5.metric("Follow-up connect", int(row["followup_connect_today"]),
                               delta=f"{round(float(row['conn_rate']),1)}%")
                    hc6.metric("Stories",   int(row["stories_today"]))
                    hc7.metric("Dream Snapshot Sent", int(row["snapshots_today"]))
                    if row["shift_name"]:
                        st.caption(f"Shift: {row['shift_name']}")
    except Exception as e:
        log_and_warn("BD agents", e)

    st.divider()

    # ── POOL SNAPSHOT ────────────────────────────────────────────────────────
    st.subheader("Contact pool snapshot")
    try:
        pool_df = query_df("""
            SELECT
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag = 'fresh'
                )                                                           AS fresh,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag IN ('in_progress','needs_followup')
                )                                                           AS in_progress,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag IN ('fresh','in_progress','needs_followup')
                      AND NOT EXISTS (
                          SELECT 1 FROM contact_allocations a
                          WHERE a.contact_id = c.id AND a.closed_at IS NULL
                      )
                      AND EXISTS (
                          SELECT 1 FROM contact_phones p
                          WHERE p.contact_id = c.id AND p.is_invalid = FALSE
                      )
                )                                                           AS callable,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag IN (
                        'shared_story','snapshot_sent',
                        'mql_in_progress','mql_qualified'
                    )
                )                                                           AS success,
                COUNT(DISTINCT c.id)                                        AS total
            FROM contacts c
            WHERE c.campaign = %(campaign)s
        """, params={"campaign": campaign})

        if not pool_df.empty:
            p = pool_df.iloc[0]
            total    = int(p["total"])
            callable_c = int(p["callable"])
            pct      = round(callable_c * 100 / total, 1) if total > 0 else 0
            pc1, pc2, pc3, pc4, pc5 = st.columns(5)
            pc1.metric("Fresh",          int(p["fresh"]))
            pc2.metric("In Progress",    int(p["in_progress"]))
            pc3.metric("Callable now",   callable_c,
                       f"{pct}% of total", delta_color="off")
            pc4.metric("Success (all)",  int(p["success"]))
            pc5.metric("Total contacts", total)

            if pct < 30:
                st.error("Pool below 30% — import new contacts urgently.")
            elif pct < 50:
                st.warning("Pool below 50% — plan a data import this month.")
    except Exception as e:
        log_and_warn("pool snapshot", e)

    st.divider()

    # ── RECENT WINS ──────────────────────────────────────────────────────────
    st.subheader("Recent wins — last 7 days")
    try:
        wins_df = query_df("""
            SELECT
                a.name                                                      AS agent,
                co.name                                                     AS company,
                TRIM(COALESCE(c.first_name,'') || ' ' || COALESCE(c.last_name,'')) AS person,
                ca.current_state,
                ca.called_at::date                                          AS date
            FROM call_actions ca
            JOIN contacts c  ON c.id  = ca.contact_id
            JOIN agents   a  ON a.id  = ca.agent_id
            LEFT JOIN companies co ON co.id = c.company_id
            WHERE c.campaign = %(campaign)s
              AND ca.current_state IN (
                  'Shared Story','Snapshot Sent','Snapshot Confirmed',
                  'Meeting Requested','Meeting Scheduled'
              )
              AND ca.called_at >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY ca.called_at DESC
            LIMIT 50
        """, params={"campaign": campaign})

        if wins_df.empty:
            st.info("No wins logged in the past 7 days.")
        else:
            st.dataframe(
                wins_df.rename(columns={
                    "agent":         "BD Agent",
                    "company":       "Company",
                    "person":        "Person",
                    "current_state": "Milestone",
                    "date":          "Date",
                }),
                use_container_width=True,
                hide_index=True,
                column_config={"Date": st.column_config.DateColumn("Date")},
            )
    except Exception as e:
        log_and_warn("recent wins", e)


# ══════════════════════════════════════════════════════════════════════════════
#  MQL HOME
# ══════════════════════════════════════════════════════════════════════════════
else:
    st.title("DT Consulting CRM — MQL")
    st.caption(f"Campaign: **{campaign.title()}** &nbsp;·&nbsp; {date.today().strftime('%A, %d %B %Y')}")
    st.divider()

    # ── TODAY AT A GLANCE ────────────────────────────────────────────────────
    st.subheader("MQL today at a glance")
    try:
        mql_today = query_df("""
            SELECT
                COUNT(DISTINCT mca.contact_id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE)      AS contacts_called,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE)      AS fu_attempts,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected')           AS connected,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected'
                              AND COALESCE(mca.follow_up_number, 1) <= 1)  AS fresh_connect,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND COALESCE(mca.follow_up_number, 1) > 1)   AS followups,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected'
                              AND COALESCE(mca.follow_up_number, 1) > 1)   AS followup_connect,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.current_state = 'Meeting Scheduled') AS meetings_today,
                COUNT(DISTINCT mca.contact_id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')) AS true_mql_today,
                COUNT(DISTINCT ma.contact_id)
                    FILTER (WHERE ma.closed_at IS NULL)                    AS active_total,
                (
                    SELECT COUNT(DISTINCT c2.id)
                    FROM contacts c2
                    WHERE c2.campaign = %(campaign)s
                      AND c2.contact_flag IN ('shared_story','snapshot_sent')
                      AND NOT EXISTS (
                          SELECT 1 FROM mql_allocations ma2
                          WHERE ma2.contact_id = c2.id AND ma2.closed_at IS NULL
                      )
                )                                                           AS pool_waiting
            FROM mql_allocations ma
            LEFT JOIN mql_call_attempts mca ON mca.allocation_id = ma.id
            WHERE ma.campaign = %(campaign)s
        """, params={"campaign": campaign})

        r = mql_today.iloc[0] if not mql_today.empty else None
        connected_today = int(r["connected"]) if r is not None else 0
        fu_attempts_today = int(r["fu_attempts"]) if r is not None else 0
        conn_pct = round(connected_today * 100.0 / fu_attempts_today, 1) if fu_attempts_today > 0 else 0.0
        mg1, mg2, mg3, mg4, mg5, mg6, mg7 = st.columns(7)
        mg1.metric("FU attempts today", fu_attempts_today)
        mg2.metric("Fresh connect (FU1)", int(r["fresh_connect"]) if r is not None else 0)
        mg3.metric("Follow-ups (FU2+)", int(r["followups"]) if r is not None else 0)
        mg4.metric("Follow-up connect (FU2+)", int(r["followup_connect"]) if r is not None else 0)
        mg5.metric("Conn %", f"{conn_pct}%")
        mg6.metric("Active contacts", int(r["active_total"]) if r is not None else 0)
        mg7.metric("Pool waiting",         int(r["pool_waiting"]) if r is not None else 0,
                   help="Contacts in pool not yet assigned to an MQL agent")

        ms1, ms2, ms3 = st.columns(3)
        ms1.metric("Contacts called", int(r["contacts_called"]) if r is not None else 0)
        ms2.metric("Meetings today", int(r["meetings_today"]) if r is not None else 0)
        ms3.metric("True MQL today", int(r["true_mql_today"]) if r is not None else 0)

        with st.expander("Metric definitions", expanded=False):
            st.markdown(
                "- Fresh connect (FU1): Connected calls where follow-up number is FU1.  \n"
                "- Follow-up connect (FU2+): Connected calls from FU2 onward.  \n"
                "- Follow-ups (FU2+): Calls from FU2 onward.  \n"
                "- True MQL today: Unique contacts with at least one Dream/Snapshot Confirmed today.  \n"
                "- Conn %: Connected / FU attempts."
            )
    except Exception as e:
        log_and_warn("MQL today stats", e)

    st.divider()

    # ── MQL AGENTS TODAY ─────────────────────────────────────────────────────
    st.subheader("MQL agents today")
    try:
        mql_agents = query_df("""
            SELECT
                a.name,
                a.shift_name,
                a.kpi_dialed,
                a.is_on_leave,
                COUNT(DISTINCT ma.contact_id)
                    FILTER (WHERE ma.closed_at IS NULL)                     AS active_contacts,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE)      AS fu_today,
                COUNT(DISTINCT mca.contact_id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected')           AS connected_today,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected'
                              AND COALESCE(mca.follow_up_number, 1) <= 1)  AS fresh_connect_today,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND COALESCE(mca.follow_up_number, 1) > 1)   AS followups_today,
                COUNT(mca.id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.call_status = 'Connected'
                              AND COALESCE(mca.follow_up_number, 1) > 1)   AS followup_connect_today,
                COUNT(DISTINCT mca.contact_id)
                    FILTER (WHERE mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')) AS total_true_mql,
                COUNT(DISTINCT mca.contact_id)
                    FILTER (WHERE DATE(mca.called_at) = CURRENT_DATE
                              AND mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')) AS true_mql_today
            FROM agents a
            LEFT JOIN mql_allocations ma
                   ON ma.agent_id = a.id AND ma.campaign = %(campaign)s
            LEFT JOIN mql_call_attempts mca ON mca.agent_id = a.id
            WHERE a.status = 'active' AND a.team = 'mql'
            GROUP BY a.id, a.name, a.shift_name, a.kpi_dialed, a.is_on_leave
            ORDER BY active_contacts DESC, a.name
        """, params={"campaign": campaign})

        if mql_agents.empty:
            st.info("No active MQL agents. Add agents and set team = 'mql' in the Agents page.")
        else:
            for _, row in mql_agents.iterrows():
                on_leave = bool(row["is_on_leave"])
                badge    = "On Leave" if on_leave else "Present"
                with st.container(border=True):
                    mc1, mc2, mc3, mc4, mc5, mc6, mc7, mc8 = st.columns([3, 1, 1, 1, 1, 1, 1, 1])
                    mc1.markdown(f"**{row['name']}** &nbsp;&nbsp; {badge}")
                    mc2.metric("Active",          int(row["active_contacts"]))
                    mc3.metric("FUs today",       int(row["fu_today"]))
                    mc4.metric("Fresh connect", int(row["fresh_connect_today"]))
                    mc5.metric("Follow-ups", int(row["followups_today"]))
                    mc6.metric("Follow-up connect", int(row["followup_connect_today"]))
                    mc7.metric("True MQL today", int(row["true_mql_today"]))
                    mc8.metric("Total True MQL", int(row["total_true_mql"]))
                    if row["shift_name"]:
                        st.caption(f"Shift: {row['shift_name']}")
    except Exception as e:
        log_and_warn("MQL agents", e)

    st.divider()

    # ── MQL PIPELINE SNAPSHOT ─────────────────────────────────────────────────
    st.subheader("MQL pipeline snapshot")
    try:
        mql_pipe = query_df("""
            SELECT
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag IN ('shared_story','snapshot_sent')
                      AND NOT EXISTS (
                          SELECT 1 FROM mql_allocations ma
                          WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
                      )
                )                                                           AS pool_unallocated,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag = 'mql_in_progress'
                )                                                           AS in_progress,
                (
                    SELECT COUNT(DISTINCT mca.contact_id)
                    FROM mql_call_attempts mca
                    JOIN mql_allocations ma ON ma.id = mca.allocation_id
                    WHERE mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')
                      AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                      AND ma.close_reason IS DISTINCT FROM 'bd_history'
                )                                                           AS true_mql,
                (
                    SELECT COUNT(*)
                    FROM mql_call_attempts mca
                    JOIN mql_allocations ma ON ma.id = mca.allocation_id
                    WHERE mca.current_state = 'Dream Snapshot Confirmed'
                      AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                      AND ma.close_reason IS DISTINCT FROM 'bd_history'
                )                                                           AS dream_snapshot_rows,
                COUNT(DISTINCT c.id) FILTER (
                    WHERE c.contact_flag = 'mql_rejected'
                )                                                           AS rejected,
                -- MQL conversion rate
                ROUND(
                    (
                        SELECT COUNT(DISTINCT mca.contact_id)
                        FROM mql_call_attempts mca
                        JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        WHERE mca.current_state IN ('Dream Snapshot Confirmed', 'Snapshot Confirmed')
                          AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                          AND ma.close_reason IS DISTINCT FROM 'bd_history'
                    ) * 100.0 / NULLIF(
                        COUNT(DISTINCT c.id) FILTER (
                            WHERE c.contact_flag IN (
                                'mql_qualified','mql_rejected'
                            )
                        ), 0
                    ), 1
                )                                                           AS qual_rate
            FROM contacts c
            WHERE c.campaign = %(campaign)s
        """, params={"campaign": campaign, "campaign_like": f"{campaign} %"})

        if not mql_pipe.empty:
            p = mql_pipe.iloc[0]
            pp1, pp2, pp3, pp4, pp5, pp6 = st.columns(6)
            pp1.metric("Pool (waiting)",  int(p["pool_unallocated"]))
            pp2.metric("In Progress",     int(p["in_progress"]))
            pp3.metric("True MQL",        int(p["true_mql"]),
                       help="Unique contacts with at least one Dream/Snapshot Confirmed call")
            pp4.metric("Dream Snapshot rows", int(p["dream_snapshot_rows"]),
                       help="Total MQL call rows where Current State = Dream Snapshot Confirmed")
            pp5.metric("Rejected",         int(p["rejected"]))
            pp6.metric("Qual Rate",
                       f"{p['qual_rate']}%" if p["qual_rate"] else "—",
                       help="% of closed MQL allocations that reached True MQL")
    except Exception as e:
        log_and_warn("MQL pipeline snapshot", e)

    st.divider()

    # ── RECENT SQL WINS ───────────────────────────────────────────────────────
    st.subheader("Recent SQL — last 7 days")
    try:
        sql_wins = query_df("""
            SELECT
                a.name                                                      AS mql_agent,
                co.name                                                     AS company,
                TRIM(COALESCE(c.first_name,'') || ' ' || COALESCE(c.last_name,'')) AS person,
                mx.bd_agent_name                                            AS bd_agent,
                mx.outcome_date::date                                       AS qualified_date,
                mx.last_follow_up                                           AS fu_depth
            FROM mql_analysis mx
            JOIN contacts c      ON c.id = mx.contact_id
            LEFT JOIN companies co ON co.id = c.company_id
            JOIN agents a        ON a.id = mx.agent_id
            WHERE mx.outcome = 'sql'
              AND c.campaign  = %(campaign)s
              AND mx.outcome_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY mx.outcome_date DESC
            LIMIT 30
        """, params={"campaign": campaign})

        if sql_wins.empty:
            st.info("No SQL wins in the past 7 days yet.")
        else:
            st.success(f"{len(sql_wins)} SQL contacts qualified in the last 7 days!")
            st.dataframe(
                sql_wins.rename(columns={
                    "mql_agent":      "MQL Agent",
                    "company":        "Company",
                    "person":         "Person",
                    "bd_agent":       "BD Agent",
                    "qualified_date": "Qualified",
                    "fu_depth":       "FUs Taken",
                }),
                use_container_width=True,
                hide_index=True,
                column_config={"Qualified": st.column_config.DateColumn("Qualified")},
            )
    except Exception as e:
        log_and_warn("recent SQL wins", e)
