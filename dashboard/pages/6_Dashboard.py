"""
pages/6_Dashboard.py — Unified Performance Dashboard

Combines Daily / Weekly / Monthly in three tabs.

DB optimisation
───────────────
• Agents list — cached once, shared across all tabs.
• Daily tab   → ONE query; pandas computes per-agent + call-status + current-status charts.
• Weekly tab  → ONE query; pandas computes summary + day-by-day pivot.
• Monthly tab → reuses same _load_period_raw(); pandas computes summary + week/day pivots.

Metrics shown
─────────────
Call Status   : Connected · DND · Call Back Later · Invalid · Referred · Did not connect
Current Status: Shared Story · Interested · Snapshot (sent + confirmed) · Meeting (req + sched)

Error handling
──────────────
Full tracebacks printed to console; friendly messages + collapsible details shown in UI.
"""

from __future__ import annotations

import calendar
import logging
import traceback
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from utils.db import query_df
from utils.errors import log_and_show, log_and_warn

logger = logging.getLogger("crm.dashboard")

campaign = st.session_state.get("campaign", "consulting")
st.title("Performance Dashboard")
st.caption(f"Campaign: **{campaign.title()}**")


# ══════════════════════════════════════════════════════════════════════════════
# CURRENT-STATE GROUPINGS
# ══════════════════════════════════════════════════════════════════════════════

_SNAPSHOT_STATES  = ["Snapshot sent", "Snapshot Sent", "Dream Snapshot Sent"]
_MEETING_STATES   = ["Meeting Requested", "Meeting Scheduled"]


# ══════════════════════════════════════════════════════════════════════════════
# CACHED DATA LOADERS
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=120, show_spinner=False)
def _load_agents() -> list[str]:
    """Return active agent names. Cached 2 min — shared by all tabs."""
    return query_df(
        "SELECT name FROM agents WHERE status = 'active' ORDER BY name"
    )["name"].tolist()


@st.cache_data(ttl=60, show_spinner="Loading daily data…")
def _load_daily_raw(sel_date: str, camp: str) -> pd.DataFrame:
    """ONE query — all call_actions for *sel_date* + *camp*.

    Columns: agent_id, agent_name, kpi_target,
             call_status, current_state, call_duration
    """
    return query_df(
        """
        SELECT
            a.id                            AS agent_id,
            a.name                          AS agent_name,
            a.kpi_dialed                    AS kpi_target,
            COALESCE(ca.call_status,    '') AS call_status,
            COALESCE(ca.current_state,  '') AS current_state,
            COALESCE(ca.call_duration,   0) AS call_duration,
            COALESCE(ca.attempt_number, 0) AS follow_up_number
        FROM call_actions ca
        JOIN agents   a ON a.id  = ca.agent_id
        JOIN contacts c ON c.id  = ca.contact_id
        WHERE DATE(ca.called_at) = %s
          AND c.campaign          = %s
        """,
        params=[sel_date, camp],
    )


@st.cache_data(ttl=60, show_spinner="Loading data…")
def _load_period_raw(
    start: str,
    end: str,
    camp: str,
    agents_tuple: tuple,
) -> pd.DataFrame:
    """ONE query — all call_actions for *start*→*end* + *camp*.

    Columns: call_date, agent_name, call_status, current_state, call_duration
    Used by both Weekly and Monthly tabs.
    """
    agent_filter_sql: str = ""
    agent_filter_params: list = []
    if agents_tuple:
        placeholders = ", ".join(["%s"] * len(agents_tuple))
        agent_filter_sql = f"AND a.name IN ({placeholders})"
        agent_filter_params = list(agents_tuple)

    return query_df(
        f"""
        SELECT
            DATE(ca.called_at)              AS call_date,
            a.name                          AS agent_name,
            COALESCE(ca.call_status,    '') AS call_status,
            COALESCE(ca.current_state,  '') AS current_state,
            COALESCE(ca.call_duration,   0) AS call_duration,
            COALESCE(ca.attempt_number, 0) AS follow_up_number
        FROM call_actions ca
        JOIN agents   a ON a.id  = ca.agent_id
        JOIN contacts c ON c.id  = ca.contact_id
        WHERE ca.called_at BETWEEN %s AND %s
          AND c.campaign = %s
        {agent_filter_sql}
        """,
        params=[start, end + " 23:59:59", camp] + agent_filter_params,
    )


# ══════════════════════════════════════════════════════════════════════════════
# PANDAS AGGREGATION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _flag_call_status(d: pd.DataFrame) -> pd.DataFrame:
    """Add boolean int columns for every call_status value."""
    d["_connected"] = (d["call_status"] == "Connected").astype(int)
    d["_dnd"]       = (d["call_status"] == "Do not Disturb").astype(int)
    d["_cbl"]       = (d["call_status"] == "Call back later").astype(int)
    d["_invalid"]   = (d["call_status"] == "Invalid Number").astype(int)
    d["_referred"]  = (d["call_status"] == "Referred").astype(int)
    d["_dnc"]       = (d["call_status"] == "Did not connect").astype(int)
    d["_fresh_connect"] = ((d["follow_up_number"] <= 1) & (d["call_status"] == "Connected")).astype(int)
    d["_followup_connect"] = ((d["follow_up_number"] > 1) & (d["call_status"] == "Connected")).astype(int)
    d["_fu_attempt"] = (d["follow_up_number"] > 1).astype(int)
    return d


def _flag_current_state(d: pd.DataFrame) -> pd.DataFrame:
    """Add boolean int columns for current_state groups."""
    d["_shared_story"] = (d["current_state"] == "Shared Story").astype(int)
    d["_interested"]   = (d["current_state"] == "Interested").astype(int)
    d["_snapshot"]     = d["current_state"].isin(_SNAPSHOT_STATES).astype(int)
    d["_meeting"]      = d["current_state"].isin(_MEETING_STATES).astype(int)
    return d


def _avg_duration(d: pd.DataFrame, group_col: str | list) -> pd.DataFrame:
    """Compute avg_duration_mins per group (only for calls with duration > 0)."""
    dur = (
        d[d["call_duration"] > 0]
        .groupby(group_col)["call_duration"]
        .mean()
        .reset_index()
    )
    dur["avg_duration_mins"] = (dur["call_duration"] / 60.0).round(1)
    key = group_col if isinstance(group_col, str) else group_col[0]
    return dur[[key, "avg_duration_mins"]]


def _compute_agent_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw call_actions into one row per agent.

    Call Status  : total_dialled, connected, dnd, cbl, invalid, referred, dnc
    Current State: shared_story, interested, snapshot, meeting
    Performance  : connection_rate_pct, avg_duration_mins
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "agent_name", "total_dialled",
            "fresh_connect", "fu_attempts", "followup_connect",
            "connected", "dnd", "cbl", "invalid", "referred", "dnc",
            "shared_story", "interested", "snapshot", "meeting",
            "connection_rate_pct", "avg_duration_mins",
        ])

    d = _flag_call_status(df.copy())
    d = _flag_current_state(d)

    agg = d.groupby("agent_name").agg(
        total_dialled = ("call_status",    "count"),
        fresh_connect = ("_fresh_connect", "sum"),
        fu_attempts   = ("_fu_attempt",    "sum"),
        followup_connect = ("_followup_connect", "sum"),
        connected     = ("_connected",     "sum"),
        dnd           = ("_dnd",           "sum"),
        cbl           = ("_cbl",           "sum"),
        invalid       = ("_invalid",       "sum"),
        referred      = ("_referred",      "sum"),
        dnc           = ("_dnc",           "sum"),
        shared_story  = ("_shared_story",  "sum"),
        interested    = ("_interested",    "sum"),
        snapshot      = ("_snapshot",      "sum"),
        meeting       = ("_meeting",       "sum"),
    ).reset_index()

    dur = _avg_duration(d, "agent_name")
    agg = agg.merge(dur, on="agent_name", how="left")
    agg["avg_duration_mins"] = agg["avg_duration_mins"].fillna(0.0)
    agg["connection_rate_pct"] = (
        agg["connected"] * 100.0 / agg["total_dialled"].clip(lower=1)
    ).round(1)

    return agg.sort_values("total_dialled", ascending=False).reset_index(drop=True)


def _compute_daily_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily raw data per agent (includes kpi_target, kpi_pct)."""
    if df.empty:
        return pd.DataFrame()

    d = _flag_call_status(df.copy())
    d = _flag_current_state(d)

    agg = d.groupby(["agent_id", "agent_name", "kpi_target"]).agg(
        total_dialled = ("call_status",    "count"),
        fresh_connect = ("_fresh_connect", "sum"),
        fu_attempts   = ("_fu_attempt",    "sum"),
        followup_connect = ("_followup_connect", "sum"),
        connected     = ("_connected",     "sum"),
        dnd           = ("_dnd",           "sum"),
        cbl           = ("_cbl",           "sum"),
        invalid       = ("_invalid",       "sum"),
        referred      = ("_referred",      "sum"),
        dnc           = ("_dnc",           "sum"),
        shared_story  = ("_shared_story",  "sum"),
        interested    = ("_interested",    "sum"),
        snapshot      = ("_snapshot",      "sum"),
        meeting       = ("_meeting",       "sum"),
    ).reset_index()

    dur = _avg_duration(d, "agent_id")
    agg = agg.merge(dur, on="agent_id", how="left")
    agg["avg_duration_mins"]   = agg["avg_duration_mins"].fillna(0.0)
    agg["kpi_pct"]             = (agg["total_dialled"] * 100.0 / agg["kpi_target"].clip(lower=1)).round(1)
    agg["connection_rate_pct"] = (agg["connected"] * 100.0 / agg["total_dialled"].clip(lower=1)).round(1)

    return agg.sort_values("total_dialled", ascending=False).reset_index(drop=True)


def _render_metric_definitions() -> None:
    with st.expander("Metric definitions", expanded=False):
        st.markdown(
            "- Fresh connect (FU1): Connected calls where follow-up number is FU1.  \n"
            "- Follow-up connect (FU2+): Connected calls from FU2 onward.  \n"
            "- Follow-ups (FU2+): Calls from FU2 onward.  \n"
            "- Conn %: Connected / Dialed."
        )


# ══════════════════════════════════════════════════════════════════════════════
# SHARED DATA
# ══════════════════════════════════════════════════════════════════════════════

try:
    agent_names = _load_agents()
except Exception as e:
    log_and_show("agent list", e)
    agent_names = []


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════

tab_daily, tab_weekly, tab_monthly = st.tabs(["Daily", "Weekly", "Monthly"])


# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 — DAILY
# ──────────────────────────────────────────────────────────────────────────────
with tab_daily:
    selected_date = st.date_input("Date", value=date.today(), key="daily_date")
    st.divider()

    try:
        raw_d = _load_daily_raw(str(selected_date), campaign)

        if raw_d.empty:
            st.info(
                f"No call data for **{selected_date}**. "
                "Either no calls were made or the nightly sync hasn't run yet."
            )
        else:
            agg = _compute_daily_agg(raw_d)

            # ── Summary metrics (call status) ────────────────────────────────
            st.subheader("Summary")
            total_d = int(agg["total_dialled"].sum())
            total_c = int(agg["connected"].sum())
            total_fu_connect = int(agg["followup_connect"].sum())
            conn_pct = round(total_c * 100 / total_d, 1) if total_d > 0 else 0.0

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Fresh connect (FU1)", int(agg["fresh_connect"].sum()))
            c2.metric("Follow-ups (FU2+)", int(agg["fu_attempts"].sum()))
            c3.metric("Follow-up connect (FU2+)", total_fu_connect)
            c4.metric("Avg connection %", f"{conn_pct}%")
            c5.metric("DND", int(agg["dnd"].sum()))
            c6.metric("Invalid", int(agg["invalid"].sum()))

            # ── Current state summary ────────────────────────────────────────
            s1, s2, s3, s4 = st.columns(4)
            s1.metric("Shared Story",  int(agg["shared_story"].sum()))
            s2.metric("Interested",    int(agg["interested"].sum()))
            s3.metric("Dream Snapshot Sent", int(agg["snapshot"].sum()))
            s4.metric("Meetings",      int(agg["meeting"].sum()))

            _render_metric_definitions()

            st.divider()

            # ── Per-agent table ──────────────────────────────────────────────
            st.subheader("Per agent")
            display = agg[[
                "agent_name", "total_dialled", "fresh_connect", "fu_attempts", "followup_connect", "kpi_target", "kpi_pct",
                "connection_rate_pct", "dnd", "cbl",
                "invalid", "referred", "dnc",
                "shared_story", "interested", "snapshot", "meeting",
            ]].rename(columns={
                "agent_name":           "Agent",
                "total_dialled":        "Dialed",
                "fresh_connect":        "Fresh connect (FU1)",
                "fu_attempts":          "Follow-ups (FU2+)",
                "followup_connect":     "Follow-up Connect",
                "kpi_target":           "KPI Target",
                "kpi_pct":              "KPI %",
                "connection_rate_pct":  "Conn %",
                "dnd":                  "DND",
                "cbl":                  "Call Back",
                "invalid":              "Invalid",
                "referred":             "Referred",
                "dnc":                  "Did Not Connect",
                "shared_story":         "Shared Story",
                "interested":           "Interested",
                "snapshot":             "Dream Snapshot Sent",
                "meeting":              "Meeting",
            })
            st.dataframe(display, use_container_width=True, hide_index=True)

            st.divider()

            # ── Per-agent cards ──────────────────────────────────────────────
            st.subheader("Agent breakdown")
            for _, row in agg.iterrows():
                with st.container(border=True):
                    st.markdown(
                        f"**{row['agent_name']}** "
                        f"&nbsp;·&nbsp; KPI {int(row['total_dialled'])}/{int(row['kpi_target'])} "
                        f"({row['kpi_pct']}%)"
                    )
                    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
                    c1.metric("Fresh connect", int(row["fresh_connect"]))
                    c2.metric("Follow-ups", int(row["fu_attempts"]))
                    c3.metric("Follow-up connect", int(row["followup_connect"]),
                              delta=f"{row['connection_rate_pct']}%")
                    c4.metric("DND",           int(row["dnd"]))
                    c5.metric("Shared Story",  int(row["shared_story"]))
                    c6.metric("Interested",    int(row["interested"]))
                    c7.metric("Meeting",       int(row["meeting"]))

            st.divider()

            # ── Call status chart ────────────────────────────────────────────
            st.subheader("Call status breakdown")
            status_counts = (
                raw_d[raw_d["call_status"] != ""]
                ["call_status"].value_counts()
                .rename_axis("call_status").reset_index(name="count")
            )
            if not status_counts.empty:
                status_chart = (
                    alt.Chart(status_counts)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                    .encode(
                        x=alt.X("count:Q", title="Calls", axis=alt.Axis(tickMinStep=1)),
                        y=alt.Y("call_status:N", sort="-x", title=""),
                        color=alt.Color(
                            "call_status:N",
                            scale=alt.Scale(
                                domain=["Connected", "Did not connect", "Do not Disturb",
                                        "Call back later", "Invalid Number", "Referred"],
                                range=["#22c55e", "#94a3b8", "#ef4444",
                                       "#f59e0b", "#f97316", "#a855f7"],
                            ),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip("call_status:N", title="Status"),
                            alt.Tooltip("count:Q",       title="Count"),
                        ],
                    )
                    .properties(height=max(180, len(status_counts) * 40))
                )
                st.altair_chart(status_chart, use_container_width=True)

            st.divider()

            # ── Current status chart ─────────────────────────────────────────
            st.subheader("Current state breakdown")
            state_counts = (
                raw_d[raw_d["current_state"] != ""]
                ["current_state"].value_counts()
                .rename_axis("current_state").reset_index(name="count")
            )
            if not state_counts.empty:
                state_chart = (
                    alt.Chart(state_counts)
                    .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
                    .encode(
                        x=alt.X("count:Q", title="Calls", axis=alt.Axis(tickMinStep=1)),
                        y=alt.Y("current_state:N", sort="-x", title=""),
                        color=alt.value("#6366f1"),
                        tooltip=[
                            alt.Tooltip("current_state:N", title="State"),
                            alt.Tooltip("count:Q",         title="Count"),
                        ],
                    )
                    .properties(height=max(180, len(state_counts) * 40))
                )
                st.altair_chart(state_chart, use_container_width=True)
            else:
                st.info("No current state data for this date.")

    except Exception as e:
        log_and_show("daily dashboard", e)


# ──────────────────────────────────────────────────────────────────────────────
# TAB 2 — WEEKLY
# ──────────────────────────────────────────────────────────────────────────────
with tab_weekly:
    col1, col2, col3 = st.columns([2, 2, 4])
    week_end   = col1.date_input("Week ending (Sunday)", value=date.today(), key="week_end")
    week_start = week_end - timedelta(days=6)

    selected_agents_w = col2.multiselect(
        "Filter by agent", options=agent_names, default=[],
        placeholder="All agents", key="weekly_agents",
    )
    col3.caption(
        f"{week_start.strftime('%d %b')} → {week_end.strftime('%d %b %Y')}"
        + (f"  ·  Agents: {', '.join(selected_agents_w)}" if selected_agents_w else "  ·  All agents")
    )
    st.divider()

    try:
        raw_w = _load_period_raw(
            str(week_start), str(week_end), campaign, tuple(selected_agents_w)
        )

        if raw_w.empty:
            st.info(f"No data for this week ({week_start} → {week_end}).")
        else:
            weekly = _compute_agent_summary(raw_w)

            # ── Summary metrics ──────────────────────────────────────────────
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Fresh connect (FU1)", int(weekly["fresh_connect"].sum()))
            c2.metric("Follow-ups (FU2+)", int(weekly["fu_attempts"].sum()))
            c3.metric("Follow-up connect (FU2+)", int(weekly["followup_connect"].sum()))
            c4.metric("Shared Story", int(weekly["shared_story"].sum()))
            c5.metric("Dream Snapshot Sent", int(weekly["snapshot"].sum()))
            c6.metric("Meetings", int(weekly["meeting"].sum()))

            _render_metric_definitions()

            st.divider()

            # ── Per-agent cards ──────────────────────────────────────────────
            for _, row in weekly.iterrows():
                with st.container(border=True):
                    st.markdown(
                        f"**{row['agent_name']}**  "
                        f"&nbsp;·&nbsp; {int(row['total_dialled'])} dialled "
                        f"&nbsp;·&nbsp; {row['connection_rate_pct']}% connection rate"
                    )
                    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
                    c1.metric("Fresh connect", int(row["fresh_connect"]))
                    c2.metric("Follow-ups", int(row["fu_attempts"]))
                    c3.metric("Follow-up connect", int(row["followup_connect"]))
                    c4.metric("DND", int(row["dnd"]))
                    c5.metric("Invalid", int(row["invalid"]))
                    c6.metric("Shared Story", int(row["shared_story"]))
                    c7.metric("Meeting", int(row["meeting"]))

            st.divider()

            # ── Full table ───────────────────────────────────────────────────
            st.subheader("Full table")
            weekly_display = weekly[[
                "agent_name", "total_dialled", "fresh_connect", "fu_attempts", "followup_connect",
                "connection_rate_pct", "dnd", "cbl", "invalid", "referred", "dnc",
                "shared_story", "interested", "snapshot", "meeting",
            ]]
            st.dataframe(
                weekly_display.rename(columns={
                    "agent_name":          "Agent",
                    "total_dialled":       "Dialed",
                    "fresh_connect":       "Fresh connect (FU1)",
                    "fu_attempts":         "Follow-ups (FU2+)",
                    "followup_connect":    "Follow-up Connect",
                    "dnd":                 "DND",
                    "cbl":                 "Call Back",
                    "invalid":             "Invalid",
                    "referred":            "Referred",
                    "dnc":                 "Did Not Connect",
                    "shared_story":        "Shared Story",
                    "interested":          "Interested",
                    "snapshot":            "Dream Snapshot Sent",
                    "meeting":             "Meeting",
                    "connection_rate_pct": "Conn %",
                }),
                use_container_width=True,
                hide_index=True,
            )

            st.divider()

            # ── Day-by-day pivot ─────────────────────────────────────────────
            st.subheader("Day-by-day breakdown")
            try:
                pivot = raw_w.groupby(["call_date", "agent_name"]).size().unstack(fill_value=0)
                st.dataframe(pivot, use_container_width=True)
                st.caption("Calls dialled per agent per day.")
            except Exception as e:
                log_and_warn("day-by-day pivot", e)

    except Exception as e:
        log_and_show("weekly dashboard", e)


# ──────────────────────────────────────────────────────────────────────────────
# TAB 3 — MONTHLY
# ──────────────────────────────────────────────────────────────────────────────
with tab_monthly:
    col1, col2, col3 = st.columns([1, 1, 4])
    today     = date.today()
    sel_year  = col1.selectbox(
        "Year", list(range(today.year, today.year - 4, -1)), index=0, key="m_year"
    )
    sel_month = col2.selectbox(
        "Month", list(range(1, 13)),
        index=today.month - 1,
        format_func=lambda m: calendar.month_name[m],
        key="m_month",
    )
    month_start = date(sel_year, sel_month, 1)
    month_end   = date(sel_year, sel_month, calendar.monthrange(sel_year, sel_month)[1])

    selected_agents_m = col3.multiselect(
        "Filter by agent", options=agent_names, default=[],
        placeholder="All agents", key="monthly_agents",
    )
    col3.caption(
        f"{calendar.month_name[sel_month]} {sel_year}  "
        f"({month_start.strftime('%d %b')} → {month_end.strftime('%d %b %Y')})"
        + (f"  ·  {', '.join(selected_agents_m)}" if selected_agents_m else "  ·  All agents")
    )
    st.divider()

    try:
        raw_m = _load_period_raw(
            str(month_start), str(month_end), campaign, tuple(selected_agents_m)
        )

        if raw_m.empty:
            st.info(f"No call data for {calendar.month_name[sel_month]} {sel_year}.")
        else:
            monthly = _compute_agent_summary(raw_m)

            # ── Summary metrics ──────────────────────────────────────────────
            total_d = int(monthly["total_dialled"].sum())
            total_c = int(monthly["connected"].sum())
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Fresh connect (FU1)", int(monthly["fresh_connect"].sum()))
            c2.metric("Follow-ups (FU2+)", int(monthly["fu_attempts"].sum()))
            c3.metric("Follow-up connect (FU2+)", int(monthly["followup_connect"].sum()))
            c4.metric("Shared Story", int(monthly["shared_story"].sum()))
            c5.metric("Meetings", int(monthly["meeting"].sum()))
            c6.metric("Avg conn %",
                      f"{round(total_c * 100 / total_d, 1) if total_d else 0}%")

            _render_metric_definitions()

            st.divider()

            # ── Per-agent cards ──────────────────────────────────────────────
            for _, row in monthly.iterrows():
                with st.container(border=True):
                    st.markdown(
                        f"**{row['agent_name']}**  "
                        f"&nbsp;·&nbsp; {int(row['total_dialled'])} dialled "
                        f"&nbsp;·&nbsp; {row['connection_rate_pct']}% connection rate"
                    )
                    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
                    c1.metric("Fresh connect", int(row["fresh_connect"]))
                    c2.metric("Follow-ups", int(row["fu_attempts"]))
                    c3.metric("Follow-up connect", int(row["followup_connect"]))
                    c4.metric("DND", int(row["dnd"]))
                    c5.metric("Invalid", int(row["invalid"]))
                    c6.metric("Shared Story", int(row["shared_story"]))
                    c7.metric("Meeting", int(row["meeting"]))

            st.divider()

            # ── Full table ───────────────────────────────────────────────────
            st.subheader("Full table")
            monthly_display = monthly[[
                "agent_name", "total_dialled", "fresh_connect", "fu_attempts", "followup_connect",
                "connection_rate_pct", "dnd", "cbl", "invalid", "referred", "dnc",
                "shared_story", "interested", "snapshot", "meeting",
            ]]
            st.dataframe(
                monthly_display.rename(columns={
                    "agent_name":          "Agent",
                    "total_dialled":       "Dialed",
                    "fresh_connect":       "Fresh connect (FU1)",
                    "fu_attempts":         "Follow-ups (FU2+)",
                    "followup_connect":    "Follow-up Connect",
                    "dnd":                 "DND",
                    "cbl":                 "Call Back",
                    "invalid":             "Invalid",
                    "referred":            "Referred",
                    "dnc":                 "Did Not Connect",
                    "shared_story":        "Shared Story",
                    "interested":          "Interested",
                    "snapshot":            "Dream Snapshot Sent",
                    "meeting":             "Meeting",
                    "connection_rate_pct": "Conn %",
                }),
                use_container_width=True,
                hide_index=True,
            )

            st.divider()

            # ── Week-by-week pivot ───────────────────────────────────────────
            st.subheader("Week-by-week breakdown")
            try:
                rm = raw_m.copy()
                rm["week_start"] = rm["call_date"].apply(
                    lambda d: d - timedelta(days=d.weekday())
                )
                rm["week_label"] = rm["week_start"].apply(
                    lambda d: f"W/C {d.strftime('%d %b')}"
                )
                pivot_w = rm.groupby(["week_label", "agent_name"]).size().unstack(fill_value=0)
                if not pivot_w.empty:
                    st.dataframe(pivot_w, use_container_width=True)
                    st.caption("Calls dialled per agent per week (week commencing Monday).")
                else:
                    st.info("No data for week breakdown.")
            except Exception as e:
                log_and_warn("week-by-week pivot", e)

            st.divider()

            # ── Day-by-day pivot ─────────────────────────────────────────────
            st.subheader("Day-by-day breakdown")
            try:
                pivot_d = raw_m.groupby(["call_date", "agent_name"]).size().unstack(fill_value=0)
                if not pivot_d.empty:
                    st.dataframe(pivot_d, use_container_width=True)
                    st.caption("Calls dialled per agent per day.")
                else:
                    st.info("No data for daily breakdown.")
            except Exception as e:
                log_and_warn("day-by-day pivot", e)

    except Exception as e:
        log_and_show("monthly dashboard", e)
