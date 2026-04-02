"""
pages/12_MQL_Dashboard.py — MQL Performance Dashboard

Designed to mirror the structure of pages/6_Dashboard.py:
  - Daily tab
  - Weekly tab
  - Monthly tab

Focus:
  - MQL call attempts and outcomes
  - Per-agent performance
  - Closure outcomes (qualified/rejected/stalled)

Definitions:
  - Stalled: contact reached FU30 without qualifying or being rejected.
  - Qualification rate: qualified / (qualified + rejected + stalled).
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from utils.db import query_df
from utils.errors import log_and_show


campaign = st.session_state.get("campaign", "consulting")
st.title("MQL Performance Dashboard")
st.caption(f"Campaign: **{campaign.title()}**")


@st.cache_data(ttl=120, show_spinner=False)
def _load_mql_agents() -> list[str]:
    df = query_df(
        """
        SELECT name
        FROM agents
        WHERE status = 'active' AND team = 'mql'
        ORDER BY name
        """
    )
    return df["name"].tolist() if not df.empty else []


@st.cache_data(ttl=60, show_spinner="Loading daily MQL data...")
def _load_daily_raw(sel_date: str, camp: str) -> pd.DataFrame:
    camp_like = f"{camp.lower()} %"
    return query_df(
        """
        SELECT
            a.id                              AS agent_id,
            a.name                            AS agent_name,
            mca.contact_id                    AS contact_id,
            a.kpi_dialed                      AS kpi_target,
            COALESCE(mca.lead_category, '')   AS lead_category,
            COALESCE(mca.call_status,   '')   AS call_status,
            COALESCE(mca.current_state, '')   AS current_state,
            COALESCE(mca.call_duration, 0)    AS call_duration,
            COALESCE(mca.follow_up_number, 0) AS follow_up_number,
            COALESCE(ma.close_reason,   '')   AS close_reason,
            DATE(mca.called_at)               AS call_date
        FROM mql_call_attempts mca
        JOIN mql_allocations ma ON ma.id = mca.allocation_id
        JOIN agents a           ON a.id  = mca.agent_id
        WHERE DATE(mca.called_at) = %s
                    AND (LOWER(ma.campaign) = %s OR LOWER(ma.campaign) LIKE %s)
          AND ma.close_reason IS DISTINCT FROM 'bd_history'
        """,
                params=[sel_date, camp.lower(), camp_like],
    )


@st.cache_data(ttl=60, show_spinner="Loading MQL period data...")
def _load_period_raw(start: str, end: str, camp: str, agents_tuple: tuple) -> pd.DataFrame:
    agent_filter_sql = ""
    agent_params = []
    camp_like = f"{camp.lower()} %"
    if agents_tuple:
        placeholders = ", ".join(["%s"] * len(agents_tuple))
        agent_filter_sql = f" AND a.name IN ({placeholders})"
        agent_params = list(agents_tuple)

    return query_df(
        f"""
        SELECT
            DATE(mca.called_at)               AS call_date,
            a.name                            AS agent_name,
            mca.contact_id                    AS contact_id,
            COALESCE(mca.lead_category, '')   AS lead_category,
            COALESCE(mca.call_status,   '')   AS call_status,
            COALESCE(mca.current_state, '')   AS current_state,
            COALESCE(mca.call_duration, 0)    AS call_duration,
            COALESCE(mca.follow_up_number, 0) AS follow_up_number,
            COALESCE(ma.close_reason,   '')   AS close_reason
        FROM mql_call_attempts mca
        JOIN mql_allocations ma ON ma.id = mca.allocation_id
        JOIN agents a           ON a.id  = mca.agent_id
        WHERE mca.called_at BETWEEN %s AND %s
                    AND (LOWER(ma.campaign) = %s OR LOWER(ma.campaign) LIKE %s)
          AND ma.close_reason IS DISTINCT FROM 'bd_history'
          {agent_filter_sql}
        """,
                params=[start, end + " 23:59:59", camp.lower(), camp_like] + agent_params,
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_closure_period(start: str, end: str, camp: str, agents_tuple: tuple) -> pd.DataFrame:
    agent_filter_sql = ""
    agent_params = []
    camp_like = f"{camp.lower()} %"
    if agents_tuple:
        placeholders = ", ".join(["%s"] * len(agents_tuple))
        agent_filter_sql = f" AND a.name IN ({placeholders})"
        agent_params = list(agents_tuple)

    return query_df(
        f"""
        SELECT
            DATE(ma.closed_at) AS close_date,
            a.name             AS agent_name,
            ma.close_reason
        FROM mql_allocations ma
        JOIN agents a ON a.id = ma.agent_id
        WHERE ma.closed_at::date BETWEEN %s AND %s
                    AND (LOWER(ma.campaign) = %s OR LOWER(ma.campaign) LIKE %s)
          AND ma.close_reason IN ('qualified', 'rejected', 'stalled')
          {agent_filter_sql}
        """,
                params=[start, end, camp.lower(), camp_like] + agent_params,
    )


def _flag_call_status(d: pd.DataFrame) -> pd.DataFrame:
    d["_connected"] = (d["call_status"] == "Connected").astype(int)
    d["_dnc"] = (d["call_status"] == "Did not connect").astype(int)
    d["_dnd"] = (d["call_status"] == "Do not Disturb").astype(int)
    d["_cbl"] = (d["call_status"] == "Call back later").astype(int)
    d["_invalid"] = (d["call_status"] == "Invalid Number").astype(int)
    d["_not_interested"] = (d["call_status"] == "Not interested").astype(int)
    d["_fresh_connect"] = ((d["follow_up_number"] <= 1) & (d["call_status"] == "Connected")).astype(int)
    d["_followup_connect"] = ((d["follow_up_number"] > 1) & (d["call_status"] == "Connected")).astype(int)
    d["_fu_attempt"] = (d["follow_up_number"] > 1).astype(int)
    return d


def _flag_current_state(d: pd.DataFrame) -> pd.DataFrame:
    d["_attempt_again"] = d["current_state"].isin(["Attempt Again", "Rescheduled"]).astype(int)
    d["_snapshot"] = d["current_state"].isin(["Dream Snapshot Confirmed", "Snapshot Confirmed"]).astype(int)
    d["_meeting"] = d["current_state"].isin(["Meeting Requested", "Meeting Scheduled"]).astype(int)
    d["_escalate"] = (d["current_state"] == "Escalate").astype(int)
    return d


def _non_blank_values(series: pd.Series | None) -> list[str]:
    if series is None:
        return []
    values = {
        str(v).strip()
        for v in series.dropna().tolist()
        if str(v).strip()
    }
    return sorted(values)


def _canonical_filter_value(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if field == "lead_category":
        low = text.lower()
        if low in {"hot", "warm", "cold"}:
            return low.title()
    return text


def _apply_attempt_filters(df: pd.DataFrame, key_prefix: str) -> pd.DataFrame:
    if df.empty:
        return df

    work = df.copy()
    if "lead_category" in work.columns:
        work["lead_category_norm"] = work["lead_category"].apply(
            lambda v: _canonical_filter_value(v, "lead_category")
        )
    if "call_status" in work.columns:
        work["call_status_norm"] = work["call_status"].apply(
            lambda v: _canonical_filter_value(v, "call_status")
        )
    if "current_state" in work.columns:
        work["current_state_norm"] = work["current_state"].apply(
            lambda v: _canonical_filter_value(v, "current_state")
        )

    cat_opts = _non_blank_values(work.get("lead_category_norm"))
    status_opts = _non_blank_values(work.get("call_status_norm"))
    state_opts = _non_blank_values(work.get("current_state_norm"))

    f1, f2, f3 = st.columns(3)
    selected_cats = f1.multiselect(
        "Lead Category",
        cat_opts,
        default=cat_opts,
        key=f"{key_prefix}_lead_category",
    )
    selected_statuses = f2.multiselect(
        "Call Status",
        status_opts,
        default=status_opts,
        key=f"{key_prefix}_call_status",
    )
    selected_states = f3.multiselect(
        "Current State",
        state_opts,
        default=state_opts,
        key=f"{key_prefix}_current_state",
    )

    filtered = work.copy()
    if cat_opts:
        filtered = filtered[filtered["lead_category_norm"].isin(selected_cats)]
    if status_opts:
        filtered = filtered[filtered["call_status_norm"].isin(selected_statuses)]
    if state_opts:
        filtered = filtered[filtered["current_state_norm"].isin(selected_states)]
    return filtered


def _compute_agent_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    d = _flag_call_status(df.copy())
    d = _flag_current_state(d)

    agg = d.groupby("agent_name").agg(
        total_calls=("call_status", "count"),
        fresh_connect=("_fresh_connect", "sum"),
        fu_attempts=("_fu_attempt", "sum"),
        followup_connect=("_followup_connect", "sum"),
        connected=("_connected", "sum"),
        dnc=("_dnc", "sum"),
        dnd=("_dnd", "sum"),
        cbl=("_cbl", "sum"),
        invalid=("_invalid", "sum"),
        not_interested=("_not_interested", "sum"),
        attempt_again=("_attempt_again", "sum"),
        snapshot=("_snapshot", "sum"),
        meeting=("_meeting", "sum"),
        escalate=("_escalate", "sum"),
    ).reset_index()

    true_mql_contacts = (
        d[d["current_state"].isin(["Dream Snapshot Confirmed", "Snapshot Confirmed"])][["agent_name", "contact_id"]]
        .dropna(subset=["contact_id"])
        .drop_duplicates()
        .groupby("agent_name")
        .size()
        .rename("true_mql")
        .reset_index()
    )
    agg = agg.merge(true_mql_contacts, on="agent_name", how="left")
    agg["true_mql"] = agg["true_mql"].fillna(0).astype(int)

    agg["connection_rate_pct"] = (
        agg["connected"] * 100.0 / agg["total_calls"].clip(lower=1)
    ).round(1)
    return agg.sort_values("total_calls", ascending=False).reset_index(drop=True)


def _closure_summary(close_df: pd.DataFrame) -> tuple[int, int, int, float]:
    if close_df.empty:
        return 0, 0, 0, 0.0
    qualified = int((close_df["close_reason"] == "qualified").sum())
    rejected = int((close_df["close_reason"] == "rejected").sum())
    stalled = int((close_df["close_reason"] == "stalled").sum())
    den = qualified + rejected + stalled
    qual_rate = round(qualified * 100.0 / den, 1) if den > 0 else 0.0
    return qualified, rejected, stalled, qual_rate


def _true_mql_unique_contacts(df: pd.DataFrame) -> int:
    if df.empty or "contact_id" not in df.columns:
        return 0
    return int(
        df[df["current_state"].isin(["Dream Snapshot Confirmed", "Snapshot Confirmed"])]["contact_id"]
        .dropna()
        .nunique()
    )


def _render_metric_definitions() -> None:
    with st.expander("Metric definitions", expanded=False):
        st.markdown(
            "- Fresh connect (FU1): Connected calls where follow-up number is FU1.  \n"
            "- Follow-up connect (FU2+): Connected calls from FU2 onward.  \n"
            "- Follow-ups: Calls from FU2 onward (second follow-up and later).  \n"
            "- Stalled: Contact reached FU30 without qualifying or being rejected.  \n"
            "- Qualification rate: Qualified / (Qualified + Rejected + Stalled)."
        )


def _build_connection_efficiency(df: pd.DataFrame, top_n: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    d = _flag_call_status(df.copy())
    eff = d.groupby("agent_name").agg(
        total_calls=("call_status", "count"),
        connected=("_connected", "sum"),
    ).reset_index()
    eff = eff[eff["total_calls"] > 0]
    eff["connection_rate_pct"] = (
        eff["connected"] * 100.0 / eff["total_calls"].clip(lower=1)
    ).round(1)

    return eff.sort_values(["connection_rate_pct", "total_calls"], ascending=[False, False]).head(top_n)


def _render_agent_calls_efficiency_chart(df: pd.DataFrame, height: int = 220, top_n: int = 8) -> None:
    if df.empty:
        st.info("No agent performance data for the selected filters.")
        return

    d = _flag_call_status(df.copy())
    chart_df = d.groupby("agent_name").agg(
        calls=("call_status", "count"),
        connected=("_connected", "sum"),
    ).reset_index()
    chart_df = chart_df[chart_df["calls"] > 0]
    if chart_df.empty:
        st.info("No agent performance data for the selected filters.")
        return

    chart_df["connection_rate_pct"] = (
        chart_df["connected"] * 100.0 / chart_df["calls"].clip(lower=1)
    ).round(1)
    chart_df = chart_df.sort_values("calls", ascending=False).head(top_n)
    chart_df["label"] = chart_df.apply(
        lambda r: f"{int(r['calls'])} | {r['connection_rate_pct']}%",
        axis=1,
    )

    bars = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
        .encode(
            x=alt.X("calls:Q", title="Calls", axis=alt.Axis(tickMinStep=1)),
            y=alt.Y("agent_name:N", sort="-x", title="Agent"),
            color=alt.Color(
                "connection_rate_pct:Q",
                title="Conn %",
                scale=alt.Scale(domain=[0, 100], scheme="redyellowgreen"),
            ),
            tooltip=[
                alt.Tooltip("agent_name:N", title="Agent"),
                alt.Tooltip("calls:Q", title="Calls"),
                alt.Tooltip("connected:Q", title="Connected"),
                alt.Tooltip("connection_rate_pct:Q", title="Conn %"),
            ],
        )
        .properties(height=height)
    )

    labels = (
        alt.Chart(chart_df)
        .mark_text(align="left", dx=6, color="#334155", fontSize=11)
        .encode(
            x=alt.X("calls:Q"),
            y=alt.Y("agent_name:N", sort="-x"),
            text="label:N",
        )
    )

    st.altair_chart((bars + labels), use_container_width=True)


def _render_connection_efficiency_chart(df: pd.DataFrame, height: int = 220, top_n: int = 8) -> None:
    eff = _build_connection_efficiency(df, top_n=top_n)
    if eff.empty:
        st.info("No connection efficiency data for the selected filters.")
        return

    chart = (
        alt.Chart(eff)
        .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
        .encode(
            x=alt.X(
                "connection_rate_pct:Q",
                title="Connection rate (%)",
                scale=alt.Scale(domain=[0, 100]),
            ),
            y=alt.Y("agent_name:N", sort="-x", title="Agent"),
            color=alt.Color(
                "connection_rate_pct:Q",
                title="Conn %",
                scale=alt.Scale(domain=[0, 100], scheme="redyellowgreen"),
            ),
            tooltip=[
                alt.Tooltip("agent_name:N", title="Agent"),
                alt.Tooltip("total_calls:Q", title="Calls"),
                alt.Tooltip("connected:Q", title="Connected"),
                alt.Tooltip("connection_rate_pct:Q", title="Conn %"),
            ],
        )
        .properties(height=height)
    )
    st.altair_chart(chart, use_container_width=True)


try:
    agent_names = _load_mql_agents()
except Exception as e:
    log_and_show("MQL agent list", e)
    agent_names = []


tab_daily, tab_weekly, tab_monthly = st.tabs(["Daily", "Weekly", "Monthly"])


with tab_daily:
    selected_date = st.date_input("Date", value=date.today(), key="mql_daily_date")
    st.divider()

    try:
        raw_d = _load_daily_raw(str(selected_date), campaign)
        close_d = _load_closure_period(str(selected_date), str(selected_date), campaign, tuple())

        if raw_d.empty and close_d.empty:
            st.info(
                f"No MQL activity for **{selected_date}**. "
                "Either no calls were made/closed or sync is pending."
            )
        else:
            agg = _compute_agent_summary(raw_d)
            qualified, rejected, stalled, qual_rate = _closure_summary(close_d)
            true_mql_unique = _true_mql_unique_contacts(raw_d)

            total_calls = int(agg["total_calls"].sum()) if not agg.empty else 0
            fresh_connect = int(agg["fresh_connect"].sum()) if not agg.empty else 0
            fu_attempts = int(agg["fu_attempts"].sum()) if not agg.empty else 0
            followup_connect = int(agg["followup_connect"].sum()) if not agg.empty else 0
            total_connected = int(agg["connected"].sum()) if not agg.empty else 0
            conn_pct = round(total_connected * 100.0 / total_calls, 1) if total_calls > 0 else 0.0

            st.subheader("Performance summary", divider=True)
            c1, c2, c3, c4, c5, c6 = st.columns(6, gap="medium")
            c1.metric("Fresh connect (FU1)", fresh_connect)
            c2.metric("Follow-ups (FU2+)", fu_attempts)
            c3.metric("Follow-up connect (FU2+)", followup_connect)
            c4.metric("Conn %", f"{conn_pct}%")
            c5.metric("True MQL", true_mql_unique)
            c6.metric("Qual %", f"{qual_rate}%")

            st.divider()
            s1, s2, s3 = st.columns(3, gap="medium")
            s1.metric("Rejected", rejected)
            s2.metric("Stalled", stalled)
            s3.metric("Meeting", int(agg["meeting"].sum()) if not agg.empty else 0)

            st.divider()
            _render_metric_definitions()
            st.divider()

            if not agg.empty:
                st.subheader("Per agent performance", divider=True)
                display = agg[[
                    "agent_name", "total_calls", "fresh_connect", "fu_attempts", "followup_connect", "connection_rate_pct",
                    "dnc", "dnd", "cbl", "invalid", "not_interested",
                    "meeting", "escalate",
                    "true_mql",
                ]].rename(columns={
                    "agent_name": "Agent",
                    "total_calls": "Dialed",
                    "fresh_connect": "Fresh Connect (FU1)",
                    "fu_attempts": "Follow-ups",
                    "followup_connect": "Follow-up Connect",
                    "connection_rate_pct": "Conn %",
                    "dnc": "Did Not Connect",
                    "dnd": "DND",
                    "cbl": "Call Back",
                    "invalid": "Invalid",
                    "not_interested": "Not Interested",
                    "meeting": "Meeting",
                    "escalate": "Escalate",
                    "true_mql": "True MQL",
                })
                st.dataframe(display, use_container_width=True, hide_index=True)

                st.divider()

                # Interactive filters for detailed analysis
                st.subheader("Call analysis filters", divider=True)
                filtered_d = _apply_attempt_filters(raw_d, "daily")
                
                if filtered_d.empty:
                    st.info("No data matches the selected filters.")
                else:
                    # Agent Performance Chart
                    agent_perf = filtered_d.groupby("agent_name").agg(
                        calls=("call_status", "count"),
                    ).reset_index().sort_values("calls", ascending=False).head(10)
                    
                    c1, c2 = st.columns([1, 1], gap="large")
                    
                    with c1:
                        st.markdown("**Agent Call Counts** (Top 10)")
                        agent_chart = (
                            alt.Chart(agent_perf)
                            .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
                            .encode(
                                x=alt.X("calls:Q", title="Calls", axis=alt.Axis(tickMinStep=1)),
                                y=alt.Y("agent_name:N", sort="-x", title="Agent"),
                                color=alt.value("#3b82f6"),
                                tooltip=[
                                    alt.Tooltip("agent_name:N", title="Agent"),
                                    alt.Tooltip("calls:Q", title="Calls")
                                ]
                            )
                            .properties(height=220)
                        )
                        st.altair_chart(agent_chart, use_container_width=True)
                    
                    with c2:
                        # Status breakdown
                        status_counts = (
                            filtered_d[filtered_d["call_status"] != ""]["call_status"]
                            .value_counts()
                            .rename_axis("call_status")
                            .reset_index(name="count")
                        )
                        if not status_counts.empty:
                            st.markdown("**Call Status Distribution**")
                            chart = (
                                alt.Chart(status_counts)
                                .mark_bar(cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
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
                                        alt.Tooltip("count:Q", title="Count")
                                    ],
                                )
                                .properties(height=220)
                            )
                            st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        log_and_show("MQL daily dashboard", e)


with tab_weekly:
    col1, col2 = st.columns([2, 4])
    week_end = col1.date_input("Week ending (Sunday)", value=date.today(), key="mql_week_end")
    week_start = week_end - timedelta(days=6)
    selected_agents_w = col2.multiselect("Agents", agent_names, default=[], key="mql_week_agents")

    st.caption(f"Range: **{week_start} → {week_end}**")
    st.divider()

    try:
        raw_w = _load_period_raw(str(week_start), str(week_end), campaign, tuple(selected_agents_w))
        close_w = _load_closure_period(str(week_start), str(week_end), campaign, tuple(selected_agents_w))

        if raw_w.empty and close_w.empty:
            st.info("No MQL activity for this week.")
        else:
            agg = _compute_agent_summary(raw_w)
            qualified, rejected, stalled, qual_rate = _closure_summary(close_w)
            true_mql_unique = _true_mql_unique_contacts(raw_w)

            total_calls = int(agg["total_calls"].sum()) if not agg.empty else 0
            fresh_connect = int(agg["fresh_connect"].sum()) if not agg.empty else 0
            fu_attempts = int(agg["fu_attempts"].sum()) if not agg.empty else 0
            followup_connect = int(agg["followup_connect"].sum()) if not agg.empty else 0
            total_connected = int(agg["connected"].sum()) if not agg.empty else 0
            conn_pct = round(total_connected * 100.0 / total_calls, 1) if total_calls > 0 else 0.0

            st.subheader("Weekly performance", divider=True)
            c1, c2, c3, c4, c5, c6 = st.columns(6, gap="medium")
            c1.metric("Fresh connect (FU1)", fresh_connect)
            c2.metric("Follow-ups (FU2+)", fu_attempts)
            c3.metric("Follow-up connect (FU2+)", followup_connect)
            c4.metric("Conn %", f"{conn_pct}%")
            c5.metric("True MQL", true_mql_unique)
            c6.metric("Qual %", f"{qual_rate}%")

            st.divider()
            s1, s2, s3 = st.columns(3, gap="medium")
            s1.metric("Rejected", rejected)
            s2.metric("Stalled", stalled)
            s3.metric("Meeting", int(agg["meeting"].sum()) if not agg.empty else 0)

            st.divider()
            _render_metric_definitions()

            if not agg.empty:
                st.divider()
                st.subheader("Per agent performance", divider=True)
                weekly_display = agg[[
                    "agent_name", "total_calls", "fresh_connect", "fu_attempts", "followup_connect", "connection_rate_pct",
                    "dnc", "dnd", "cbl", "invalid", "not_interested",
                    "meeting", "true_mql",
                ]].rename(columns={
                    "agent_name": "Agent",
                    "total_calls": "Dialed",
                    "fresh_connect": "Fresh Connect (FU1)",
                    "fu_attempts": "Follow-ups",
                    "followup_connect": "Follow-up Connect",
                    "connection_rate_pct": "Conn %",
                    "dnc": "Did Not Connect",
                    "dnd": "DND",
                    "cbl": "Call Back",
                    "invalid": "Invalid",
                    "not_interested": "Not Interested",
                    "meeting": "Meeting",
                    "true_mql": "True MQL",
                })
                st.dataframe(weekly_display, use_container_width=True, hide_index=True)

            st.divider()

            if not raw_w.empty:
                # Interactive filters
                st.subheader("Trends and agent analysis", divider=True)
                
                st.markdown("**Daily Trend (All Agents)**")
                day_df = raw_w.copy()
                day_df = _flag_call_status(day_df)
                day_df = _flag_current_state(day_df)
                trend = day_df.groupby("call_date").agg(
                    attempts=("call_status", "count"),
                    connected=("_connected", "sum"),
                ).reset_index()
                melted = trend.melt(
                    id_vars="call_date",
                    value_vars=["attempts", "connected"],
                    var_name="metric",
                    value_name="count",
                )
                melted["metric"] = melted["metric"].map({
                    "attempts": "Follow-ups",
                    "connected": "Connected",
                })

                st.altair_chart(
                    alt.Chart(melted).mark_line(point=True, size=2).encode(
                        x=alt.X("call_date:T", title="Day", axis=alt.Axis(format="%b %d")),
                        y=alt.Y("count:Q", title="Count"),
                        color=alt.Color(
                            "metric:N",
                            scale=alt.Scale(
                                domain=["Follow-ups", "Connected"],
                                range=["#6366f1", "#22c55e"],
                            ),
                            legend=alt.Legend(orient="bottom"),
                        ),
                        tooltip=[
                            alt.Tooltip("call_date:T", title="Date", format="%b %d"),
                            alt.Tooltip("metric:N", title="Metric"),
                            alt.Tooltip("count:Q", title="Count"),
                        ],
                    ).properties(height=200),
                    use_container_width=True,
                )

                # Agent analysis with filters
                st.divider()
                st.markdown("**Agent Performance Analysis**")
                filtered_w = _apply_attempt_filters(day_df, "weekly")
                
                if filtered_w.empty:
                    st.info("No data matches the selected filters.")
                else:
                    st.markdown("**Top Agents (Calls) + Connection Efficiency**")
                    _render_agent_calls_efficiency_chart(filtered_w, height=220, top_n=8)

    except Exception as e:
        log_and_show("MQL weekly dashboard", e)


with tab_monthly:
    today = date.today()
    m1, m2 = st.columns([2, 4])
    sel_month = m1.selectbox(
        "Month",
        list(range(1, 13)),
        index=today.month - 1,
        format_func=lambda m: calendar.month_name[m],
        key="mql_month",
    )
    sel_year = m1.number_input("Year", min_value=2024, max_value=2100, value=today.year, step=1, key="mql_year")
    selected_agents_m = m2.multiselect("Agents", agent_names, default=[], key="mql_month_agents")

    month_start = date(int(sel_year), int(sel_month), 1)
    next_month = date(int(sel_year) + (1 if int(sel_month) == 12 else 0), 1 if int(sel_month) == 12 else int(sel_month) + 1, 1)
    month_end = next_month - timedelta(days=1)

    st.caption(f"Range: **{month_start} → {month_end}**")
    st.divider()

    try:
        raw_m = _load_period_raw(str(month_start), str(month_end), campaign, tuple(selected_agents_m))
        close_m = _load_closure_period(str(month_start), str(month_end), campaign, tuple(selected_agents_m))

        if raw_m.empty and close_m.empty:
            st.info("No MQL activity for this month.")
        else:
            agg = _compute_agent_summary(raw_m)
            qualified, rejected, stalled, qual_rate = _closure_summary(close_m)
            true_mql_unique = _true_mql_unique_contacts(raw_m)

            total_calls = int(agg["total_calls"].sum()) if not agg.empty else 0
            fresh_connect = int(agg["fresh_connect"].sum()) if not agg.empty else 0
            fu_attempts = int(agg["fu_attempts"].sum()) if not agg.empty else 0
            followup_connect = int(agg["followup_connect"].sum()) if not agg.empty else 0
            total_connected = int(agg["connected"].sum()) if not agg.empty else 0
            conn_pct = round(total_connected * 100.0 / total_calls, 1) if total_calls > 0 else 0.0

            st.subheader("Monthly performance", divider=True)
            c1, c2, c3, c4, c5, c6 = st.columns(6, gap="medium")
            c1.metric("Fresh connect (FU1)", fresh_connect)
            c2.metric("Follow-ups (FU2+)", fu_attempts)
            c3.metric("Follow-up connect (FU2+)", followup_connect)
            c4.metric("Conn %", f"{conn_pct}%")
            c5.metric("True MQL", true_mql_unique)
            c6.metric("Qual %", f"{qual_rate}%")

            st.divider()
            s1, s2, s3 = st.columns(3, gap="medium")
            s1.metric("Rejected", rejected)
            s2.metric("Stalled", stalled)
            s3.metric("Meeting", int(agg["meeting"].sum()) if not agg.empty else 0)

            st.divider()
            _render_metric_definitions()

            if not agg.empty:
                st.divider()
                st.subheader("Per agent performance", divider=True)
                monthly_display = agg[[
                    "agent_name", "total_calls", "fresh_connect", "fu_attempts", "followup_connect", "connection_rate_pct",
                    "dnc", "dnd", "cbl", "invalid", "not_interested",
                    "meeting", "true_mql",
                ]].rename(columns={
                    "agent_name": "Agent",
                    "total_calls": "Dialed",
                    "fresh_connect": "Fresh Connect (FU1)",
                    "fu_attempts": "Follow-ups",
                    "followup_connect": "Follow-up Connect",
                    "connection_rate_pct": "Conn %",
                    "dnc": "Did Not Connect",
                    "dnd": "DND",
                    "cbl": "Call Back",
                    "invalid": "Invalid",
                    "not_interested": "Not Interested",
                    "meeting": "Meeting",
                    "true_mql": "True MQL",
                })
                st.dataframe(monthly_display, use_container_width=True, hide_index=True)

            st.divider()

            if not raw_m.empty:
                # Interactive filters and trends
                st.subheader("Trends and agent analysis", divider=True)
                
                # Main trend line
                st.markdown("**Daily Trend (All Agents)**")
                trend_m = raw_m.copy()
                trend_m = _flag_call_status(trend_m)
                trend_m = _flag_current_state(trend_m)
                trend_m = trend_m.groupby("call_date").agg(
                    attempts=("call_status", "count"),
                    connected=("_connected", "sum"),
                ).reset_index()
                melted_m = trend_m.melt(
                    id_vars="call_date",
                    value_vars=["attempts", "connected"],
                    var_name="metric",
                    value_name="count",
                )
                melted_m["metric"] = melted_m["metric"].map({
                    "attempts": "Follow-ups",
                    "connected": "Connected",
                })

                st.altair_chart(
                    alt.Chart(melted_m).mark_line(point=True, size=2).encode(
                        x=alt.X("call_date:T", title="Date", axis=alt.Axis(format="%b %d")),
                        y=alt.Y("count:Q", title="Count"),
                        color=alt.Color(
                            "metric:N",
                            scale=alt.Scale(
                                domain=["Follow-ups", "Connected"],
                                range=["#6366f1", "#22c55e"],
                            ),
                            legend=alt.Legend(orient="bottom"),
                        ),
                        tooltip=[
                            alt.Tooltip("call_date:T", title="Date", format="%b %d"),
                            alt.Tooltip("metric:N", title="Metric"),
                            alt.Tooltip("count:Q", title="Count"),
                        ],
                    ).properties(height=200),
                    use_container_width=True,
                )

                # Agent analysis with filters
                st.divider()
                st.markdown("**Agent Performance Analysis**")
                filtered_m = _apply_attempt_filters(raw_m, "monthly")
                
                if filtered_m.empty:
                    st.info("No data matches the selected filters.")
                else:
                    st.markdown("**Top Agents (Calls) + Connection Efficiency**")
                    _render_agent_calls_efficiency_chart(filtered_m, height=260, top_n=10)

    except Exception as e:
        log_and_show("MQL monthly dashboard", e)
