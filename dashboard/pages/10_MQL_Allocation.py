"""
pages/10_MQL_Allocation.py — MQL Allocation
- View MQL-eligible contacts (shared_story + snapshot_sent)
- Select an MQL agent, set count, allocate → writes to their Google Sheet
- Run MQL sync (sheet outcomes → DB)
- Delete MQL test data by date
"""

import streamlit as st
import pandas as pd
from datetime import date
from utils.db import query_df
from utils.mql_engine import run_mql_allocation, run_mql_sync
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
campaign_like = f"{campaign} %"
st.title("MQL Allocation")
st.caption(f"Campaign: **{campaign.title()}**")

st.divider()

# ── ESCALATION ALERTS ────────────────────────────────────────
try:
    esc_df = query_df("""
        WITH esc AS (
            SELECT
                ma.contact_id,
                ma.closed_at,
                a.name AS escalated_by,
                ma.campaign
            FROM mql_allocations ma
            JOIN agents a ON a.id = ma.agent_id
            LEFT JOIN LATERAL (
                SELECT mca.current_state
                FROM mql_call_attempts mca
                WHERE mca.allocation_id = ma.id
                ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                LIMIT 1
            ) last_try ON TRUE
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason = 'escalated'
              AND COALESCE(last_try.current_state, 'Escalate') = 'Escalate'
              AND NOT EXISTS (
                  SELECT 1
                  FROM mql_allocations nx
                  WHERE nx.contact_id = ma.contact_id
                    AND (nx.campaign = %(campaign)s OR nx.campaign ILIKE %(campaign_like)s)
                    AND nx.id > ma.id
              )
        )
        SELECT escalated_by, campaign, COUNT(*) AS pending
        FROM esc
        GROUP BY escalated_by, campaign
        ORDER BY pending DESC, escalated_by
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if not esc_df.empty:
        total_pending = int(esc_df["pending"].sum())
        st.warning(f"{total_pending} escalated contact(s) are pending reassignment.")
        st.dataframe(esc_df, use_container_width=True, hide_index=True)
        st.caption("Grouped by escalated-by agent and campaign.")
        st.divider()
except Exception as e:
    log_and_warn("MQL escalation alert", e)

# ── MQL POOL HEALTH ───────────────────────────────────────────
st.subheader("MQL Pool")

try:
    pool_df = query_df("""
        SELECT
            COUNT(*) FILTER (
                WHERE (
                    c.contact_flag IN ('shared_story', 'snapshot_sent', 'mql_in_progress', 'mql_qualified', 'mql_rejected')
                    OR EXISTS (
                        SELECT 1
                        FROM call_actions ca
                        WHERE ca.contact_id = c.id
                          AND ca.current_state = 'Shared Story'
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM mql_call_attempts mca
                        JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        WHERE mca.contact_id = c.id
                          AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                          AND ma.close_reason = 'bd_history'
                          AND mca.current_state = 'Shared Story'
                    )
                )
            )                                                          AS reached_shared_story,
            COUNT(*) FILTER (
                WHERE (
                    c.contact_flag IN ('snapshot_sent', 'mql_in_progress', 'mql_qualified', 'mql_rejected')
                    OR EXISTS (
                        SELECT 1
                        FROM call_actions ca
                        WHERE ca.contact_id = c.id
                          AND ca.current_state IN ('Snapshot Sent', 'Dream Snapshot Sent')
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM mql_call_attempts mca
                        JOIN mql_allocations ma ON ma.id = mca.allocation_id
                        WHERE mca.contact_id = c.id
                          AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
                          AND ma.close_reason = 'bd_history'
                          AND mca.current_state IN ('Snapshot Sent', 'Dream Snapshot Sent')
                    )
                )
            )                                                          AS snapshot_sent_unique,
            COUNT(*) FILTER (
                WHERE c.contact_flag = 'attempt_3_months'
                  AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                        <= (CURRENT_DATE - INTERVAL '90 days')::date
                  AND NOT EXISTS (
                      SELECT 1 FROM mql_allocations ma
                      WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
                  )
            )                                                          AS three_month_ready,
            COUNT(*) FILTER (
                WHERE (
                        c.contact_flag IN ('shared_story', 'snapshot_sent')
                        OR (
                            c.contact_flag = 'attempt_3_months'
                            AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                                <= (CURRENT_DATE - INTERVAL '90 days')::date
                        )
                      )
                  AND NOT EXISTS (
                      SELECT 1 FROM mql_allocations ma
                      WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
                  )
            )                                                          AS unallocated,
            COUNT(*) FILTER (WHERE c.contact_flag = 'mql_in_progress') AS in_progress,
            COUNT(*) FILTER (WHERE c.contact_flag = 'mql_qualified')   AS qualified,
            COUNT(*) FILTER (WHERE c.contact_flag = 'mql_rejected')    AS rejected
        FROM contacts c
        WHERE c.campaign = %(campaign)s
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if not pool_df.empty:
        p = pool_df.iloc[0]
        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Reached Shared Story", int(p["reached_shared_story"]))
        c2.metric("Snapshot Sent (Unique)", int(p["snapshot_sent_unique"]))
        c3.metric("3M Ready", int(p["three_month_ready"]))
        c4.metric("Unallocated",  int(p["unallocated"]),
                  help="Ready to assign to MQL agents")
        c5.metric("In Progress",  int(p["in_progress"]))
        c6.metric("Qualified",    int(p["qualified"]))
        c7.metric("Rejected",     int(p["rejected"]))

        if int(p["unallocated"]) == 0:
            st.info("No unallocated MQL contacts. Wait for BD agents to produce Shared Story / Snapshot Sent contacts.")

except Exception as e:
    log_and_warn("MQL pool stats", e)

st.divider()

# ── LOAD MQL AGENTS (shared by manual + run sections) ─────────────────────
try:
    mql_agents = query_df("""
        SELECT id, name, kpi_dialed, is_on_leave
        FROM agents
        WHERE status = 'active' AND team = 'mql'
        ORDER BY is_on_leave, name
    """)
except Exception as e:
    log_and_show("MQL agents", e)
    st.stop()

if mql_agents.empty:
    st.warning("No active MQL agents found (team='mql'). Add MQL agents in the Agents page.")
    st.stop()

agent_options = {
    f"{row['name']}{' (on leave)' if row['is_on_leave'] else ''}": row
    for _, row in mql_agents.iterrows()
}

if "mql_agent_select" not in st.session_state:
    st.session_state["mql_agent_select"] = next(iter(agent_options))

# ── ELIGIBLE CONTACTS TABLE ───────────────────────────────────
st.subheader("Unallocated MQL contacts")

category_options: list[str] = []
selected_categories: list[str] = []

try:
    eligible_df = query_df("""
        SELECT
            c.id,
            co.name         AS company,
            c.first_name || ' ' || c.last_name AS person,
            c.bd_category    AS category,
            c.contact_flag,
            c.flag_updated_at::date AS flag_date,
            ag.name         AS bd_agent,
            ca.current_state AS bd_state,
            ca.remark        AS bd_remark,
            ca.recording_link
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
        LEFT JOIN LATERAL (
            SELECT agent_id, current_state, remark, recording_link, called_at
            FROM call_actions
            WHERE contact_id = c.id
            ORDER BY called_at DESC LIMIT 1
        ) ca ON TRUE
        LEFT JOIN agents ag ON ag.id = ca.agent_id
        WHERE c.campaign = %(campaign)s
          AND (
                c.contact_flag IN ('shared_story', 'snapshot_sent')
                OR (
                    c.contact_flag = 'attempt_3_months'
                    AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                        <= (CURRENT_DATE - INTERVAL '90 days')::date
                )
              )
          AND NOT EXISTS (
              SELECT 1 FROM mql_allocations ma
              WHERE ma.contact_id = c.id AND ma.closed_at IS NULL
          )
        ORDER BY c.flag_updated_at DESC NULLS LAST
    """, params={"campaign": campaign})

    if eligible_df.empty:
        st.info("No unallocated MQL contacts.")
    else:
        flag_counts = eligible_df["contact_flag"].value_counts()
        f1, f2, f3 = st.columns(3)
        f1.metric("Shared Story", int(flag_counts.get("shared_story", 0)))
        f2.metric("Snapshot Sent", int(flag_counts.get("snapshot_sent", 0)))
        f3.metric("3-month ready", int(flag_counts.get("attempt_3_months", 0)))

        category_options = sorted(
            [c for c in eligible_df["category"].dropna().astype(str).str.strip().unique() if c]
        )
        selected_categories = st.multiselect(
            "Filter unallocated by category",
            options=category_options,
            key="mql_unallocated_category_filter",
            help="Leave empty to show all categories",
        )

        if selected_categories:
            filtered_eligible_df = eligible_df[
                eligible_df["category"].fillna("").isin(selected_categories)
            ].copy()
        else:
            filtered_eligible_df = eligible_df.copy()

        st.caption(
            f"Total eligible unallocated: {len(eligible_df)}"
            f" | Visible after filter: {len(filtered_eligible_df)}"
        )
        st.dataframe(
            filtered_eligible_df.drop(columns=["id"]),
            use_container_width=True,
            hide_index=True,
            column_config={
                "recording_link": st.column_config.LinkColumn("BD Recording"),
                "flag_date":      st.column_config.DateColumn("Flag Date"),
            }
        )

        st.divider()
        st.subheader("Manual allocation")
        st.caption("Select exact contacts and allocate to one MQL agent.")

        manual_options = {}
        for _, row in filtered_eligible_df.iterrows():
            cid = int(row["id"])
            company = str(row.get("company") or "-")
            person = str(row.get("person") or "-")
            cat = str(row.get("category") or "Uncategorized")
            label = f"#{cid} | {company} | {person} | {cat}"
            manual_options[label] = cid

        selected_manual_labels = st.multiselect(
            "Choose contacts for manual allocation",
            options=list(manual_options.keys()),
            key="mql_manual_contact_select",
            help="Pick one or more contacts from the filtered list",
        )
        selected_manual_ids = [manual_options[lbl] for lbl in selected_manual_labels]

        st.caption(f"Selected contacts: {len(selected_manual_ids)}")

        st.subheader("Manual allocation actions")
        selected_label_for_manual = st.session_state.get("mql_agent_select", next(iter(agent_options)))
        sel_agent_manual = agent_options[selected_label_for_manual]
        manual_agent_id = int(sel_agent_manual["id"])
        manual_agent_name = sel_agent_manual["name"]
        st.caption(
            f"Allocate selected contacts to **{manual_agent_name}**. "
            "(Agent selection is controlled in Run MQL allocation.)"
        )

        man_c1, man_c2 = st.columns(2, gap="large")
        with man_c1:
            with st.container(border=True):
                st.markdown("**Preview manual allocation**")
                st.caption("Shows what would be allocated without writing anything.")
                if st.button(
                    "Preview manual allocation",
                    use_container_width=True,
                    disabled=len(selected_manual_ids) == 0,
                    key="mql_manual_preview",
                ):
                    with st.spinner(f"Previewing manual allocation for {manual_agent_name}..."):
                        success, output = run_mql_allocation(
                            agent_id=manual_agent_id,
                            count=len(selected_manual_ids),
                            campaign=campaign,
                            dry_run=True,
                            contact_ids=selected_manual_ids,
                        )
                    st.code(output, language=None)

        with man_c2:
            with st.container(border=True):
                st.markdown("**Run manual allocation**")
                st.caption(f"Allocates selected contacts to {manual_agent_name} and writes to their sheet.")
                manual_confirm = st.checkbox(
                    "Confirm manual allocation",
                    key="mql_manual_confirm",
                )
                if st.button(
                    "Allocate selected now",
                    use_container_width=True,
                    disabled=(len(selected_manual_ids) == 0 or not manual_confirm),
                    key="mql_manual_live",
                ):
                    with st.spinner(f"Allocating selected contacts to {manual_agent_name}..."):
                        success, output = run_mql_allocation(
                            agent_id=manual_agent_id,
                            count=len(selected_manual_ids),
                            campaign=campaign,
                            dry_run=False,
                            contact_ids=selected_manual_ids,
                        )
                    if success:
                        st.success(f"Manual allocation complete for {manual_agent_name}!")
                    else:
                        st.error("Manual allocation finished with errors. Check output below.")
                    st.code(output, language=None)

except Exception as e:
    log_and_show("eligible contacts list", e)

st.divider()

# ── ALLOCATE ──────────────────────────────────────────────────
st.subheader("Run MQL allocation")
st.caption("Select an MQL agent, set contact count, then allocate.")

selected_label = st.selectbox("Select MQL agent", list(agent_options.keys()), key="mql_agent_select")
sel_agent      = agent_options[selected_label]
agent_id       = int(sel_agent["id"])
agent_name     = sel_agent["name"]
default_count  = int(sel_agent["kpi_dialed"]) if sel_agent["kpi_dialed"] else 12

# Already allocated today?
try:
    already = query_df("""
        SELECT COUNT(*) AS cnt FROM mql_allocations
        WHERE agent_id = %(agent_id)s AND allocated_date = CURRENT_DATE
    """, params={"agent_id": agent_id})
    count_today = int(already.iloc[0]["cnt"])
    if count_today > 0:
        st.info(f"Already allocated {count_today} contacts to {agent_name} today. Running again will add more.")
except Exception:
    count_today = 0

# Active MQL contacts for this agent
try:
    active = query_df("""
        SELECT COUNT(*) AS cnt FROM mql_allocations
        WHERE agent_id = %(agent_id)s AND closed_at IS NULL
    """, params={"agent_id": agent_id})
    active_count = int(active.iloc[0]["cnt"])
    st.caption(f"{agent_name} currently has **{active_count}** active MQL contacts.")
except Exception:
    pass

contact_count = st.number_input(
    f"Contacts to allocate to {agent_name}",
    min_value=1, max_value=200,
    value=default_count,
    step=1,
    help=f"Default from agent KPI: {default_count}",
)

allocation_categories = st.multiselect(
    "Allocation category filter",
    options=category_options,
    default=selected_categories,
    key="mql_allocate_category_filter",
    help="Only allocate contacts whose category matches these values",
)

if "selected_manual_ids" not in locals():
    selected_manual_ids = []

col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("**Dry run — preview only**")
        st.caption("Shows what would be allocated without writing anything.")
        if st.button("Preview", use_container_width=True, key="mql_dry"):
            with st.spinner(f"Previewing MQL allocation for {agent_name}..."):
                success, output = run_mql_allocation(
                    agent_id=agent_id, count=contact_count,
                    campaign=campaign, dry_run=True,
                    categories=allocation_categories or None,
                )
            st.code(output, language=None)

with col2:
    with st.container(border=True):
        st.markdown("**Live run — allocate and write to sheet**")
        st.caption(f"Allocates {contact_count} MQL contacts to {agent_name}, writes to their sheet.")
        confirmed = st.checkbox("I have reviewed the preview and want to proceed", key="mql_confirm")
        if st.button("Allocate now", disabled=not confirmed,
                     use_container_width=True, key="mql_live"):
            with st.spinner(f"Allocating for {agent_name}..."):
                success, output = run_mql_allocation(
                    agent_id=agent_id, count=contact_count,
                    campaign=campaign, dry_run=False,
                    categories=allocation_categories or None,
                )
            if success:
                st.success(f"MQL allocation complete for {agent_name}!")
            else:
                st.error("Allocation finished with errors. Check output below.")
            st.code(output, language=None)

st.divider()

# ── MQL SYNC ──────────────────────────────────────────────────
st.subheader("Run MQL sync (sheet outcomes → DB)")

col3, col4 = st.columns(2)

with col3:
    with st.container(border=True):
        st.markdown("**Sync dry run**")
        st.caption("Shows which rows would be synced without writing anything.")
        if st.button("Preview sync", use_container_width=True, key="mql_sync_dry"):
            with st.spinner("Running MQL sync dry run..."):
                success, output = run_mql_sync(dry_run=True, campaign=campaign)
            st.code(output, language=None)

with col4:
    with st.container(border=True):
        st.markdown("**Live sync**")
        st.caption("Reads MQL agent sheets, pushes outcomes to DB, marks rows ✓ Synced.")
        sync_date = st.date_input("Sync date", value=date.today(), key="mql_sync_date")
        if st.button("Run MQL sync now", use_container_width=True, key="mql_sync_live"):
            with st.spinner("Syncing MQL sheets... this may take 60-90 seconds."):
                date_str = str(sync_date) if sync_date != date.today() else None
                success, output = run_mql_sync(
                    dry_run=False, date_str=date_str, campaign=campaign
                )
            if success:
                st.success("MQL sync complete!")
            else:
                st.error("Sync finished with errors.")
            st.code(output, language=None)

st.divider()
