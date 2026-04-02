"""
pages/4_Allocation.py — Daily Allocation
- Mark agents on leave
- Per-agent: see FU1-FU5 pending counts, set fresh count, run allocation
- Run sync manually
- Delete test data by date
"""

import streamlit as st
import pandas as pd
from datetime import date
from utils.db import query_df, execute_many
from utils.engine import run_allocation, run_sync
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
stage    = st.session_state.get("stage_select", "BD")
team     = "mql" if "MQL" in stage else "bd"

st.title("Allocation")
st.caption(f"Campaign: **{campaign.title()}**")

# ── LEAVE MANAGEMENT ──────────────────────────────────────────
st.subheader("Agent availability today")

try:
    avail = query_df("""
        SELECT id, name, shift_name, is_on_leave
        FROM agents
        WHERE status = 'active'
          AND (team = %(team)s OR (%(team)s = 'bd' AND team IS NULL))
        ORDER BY is_on_leave, name
    """, params={"team": team})

    if avail.empty:
        st.warning("No active agents in DB.")
    else:
        present = avail[avail["is_on_leave"] == False]
        absent  = avail[avail["is_on_leave"] == True]

        c1, c2 = st.columns(2)
        c1.metric("Present today", len(present))
        c2.metric("On leave",      len(absent))

        st.caption("Check **On Leave** to mark an agent absent today, then click **Save leave**.")

        leave_df = avail[["name", "shift_name", "is_on_leave"]].copy()
        leave_df = leave_df.rename(columns={
            "name": "Agent", "shift_name": "Shift", "is_on_leave": "On Leave"
        })
        leave_edited = st.data_editor(
            leave_df,
            use_container_width=True,
            hide_index=True,
            disabled=["Agent", "Shift"],
            column_config={
                "On Leave": st.column_config.CheckboxColumn("On Leave")
            },
        )

        if st.button("Save leave", type="primary"):
            try:
                updates = [
                    (bool(leave_edited.iloc[i]["On Leave"]), int(avail.iloc[i]["id"]))
                    for i in range(len(avail))
                ]
                execute_many(
                    "UPDATE agents SET is_on_leave = %s, updated_at = NOW() WHERE id = %s",
                    updates,
                )
                st.toast("Leave status saved.")
                st.rerun()
            except Exception as e:
                log_and_show("leave save", e)

except Exception as e:
    log_and_show("agents (availability)", e)

st.divider()

# ── PER-AGENT ALLOCATION ───────────────────────────────────────
st.subheader("Run allocation")
st.caption("Select an agent, review their pending follow-ups, set fresh count, then allocate.")

try:
    active_agents = query_df("""
        SELECT id, name, kpi_dialed, is_on_leave
        FROM agents
        WHERE status = 'active'
          AND (team = %(team)s OR (%(team)s = 'bd' AND team IS NULL))
        ORDER BY is_on_leave, name
    """, params={"team": team})
except Exception as e:
    log_and_show("agents (allocation)", e)
    st.stop()

if active_agents.empty:
    st.info("No active agents.")
    st.stop()

# Agent selector
agent_options = {
    f"{row['name']}{' (on leave)' if row['is_on_leave'] else ''}": row
    for _, row in active_agents.iterrows()
}
selected_label = st.selectbox("Select agent", list(agent_options.keys()))
sel_agent      = agent_options[selected_label]
agent_id       = int(sel_agent["id"])
agent_name     = sel_agent["name"]

# ── Pending FU breakdown for this agent ───────────────────────
try:
    fu_counts = query_df("""
        SELECT
            ca.attempt_number,
            COUNT(*) AS pending
        FROM contact_allocations ca
        JOIN contacts c ON c.id = ca.contact_id
        WHERE ca.agent_id   = %(agent_id)s
          AND ca.closed_at IS NULL
          AND c.campaign    = %(campaign)s
          AND NOT EXISTS (
              SELECT 1 FROM call_actions x
              WHERE x.contact_id    = ca.contact_id
                AND x.allocation_id = ca.id
          )
        GROUP BY ca.attempt_number
        ORDER BY ca.attempt_number
    """, params={"agent_id": agent_id, "campaign": campaign})

    ATTEMPT_LABELS = {
        0: "New Contact (unresponded)",
        1: "FU 1",
        2: "FU 2",
        3: "FU 3",
        4: "FU 4",
        5: "FU 5",
    }

    # Build a dict keyed by attempt number
    counts = {int(r["attempt_number"]): int(r["pending"]) for _, r in fu_counts.iterrows()}
    total_pending = sum(counts.values())

    st.markdown(f"**Pending follow-ups for {agent_name}** — total: **{total_pending}**")

    # Display as metric columns
    cols = st.columns(6)
    for attempt, label in ATTEMPT_LABELS.items():
        cols[attempt].metric(label, counts.get(attempt, 0))

except Exception as e:
    log_and_warn("pending FU counts", e)
    total_pending = 0

st.caption("All pending follow-ups above will be included automatically. Set how many **fresh** contacts to add on top.")

# Fresh count input
default_fresh = int(sel_agent["kpi_dialed"])
fresh_count   = st.number_input(
    f"Fresh contacts to allocate for {agent_name}",
    min_value=0, max_value=500,
    value=default_fresh,
    step=5,
    help=f"Default from agent settings: {default_fresh}. Change for today only (updates agent record).",
)

# Already run today for this agent?
try:
    already = query_df("""
        SELECT COUNT(*) AS cnt FROM contact_allocations
        WHERE agent_id = %(agent_id)s AND allocated_date = CURRENT_DATE
    """, params={"agent_id": agent_id})
    count_today = int(already.iloc[0]["cnt"])
    if count_today > 0:
        st.info(f"Already allocated {count_today} contacts to {agent_name} today. Running again will append.")
except Exception:
    count_today = 0

# Allocation buttons
col1, col2 = st.columns(2)

with col1:
    with st.container(border=True):
        st.markdown("**Dry run — preview only**")
        st.caption("Shows what would be allocated without writing anything.")
        if st.button("Preview", use_container_width=True, key="dry_run_btn"):
            with st.spinner(f"Previewing allocation for {agent_name}..."):
                success, output = run_allocation(
                    dry_run=True, agent_id=agent_id, fresh_count=fresh_count,
                    campaign=campaign,
                )
            if success:
                st.success("Dry run complete.")
            else:
                st.warning("Dry run finished with warnings.")
            st.code(output, language=None)

with col2:
    with st.container(border=True):
        st.markdown("**Live run — allocate and write to sheet**")
        st.caption(f"Allocates {total_pending} follow-ups + {fresh_count} fresh for {agent_name}.")
        confirmed = st.checkbox("I have reviewed the preview and want to proceed", key="live_confirm")
        if st.button("Allocate now", disabled=not confirmed,
                     use_container_width=True, key="live_run_btn"):
            with st.spinner(f"Allocating for {agent_name}..."):
                success, output = run_allocation(
                    dry_run=False, agent_id=agent_id, fresh_count=fresh_count,
                    campaign=campaign,
                )
            if success:
                st.success(f"Allocation complete for {agent_name}!")
            else:
                st.error("Allocation finished with errors. Check output below.")
            st.code(output, language=None)

st.divider()

# ── SYNC CONTROLS ─────────────────────────────────────────────
st.subheader("Run sync (push sheet outcomes to DB)")

col3, col4 = st.columns(2)

with col3:
    with st.container(border=True):
        st.markdown("**Sync dry run**")
        st.caption("Shows which rows would be synced without writing anything.")
        if st.button("Preview sync", use_container_width=True):
            with st.spinner("Running sync dry run..."):
                success, output = run_sync(dry_run=True, campaign=campaign)
            st.code(output, language=None)

with col4:
    with st.container(border=True):
        st.markdown("**Live sync**")
        st.caption("Reads agent sheets, pushes outcomes to DB, marks rows ✓ Synced.")
        sync_date = st.date_input("Sync date", value=date.today())
        if st.button("Run sync now", use_container_width=True):
            with st.spinner("Syncing... this may take 60-90 seconds."):
                date_str = str(sync_date) if sync_date != date.today() else None
                success, output = run_sync(dry_run=False, date_str=date_str, campaign=campaign)
            if success:
                st.success("Sync complete!")
            else:
                st.error("Sync finished with errors.")
            st.code(output, language=None)

st.divider()
