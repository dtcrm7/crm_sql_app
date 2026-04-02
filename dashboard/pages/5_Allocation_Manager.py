"""
pages/5_Allocation_Manager.py — Allocation Visibility & Management

Sections:
  1. Allocation summary — per-agent counts for any date
  2. Browse allocations — full contact list for a specific agent + date
  3. Delete allocation — remove a mistaken allocation from DB and Google Sheet
"""

import streamlit as st
import pandas as pd
from datetime import date

from utils.db import query_df, execute
from utils.sheets import delete_contact_from_sheet, sheet_is_configured
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
st.title("Allocation Manager")
st.caption(
    f"Campaign: **{campaign.title()}** — view how many contacts are allocated to each agent, "
    "browse individual allocations, and delete mistaken entries from both DB and Google Sheet."
)

ATTEMPT_LABELS = {
    0: "New Contact",
    1: "FU 1",
    2: "FU 2",
    3: "FU 3",
    4: "FU 4",
    5: "FU 5",
}

# ── SECTION 1: ALLOCATION SUMMARY ─────────────────────────────────────────────
st.subheader("Allocation summary")

col_date, _ = st.columns([2, 5])
with col_date:
    view_date = st.date_input("View date", value=date.today(), key="summary_date")

try:
    summary = query_df(
        """
        SELECT
            a.name                                                          AS "Agent",
            COUNT(*)                                                        AS "Total",
            COUNT(*) FILTER (WHERE ca.attempt_number = 0)                  AS "Fresh",
            COUNT(*) FILTER (WHERE ca.attempt_number = 1)                  AS "FU 1",
            COUNT(*) FILTER (WHERE ca.attempt_number = 2)                  AS "FU 2",
            COUNT(*) FILTER (WHERE ca.attempt_number = 3)                  AS "FU 3",
            COUNT(*) FILTER (WHERE ca.attempt_number = 4)                  AS "FU 4",
            COUNT(*) FILTER (WHERE ca.attempt_number = 5)                  AS "FU 5",
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1 FROM call_actions x WHERE x.allocation_id = ca.id
                )
            )                                                               AS "Worked",
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1 FROM call_actions x WHERE x.allocation_id = ca.id
                )
            )                                                               AS "Pending"
        FROM contact_allocations ca
        JOIN agents   a ON a.id  = ca.agent_id
        JOIN contacts c ON c.id  = ca.contact_id
        WHERE ca.allocated_date = %(dt)s
          AND c.campaign        = %(campaign)s
                    AND (a.team IS NULL OR a.team != 'mql')
        GROUP BY a.id, a.name
        ORDER BY a.name
        """,
        params={"dt": str(view_date), "campaign": campaign},
    )

    if summary.empty:
        st.info(f"No allocations found for {view_date}.")
    else:
        # Top metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Agents allocated", len(summary))
        m2.metric("Total contacts", int(summary["Total"].sum()))
        m3.metric("Worked", int(summary["Worked"].sum()))
        m4.metric("Pending (unworked)", int(summary["Pending"].sum()))

        st.dataframe(summary, use_container_width=True, hide_index=True)

except Exception as e:
    log_and_show("allocation summary", e)

st.divider()

# ── SECTION 2: BROWSE ALLOCATIONS FOR AN AGENT ────────────────────────────────
st.subheader("Browse allocations")
st.caption("Select an agent and date to see every contact allocated to them.")

try:
    all_agents = query_df(
        """
        SELECT id, name
        FROM agents
        WHERE status = 'active'
          AND (team IS NULL OR team != 'mql')
        ORDER BY name
        """
    )
except Exception as e:
    log_and_show("agents list", e)
    st.stop()

if all_agents.empty:
    st.info("No active agents found.")
    st.stop()

col_a, col_b = st.columns([2, 2])
with col_a:
    agent_map = {row["name"]: int(row["id"]) for _, row in all_agents.iterrows()}
    selected_name = st.selectbox("Agent", list(agent_map.keys()), key="mgr_agent")
    selected_id = agent_map[selected_name]
with col_b:
    mgr_date = st.date_input("Date", value=date.today(), key="mgr_date")

try:
    allocs = query_df(
        """
        SELECT
            ca.id                                                   AS allocation_id,
            ca.contact_id,
            CASE c.source
                WHEN 'rocketreach' THEN 'RR | ' || c.source_id
                WHEN 'msme'        THEN 'MS | ' || c.source_id
                WHEN 'pharma'      THEN 'PH | ' || c.source_id
                ELSE                    'MN | ' || c.source_id
            END                                                     AS unique_id,
            co.name                                                 AS company,
            TRIM(
                COALESCE(c.first_name, '') || ' ' || COALESCE(c.last_name, '')
            )                                                       AS person_name,
            ca.attempt_number,
            c.contact_flag,
            ca.allocated_at,
            ca.closed_at,
            ca.close_reason,
            (
                SELECT COUNT(*) FROM call_actions x
                WHERE x.allocation_id = ca.id
            )                                                       AS call_actions,
            ags.sheet_id
        FROM contact_allocations ca
        JOIN contacts c   ON c.id  = ca.contact_id
        JOIN agents   a   ON a.id  = ca.agent_id
        LEFT JOIN companies co     ON co.id = c.company_id
        LEFT JOIN agent_sheets ags ON ags.agent_id = a.id AND ags.campaign = %(campaign)s
        WHERE ca.agent_id      = %(agent_id)s
          AND ca.allocated_date = %(dt)s
          AND c.campaign        = %(campaign)s
        ORDER BY ca.attempt_number, ca.allocated_at
        """,
        params={"agent_id": selected_id, "dt": str(mgr_date), "campaign": campaign},
    )

    if allocs.empty:
        st.info(f"No allocations for {selected_name} on {mgr_date}.")
    else:
        # Mini metrics
        ma, mb, mc = st.columns(3)
        ma.metric(f"Total allocated to {selected_name}", len(allocs))
        mb.metric("With outcome logged", int((allocs["call_actions"] > 0).sum()))
        mc.metric("No outcome yet", int((allocs["call_actions"] == 0).sum()))

        # Display table
        display = allocs[
            ["unique_id", "company", "person_name", "attempt_number",
             "contact_flag", "allocated_at", "closed_at", "call_actions"]
        ].copy()
        display["attempt_number"] = display["attempt_number"].map(ATTEMPT_LABELS)
        display = display.rename(columns={
            "unique_id":      "Unique ID",
            "company":        "Company",
            "person_name":    "Name",
            "attempt_number": "Tab",
            "contact_flag":   "Flag",
            "allocated_at":   "Allocated At",
            "closed_at":      "Closed At",
            "call_actions":   "Call Actions",
        })
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.divider()

        # ── SECTION 3: DELETE OPTIONS ───────────────────────────────────────
        st.subheader("Delete allocations")

        agent_sheet_id = allocs["sheet_id"].iloc[0] or "" if not allocs.empty else ""

        del_tab, bulk_tab = st.tabs(["Delete one contact", "Delete ALL for this agent & date"])

        # ── TAB A: Single delete ────────────────────────────────────────────
        with del_tab:
            st.caption(
                "Removes one allocation from the database and deletes the row from "
                "the agent's Google Sheet. Use this when a single contact was assigned by mistake."
            )

            uid_list = allocs["unique_id"].tolist()
            del_uid = st.selectbox(
                "Select contact to remove", uid_list, key="del_uid"
            )

            if del_uid:
                del_row = allocs[allocs["unique_id"] == del_uid].iloc[0]
                del_alloc_id   = int(del_row["allocation_id"])
                del_contact_id = int(del_row["contact_id"])
                del_attempt    = int(del_row["attempt_number"])
                del_actions    = int(del_row["call_actions"])
                del_sheet_id   = del_row["sheet_id"] or ""

                # Info card
                with st.container(border=True):
                    c1, c2, c3, c4 = st.columns(4)
                    c1.markdown(f"**Unique ID**  \n{del_uid}")
                    c2.markdown(f"**Company**  \n{del_row['company'] or '—'}")
                    c3.markdown(f"**Name**  \n{del_row['person_name'] or '—'}")
                    c4.markdown(f"**Tab**  \n{ATTEMPT_LABELS.get(del_attempt, del_attempt)}")

                if del_actions > 0:
                    st.warning(
                        f"This contact has **{del_actions}** call action(s) logged. "
                        "Deleting the allocation will also delete those call records."
                    )

                if not sheet_is_configured(del_sheet_id):
                    st.info(
                        "No Google Sheet ID is configured for this agent. "
                        "Only the DB record will be deleted."
                    )

                also_delete_sheet = st.checkbox(
                    "Also delete from agent's Google Sheet",
                    value=sheet_is_configured(del_sheet_id),
                    disabled=not sheet_is_configured(del_sheet_id),
                    key="also_del_sheet",
                )

                confirm = st.checkbox(
                    "I confirm — delete this allocation permanently",
                    key="confirm_single_del",
                )

                if st.button(
                    "Delete allocation",
                    disabled=not confirm,
                    type="primary",
                    key="del_alloc_btn",
                ):
                    errors = []

                    # Step 1 — Remove from Google Sheet
                    if also_delete_sheet and sheet_is_configured(del_sheet_id):
                        with st.spinner("Removing from Google Sheet..."):
                            ok, msg = delete_contact_from_sheet(del_sheet_id, del_uid)
                        if ok:
                            st.success(f"Sheet: {msg}")
                        else:
                            st.warning(f"Sheet: {msg}")

                    # Step 2 — Delete call_actions linked to this allocation
                    try:
                        execute(
                            "DELETE FROM call_actions WHERE allocation_id = %s",
                            (del_alloc_id,),
                        )
                    except Exception as e:
                        errors.append(f"call_actions delete failed: {e}")

                    # Step 3 — Delete the allocation record
                    try:
                        execute(
                            "DELETE FROM contact_allocations WHERE id = %s",
                            (del_alloc_id,),
                        )
                    except Exception as e:
                        errors.append(f"contact_allocations delete failed: {e}")

                    # Step 4 — Reset contact flag to 'fresh' if no history remains
                    try:
                        execute(
                            """
                            UPDATE contacts
                            SET contact_flag    = 'fresh',
                                flag_updated_at = NULL
                            WHERE id = %s
                              AND NOT EXISTS (
                                  SELECT 1 FROM call_actions
                                  WHERE contact_id = %s
                              )
                              AND NOT EXISTS (
                                  SELECT 1 FROM contact_allocations
                                  WHERE contact_id = %s
                                    AND closed_at IS NOT NULL
                              )
                            """,
                            (del_contact_id, del_contact_id, del_contact_id),
                        )
                    except Exception as e:
                        errors.append(f"flag reset failed: {e}")

                    if errors:
                        st.error("Errors during deletion:\n" + "\n".join(errors))
                    else:
                        st.toast(
                            f"Allocation for {del_uid} deleted. Contact returned to fresh pool."
                        )
                        st.rerun()

        # ── TAB B: Bulk delete ──────────────────────────────────────────────
        with bulk_tab:
            total_allocs   = len(allocs)
            with_actions   = int((allocs["call_actions"] > 0).sum())
            without_action = total_allocs - with_actions

            st.caption(
                f"Deletes **all {total_allocs} allocations** for **{selected_name}** on **{mgr_date}** "
                "from the database and removes every corresponding row from their Google Sheet."
            )

            # Summary of what will be deleted
            with st.container(border=True):
                bc1, bc2, bc3 = st.columns(3)
                bc1.metric("Contacts to delete", total_allocs)
                bc2.metric("With call actions (also deleted)", with_actions)
                bc3.metric("No outcome — safe to remove", without_action)

            if with_actions > 0:
                st.warning(
                    f"{with_actions} contact(s) have logged call actions. "
                    "Those records will also be permanently deleted."
                )

            bulk_also_sheet = st.checkbox(
                "Also delete all rows from agent's Google Sheet",
                value=sheet_is_configured(agent_sheet_id),
                disabled=not sheet_is_configured(agent_sheet_id),
                key="bulk_also_sheet",
            )
            if not sheet_is_configured(agent_sheet_id):
                st.info("No sheet ID configured for this agent — only DB records will be deleted.")

            bulk_confirm = st.checkbox(
                f"I confirm — delete ALL {total_allocs} allocations for {selected_name} on {mgr_date}",
                key="bulk_confirm",
            )

            if st.button(
                f"Delete all {total_allocs} allocations",
                disabled=not bulk_confirm,
                type="primary",
                key="bulk_del_btn",
            ):
                bulk_errors = []
                sheet_results = {"deleted": 0, "not_found": 0, "errors": 0}

                # Step 1 — Remove each contact from Google Sheet
                if bulk_also_sheet and sheet_is_configured(agent_sheet_id):
                    with st.spinner(
                        f"Removing {total_allocs} rows from Google Sheet… this may take a moment."
                    ):
                        for _, row in allocs.iterrows():
                            ok, _ = delete_contact_from_sheet(
                                agent_sheet_id, row["unique_id"]
                            )
                            if ok:
                                sheet_results["deleted"] += 1
                            else:
                                sheet_results["not_found"] += 1

                    st.info(
                        f"Sheet: {sheet_results['deleted']} row(s) deleted, "
                        f"{sheet_results['not_found']} not found (already removed)."
                    )

                # Step 2 — Delete all call_actions for these allocations
                alloc_ids = allocs["allocation_id"].tolist()
                try:
                    execute(
                        "DELETE FROM call_actions WHERE allocation_id = ANY(%s)",
                        (alloc_ids,),
                    )
                except Exception as e:
                    bulk_errors.append(f"call_actions delete failed: {e}")

                # Step 3 — Delete all allocation records
                try:
                    execute(
                        """
                        DELETE FROM contact_allocations
                        WHERE agent_id      = %s
                          AND allocated_date = %s
                        """,
                        (selected_id, str(mgr_date)),
                    )
                except Exception as e:
                    bulk_errors.append(f"contact_allocations delete failed: {e}")

                # Step 4 — Reset contact flags to 'fresh' where no history remains
                contact_ids = allocs["contact_id"].tolist()
                try:
                    execute(
                        """
                        UPDATE contacts
                        SET contact_flag    = 'fresh',
                            flag_updated_at = NULL
                        WHERE id = ANY(%s)
                          AND NOT EXISTS (
                              SELECT 1 FROM call_actions
                              WHERE contact_id = contacts.id
                          )
                          AND NOT EXISTS (
                              SELECT 1 FROM contact_allocations
                              WHERE contact_id = contacts.id
                                AND closed_at IS NOT NULL
                          )
                        """,
                        (contact_ids,),
                    )
                except Exception as e:
                    bulk_errors.append(f"flag reset failed: {e}")

                if bulk_errors:
                    st.error("Errors during bulk deletion:\n" + "\n".join(bulk_errors))
                else:
                    st.toast(
                        f"All {total_allocs} allocations for {selected_name} on {mgr_date} deleted."
                    )
                    st.rerun()

except Exception as e:
    log_and_show("allocations browser", e)
