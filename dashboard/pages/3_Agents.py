"""
pages/3_Agents.py — Agent Management
Tabs:
  1. Today      — presence toggle for all agents
  2. Settings   — select one agent → edit KPI, team, status, sheet ID in one place
  3. Add Agent  — add a new agent
"""

import streamlit as st
import pandas as pd
from utils.db import query_df, execute
from utils.errors import log_and_show, log_and_warn
st.title("Agents")

TEAM_LABELS = {"bd": "BD", "mql": "MQL", "sales": "Sales"}

tab_today, tab_settings, tab_add = st.tabs(["Today", "Settings", "Add Agent"])

# ══════════════════════════════════════════════════════════════
# TAB 1 — TODAY'S PRESENCE
# ══════════════════════════════════════════════════════════════
with tab_today:
    st.caption("Toggle leave status. Allocation engine reads this before each run.")

    try:
        agents = query_df("""
            SELECT id, name, shift_name, kpi_dialed, is_on_leave,
                   COALESCE(team, 'bd') AS team
            FROM agents
            WHERE status = 'active'
            ORDER BY team, name
        """)

        if agents.empty:
            st.info("No active agents yet. Add one in the **Add Agent** tab.")
        else:
            for _, row in agents.iterrows():
                team_badge = TEAM_LABELS.get(row["team"], "BD")
                current    = bool(row["is_on_leave"])
                with st.container(border=True):
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                    c1.markdown(f"**{row['name']}** &nbsp; `{team_badge}`")
                    c2.caption(f"Shift: {row['shift_name'] or '—'}")
                    c3.caption(f"KPI: {row['kpi_dialed']} contacts/day")
                    toggle = c4.toggle(
                        "On Leave" if current else "Present",
                        value=current,
                        key=f"leave_{row['id']}",
                    )
                    if toggle != current:
                        try:
                            execute(
                                "UPDATE agents SET is_on_leave = %s, updated_at = NOW() WHERE id = %s",
                                (toggle, int(row["id"]))
                            )
                            st.rerun()
                        except Exception as e:
                            log_and_show("leave update", e)

    except Exception as e:
        log_and_show("agents (today)", e)

# ══════════════════════════════════════════════════════════════
# TAB 2 — SETTINGS (select one agent → edit everything)
# ══════════════════════════════════════════════════════════════
with tab_settings:
    try:
        all_agents = query_df("""
            SELECT id, name, kpi_dialed, status,
                   COALESCE(team, 'bd') AS team
            FROM agents ORDER BY status DESC, name
        """)

        if all_agents.empty:
            st.info("No agents yet.")
        else:
            # Add status filter
            st.markdown("**Filter Agents by Status**")
            f1, f2 = st.columns(2)
            status_filter = f1.radio(
                "Show agents:",
                ["Active", "Inactive"],
                horizontal=True,
                key="agent_status_filter"
            )
            
            # Filter agents based on status selection
            filtered_status = "active" if status_filter == "Active" else "inactive"
            filtered_agents = all_agents[all_agents["status"] == filtered_status]
            
            if filtered_agents.empty:
                st.info(f"No {status_filter.lower()} agents found.")
            else:
                agent_opts = {
                    r['name']: r
                    for _, r in filtered_agents.iterrows()
                }
                selected_label = st.selectbox("Select agent", list(agent_opts.keys()))
                sel = agent_opts[selected_label]
                agent_id   = int(sel["id"])
                agent_name = sel["name"]

                st.divider()

                # ── KPI ──────────────────────────────────────────
                col_a, col_b, col_c = st.columns(3)

                with col_a:
                    st.markdown("**Daily KPI**")
                    new_kpi = st.number_input(
                        "Fresh contacts per day",
                        min_value=1, max_value=500,
                        value=int(sel["kpi_dialed"]),
                        key="kpi_input",
                    )
                    if st.button("Save KPI", key="save_kpi"):
                        try:
                            execute(
                                "UPDATE agents SET kpi_dialed = %s, updated_at = NOW() WHERE id = %s",
                                (new_kpi, agent_id)
                            )
                            st.toast("KPI updated.")
                            st.rerun()
                        except Exception as e:
                            log_and_show("KPI update", e)

                # ── TEAM ─────────────────────────────────────────
                with col_b:
                    st.markdown("**Team**")
                    current_team = sel["team"]
                    new_team = st.radio(
                        "Team",
                        ["bd", "mql", "sales"],
                        format_func=lambda t: TEAM_LABELS[t],
                        index=["bd", "mql", "sales"].index(current_team)
                              if current_team in ["bd","mql","sales"] else 0,
                        key="team_radio",
                        label_visibility="collapsed",
                    )
                    if st.button("Save team", key="save_team", disabled=(new_team == current_team)):
                        try:
                            execute(
                                "UPDATE agents SET team = %s, updated_at = NOW() WHERE id = %s",
                                (new_team, agent_id)
                            )
                            st.toast(f"Team set to {TEAM_LABELS[new_team]}.")
                            st.rerun()
                        except Exception as e:
                            log_and_show("team update", e)

                # ── STATUS ────────────────────────────────────────
                with col_c:
                    st.markdown("**Account status**")
                    current_status = sel["status"]
                    new_status = st.radio(
                        "Status",
                        ["active", "inactive"],
                        index=0 if current_status == "active" else 1,
                        key="status_radio",
                        label_visibility="collapsed",
                    )
                    if st.button("Save status", key="save_status", disabled=(new_status == current_status)):
                        try:
                            execute(
                                "UPDATE agents SET status = %s, updated_at = NOW() WHERE id = %s",
                                (new_status, agent_id)
                            )
                            st.toast(f"Status set to {new_status}.")
                            st.rerun()
                        except Exception as e:
                            log_and_show("status update", e)

            st.divider()

            # ── GOOGLE SHEET IDs ──────────────────────────────
            st.markdown("**Google Sheet IDs**")
            st.caption("Get the Sheet ID from the URL: `docs.google.com/spreadsheets/d/SHEET_ID/edit`")

            try:
                sheets_df = query_df("""
                    SELECT campaign, sheet_type, sheet_id
                    FROM agent_sheets
                    WHERE agent_id = %(aid)s
                      AND sheet_type <> 'meetings'
                    ORDER BY campaign, sheet_type
                """, params={"aid": agent_id})

                if not sheets_df.empty:
                    st.dataframe(sheets_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No sheet IDs set yet.")

                campaigns_list = query_df(
                    "SELECT name FROM campaigns WHERE is_active = TRUE ORDER BY name"
                )["name"].tolist()

                if campaigns_list:
                    # Calling sheet (all agents)
                    with st.form(f"sheet_form_{agent_id}"):
                        st.caption("**Calling sheet** (BD / MQL calling sheet)")
                        sc1, sc2 = st.columns([1, 2])
                        sheet_campaign = sc1.selectbox("Campaign", campaigns_list,
                                                        key=f"camp_calling_{agent_id}")
                        new_sid = sc2.text_input("Sheet ID",
                                                  placeholder="Paste calling sheet ID here")
                        if st.form_submit_button("Save Calling Sheet ID"):
                            try:
                                execute("""
                                    INSERT INTO agent_sheets
                                        (agent_id, campaign, sheet_type, sheet_id, updated_at)
                                    VALUES (%s, %s, 'calling', %s, NOW())
                                    ON CONFLICT (agent_id, campaign, sheet_type)
                                    DO UPDATE SET sheet_id = EXCLUDED.sheet_id,
                                                  updated_at = NOW()
                                """, (agent_id, sheet_campaign, new_sid.strip() or None))
                                st.toast(f"Calling sheet saved for {agent_name} / {sheet_campaign}.")
                                st.rerun()
                            except Exception as e:
                                log_and_show("calling sheet ID save", e)

            except Exception as e:
                log_and_warn("sheet IDs", e)

            st.divider()

            # ── ALL AGENTS TABLE ──────────────────────────────
            st.markdown("**All agents**")
            try:
                tbl = query_df("""
                    SELECT name, COALESCE(team,'bd') AS team, status,
                           kpi_dialed, shift_name, email, joining_date, is_on_leave
                    FROM agents ORDER BY status DESC, team, name
                """)
                st.dataframe(tbl, use_container_width=True, hide_index=True)
            except Exception as e:
                log_and_warn("all agents table", e)

            st.divider()

            # ── DELETE AGENT ──────────────────────────────────
            st.markdown("**Danger zone**")
            try:
                # Count call history for this agent
                history = query_df("""
                    SELECT
                        (SELECT COUNT(*) FROM call_actions     WHERE agent_id = %(aid)s) AS bd_calls,
                        (SELECT COUNT(*) FROM mql_call_attempts WHERE agent_id = %(aid)s) AS mql_calls
                """, params={"aid": agent_id})

                bd_calls  = int(history.iloc[0]["bd_calls"])  if not history.empty else 0
                mql_calls = int(history.iloc[0]["mql_calls"]) if not history.empty else 0
                total_calls = bd_calls + mql_calls

                if total_calls > 0:
                    st.info(
                        f"**{agent_name}** has **{total_calls:,}** call records "
                        f"({bd_calls:,} BD · {mql_calls:,} MQL) and cannot be deleted. "
                        f"Mark them **inactive** in the Account status section above instead."
                    )
                else:
                    st.warning(
                        f"Deleting **{agent_name}** is permanent and cannot be undone."
                    )
                    confirm = st.checkbox(
                        f"Yes, I want to permanently delete {agent_name}",
                        key="confirm_delete_agent"
                    )
                    if st.button("Delete agent", type="primary",
                                 disabled=not confirm, key="delete_agent_btn"):
                        try:
                            execute("DELETE FROM agent_sheets WHERE agent_id = %s", (agent_id,))
                            execute("DELETE FROM agents WHERE id = %s", (agent_id,))
                            st.toast(f"Agent '{agent_name}' deleted.")
                            st.rerun()
                        except Exception as e:
                            log_and_show("delete agent", e)
            except Exception as e:
                log_and_warn("delete agent check", e)

    except Exception as e:
        log_and_show("agents (settings)", e)

# ══════════════════════════════════════════════════════════════
# TAB 3 — ADD NEW AGENT
# ══════════════════════════════════════════════════════════════
with tab_add:
    with st.form("add_agent_form"):
        c1, c2 = st.columns(2)
        name         = c1.text_input("Full name *")
        email        = c2.text_input("Email")
        phone        = c1.text_input("Phone")
        joining_date = c2.date_input("Joining date")
        shift_name   = c1.text_input("Shift name (e.g. Afternoon)")
        shift_start  = c1.time_input("Shift start")
        shift_end    = c2.time_input("Shift end")
        kpi_dialed   = c1.number_input(
            "Fresh contacts per day", min_value=1, value=120,
            help="New contacts to allocate daily. Follow-ups are always included on top.",
        )
        team = c2.selectbox(
            "Team",
            ["bd", "mql", "sales"],
            format_func=lambda t: TEAM_LABELS[t],
            help="bd = BD calling · mql = MQL follow-up · sales = Sales closers",
        )

        st.caption("* Required — set Google Sheet IDs in the Settings tab after adding the agent.")
        if st.form_submit_button("Add agent"):
            if not name.strip():
                st.error("Name is required.")
            else:
                try:
                    from utils.db import query_df as _qdf
                    existing = _qdf(
                        "SELECT id FROM agents WHERE LOWER(name) = LOWER(%s) LIMIT 1",
                        params=(name.strip(),)
                    )
                    if not existing.empty:
                        st.error(f"An agent named '{name.strip()}' already exists. Use a different name.")
                    else:
                        execute("""
                            INSERT INTO agents
                            (name, email, phone, joining_date, shift_name,
                             shift_start, shift_end, kpi_dialed, team, status, is_on_leave)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', FALSE)
                        """, (
                            name.strip(), email or None, phone or None,
                            joining_date, shift_name or None,
                            str(shift_start), str(shift_end),
                            kpi_dialed, team,
                        ))
                        st.toast(f"Agent '{name}' added. Go to Settings tab to set their Sheet ID.")
                        st.rerun()
                except Exception as e:
                    log_and_show("add agent", e)
