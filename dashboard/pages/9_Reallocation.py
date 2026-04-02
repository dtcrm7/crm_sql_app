"""
9_Reallocation.py — Filter-Based Re-Allocation Campaigns

Workflow:
  1. Build Filters  → choose call_status, current_state, and optional preset rules
  2. Preview        → see exactly how many contacts match + sample rows
  3. Save Campaign  → snapshot the contact list into reallocation_campaign_contacts
  4. Allocate       → distribute contacts across chosen agents (contact_allocations)

Only contacts that:
  • Had at least one call logged with the chosen call_status &amp; current_state
  • Are NOT in terminal flags (shared_story, snapshot_sent, mql_*, invalid, referred, language_issue)
  • Do NOT currently have an active allocation
...are included in the pool.
"""

import json
from datetime import date

import pandas as pd
import streamlit as st

from utils.db import execute, execute_many, get_conn, query_df
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
st.title("Re-allocation Campaigns")
st.caption(f"Campaign: **{campaign.title()}**  ·  Pool contacts with prior engagement back into the calling queue.")

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

ALL_CALL_STATUSES   = ["Connected", "Did not connect", "Do not Disturb",
                       "Call back later", "Invalid Number", "Referred"]

# Current state values agents mark in the sheet — used as include filter
ALL_CURRENT_STATES  = [
    "Interested",
    "Rescheduled",
    "Attempt Again",
    "Attempt Again after 3 months",
    "Shared Story",
    "Do not Disturb",
    "Meeting Requested",
    "Meeting Scheduled",
    "Allocate Again",
    "Allocate Again 3 months",
    "Not interested",
    "Escalate",
    "Respondent",
    "Dream Snapshot Confirmed",
    "Snapshot Sent",
    "Snapshot Confirmed",
]

# Terminal flags always excluded from the pool
_ALWAYS_EXCLUDE_FLAGS = (
    "shared_story", "snapshot_sent",
    "mql_in_progress", "mql_qualified", "mql_rejected",
    "invalid_number", "referred", "language_issue",
)

DEFAULT_CALL_STATUSES  = ["Connected"]
DEFAULT_CURRENT_STATES = ["Interested", "Rescheduled", "Attempt Again"]


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def build_filter_sql(filters: dict) -> tuple[str, dict]:
    """
    Build the SELECT SQL + params dict from a filters dict.
    Returns (sql, params).  The query returns one row per contact.
    """
    sql = """
        SELECT DISTINCT
            c.id               AS contact_id,
            c.first_name,
            c.last_name,
            co.name            AS company,
            c.contact_flag,
            c.campaign,
            MAX(ca.current_state)
                FILTER (WHERE ca.current_state IS NOT NULL)
                                AS last_current_state,
            MAX(ca.called_at)  AS last_called_at,
            COUNT(ca.id)       AS total_calls
        FROM contacts c
        JOIN call_actions ca ON ca.contact_id = c.id
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE
            c.campaign = %(campaign)s
            AND ca.call_status = ANY(%(call_statuses)s)
            AND (
                %(current_states)s IS NULL
                OR ca.current_state = ANY(%(current_states)s)
            )
            AND c.contact_flag NOT IN %(flags_exclude)s
            AND NOT EXISTS (
                SELECT 1 FROM contact_allocations a
                WHERE a.contact_id = c.id
                  AND a.closed_at IS NULL
            )
    """
    params: dict = {
        "campaign":       filters.get("campaign", "consulting"),
        "campaign_like":  f"{filters.get('campaign', 'consulting')} %",
        "call_statuses":  filters.get("call_statuses", DEFAULT_CALL_STATUSES),
        "current_states": filters.get("current_states") or None,
        "flags_exclude":  tuple(_ALWAYS_EXCLUDE_FLAGS),
    }

    # Preset: Interested but not Shared Story
    if filters.get("preset_interested_not_story"):
        sql += """
            AND EXISTS (
                SELECT 1 FROM call_actions ca2
                WHERE ca2.contact_id = c.id
                  AND ca2.current_state = 'Interested'
            )
        """

    # Preset: Allocate Again after 90 days (window has passed)
    if filters.get("preset_3months_ready"):
        sql += """
            AND (
                EXISTS (
                    SELECT 1 FROM call_actions ca3
                    WHERE ca3.contact_id = c.id
                      AND ca3.current_state IN (
                          'Attempt Again after 3 months',
                          'Allocate Again 3 months'
                      )
                      AND ca3.called_at <= NOW() - INTERVAL '90 days'
                )
                OR EXISTS (
                    SELECT 1
                    FROM mql_call_attempts mca3
                    JOIN mql_allocations ma3 ON ma3.id = mca3.allocation_id
                    JOIN agents am3 ON am3.id = mca3.agent_id
                    WHERE mca3.contact_id = c.id
                      AND (ma3.campaign = %(campaign)s OR ma3.campaign ILIKE %(campaign_like)s)
                      AND ma3.close_reason IS DISTINCT FROM 'bd_history'
                      AND am3.team = 'mql'
                      AND mca3.current_state IN (
                          'Attempt Again after 3 months',
                          'Allocate Again 3 months'
                      )
                      AND mca3.called_at <= NOW() - INTERVAL '90 days'
                )
            )
        """

    # optional date range
    date_from = filters.get("date_from")
    date_to   = filters.get("date_to")
    if date_from:
        sql += "\n            AND ca.called_at >= %(date_from)s"
        params["date_from"] = date_from
    if date_to:
        sql += "\n            AND ca.called_at <  %(date_to)s + INTERVAL '1 day'"
        params["date_to"] = date_to

    sql += """
        GROUP BY c.id, c.first_name, c.last_name, co.name, c.contact_flag, c.campaign
        ORDER BY MAX(ca.called_at) DESC
    """
    return sql, params


def run_preview(filters: dict) -> pd.DataFrame:
    sql, params = build_filter_sql(filters)
    return query_df(sql, params=params)


def load_campaigns() -> pd.DataFrame:
    return query_df("""
        SELECT id, name, status, contact_count, created_at, allocated_at,
               description, filters::text AS filters
        FROM reallocation_campaigns
        ORDER BY created_at DESC
        LIMIT 50
    """)


def load_campaign_contacts(campaign_id: int) -> pd.DataFrame:
    return query_df("""
        SELECT
            rcc.contact_id,
            c.first_name,
            c.last_name,
            co.name   AS company,
            c.contact_flag,
            ag.name   AS assigned_agent,
            rcc.allocated_at
        FROM reallocation_campaign_contacts rcc
        JOIN contacts c  ON c.id  = rcc.contact_id
        LEFT JOIN companies co ON co.id = c.company_id
        LEFT JOIN agents ag    ON ag.id = rcc.agent_id
        WHERE rcc.campaign_id = %(cid)s
        ORDER BY rcc.allocated_at NULLS LAST, c.last_name, c.first_name
    """, params={"cid": campaign_id})


# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────

tab_create, tab_manage = st.tabs(["Create Campaign", "Manage Campaigns"])


# ═════════════════════════════════════════════════════════════
# TAB 1 — CREATE CAMPAIGN
# ═════════════════════════════════════════════════════════════

with tab_create:

    st.subheader("Step 1 — Build Filters")
    st.caption("Define who should be re-allocated. The preview shows matching contacts before you commit.")

    # ── Quick preset ──────────────────────────────────────────
    col_p1, col_p2 = st.columns(2)

    preset_interested = col_p1.checkbox(
        "Preset: Interested but not yet Shared Story",
        value=False,
        help=(
            "Contacts who were marked Interested by a BD agent but "
            "haven't reached Shared Story in subsequent calls. "
            "Great for warm re-contact campaigns."
        ),
    )

    preset_3months = col_p2.checkbox(
        "Preset: Allocate Again after 90 days (window passed)",
        value=False,
        help=(
            "Contacts marked 'Attempt Again after 3 months' where the 90-day "
            "wait period has now elapsed. This filter updates automatically — "
            "new contacts are included as their window passes."
        ),
    )

    if preset_interested:
        st.info(
            "Preset active: contacts with at least one call marked **Interested** "
            "that have not yet reached Shared Story or MQL stages."
        )
    if preset_3months:
        st.info(
            "Preset active: contacts marked **Attempt Again after 3 months** or "
            "**Allocate Again 3 months** where the call was logged more than 90 days ago "
            "(checked across BD and MQL logs). The pool grows automatically as time passes."
        )

    with st.form("filter_form"):
        col1, col2 = st.columns(2)

        with col1:
            call_statuses = st.multiselect(
                "Call Status (at least one call must match)",
                options=ALL_CALL_STATUSES,
                default=DEFAULT_CALL_STATUSES,
                help="Include contacts who had at least one call with this status.",
            )
            current_states = st.multiselect(
                "Current State (leave empty to include all)",
                options=ALL_CURRENT_STATES,
                default=[] if preset_interested else DEFAULT_CURRENT_STATES,
                help="Include contacts where at least one call had this Current State marked by the agent.",
            )

        with col2:
            st.caption(
                "**Auto-excluded:** Contacts with flags shared_story, snapshot_sent, "
                "mql_in_progress, mql_qualified, mql_rejected, invalid_number, referred, "
                "or language_issue are always excluded from the pool."
            )
            c_from, c_to = st.columns(2)
            with c_from:
                date_from = st.date_input("Called from (optional)", value=None,
                                          help="Filter call_actions from this date onwards.")
            with c_to:
                date_to   = st.date_input("Called to (optional)",   value=None,
                                          help="Filter call_actions up to and including this date.")

        preview_btn = st.form_submit_button("Preview matching contacts", type="primary",
                                            use_container_width=True)

    # ── Preview result ──────────────────────────────────────
    if preview_btn:
        if not call_statuses:
            st.error("Select at least one Call Status.")
            st.stop()

        filters_built = {
            "campaign":                    campaign,
            "call_statuses":               call_statuses,
            "current_states":              current_states if current_states else None,
            "preset_interested_not_story": preset_interested,
            "preset_3months_ready":        preset_3months,
            "date_from":                   str(date_from) if date_from else None,
            "date_to":                     str(date_to)   if date_to   else None,
        }

        with st.spinner("Running filter query…"):
            try:
                df_preview = run_preview(filters_built)
                st.session_state["rc_preview_df"]      = df_preview
                st.session_state["rc_preview_filters"] = filters_built
            except Exception as e:
                log_and_show("reallocation preview", e)
                df_preview = pd.DataFrame()
                st.session_state.pop("rc_preview_df",      None)
                st.session_state.pop("rc_preview_filters", None)

    df_preview = st.session_state.get("rc_preview_df")

    if df_preview is not None:
        st.divider()
        st.subheader("Step 2 — Verify Results")

        n = len(df_preview)
        flag_counts = df_preview["contact_flag"].value_counts()

        m1, m2, m3 = st.columns(3)
        m1.metric("Contacts in pool", n)
        m2.metric("Unique companies",
                  df_preview["company"].nunique() if not df_preview.empty else 0)
        m3.metric("Distinct current states",
                  df_preview["last_current_state"].nunique() if not df_preview.empty else 0)

        if not df_preview.empty:
            # Flag breakdown
            with st.expander("Contact flag breakdown", expanded=False):
                fc_df = flag_counts.reset_index()
                fc_df.columns = ["flag", "count"]
                st.dataframe(fc_df, hide_index=True, use_container_width=True)

            # Sample rows (first 100)
            st.markdown(f"**Sample rows** (showing first 100 of {n}):")
            st.dataframe(
                df_preview.head(100)[[
                    "contact_id", "first_name", "last_name", "company",
                    "contact_flag", "last_current_state", "last_called_at", "total_calls"
                ]],
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No contacts matched the filters. Adjust and preview again.")

        # ── Save Campaign ──────────────────────────────────
        if n > 0:
            st.divider()
            st.subheader("Step 3 — Save Campaign")

            with st.form("save_campaign_form"):
                camp_name = st.text_input(
                    "Campaign name",
                    placeholder="e.g. March Warm Re-connect",
                    help="Unique name to identify this batch.",
                )
                camp_desc = st.text_area(
                    "Description (optional)",
                    placeholder="Who is this for? Any notes.",
                    height=80,
                )
                save_btn = st.form_submit_button("Save Campaign & Contact List",
                                                 use_container_width=True, type="primary")

            if save_btn:
                if not camp_name.strip():
                    st.error("Campaign name is required.")
                else:
                    filters_to_save = st.session_state.get("rc_preview_filters", {})
                    try:
                        # Insert campaign and get the new id
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO reallocation_campaigns
                                        (name, description, filters, status,
                                         contact_count, created_by)
                                    VALUES (%s, %s, %s::jsonb, 'verified', %s, 'dashboard')
                                    RETURNING id
                                """, (
                                    camp_name.strip(),
                                    camp_desc.strip() or None,
                                    json.dumps(filters_to_save),
                                    n,
                                ))
                                new_id = cur.fetchone()[0]
                            conn.commit()

                        # Insert contacts
                        contact_ids = df_preview["contact_id"].tolist()
                        execute_many(
                            """INSERT INTO reallocation_campaign_contacts
                               (campaign_id, contact_id)
                               VALUES (%s, %s)
                               ON CONFLICT DO NOTHING""",
                            [(new_id, cid) for cid in contact_ids],
                        )

                        st.toast(f"Campaign '{camp_name}' saved with {n} contacts.")
                        # Clear preview state
                        st.session_state.pop("rc_preview_df",      None)
                        st.session_state.pop("rc_preview_filters", None)
                        st.rerun()

                    except Exception as e:
                        log_and_show("save campaign", e)


# ═════════════════════════════════════════════════════════════
# TAB 2 — MANAGE CAMPAIGNS
# ═════════════════════════════════════════════════════════════

with tab_manage:

    st.subheader("Saved Campaigns")

    try:
        camps = load_campaigns()
    except Exception as e:
        log_and_show("load campaigns", e)
        camps = pd.DataFrame()

    if camps.empty:
        st.info("No re-allocation campaigns yet. Create one in the **Create Campaign** tab.")
        st.stop()

    STATUS_COLOURS = {
        "draft":     "⚪",
        "verified":  "🟡",
        "allocated": "🟢",
        "closed":    "🔴",
    }

    display_camps = camps.copy()
    display_camps["status"] = display_camps["status"].map(
        lambda s: f"{STATUS_COLOURS.get(s, '')} {s}"
    )
    st.dataframe(
        display_camps[["id", "name", "status", "contact_count", "created_at", "allocated_at", "description"]],
        use_container_width=True, hide_index=True,
    )

    st.divider()

    # ── Campaign selector ──────────────────────────────────
    verified_camps = camps[camps["status"].isin(["verified", "allocated"])]

    if verified_camps.empty:
        st.info("No verified campaigns to allocate. Save a campaign first.")
        st.stop()

    camp_options = {
        f"[{row['status'].upper()}] {row['name']} ({row['contact_count']} contacts)": row
        for _, row in verified_camps.iterrows()
    }
    selected_label = st.selectbox("Select campaign to view / allocate", list(camp_options.keys()))
    sel_camp       = camp_options[selected_label]
    sel_camp_id    = int(sel_camp["id"])
    sel_camp_name  = sel_camp["name"]
    sel_camp_status = sel_camp["status"]

    # Show filters used
    fdata_raw = sel_camp["filters"] if "filters" in sel_camp.index else None
    if fdata_raw and fdata_raw != "{}":
        with st.expander("Filters used for this campaign", expanded=False):
            try:
                fdata = json.loads(fdata_raw) if isinstance(fdata_raw, str) else fdata_raw
                st.json(fdata)
            except Exception:
                st.text(sel_camp.get("filters", ""))

    # ── Contact list for selected campaign ────────────────
    try:
        contacts_df = load_campaign_contacts(sel_camp_id)
    except Exception as e:
        log_and_show("load campaign contacts", e)
        contacts_df = pd.DataFrame()

    n_contacts = len(contacts_df)
    n_allocated = contacts_df["assigned_agent"].notna().sum() if not contacts_df.empty else 0

    col_a, col_b = st.columns(2)
    col_a.metric("Total contacts", n_contacts)
    col_b.metric("Already allocated", n_allocated)

    if not contacts_df.empty:
        st.dataframe(
            contacts_df[["contact_id", "first_name", "last_name", "company",
                         "contact_flag", "assigned_agent", "allocated_at"]],
            use_container_width=True, hide_index=True,
        )

    # ── Allocate section ──────────────────────────────────
    n_pending   = int(contacts_df["assigned_agent"].isna().sum()) if not contacts_df.empty else 0
    n_done      = n_contacts - n_pending

    # Progress bar (always visible)
    if n_contacts > 0:
        st.progress(n_done / n_contacts,
                    text=f"Progress: **{n_done} / {n_contacts}** contacts allocated"
                         + (f" — {n_pending} remaining in pool" if n_pending else " — pool complete ✓"))

    if sel_camp_status == "allocated":
        st.success("All contacts from this campaign have been allocated.")

    elif sel_camp_status == "verified" and n_pending > 0:
        st.divider()
        st.subheader("Allocate Today's Batch")
        st.caption(
            "Contacts are allocated in daily batches — come back each day and run this "
            "to pull the next slice from the pool. The pool empties gradually over several days."
        )

        # Load active agents
        try:
            agents_df = query_df("""
                SELECT id, name, kpi_dialed, shift_name, is_on_leave
                FROM agents
                WHERE status = 'active'
                ORDER BY is_on_leave, name
            """)
        except Exception as e:
            log_and_show("load agents", e)
            st.stop()

        if agents_df.empty:
            st.warning("No active agents found.")
            st.stop()

        agent_options = [
            f"{row['name']}{' (on leave)' if row['is_on_leave'] else ''}"
            for _, row in agents_df.iterrows()
        ]
        selected_agents = st.multiselect(
            "Agents for today's batch",
            options=agent_options,
            default=[a for a in agent_options if "(on leave)" not in a],
            help="Only select agents who are working today.",
        )

        col_b1, col_b2 = st.columns(2)
        with col_b1:
            per_agent_limit = st.number_input(
                "Contacts per agent today",
                min_value=1, max_value=500, value=60, step=10,
                help="How many contacts to push to each agent in this batch. "
                     "Typical: 60 (half their daily KPI, leaving room for regular fresh contacts).",
            )
        with col_b2:
            attempt_num = st.number_input(
                "Attempt number",
                min_value=0, max_value=5, value=1,
                help="0 = New Contact, 1 = FU1, … 5 = FU5.",
            )

        # Live batch size preview
        if selected_agents:
            n_agents      = len(selected_agents)
            batch_total   = min(n_agents * per_agent_limit, n_pending)
            after_today   = n_pending - batch_total
            days_left     = -(-after_today // (n_agents * per_agent_limit)) if per_agent_limit > 0 else "?"

            st.info(
                f"Today: **{batch_total}** contacts pushed "
                f"(**{per_agent_limit}** each to {n_agents} agent(s))  ·  "
                f"Remaining after today: **{after_today}**"
                + (f"  ·  ~{days_left} more day(s) at this rate" if after_today > 0 else "  ·  Pool will be empty after today ✓")
            )

        confirmed = st.checkbox(
            f"I confirm: push today's batch of up to "
            f"{len(selected_agents) * per_agent_limit if selected_agents else 0} contacts"
        )
        allocate_btn = st.button(
            "Allocate Today's Batch",
            disabled=(not confirmed or not selected_agents),
            type="primary",
            use_container_width=True,
        )

        if allocate_btn and confirmed and selected_agents:
            # Resolve agent ids
            sel_agent_ids = []
            for label in selected_agents:
                name_only = label.replace(" (on leave)", "").strip()
                match = agents_df[agents_df["name"] == name_only]
                if not match.empty:
                    sel_agent_ids.append(int(match.iloc[0]["id"]))

            if not sel_agent_ids:
                st.error("Could not resolve agent IDs.")
                st.stop()

            # Grab only the pending contacts (those not yet allocated)
            pool = contacts_df[contacts_df["assigned_agent"].isna()]["contact_id"].tolist()

            # Slice: per_agent_limit per agent, round-robin
            # Build per-agent buckets up to limit, then interleave
            buckets: dict[int, list[int]] = {aid: [] for aid in sel_agent_ids}
            for idx, cid in enumerate(pool):
                aid = sel_agent_ids[idx % len(sel_agent_ids)]
                if len(buckets[aid]) < per_agent_limit:
                    buckets[aid].append(cid)
                # stop once all buckets are full
                if all(len(b) >= per_agent_limit for b in buckets.values()):
                    break

            assignments = [(cid, aid) for aid, cids in buckets.items() for cid in cids]

            if not assignments:
                st.warning("No contacts to allocate (pool may be empty or limit is 0).")
                st.stop()

            with st.spinner(f"Allocating {len(assignments)} contacts…"):
                try:
                    # Insert into contact_allocations
                    execute_many("""
                        INSERT INTO contact_allocations
                            (contact_id, agent_id, allocated_date, attempt_number, allocated_at)
                        VALUES (%s, %s, CURRENT_DATE, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """, [(cid, aid, int(attempt_num)) for cid, aid in assignments])

                    # Mark these contacts as allocated in campaign contacts table
                    execute_many("""
                        UPDATE reallocation_campaign_contacts
                           SET agent_id = %s, allocated_at = NOW()
                         WHERE campaign_id = %s AND contact_id = %s
                    """, [(aid, sel_camp_id, cid) for cid, aid in assignments])

                    # Update contact_flag → needs_followup
                    execute("""
                        UPDATE contacts
                           SET contact_flag    = 'needs_followup',
                               flag_updated_at = NOW()
                         WHERE id = ANY(%s)
                           AND contact_flag NOT IN (
                               'invalid_number','referred','language_issue',
                               'shared_story','snapshot_sent'
                           )
                    """, ([cid for cid, _ in assignments],))

                    # If pool is now empty, mark campaign as fully allocated
                    remaining = n_pending - len(assignments)
                    if remaining <= 0:
                        execute("""
                            UPDATE reallocation_campaigns
                               SET status = 'allocated', allocated_at = NOW(),
                                   updated_at = NOW()
                             WHERE id = %s
                        """, (sel_camp_id,))
                        st.toast(
                            f"Pool complete! All {n_contacts} contacts from '{sel_camp_name}' allocated."
                        )
                    else:
                        st.toast(
                            f"Batch done: {len(assignments)} contacts allocated. {remaining} remaining."
                        )
                    st.rerun()

                except Exception as e:
                    log_and_show("reallocation execute", e)

    # ── Close campaign ─────────────────────────────────────
    st.divider()
    with st.expander("Archive / close this campaign", expanded=False):
        st.caption("Marks the campaign as closed. Does not affect any allocations.")
        if st.button("Close campaign", key="close_camp_btn"):
            try:
                execute("""
                    UPDATE reallocation_campaigns
                       SET status = 'closed', updated_at = NOW()
                     WHERE id = %s
                """, (sel_camp_id,))
                st.toast(f"Campaign '{sel_camp_name}' closed.")
                st.rerun()
            except Exception as e:
                log_and_show("close campaign", e)
