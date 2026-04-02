"""
pages/2_Contacts.py — Contact Search & History
- Multi-filter search: text, flag, date range, agent
- Summary metrics: total count + flag breakdown
- Table with Filled By + Recording / Transcript / Dream Snapshot links
- Contact detail: phones, emails, full call history, allocation history
"""

import streamlit as st
import pandas as pd
from utils.db import query_df
from utils.errors import log_and_show, log_and_warn

campaign = st.session_state.get("campaign", "consulting")
st.title("Contacts")
st.caption(f"Campaign: **{campaign.title()}**")

# ── FILTERS ───────────────────────────────────────────────────
st.subheader("Search & Filter")

# Row 1: search text + flag
r1a, r1b = st.columns([3, 1])
search_term = r1a.text_input(
    "Search by name, company, phone, or Unique ID",
    placeholder="e.g. Raj, Reliance, 9876543210, RR | 75863932",
)
search_flag = r1b.selectbox("Flag", options=[
    "All",
    "shared_story", "snapshot_sent",
    "in_progress", "needs_followup", "fresh",
    "not_interested", "dnd", "attempt_3_months",
    "invalid_number", "referred", "language_issue",
])

# Row 2: date range + agent
r2a, r2b, r2c = st.columns([1, 1, 2])
date_from       = r2a.date_input("Flag date from", value=None)
date_to         = r2b.date_input("Flag date to",   value=None)

try:
    _agents      = query_df("SELECT name FROM agents WHERE status = 'active' ORDER BY name")
    agent_options = _agents["name"].tolist()
except Exception:
    agent_options = []
selected_agents = r2c.multiselect("Filled by (agent)", options=agent_options)

any_filter = bool(
    search_term.strip() or search_flag != "All"
    or date_from or date_to or selected_agents
)

if not any_filter:
    st.info("Enter a search term or apply a filter to see contacts.")
else:
    try:
        # ── Build WHERE ──────────────────────────────────────
        where_parts: list[str] = ["c.campaign = %s"]
        params:      list      = [campaign]

        if search_term.strip():
            where_parts.append("""(
                c.first_name ILIKE %s
                OR c.last_name  ILIKE %s
                OR co.name      ILIKE %s
                OR c.source_id  ILIKE %s
                OR EXISTS (
                    SELECT 1 FROM contact_phones p
                    WHERE p.contact_id = c.id AND p.phone_number ILIKE %s
                )
            )""")
            like = f"%{search_term.strip()}%"
            params += [like, like, like, like, like]

        if search_flag != "All":
            where_parts.append("c.contact_flag = %s")
            params.append(search_flag)

        if date_from:
            where_parts.append("c.flag_updated_at::date >= %s")
            params.append(date_from)

        if date_to:
            where_parts.append("c.flag_updated_at::date <= %s")
            params.append(date_to)

        if selected_agents:
            where_parts.append("lat.last_agent = ANY(%s)")
            params.append(selected_agents)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        # LATERAL join: most recent call per contact
        LATERAL = """
            LEFT JOIN LATERAL (
                SELECT
                    ca.recording_link,
                    ca.transcript_link,
                    ca.dream_snapshot_link,
                    a.name       AS last_agent,
                    ca.called_at AS last_called
                FROM call_actions ca
                JOIN agents a ON a.id = ca.agent_id
                WHERE ca.contact_id = c.id
                ORDER BY ca.called_at DESC
                LIMIT 1
            ) lat ON TRUE
        """

        FROM_CLAUSE = f"""
            FROM contacts c
            LEFT JOIN companies co ON co.id = c.company_id
            {LATERAL}
            {where_sql}
        """

        # ── Total count (no LIMIT) ────────────────────────────
        count_row   = query_df(f"SELECT COUNT(*) AS total {FROM_CLAUSE}", params=params)
        total_count = int(count_row["total"].iloc[0])

        # ── Flag breakdown ────────────────────────────────────
        breakdown = query_df(
            f"SELECT c.contact_flag AS flag, COUNT(*) AS cnt {FROM_CLAUSE} "
            f"GROUP BY c.contact_flag ORDER BY cnt DESC",
            params=params,
        )

        # ── Main results (max 200) ────────────────────────────
        results = query_df(f"""
            SELECT
                c.id,
                CASE c.source
                    WHEN 'rocketreach' THEN 'RR | ' || c.source_id
                    WHEN 'msme'        THEN 'MS | ' || c.source_id
                    WHEN 'pharma'      THEN 'PH | ' || c.source_id
                    ELSE 'MANUAL'
                END                          AS unique_id,
                c.first_name || ' ' || COALESCE(c.last_name, '') AS name,
                co.name                      AS company,
                c.designation,
                c.contact_flag,
                c.flag_updated_at::date      AS flag_date,
                lat.last_agent,
                lat.recording_link,
                lat.transcript_link,
                lat.dream_snapshot_link
            {FROM_CLAUSE}
            ORDER BY c.flag_updated_at DESC NULLS LAST
            LIMIT 200
        """, params=params)

        # ── Metrics bar ───────────────────────────────────────
        st.divider()

        m_cols = st.columns(min(len(breakdown) + 1, 8))
        m_cols[0].metric("Total contacts", f"{total_count:,}",
                         delta=f"showing {len(results)}" if total_count > len(results) else None,
                         delta_color="off")

        for i, (_, brow) in enumerate(breakdown.iterrows(), start=1):
            if i < len(m_cols):
                m_cols[i].metric(brow["flag"], int(brow["cnt"]))

        st.divider()

        # ── Results table ─────────────────────────────────────
        if results.empty:
            st.info("No contacts match the current filters.")
        else:
            # Clean link columns: replace NaN / bare "None" string / empty with None
            for link_col in ["recording_link", "transcript_link", "dream_snapshot_link"]:
                mask = (
                    results[link_col].isna()
                    | (results[link_col].astype(str).str.strip() == "")
                    | (results[link_col].astype(str) == "None")
                )
                results.loc[mask, link_col] = None

            st.dataframe(
                results.rename(columns={
                    "id":                  "ID",
                    "unique_id":           "Unique ID",
                    "name":                "Name",
                    "company":             "Company",
                    "designation":         "Title",
                    "contact_flag":        "Flag",
                    "flag_date":           "Flag Date",
                    "last_agent":          "Filled By",
                    "recording_link":      "Recording",
                    "transcript_link":     "Transcript",
                    "dream_snapshot_link": "Dream Snapshot",
                }),
                column_config={
                    "Recording":      st.column_config.LinkColumn("Recording",      display_text="Open"),
                    "Transcript":     st.column_config.LinkColumn("Transcript",     display_text="Open"),
                    "Dream Snapshot": st.column_config.LinkColumn("Dream Snapshot", display_text="Open"),
                },
                use_container_width=True,
                hide_index=True,
            )

    except Exception as e:
        log_and_show("contact search", e)

st.divider()

# ── CONTACT DETAIL ────────────────────────────────────────────
st.subheader("Contact detail")
contact_id = st.number_input("Enter Contact ID to see full history", min_value=1, step=1)

if contact_id:
    try:
        contact = query_df("""
            SELECT
                c.id,
                CASE c.source
                    WHEN 'rocketreach' THEN 'RR | ' || c.source_id
                    ELSE c.source || ' | ' || COALESCE(c.source_id, 'manual')
                END AS unique_id,
                c.first_name, c.last_name, c.designation,
                co.name AS company, co.raw_address,
                c.contact_flag, c.flag_updated_at,
                c.source, c.status
            FROM contacts c
            LEFT JOIN companies co ON co.id = c.company_id
            WHERE c.id = %s
        """, params=(int(contact_id),))

        if contact.empty:
            st.warning(f"No contact with ID {contact_id}.")
        else:
            row = contact.iloc[0]

            col1, col2, col3 = st.columns([2, 2, 1])
            col1.markdown(f"### {row['first_name']} {row['last_name'] or ''}")
            col1.caption(f"{row['designation'] or '—'} · {row['company'] or '—'}")
            col2.markdown(f"**Unique ID:** {row['unique_id']}")
            col2.caption(f"Source: {row['source']} · Status: {row['status']}")
            col3.metric("Current flag", row["contact_flag"])

            phones = query_df("""
                SELECT phone_number, rank, is_invalid, invalidated_at
                FROM contact_phones WHERE contact_id = %s ORDER BY rank
            """, params=(int(contact_id),))

            emails = query_df("""
                SELECT email, rank FROM contact_emails
                WHERE contact_id = %s ORDER BY rank LIMIT 3
            """, params=(int(contact_id),))

            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Phone numbers**")
                if phones.empty:
                    st.caption("None")
                else:
                    for _, p in phones.iterrows():
                        badge = " [invalid]" if p["is_invalid"] else ""
                        st.caption(f"#{int(p['rank'])}: {p['phone_number']}{badge}")
            with c2:
                st.markdown("**Emails**")
                if emails.empty:
                    st.caption("None")
                else:
                    for _, e in emails.iterrows():
                        st.caption(f"#{int(e['rank'])}: {e['email']}")

            st.divider()

            st.markdown("**Call history**")
            history = query_df("""
                SELECT
                    ca.called_at,
                    a.name AS agent,
                    ca.attempt_number,
                    ca.phone_number_used,
                    ca.call_status,
                    ca.current_state,
                    ca.lead_category,
                    ca.call_duration,
                    ca.remark,
                    ca.recording_link,
                    ca.transcript_link,
                    ca.dream_snapshot_link
                FROM call_actions ca
                JOIN agents a ON a.id = ca.agent_id
                WHERE ca.contact_id = %s
                ORDER BY ca.called_at DESC
            """, params=(int(contact_id),))

            if history.empty:
                st.caption("No calls recorded for this contact.")
            else:
                st.caption(f"{len(history)} call(s) on record.")
                for _, h in history.iterrows():
                    attempt_label = ["New Contact","FU1","FU2","FU3","FU4","FU5"][int(h["attempt_number"])]
                    with st.expander(
                        f"{h['called_at'].strftime('%d %b %Y %H:%M') if h['called_at'] else '?'} "
                        f"· {h['agent']} · {attempt_label} · {h['call_status']}"
                    ):
                        c1, c2, c3 = st.columns(3)
                        c1.caption(f"**Phone:** {h['phone_number_used']}")
                        c2.caption(f"**Status:** {h['call_status']}")
                        c3.caption(
                            f"**Duration:** {int(h['call_duration'])//60}m {int(h['call_duration'])%60}s"
                            if h["call_duration"] else "**Duration:** —"
                        )
                        st.caption(f"**State:** {h['current_state'] or '—'}")
                        st.caption(f"**Category:** {h['lead_category'] or '—'}")
                        if h["remark"]:
                            st.markdown(f"> {h['remark']}")
                        if h["recording_link"]:
                            st.markdown(f"[Recording]({h['recording_link']})")
                        if h["transcript_link"]:
                            st.markdown(f"[Transcript]({h['transcript_link']})")
                        if h["dream_snapshot_link"]:
                            st.markdown(f"[Dream Snapshot]({h['dream_snapshot_link']})")

            st.markdown("**Allocation history**")
            allocs = query_df("""
                SELECT
                    ca.allocated_date,
                    a.name AS agent,
                    ca.attempt_number,
                    ca.closed_at::date AS closed,
                    ca.close_reason
                FROM contact_allocations ca
                JOIN agents a ON a.id = ca.agent_id
                WHERE ca.contact_id = %s
                ORDER BY ca.allocated_date DESC
            """, params=(int(contact_id),))

            if not allocs.empty:
                st.dataframe(allocs.rename(columns={
                    "allocated_date": "Allocated",
                    "agent":          "Agent",
                    "attempt_number": "Attempt",
                    "closed":         "Closed",
                    "close_reason":   "Reason",
                }), use_container_width=True, hide_index=True)

    except Exception as e:
        log_and_show("contact detail", e)
