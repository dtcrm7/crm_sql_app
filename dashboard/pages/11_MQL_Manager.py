"""
pages/11_MQL_Manager.py — MQL Allocation Manager
- Summary: active/closed allocations per MQL agent
- Browse all MQL allocations with filters
- Close/reallocate individual contacts
"""

import streamlit as st
import pandas as pd
from datetime import date
from pathlib import Path
import os
import re

import gspread
from google.oauth2.service_account import Credentials
from utils.db import query_df, execute, get_conn
from utils.errors import log_and_show, log_and_warn
from utils.mql_engine import run_mql_allocation

campaign = st.session_state.get("campaign", "consulting")
campaign_like = f"{campaign} %"
st.title("MQL Manager")
st.caption(f"Campaign: **{campaign.title()}**")

try:
    _esc_badge_df = query_df("""
        SELECT COUNT(*) AS escalated_count
        FROM mql_allocations ma
        WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
          AND ma.close_reason = 'escalated'
    """, params={"campaign": campaign, "campaign_like": campaign_like})
    _esc_badge = int(_esc_badge_df.iloc[0]["escalated_count"]) if not _esc_badge_df.empty else 0
    b1, _ = st.columns([1, 5])
    b1.metric("Escalations made", _esc_badge)
except Exception as e:
    log_and_warn("MQL escalation badge", e)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", str(PROJECT_ROOT / "credentials.json"))

MQL_TAB_1 = "MQL FU 1-15"
CONTACT_COLS = 12
FU_BLOCK_SIZE = 11

CONTACT_HEADERS = [
    "Unique ID", "Company Name", "Person Name", "Phone", "Email",
    "BD Agent", "BD Call Date", "BD Remark", "BD Recording Link",
    "Category", "BD Transcript", "Dream Snapshot",
]
FU_BLOCK_HEADERS = [
    "MQL Category", "Call Status", "Current State", "Call Duration",
    "Remark", "Recording Link", "Transcript", "Message Status",
    "Timestamp", "Follow-up Stage", "Sync Status",
]


def _build_tab_headers(fu_start: int, fu_end: int) -> list[str]:
    headers = list(CONTACT_HEADERS)
    for fu_num in range(fu_start, fu_end + 1):
        for col in FU_BLOCK_HEADERS:
            headers.append(f"FU{fu_num} — {col}")
    return headers


def _build_unique_id(source: str, source_id: str) -> str:
    if source_id:
        for pfx in ("BD", "CC", "AV", "BW"):
            if source_id.startswith(f"{pfx}-"):
                return f"{pfx} | {source_id[len(pfx)+1:]}"
        if source_id.startswith("ID-"):
            return source_id

    prefix_map = {
        "rocketreach": "RR",
        "msme": "MS",
        "pharma": "PH",
        "manual": "MN",
    }
    prefix = prefix_map.get((source or "").lower(), "RR")
    return f"{prefix} | {source_id}" if source_id else prefix


def _get_sheets_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def _ensure_mql_tab(sheet, tab_name: str, fu_start: int, fu_end: int):
    headers = _build_tab_headers(fu_start, fu_end)
    total_cols = len(headers)
    existing = [ws.title for ws in sheet.worksheets()]
    if tab_name not in existing:
        ws = sheet.add_worksheet(title=tab_name, rows=2000, cols=total_cols + 5)
        ws.update(range_name="A1", values=[headers])
        return ws
    ws = sheet.worksheet(tab_name)
    row1 = ws.row_values(1)
    if len(row1) != total_cols or (row1 and row1[0] != headers[0]):
        ws.update(range_name="A1", values=[headers])
    return ws


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    return digits[-10:] if len(digits) >= 10 else ""

# ── SUMMARY ───────────────────────────────────────────────────
st.subheader("Allocation summary")

try:
    summary_df = query_df("""
        SELECT
            a.name                                                  AS agent,
            COUNT(*) FILTER (WHERE ma.closed_at IS NULL)           AS active,
            COUNT(*) FILTER (WHERE ma.close_reason = 'qualified')  AS qualified,
            COUNT(*) FILTER (WHERE ma.close_reason = 'rejected')   AS rejected,
            COUNT(*) FILTER (WHERE ma.close_reason = 'stalled')    AS stalled,
            COUNT(*) FILTER (WHERE ma.close_reason = 'escalated')  AS escalated,
            COUNT(*) FILTER (WHERE ma.close_reason = 'reallocated') AS reallocated,
            COUNT(*)                                                AS total
        FROM mql_allocations ma
        JOIN agents a ON a.id = ma.agent_id
        WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
        GROUP BY a.name
        ORDER BY a.name
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if summary_df.empty:
        st.info("No MQL allocations yet for this campaign.")
    else:
        # Totals row
        totals = summary_df[["active","qualified","rejected","stalled","escalated","reallocated","total"]].sum()
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Active",       int(totals["active"]))
        c2.metric("Qualified",    int(totals["qualified"]))
        c3.metric("Rejected",     int(totals["rejected"]))
        c4.metric("Stalled",      int(totals["stalled"]))
        c5.metric("Escalated (Hist)",    int(totals["escalated"]))
        c6.metric("Reallocated (Hist)",  int(totals["reallocated"]))

        st.dataframe(summary_df, width="stretch", hide_index=True)

except Exception as e:
    log_and_warn("MQL summary", e)

st.divider()

# ── 3-MONTH READY QUEUE ─────────────────────────────────────
st.subheader("MQL 3-month queue")
st.caption("Ready contacts from Attempt Again after 3 months. Assign directly from here.")

try:
    three_month_df = query_df("""
        SELECT
            c.id,
            co.name AS company,
            c.first_name || ' ' || c.last_name AS person,
            c.bd_category AS category,
            COALESCE(c.flag_updated_at::date, CURRENT_DATE) AS flagged_on,
            (CURRENT_DATE - COALESCE(c.flag_updated_at::date, CURRENT_DATE))::int AS age_days,
            ag.name AS bd_agent,
            ca.current_state AS bd_state
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
        LEFT JOIN LATERAL (
            SELECT agent_id, current_state, called_at
            FROM call_actions
            WHERE contact_id = c.id
            ORDER BY called_at DESC NULLS LAST
            LIMIT 1
        ) ca ON TRUE
        LEFT JOIN agents ag ON ag.id = ca.agent_id
        WHERE c.campaign = %(campaign)s
          AND c.contact_flag = 'attempt_3_months'
          AND COALESCE(c.flag_updated_at::date, CURRENT_DATE)
                <= (CURRENT_DATE - INTERVAL '90 days')::date
          AND NOT EXISTS (
              SELECT 1 FROM mql_allocations ma
              WHERE ma.contact_id = c.id
                AND ma.closed_at IS NULL
                AND (
                    ma.campaign = %(campaign)s
                    OR ma.campaign ILIKE %(campaign_like)s
                )
          )
        ORDER BY age_days DESC, flagged_on ASC, c.id
        LIMIT 200
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if three_month_df.empty:
        st.info("No 3-month-ready contacts at the moment.")
    else:
        q1, q2, q3 = st.columns(3)
        q1.metric("3M Ready", int(len(three_month_df)))
        q2.metric("Avg aging", f"{int(round(three_month_df['age_days'].mean()))} days")
        q3.metric("Max aging", f"{int(three_month_df['age_days'].max())} days")
        st.caption("Counts update automatically after MQL sync and manager reallocation actions.")

        mql_targets_df = query_df("""
            SELECT id, name
            FROM agents
            WHERE status = 'active' AND team = 'mql'
            ORDER BY name
        """)

        if mql_targets_df.empty:
            st.warning("No active MQL agents available for 3-month assignment.")
        else:
            target_options = {
                row["name"]: int(row["id"])
                for _, row in mql_targets_df.iterrows()
            }
            selected_3m_agent = st.selectbox(
                "Assign 3-month contacts to",
                list(target_options.keys()),
                key="mql_3m_target_agent",
            )
            selected_3m_agent_id = target_options[selected_3m_agent]

            for _, row in three_month_df.iterrows():
                cid = int(row["id"])
                company = row["company"] or "No company"
                person = row["person"] or "Unknown"
                category = row["category"] or "Uncategorized"
                age_days = int(row["age_days"] or 0)
                bd_agent = row["bd_agent"] or "-"
                bd_state = row["bd_state"] or "-"

                rc1, rc2, rc3, rc4, rc5 = st.columns([3, 2, 2, 2, 1])
                rc1.markdown(f"**#{cid} {person}**")
                rc1.caption(company)
                rc2.markdown(f"Category: {category}")
                rc3.markdown(f"Aging: {age_days} days")
                rc4.markdown(f"BD context: {bd_agent} - {bd_state}")
                if rc5.button("Assign", key=f"mql_mgr_3m_assign_{cid}"):
                    with st.spinner(f"Assigning contact #{cid} to {selected_3m_agent}..."):
                        success, output = run_mql_allocation(
                            agent_id=selected_3m_agent_id,
                            count=1,
                            campaign=campaign,
                            dry_run=False,
                            contact_ids=[cid],
                        )
                    if success:
                        st.success(f"Assigned contact #{cid} to {selected_3m_agent}.")
                        st.rerun()
                    else:
                        st.error(f"Assignment failed for contact #{cid}. Check output below.")
                        st.code(output, language=None)
except Exception as e:
    log_and_warn("MQL 3-month queue", e)

st.divider()

# ── ESCALATION QUEUE ─────────────────────────────────────────
st.subheader("Escalation queue")
st.caption("Pending-now queue only: latest escalated allocation with no later allocation for that contact.")

try:
    esc_counts = query_df("""
        WITH escalated_base AS (
            SELECT
                ma.id AS old_alloc_id,
                ma.contact_id,
                ma.campaign,
                ma.closed_at,
                COALESCE(last_try.called_at, ma.closed_at, ma.allocated_at) AS escalated_at,
                last_try.current_state AS latest_state
            FROM mql_allocations ma
            LEFT JOIN LATERAL (
                SELECT mca.called_at, mca.current_state
                FROM mql_call_attempts mca
                WHERE mca.allocation_id = ma.id
                ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                LIMIT 1
            ) last_try ON TRUE
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason = 'escalated'
        )
        SELECT
            COUNT(*) AS escalated_historical,
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1
                    FROM mql_allocations nx
                    WHERE nx.contact_id = eb.contact_id
                      AND (nx.campaign = %(campaign)s OR nx.campaign ILIKE %(campaign_like)s)
                      AND nx.id > eb.old_alloc_id
                )
            ) AS reallocated_after,
            COUNT(*) FILTER (
                WHERE COALESCE(eb.latest_state, 'Escalate') = 'Escalate'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM mql_allocations nx
                      WHERE nx.contact_id = eb.contact_id
                        AND (nx.campaign = %(campaign)s OR nx.campaign ILIKE %(campaign_like)s)
                        AND nx.id > eb.old_alloc_id
                  )
            ) AS pending_now_entries,
            COUNT(DISTINCT eb.contact_id) FILTER (
                WHERE COALESCE(eb.latest_state, 'Escalate') = 'Escalate'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM mql_allocations nx
                      WHERE nx.contact_id = eb.contact_id
                        AND (nx.campaign = %(campaign)s OR nx.campaign ILIKE %(campaign_like)s)
                        AND nx.id > eb.old_alloc_id
                  )
            ) AS pending_now_contacts
        FROM escalated_base eb
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if not esc_counts.empty:
        ec = esc_counts.iloc[0]
        e1, e2, e3, e4 = st.columns(4)
        e1.metric("Escalated (historical)", int(ec["escalated_historical"]))
        e2.metric("Reallocated after escalation", int(ec["reallocated_after"]))
        e3.metric("Pending now (entries)", int(ec["pending_now_entries"]))
        e4.metric("Pending now (contacts)", int(ec["pending_now_contacts"]))

    esc_queue = query_df("""
        WITH escalated AS (
            SELECT
                ma.id            AS old_alloc_id,
                ma.contact_id,
                ma.campaign,
                ma.closed_at,
                a.name           AS escalated_by,
                co.name          AS company,
                c.first_name || ' ' || c.last_name AS person,
                COALESCE(mca.called_at, ma.closed_at, ma.allocated_at) AS escalated_at,
                mca.current_state AS latest_state,
                mca.remark       AS escalate_remark
            FROM mql_allocations ma
            JOIN contacts c      ON c.id = ma.contact_id
            LEFT JOIN companies co ON co.id = c.company_id
            JOIN agents a        ON a.id = ma.agent_id
            LEFT JOIN LATERAL (
                SELECT mca.called_at, mca.current_state, mca.remark
                FROM mql_call_attempts mca
                WHERE mca.allocation_id = ma.id
                ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                LIMIT 1
            ) mca ON TRUE
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND ma.close_reason = 'escalated'
              AND COALESCE(mca.current_state, 'Escalate') = 'Escalate'
              AND NOT EXISTS (
                  SELECT 1
                  FROM mql_allocations nx
                  WHERE nx.contact_id = ma.contact_id
                    AND (nx.campaign = %(campaign)s OR nx.campaign ILIKE %(campaign_like)s)
                    AND nx.id > ma.id
              )
        )
        SELECT *
        FROM escalated
        ORDER BY escalated_at DESC NULLS LAST, old_alloc_id DESC
    """, params={"campaign": campaign, "campaign_like": campaign_like})

    if esc_queue.empty:
        st.info("No escalated contacts are pending reassignment.")
    else:
        grp = (
            esc_queue.groupby(["escalated_by", "campaign"], as_index=False)
            .agg(
                pending_contacts=("contact_id", "nunique"),
                pending_entries=("old_alloc_id", "count"),
            )
            .sort_values(["pending_contacts", "escalated_by"], ascending=[False, True])
        )
        st.warning(f"{int(grp['pending_contacts'].sum())} contact(s) pending reassignment now.")
        st.dataframe(grp, use_container_width=True, hide_index=True)
        st.dataframe(
            esc_queue[[
                "old_alloc_id", "person", "company", "escalated_by",
                "escalated_at", "escalate_remark",
            ]],
            use_container_width=True,
            hide_index=True,
        )
except Exception as e:
    esc_queue = pd.DataFrame()
    log_and_warn("MQL escalation queue", e)

if not esc_queue.empty:
    st.subheader("Manual reassignment")
    st.caption("Assign escalated contacts to a selected MQL agent. Reassigned contact starts from FU1 in MQL FU 1-15.")

    try:
        mql_targets = query_df("""
            SELECT
                a.id,
                a.name,
                ags.sheet_id
            FROM agents a
            LEFT JOIN agent_sheets ags
              ON ags.agent_id = a.id
             AND ags.campaign = %(campaign)s
             AND ags.sheet_type = 'calling'
            WHERE a.status = 'active' AND a.team = 'mql'
            ORDER BY a.name
        """, params={"campaign": campaign})
    except Exception as e:
        mql_targets = pd.DataFrame()
        log_and_warn("MQL target agents", e)

    if mql_targets.empty:
        st.info("No active MQL target agents available for reassignment.")
    else:
        esc_options = {
            f"{row['person']} — {row['company'] or 'No company'} (by {row['escalated_by']})": row
            for _, row in esc_queue.iterrows()
        }
        tgt_options = {
            f"{row['name']}{'' if row['sheet_id'] else ' (no sheet id)'}": row
            for _, row in mql_targets.iterrows()
        }

        col_r1, col_r2 = st.columns(2)
        sel_esc_label = col_r1.selectbox("Escalated contact", list(esc_options.keys()), key="esc_reassign_contact")
        sel_tgt_label = col_r2.selectbox("Assign to MQL agent", list(tgt_options.keys()), key="esc_reassign_target")

        sel_esc = esc_options[sel_esc_label]
        sel_tgt = tgt_options[sel_tgt_label]
        same_agent_reassign = (
            str(sel_tgt["name"]).strip().lower() == str(sel_esc["escalated_by"]).strip().lower()
        )

        if not sel_tgt["sheet_id"]:
            st.error("Selected target agent has no sheet configured. Update sheet ID in Agents page first.")
        if same_agent_reassign:
            st.warning("Same-agent reassignment is blocked for escalated queue. Choose a different MQL agent.")

        confirm_reassign = st.checkbox(
            "I confirm this reassignment and want to push the contact to target MQL sheet now.",
            key="esc_reassign_confirm",
        )

        if st.button(
            "Reassign escalated contact",
            type="primary",
            disabled=(not confirm_reassign) or (not bool(sel_tgt["sheet_id"])) or same_agent_reassign,
            key="esc_reassign_btn",
        ):
            conn = None
            try:
                if same_agent_reassign:
                    raise ValueError("Cannot reassign an escalated contact to the same agent who escalated it.")
                conn = get_conn()
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT
                            c.id AS contact_id,
                            c.source,
                            c.source_id,
                            c.first_name,
                            c.last_name,
                            c.bd_category,
                            c.campaign,
                            co.name AS company_name,
                            (
                                SELECT cp.phone_number
                                FROM contact_phones cp
                                WHERE cp.contact_id = c.id
                                  AND COALESCE((to_jsonb(cp)->>'is_invalid')::boolean, FALSE) = FALSE
                                ORDER BY cp.rank
                                LIMIT 1
                            ) AS primary_phone,
                            (
                                SELECT ce.email
                                FROM contact_emails ce
                                WHERE ce.contact_id = c.id
                                  AND COALESCE((to_jsonb(ce)->>'is_invalid')::boolean, FALSE) = FALSE
                                ORDER BY ce.rank
                                LIMIT 1
                            ) AS primary_email,
                            (
                                SELECT array_agg(cp2.phone_number ORDER BY cp2.rank)
                                FROM contact_phones cp2
                                WHERE cp2.contact_id = c.id
                                  AND COALESCE((to_jsonb(cp2)->>'is_invalid')::boolean, FALSE) = FALSE
                            ) AS all_phones,
                            (
                                SELECT a2.name
                                FROM call_actions ca2
                                JOIN agents a2 ON a2.id = ca2.agent_id
                                WHERE ca2.contact_id = c.id
                                ORDER BY ca2.called_at DESC
                                LIMIT 1
                            ) AS bd_agent_name,
                            (
                                SELECT ca3.called_at
                                FROM call_actions ca3
                                WHERE ca3.contact_id = c.id
                                ORDER BY ca3.called_at DESC
                                LIMIT 1
                            ) AS bd_call_date,
                            (
                                SELECT ca4.transcript_link
                                FROM call_actions ca4
                                WHERE ca4.contact_id = c.id
                                  AND ca4.current_state = 'Shared Story'
                                ORDER BY ca4.called_at DESC
                                LIMIT 1
                            ) AS story_transcript,
                            (
                                SELECT ca5.recording_link
                                FROM call_actions ca5
                                WHERE ca5.contact_id = c.id
                                  AND ca5.current_state = 'Shared Story'
                                ORDER BY ca5.called_at DESC
                                LIMIT 1
                            ) AS story_recording,
                            (
                                SELECT COALESCE(ca6.snapshot_link, ca6.dream_snapshot_link)
                                FROM call_actions ca6
                                WHERE ca6.contact_id = c.id
                                ORDER BY ca6.called_at DESC
                                LIMIT 1
                            ) AS bd_snapshot,
                            (
                                SELECT mca1.remark
                                FROM mql_call_attempts mca1
                                WHERE mca1.allocation_id = %s
                                ORDER BY mca1.called_at DESC NULLS LAST, mca1.id DESC
                                LIMIT 1
                            ) AS last_mql_remark,
                            (
                                SELECT mca2.recording_link
                                FROM mql_call_attempts mca2
                                WHERE mca2.allocation_id = %s
                                ORDER BY mca2.called_at DESC NULLS LAST, mca2.id DESC
                                LIMIT 1
                            ) AS last_mql_recording,
                            (
                                SELECT mca3.dream_snapshot_link
                                FROM mql_call_attempts mca3
                                WHERE mca3.allocation_id = %s
                                ORDER BY mca3.called_at DESC NULLS LAST, mca3.id DESC
                                LIMIT 1
                            ) AS last_mql_snapshot,
                            (
                                SELECT ma.bd_agent_name
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_agent,
                            (
                                SELECT ma.bd_call_date
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_date,
                            (
                                SELECT ma.bd_current_state
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_state,
                            (
                                SELECT ma.bd_remark
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_remark,
                            (
                                SELECT ma.bd_recording_link
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_recording,
                            (
                                SELECT ma.bd_snapshot_link
                                FROM mql_analysis ma
                                WHERE ma.allocation_id = %s
                                LIMIT 1
                            ) AS analysis_bd_snapshot
                        FROM contacts c
                        LEFT JOIN companies co ON co.id = c.company_id
                        WHERE c.id = %s
                        LIMIT 1
                    """, (
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["old_alloc_id"]),
                        int(sel_esc["contact_id"]),
                    ))
                    ctx = cur.fetchone()

                    if not ctx:
                        raise RuntimeError("Could not load contact context for reassignment")

                    contact_id = int(ctx[0])
                    source = ctx[1] or ""
                    source_id = ctx[2] or ""
                    person = f"{ctx[3] or ''} {ctx[4] or ''}".strip()
                    category = ctx[5] or ctx[6] or ""
                    company_name = ctx[7] or ""
                    primary_phone = _normalize_phone(ctx[8] or "")
                    primary_email = ctx[9] or ""
                    all_phones = ctx[10] or []
                    bd_agent_name = ctx[11] or ctx[22] or ""
                    bd_call_date = ctx[12] or ctx[23]
                    story_transcript = ctx[13] or ""
                    story_recording = ctx[14] or ctx[27] or ""
                    bd_snapshot = ctx[15] or ctx[29] or ""
                    last_mql_remark = ctx[16] or ""
                    last_mql_recording = ctx[17] or ""
                    last_mql_snapshot = ctx[18] or ""
                    analysis_bd_state = ctx[24] or ""
                    analysis_bd_remark = ctx[25] or ""

                    all_phone_text = ", ".join([p for p in all_phones if p])
                    remark_parts = [
                        f"Escalated from {sel_esc['escalated_by']}",
                        f"BD state: {analysis_bd_state}" if analysis_bd_state else "",
                        f"BD remark: {analysis_bd_remark}" if analysis_bd_remark else "",
                        f"Last MQL remark: {last_mql_remark}" if last_mql_remark else "",
                        f"All phones: {all_phone_text}" if all_phone_text else "",
                    ]
                    merged_remark = " | ".join([r for r in remark_parts if r])

                    recording_values = [v for v in [story_recording, last_mql_recording] if v]
                    merged_recording = "; ".join(recording_values)
                    merged_snapshot = last_mql_snapshot or bd_snapshot

                    # Insert new MQL allocation (reassigned starts from FU1 in FU 1-15 tab).
                    cur.execute("""
                        INSERT INTO mql_allocations
                            (contact_id, agent_id, campaign, allocated_date, filled_by)
                        VALUES (%s, %s, %s, CURRENT_DATE, %s)
                        RETURNING id
                    """, (contact_id, int(sel_tgt["id"]), campaign, int(sel_tgt["id"])))
                    new_alloc_id = int(cur.fetchone()[0])

                    cur.execute("""
                        INSERT INTO mql_analysis
                            (contact_id, allocation_id, agent_id,
                             bd_agent_name, bd_call_date, bd_current_state,
                             bd_remark, bd_recording_link, bd_snapshot_link,
                             outcome, outcome_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        contact_id,
                        new_alloc_id,
                        int(sel_tgt["id"]),
                        bd_agent_name,
                        bd_call_date.date() if bd_call_date else None,
                        analysis_bd_state,
                        merged_remark,
                        merged_recording,
                        merged_snapshot,
                        f"reassigned_from_escalation:{int(sel_esc['old_alloc_id'])}",
                    ))

                    cur.execute("""
                        UPDATE contacts
                        SET contact_flag = 'mql_in_progress',
                            flag_updated_at = NOW()
                        WHERE id = %s
                    """, (contact_id,))

                    # Write reassigned contact row to target agent sheet.
                    gc = _get_sheets_client()
                    sh = gc.open_by_key(sel_tgt["sheet_id"])
                    ws = _ensure_mql_tab(sh, MQL_TAB_1, 1, 15)

                    total_cols = CONTACT_COLS + 15 * FU_BLOCK_SIZE
                    row_data = [
                        _build_unique_id(source, source_id),
                        company_name,
                        person,
                        primary_phone,
                        primary_email,
                        bd_agent_name,
                        bd_call_date.strftime("%d/%m/%Y") if bd_call_date else "",
                        merged_remark,
                        merged_recording,
                        category,
                        story_transcript,
                        merged_snapshot,
                    ] + [""] * (total_cols - CONTACT_COLS)

                    col_a = ws.col_values(1)
                    filled = len([v for v in col_a[1:] if (v or "").strip()])
                    next_row = max(2, filled + 2)
                    ws.update(range_name=f"A{next_row}", values=[row_data])

                    conn.commit()
                    st.success(
                        f"Reassigned {person} to {sel_tgt['name']} and pushed to {MQL_TAB_1} (FU1 start)."
                    )
                    st.rerun()
            except Exception as e:
                try:
                    if conn is not None:
                        conn.rollback()
                except Exception:
                    pass
                log_and_show("MQL escalated reassignment", e)

st.divider()

# ── BROWSE ALLOCATIONS ────────────────────────────────────────
st.subheader("Browse MQL allocations")

# Filters
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    status_filter = st.selectbox(
        "Status", ["All", "Active", "Qualified", "Rejected", "Stalled", "Escalated", "Reallocated"]
    )
with col_f2:
    try:
        agent_list_df = query_df("""
            SELECT DISTINCT a.name FROM agents a
            JOIN mql_allocations ma ON ma.agent_id = a.id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
            ORDER BY a.name
        """, params={"campaign": campaign, "campaign_like": campaign_like})
        agent_list = ["All"] + agent_list_df["name"].tolist()
    except Exception:
        agent_list = ["All"]
    agent_filter = st.selectbox("MQL Agent", agent_list)
with col_f3:
    date_from = st.date_input("From date", value=date(2026, 1, 1), key="browse_from")

# Build query
where_parts = ["(ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)", "ma.allocated_date >= %(date_from)s"]
params      = {"campaign": campaign, "campaign_like": campaign_like, "date_from": str(date_from)}

if status_filter == "Active":
    where_parts.append("ma.closed_at IS NULL")
elif status_filter != "All":
    where_parts.append("ma.close_reason = %(close_reason)s")
    params["close_reason"] = status_filter.lower()

if agent_filter != "All":
    where_parts.append("a.name = %(agent_name)s")
    params["agent_name"] = agent_filter

where_sql = " AND ".join(where_parts)

try:
    alloc_df = query_df(f"""
        SELECT
            ma.id,
            co.name         AS company,
            c.first_name || ' ' || c.last_name AS person,
            c.contact_flag,
            a.name          AS mql_agent,
            ma.allocated_date,
            ma.closed_at::date AS closed_date,
            ma.close_reason,
            -- BD context
            mx.bd_agent_name    AS bd_agent,
            mx.bd_current_state AS bd_state,
            -- Latest FU attempt
            (SELECT follow_up_number FROM mql_call_attempts
             WHERE allocation_id = ma.id
             ORDER BY follow_up_number DESC LIMIT 1) AS last_fu,
            -- Analysis outcome
            mx.outcome
        FROM mql_allocations ma
        JOIN contacts c  ON c.id  = ma.contact_id
        LEFT JOIN companies co ON co.id = c.company_id
        JOIN agents a    ON a.id  = ma.agent_id
        LEFT JOIN mql_analysis mx ON mx.allocation_id = ma.id
        WHERE {where_sql}
        ORDER BY ma.allocated_date DESC, ma.id DESC
        LIMIT 500
    """, params=params)

    if alloc_df.empty:
        st.info("No allocations match these filters.")
    else:
        st.caption(f"Showing up to 500 results. Found: {len(alloc_df)}")
        st.dataframe(
            alloc_df.drop(columns=["id"]),
            width="stretch",
            hide_index=True,
            column_config={
                "allocated_date": st.column_config.DateColumn("Allocated"),
                "closed_date":    st.column_config.DateColumn("Closed"),
            }
        )

except Exception as e:
    log_and_show("MQL allocations browse", e)

st.divider()

# ── DELETE MQL ALLOCATIONS ────────────────────────────────────
st.subheader("Delete MQL allocations")
st.caption("Select an agent and a specific date to delete one or all allocations for that day.")

try:
    del_agents_df = query_df("""
        SELECT id, name FROM agents
        WHERE status = 'active' AND team = 'mql'
        ORDER BY name
    """)
except Exception as e:
    log_and_show("MQL agents for delete", e)
    del_agents_df = None

if del_agents_df is not None and not del_agents_df.empty:
    da_col, db_col = st.columns([2, 2])
    with da_col:
        del_agent_map = {row["name"]: int(row["id"]) for _, row in del_agents_df.iterrows()}
        del_agent_name = st.selectbox("Agent", list(del_agent_map.keys()), key="del_mql_agent")
        del_agent_id   = del_agent_map[del_agent_name]
    with db_col:
        del_date = st.date_input("Date", value=date.today(), key="del_mql_date")

    try:
        del_allocs = query_df("""
            SELECT
                ma.id           AS allocation_id,
                ma.contact_id,
                co.name         AS company,
                c.first_name || ' ' || c.last_name AS person,
                c.contact_flag,
                ma.allocated_date,
                ma.closed_at,
                ma.close_reason,
                (SELECT COUNT(*) FROM mql_call_attempts x WHERE x.allocation_id = ma.id) AS attempts,
                (SELECT COUNT(*) FROM mql_analysis     x WHERE x.allocation_id = ma.id) AS analysis_rows
            FROM mql_allocations ma
            JOIN contacts c  ON c.id  = ma.contact_id
            LEFT JOIN companies co ON co.id = c.company_id
            WHERE ma.agent_id      = %(agent_id)s
              AND ma.allocated_date = %(dt)s
                            AND (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
            ORDER BY ma.id
                """, params={"agent_id": del_agent_id, "dt": str(del_date), "campaign": campaign, "campaign_like": campaign_like})
    except Exception as e:
        log_and_show("MQL delete browse", e)
        del_allocs = None

    if del_allocs is not None:
        if del_allocs.empty:
            st.info(f"No MQL allocations for {del_agent_name} on {del_date}.")
        else:
            total_allocs   = len(del_allocs)
            with_attempts  = int((del_allocs["attempts"] > 0).sum())
            no_attempts    = total_allocs - with_attempts

            da1, da2, da3 = st.columns(3)
            da1.metric(f"Total ({del_agent_name})", total_allocs)
            da2.metric("With call attempts", with_attempts)
            da3.metric("No attempts yet", no_attempts)

            st.dataframe(
                del_allocs[["company", "person", "contact_flag", "allocated_date",
                             "closed_at", "close_reason", "attempts"]],
                width="stretch", hide_index=True,
            )

            del_single_tab, del_bulk_tab = st.tabs(
                ["Delete one contact", "Delete ALL for this agent & date"]
            )

            # ── TAB A: Single delete ────────────────────────────────
            with del_single_tab:
                st.caption(
                    "Removes one MQL allocation and all linked call attempts / analysis rows. "
                    "Contact is returned to MQL pool (shared_story) if no other allocation exists."
                )
                contact_labels = (
                    del_allocs["person"] + " — " + del_allocs["company"]
                ).tolist()
                if not contact_labels:
                    st.info("No contacts to remove.")
                    st.stop()
                del_idx = st.selectbox(
                    "Select contact to remove", range(len(contact_labels)),
                    format_func=lambda i: contact_labels[i],
                    key="del_mql_single_idx",
                )
                sel = del_allocs.iloc[del_idx]
                sel_alloc_id   = int(sel["allocation_id"])
                sel_contact_id = int(sel["contact_id"])
                sel_attempts   = int(sel["attempts"])

                with st.container(border=True):
                    sc1, sc2, sc3, sc4 = st.columns(4)
                    sc1.markdown(f"**Person**  \n{sel['person']}")
                    sc2.markdown(f"**Company**  \n{sel['company'] or '—'}")
                    sc3.markdown(f"**Flag**  \n{sel['contact_flag']}")
                    sc4.markdown(f"**Attempts**  \n{sel_attempts}")

                if sel_attempts > 0:
                    st.warning(
                        f"This contact has **{sel_attempts}** call attempt(s) logged. "
                        "Those records will also be deleted."
                    )

                s_confirm = st.checkbox(
                    "I confirm — delete this MQL allocation permanently",
                    key="del_mql_single_confirm",
                )
                if st.button(
                    "Delete allocation",
                    disabled=not s_confirm,
                    type="primary",
                    key="del_mql_single_btn",
                ):
                    errs = []
                    try:
                        execute(
                            "DELETE FROM mql_call_attempts WHERE allocation_id = %s",
                            (sel_alloc_id,),
                        )
                    except Exception as e:
                        errs.append(f"call attempts delete failed: {e}")
                    try:
                        execute(
                            "DELETE FROM mql_analysis WHERE allocation_id = %s",
                            (sel_alloc_id,),
                        )
                    except Exception as e:
                        errs.append(f"analysis delete failed: {e}")
                    try:
                        execute(
                            "DELETE FROM mql_allocations WHERE id = %s",
                            (sel_alloc_id,),
                        )
                    except Exception as e:
                        errs.append(f"allocation delete failed: {e}")
                    # Restore contact flag if no remaining active allocations
                    try:
                        execute("""
                            UPDATE contacts
                            SET contact_flag    = 'shared_story',
                                flag_updated_at = NOW()
                            WHERE id = %s
                              AND contact_flag = 'mql_in_progress'
                              AND NOT EXISTS (
                                  SELECT 1 FROM mql_allocations
                                  WHERE contact_id = %s AND closed_at IS NULL
                              )
                        """, (sel_contact_id, sel_contact_id))
                    except Exception as e:
                        errs.append(f"flag reset failed: {e}")

                    if errs:
                        st.error("Errors during deletion:\n" + "\n".join(errs))
                    else:
                        st.toast(
                            f"MQL allocation for {sel['person']} deleted. "
                            "Contact returned to MQL pool.",
                        )
                        st.rerun()

            # ── TAB B: Bulk delete ──────────────────────────────────
            with del_bulk_tab:
                st.caption(
                    f"Deletes **all {total_allocs} MQL allocations** for "
                    f"**{del_agent_name}** on **{del_date}**, including all linked "
                    "call attempts and analysis rows."
                )
                with st.container(border=True):
                    bb1, bb2, bb3 = st.columns(3)
                    bb1.metric("Allocations to delete", total_allocs)
                    bb2.metric("With call attempts (also deleted)", with_attempts)
                    bb3.metric("No attempts — safe to remove", no_attempts)

                if with_attempts > 0:
                    st.warning(
                        f"{with_attempts} contact(s) have logged call attempts. "
                        "Those records will also be permanently deleted."
                    )

                bulk_confirm = st.checkbox(
                    f"I confirm — delete ALL {total_allocs} MQL allocations for "
                    f"{del_agent_name} on {del_date}",
                    key="del_mql_bulk_confirm",
                )
                if st.button(
                    f"Delete all {total_allocs} allocations",
                    disabled=not bulk_confirm,
                    type="primary",
                    key="del_mql_bulk_btn",
                ):
                    alloc_ids   = del_allocs["allocation_id"].tolist()
                    contact_ids = del_allocs["contact_id"].tolist()
                    bulk_errs   = []

                    try:
                        execute(
                            "DELETE FROM mql_call_attempts WHERE allocation_id = ANY(%s)",
                            (alloc_ids,),
                        )
                    except Exception as e:
                        bulk_errs.append(f"call attempts delete failed: {e}")
                    try:
                        execute(
                            "DELETE FROM mql_analysis WHERE allocation_id = ANY(%s)",
                            (alloc_ids,),
                        )
                    except Exception as e:
                        bulk_errs.append(f"analysis delete failed: {e}")
                    try:
                        execute("""
                            DELETE FROM mql_allocations
                            WHERE agent_id      = %s
                              AND allocated_date = %s
                              AND (campaign = %s OR campaign ILIKE %s)
                        """, (del_agent_id, str(del_date), campaign, campaign_like))
                    except Exception as e:
                        bulk_errs.append(f"allocations delete failed: {e}")
                    # Restore contact flags
                    try:
                        execute("""
                            UPDATE contacts
                            SET contact_flag    = 'shared_story',
                                flag_updated_at = NOW()
                            WHERE id = ANY(%s)
                              AND contact_flag = 'mql_in_progress'
                              AND NOT EXISTS (
                                  SELECT 1 FROM mql_allocations
                                  WHERE contact_id = contacts.id AND closed_at IS NULL
                              )
                        """, (contact_ids,))
                    except Exception as e:
                        bulk_errs.append(f"flag reset failed: {e}")

                    if bulk_errs:
                        st.error("Errors during bulk deletion:\n" + "\n".join(bulk_errs))
                    else:
                        st.toast(
                            f"All {total_allocs} MQL allocations for "
                            f"{del_agent_name} on {del_date} deleted.",
                        )
                        st.rerun()

st.divider()

# ── CLOSE / REALLOCATE ────────────────────────────────────────
st.subheader("Close or reallocate a contact")
st.caption("Find a contact by name or company, then manually close or reassign their MQL allocation.")

search_q = st.text_input("Search by person name or company")

if search_q:
    try:
        search_df = query_df("""
            SELECT
                ma.id           AS alloc_id,
                co.name         AS company,
                c.first_name || ' ' || c.last_name AS person,
                c.contact_flag,
                a.name          AS mql_agent,
                ma.allocated_date,
                ma.closed_at
            FROM mql_allocations ma
            JOIN contacts c  ON c.id = ma.contact_id
            LEFT JOIN companies co ON co.id = c.company_id
            JOIN agents a    ON a.id = ma.agent_id
            WHERE (ma.campaign = %(campaign)s OR ma.campaign ILIKE %(campaign_like)s)
              AND (
                  LOWER(c.first_name || ' ' || c.last_name) LIKE %(q)s
                  OR LOWER(co.name) LIKE %(q)s
              )
            ORDER BY ma.allocated_date DESC
            LIMIT 20
        """, params={"campaign": campaign, "campaign_like": campaign_like, "q": f"%{search_q.lower()}%"})

        if search_df.empty:
            st.info("No matching allocations found.")
        else:
            for _, row in search_df.iterrows():
                is_active = pd.isna(row["closed_at"])
                status_badge = "Active" if is_active else "Closed"
                with st.container(border=True):
                    cc1, cc2, cc3, cc4 = st.columns([3, 2, 2, 2])
                    cc1.markdown(f"**{row['person']}** — {row['company']}")
                    cc2.write(f"Agent: {row['mql_agent']}")
                    cc3.write(status_badge)
                    cc4.write(f"Allocated: {row['allocated_date']}")

                    if is_active:
                        b1, b2, b3 = st.columns(3)
                        alloc_id = int(row["alloc_id"])
                        contact_flag = row["contact_flag"]

                        if b1.button("Mark Qualified", key=f"qual_{alloc_id}"):
                            try:
                                execute("""
                                    UPDATE mql_allocations
                                    SET closed_at = NOW(), close_reason = 'qualified'
                                    WHERE id = %s
                                """, (alloc_id,))
                                execute("""
                                    UPDATE contacts SET contact_flag = 'mql_qualified',
                                    flag_updated_at = NOW() WHERE id = (
                                        SELECT contact_id FROM mql_allocations WHERE id = %s
                                    )
                                """, (alloc_id,))
                                execute("""
                                    UPDATE mql_analysis SET outcome = 'sql', outcome_date = NOW(),
                                    updated_at = NOW() WHERE allocation_id = %s
                                """, (alloc_id,))
                                st.toast("Marked as qualified (SQL).")
                                st.rerun()
                            except Exception as e:
                                log_and_show("mark qualified", e)

                        if b2.button("Mark Rejected", key=f"rej_{alloc_id}"):
                            try:
                                execute("""
                                    UPDATE mql_allocations
                                    SET closed_at = NOW(), close_reason = 'rejected'
                                    WHERE id = %s
                                """, (alloc_id,))
                                execute("""
                                    UPDATE contacts SET contact_flag = 'mql_rejected',
                                    flag_updated_at = NOW() WHERE id = (
                                        SELECT contact_id FROM mql_allocations WHERE id = %s
                                    )
                                """, (alloc_id,))
                                execute("""
                                    UPDATE mql_analysis SET outcome = 'back_to_bd',
                                    outcome_date = NOW(), updated_at = NOW()
                                    WHERE allocation_id = %s
                                """, (alloc_id,))
                                st.toast("Marked as rejected (back to BD).")
                                st.rerun()
                            except Exception as e:
                                log_and_show("mark rejected", e)

                        if b3.button("Reallocate", key=f"realloc_{alloc_id}"):
                            try:
                                execute("""
                                    UPDATE mql_allocations
                                    SET closed_at = NOW(), close_reason = 'reallocated'
                                    WHERE id = %s
                                """, (alloc_id,))
                                # Return contact to shared_story so it re-enters MQL pool
                                execute("""
                                    UPDATE contacts SET contact_flag = 'shared_story',
                                    flag_updated_at = NOW() WHERE id = (
                                        SELECT contact_id FROM mql_allocations WHERE id = %s
                                    ) AND contact_flag = 'mql_in_progress'
                                """, (alloc_id,))
                                st.toast("Allocation closed. Contact returned to MQL pool.")
                                st.rerun()
                            except Exception as e:
                                log_and_show("reallocate", e)

    except Exception as e:
        log_and_show("contact search", e)

st.divider()

# ── SQL READY CONTACTS ────────────────────────────────────────
st.subheader("SQL Ready contacts")
st.caption("Contacts qualified by MQL team — ready for sales allocation.")

try:
    sql_ready_df = query_df("""
        SELECT
            co.name             AS company,
            c.first_name || ' ' || c.last_name AS person,
            em.email,
            p.phone_number      AS phone,
            mx.bd_agent_name    AS bd_agent,
            mx.bd_recording_link AS bd_recording,
            mx.bd_snapshot_link  AS dream_snapshot,
            mx.bd_current_state  AS bd_state,
            a.name               AS mql_agent,
            mx.outcome_date::date AS qualified_date
        FROM mql_analysis mx
        JOIN contacts c      ON c.id = mx.contact_id
        LEFT JOIN companies co ON co.id = c.company_id
        JOIN agents a        ON a.id = mx.agent_id
        LEFT JOIN LATERAL (
                        SELECT cp.phone_number FROM contact_phones cp
                        WHERE cp.contact_id = c.id
                            AND COALESCE((to_jsonb(cp)->>'is_invalid')::boolean, FALSE) = FALSE
            ORDER BY rank LIMIT 1
        ) p ON TRUE
        LEFT JOIN LATERAL (
                        SELECT ce.email FROM contact_emails ce
                        WHERE ce.contact_id = c.id
                            AND COALESCE((to_jsonb(ce)->>'is_invalid')::boolean, FALSE) = FALSE
            ORDER BY rank LIMIT 1
        ) em ON TRUE
        WHERE mx.outcome = 'sql'
                    AND (c.campaign = %(campaign)s OR c.campaign ILIKE %(campaign_like)s)
        ORDER BY mx.outcome_date DESC NULLS LAST
        """, params={"campaign": campaign, "campaign_like": campaign_like})

    if sql_ready_df.empty:
        st.info("No SQL-ready contacts yet.")
    else:
        st.success(f"{len(sql_ready_df)} contacts ready for sales!")
        st.dataframe(
            sql_ready_df,
            width="stretch",
            hide_index=True,
            column_config={
                "bd_recording":    st.column_config.LinkColumn("BD Recording"),
                "dream_snapshot":  st.column_config.LinkColumn("Dream Snapshot"),
                "qualified_date":  st.column_config.DateColumn("Qualified Date"),
            }
        )

except Exception as e:
    log_and_show("SQL ready contacts", e)
