"""
app.py — Navigation Controller
Run with: streamlit run app.py
"""

import importlib.util
import sys
from pathlib import Path

import streamlit as st
from utils.campaign import get_campaign
from utils.db import query_df
from utils.auth import (
    is_logged_in, is_admin, get_user,
    show_login_form, logout, require_admin,
    init_cookie_auth,
)

st.set_page_config(
    page_title="DT Consulting CRM",
    page_icon="📞",
    layout="wide",
    initial_sidebar_state="expanded",
)

PAGES_DIR     = Path(__file__).resolve().parent / "pages"
DASHBOARD_DIR = Path(__file__).resolve().parent

if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

# ── Sidebar CSS ────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stSidebarNav"] {
    display: none !important;
}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
    gap: 4px !important;
}
[data-testid="stSidebar"] div.section-header,
[data-testid="stSidebar"] div.page-item {
    margin: 0 !important;
    padding: 0 !important;
}
[data-testid="stSidebar"] div.section-header button p,
[data-testid="stSidebar"] div.page-item button p {
    display: flex !important;
    align-items: center !important;
    gap: 7px !important;
    margin: 0 !important;
    line-height: 1.5 !important;
}
[data-testid="stSidebar"] div.section-header > div > button {
    background: #1e2130 !important;
    border: 1px solid #2d3148 !important;
    border-radius: 6px !important;
    color: #8b92a8 !important;
    font-size: 11px !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    padding: 7px 12px !important;
    margin: 0 !important;
    justify-content: flex-start !important;
}
[data-testid="stSidebar"] div.section-header > div > button:hover {
    background: #262b42 !important;
    color: #c8cde0 !important;
    border-color: #3d4466 !important;
}
[data-testid="stSidebar"] div.page-item > div > button {
    border-radius: 6px !important;
    font-size: 13px !important;
    font-weight: 400 !important;
    margin: 0 !important;
    justify-content: flex-start !important;
}
</style>
""", unsafe_allow_html=True)


# ── Page runner ────────────────────────────────────────────────
def run_page(filename: str) -> None:
    path = PAGES_DIR / filename
    spec = importlib.util.spec_from_file_location("_crm_page", path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)


@st.cache_data(ttl=60, show_spinner=False)
def _pending_escalation_count(campaign: str) -> int:
    try:
        df = query_df("""
            SELECT COUNT(*) AS cnt
            FROM mql_allocations ma
            LEFT JOIN LATERAL (
                SELECT mca.current_state
                FROM mql_call_attempts mca
                WHERE mca.allocation_id = ma.id
                ORDER BY mca.called_at DESC NULLS LAST, mca.id DESC
                LIMIT 1
            ) last_try ON TRUE
            WHERE ma.campaign = %(campaign)s
                            AND (
                                    ma.close_reason = 'escalated'
                                    OR (ma.close_reason = 'reallocated' AND last_try.current_state = 'Escalate')
                            )
              AND NOT EXISTS (
                  SELECT 1
                  FROM mql_allocations nx
                  WHERE nx.contact_id = ma.contact_id
                    AND nx.campaign = ma.campaign
                    AND nx.closed_at IS NULL
              )
        """, params={"campaign": campaign})
        return int(df.iloc[0]["cnt"]) if not df.empty else 0
    except Exception:
        return 0


# ── Role-based page groups ─────────────────────────────────────
# Pages that require admin role
ADMIN_PAGES = {
    "4_Allocation.py",
    "5_Allocation_Manager.py",
    "9_Reallocation.py",
    "7_Upload.py",
    "3_Agents.py",
    "10_MQL_Allocation.py",
    "11_MQL_Manager.py",
}

BD_GROUPS_ADMIN = {
    "Overview": [
        ("Home",          "0_Home.py"),
        ("Dashboard",     "6_Dashboard.py"),
        ("Pipeline",      "1_Pipeline.py"),
    ],
    "Team": [
        ("Agents",        "3_Agents.py"),
        ("Contacts",      "2_Contacts.py"),
    ],
    "Allocation": [
        ("Allocation",    "4_Allocation.py"),
        ("Alloc Manager", "5_Allocation_Manager.py"),
        ("Reallocation",  "9_Reallocation.py"),
    ],
    "Data": [
        ("Upload",        "7_Upload.py"),
    ],
    "AI Query": [
        ("AI Query",      "8_AI_Query.py"),
    ],
}

BD_GROUPS_USER = {
    "Overview": [
        ("Home",          "0_Home.py"),
        ("Dashboard",     "6_Dashboard.py"),
        ("Pipeline",      "1_Pipeline.py"),
    ],
    "Team": [
        ("Contacts",      "2_Contacts.py"),
    ],
    "AI Query": [
        ("AI Query",      "8_AI_Query.py"),
    ],
}

MQL_GROUPS_ADMIN = {
    "Overview": [
        ("Home",           "0_Home.py"),
        ("MQL Dashboard",  "12_MQL_Dashboard.py"),
        ("Pipeline",       "1_Pipeline.py"),
    ],
    "Team": [
        ("Agents",         "3_Agents.py"),
    ],
    "Allocation": [
        ("MQL Allocation", "10_MQL_Allocation.py"),
        ("MQL Manager",    "11_MQL_Manager.py"),
    ],
    "AI Query": [
        ("AI Query",       "8_AI_Query.py"),
    ],
}

MQL_GROUPS_USER = {
    "Overview": [
        ("Home",           "0_Home.py"),
        ("MQL Dashboard",  "12_MQL_Dashboard.py"),
        ("Pipeline",       "1_Pipeline.py"),
    ],
    "AI Query": [
        ("AI Query",       "8_AI_Query.py"),
    ],
}


# ── on_click callbacks ─────────────────────────────────────────
def _toggle(section: str) -> None:
    st.session_state["open_section"] = (
        None if st.session_state.get("open_section") == section else section
    )


def _navigate(filename: str, section: str) -> None:
    st.session_state["current_page"] = filename
    st.session_state["open_section"] = section


def _do_logout() -> None:
    logout()


# ── Session-state defaults ─────────────────────────────────────
if "current_page"  not in st.session_state:
    st.session_state["current_page"]  = "0_Home.py"
if "open_section"  not in st.session_state:
    st.session_state["open_section"]  = "Overview"
if "stage_select"  not in st.session_state:
    st.session_state["stage_select"]  = "BD"
if "auth_logged_in" not in st.session_state:
    st.session_state["auth_logged_in"] = False
if "auth_user"      not in st.session_state:
    st.session_state["auth_user"]      = None


# ── Cookie auto-login (must run before the gate check) ────────
init_cookie_auth()

# ── Gate: show login if not authenticated ─────────────────────
if not is_logged_in():
    show_login_form()
    st.stop()


# ── Authenticated — build sidebar ─────────────────────────────
user  = get_user()
admin = is_admin()

with st.sidebar:
    # ── User badge + logout on one row ────────────────────────
    role_color = "#e74c3c" if admin else "#3498db"
    badge_col, logout_col = st.columns([3, 1])
    with badge_col:
        st.markdown(
            f"<div style='font-size:12px;color:#8b92a8;padding:6px 2px 0;line-height:1.6;'>"
            f"<b style='color:#c8cde0;'>{user['name']}</b><br>"
            f"<span style='background:{role_color};color:#fff;border-radius:3px;"
            f"padding:1px 6px;font-size:10px;font-weight:600;'>{user['role'].upper()}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with logout_col:
        st.markdown("<div style='padding-top:6px;'>", unsafe_allow_html=True)
        st.button("↪", on_click=_do_logout, use_container_width=True,
                  help="Logout", type="secondary")
        st.markdown("</div>", unsafe_allow_html=True)
    st.divider()

    get_campaign()
    st.divider()

    st.selectbox(
        "Stage",
        ["BD", "MQL"],
        key="stage_select",
        label_visibility="collapsed",
    )
    st.divider()

    stage = st.session_state["stage_select"]

    # Pick groups based on stage + role
    if "MQL" in stage:
        groups = MQL_GROUPS_ADMIN if admin else MQL_GROUPS_USER
    else:
        groups = BD_GROUPS_ADMIN if admin else BD_GROUPS_USER

    if admin and "MQL" in stage:
        campaign = st.session_state.get("campaign", "consulting")
        pending_esc = _pending_escalation_count(campaign)
        if pending_esc > 0:
            groups = {k: list(v) for k, v in groups.items()}
            groups["Allocation"] = [
                (
                    f"MQL Allocation ({pending_esc})" if f == "10_MQL_Allocation.py" else
                    f"MQL Manager ({pending_esc})" if f == "11_MQL_Manager.py" else lbl,
                    f,
                )
                for (lbl, f) in groups.get("Allocation", [])
            ]

    # Reset to Home when current page is not in visible group
    valid_files = {f for grp in groups.values() for _, f in grp}
    if st.session_state["current_page"] not in valid_files:
        st.session_state["current_page"] = "0_Home.py"
        st.session_state["open_section"] = "Overview"

    current      = st.session_state["current_page"]
    open_section = st.session_state["open_section"]

    for section, pages in groups.items():
        is_open = open_section == section

        if len(pages) == 1:
            label, filename = pages[0]
            is_active = filename == current
            st.markdown('<div class="section-header">', unsafe_allow_html=True)
            st.button(
                f"{'●' if is_active else '○'}  {section}",
                key=f"sec_{section}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
                on_click=_navigate,
                args=(filename, section),
            )
            st.markdown('</div>', unsafe_allow_html=True)
            continue

        arrow = "▼" if is_open else "▶"
        st.markdown('<div class="section-header">', unsafe_allow_html=True)
        st.button(
            f"{arrow}  {section}",
            key=f"sec_{section}",
            use_container_width=True,
            on_click=_toggle,
            args=(section,),
        )
        st.markdown('</div>', unsafe_allow_html=True)

        if is_open:
            _, col = st.columns([0.05, 0.95])
            with col:
                for label, filename in pages:
                    st.markdown('<div class="page-item">', unsafe_allow_html=True)
                    st.button(
                        label,
                        key=f"nav_{filename}",
                        use_container_width=True,
                        type="primary" if filename == current else "secondary",
                        on_click=_navigate,
                        args=(filename, section),
                    )
                    st.markdown('</div>', unsafe_allow_html=True)


# ── Run selected page (with server-side role guard) ────────────
requested = st.session_state["current_page"]

if requested in ADMIN_PAGES and not admin:
    st.error("Access denied — this page requires admin role.")
    st.info("Contact your administrator if you need access.")
    st.stop()

run_page(requested)
