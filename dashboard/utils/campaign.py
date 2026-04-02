"""Campaign selector utility for the Streamlit dashboard.

Every page calls `get_campaign()` at the top.
The selected campaign is stored in st.session_state and persists
across page navigations within the same session.

Usage in any page:
    from utils.campaign import get_campaign
    campaign = get_campaign()          # renders sidebar selector, returns name
    # then use in queries:
    # WHERE c.campaign = %(campaign)s
"""

from __future__ import annotations

import streamlit as st


def get_campaign() -> str:
    """
    Render the campaign selector in the sidebar.
    Returns the currently selected campaign name (str).
    Persists across pages via st.session_state.
    """
    # Load available campaigns from DB
    try:
        from utils.db import query_df
        df = query_df(
            "SELECT name FROM campaigns WHERE is_active = TRUE ORDER BY name"
        )
        campaign_names: list[str] = df["name"].tolist()
    except Exception:
        # Fallback if campaigns table doesn't exist yet (pre-migration)
        campaign_names = ["consulting"]

    if not campaign_names:
        campaign_names = ["consulting"]

    # Initialise session state on first load
    if "campaign" not in st.session_state:
        st.session_state["campaign"] = campaign_names[0]

    # If the stored campaign is no longer valid, reset to first option
    if st.session_state["campaign"] not in campaign_names:
        st.session_state["campaign"] = campaign_names[0]

    current_idx = campaign_names.index(st.session_state["campaign"])

    selected = st.sidebar.selectbox(
        "Campaign",
        campaign_names,
        index=current_idx,
        key="sidebar_campaign",
        format_func=lambda s: s.title(),
        help="All data on this page is filtered to the selected campaign.",
    )
    st.session_state["campaign"] = selected
    return selected
