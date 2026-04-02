"""Shared error-handling utilities for the CRM Streamlit dashboard.

Usage in any page:
    from utils.errors import log_and_show, log_and_warn

    try:
        ...
    except Exception as e:
        log_and_show("pool data", e)        # st.error  + full traceback expander
        # or
        log_and_warn("day breakdown", e)    # st.warning + full traceback expander

Both functions:
  - Print the FULL Python traceback to the console (terminal where `streamlit run` executes).
  - Show a clean, user-friendly message in the Streamlit UI.
  - Include a collapsible expander so developers can see the raw traceback inline.
"""

from __future__ import annotations

import logging
import traceback

import streamlit as st

_logger = logging.getLogger("crm.dashboard")

# Make sure the root logger emits at least ERROR level so our messages appear
# even if the user hasn't configured logging explicitly.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log_and_show(context: str, exc: Exception) -> None:
    """Log full traceback to console; show st.error + collapsible details in UI."""
    tb = traceback.format_exc()
    _logger.error("ERROR loading %s: %s\n%s", context, exc, tb)
    st.error(f"Unable to load **{context}**. Please try refreshing the page.")
    with st.expander("Technical details (for developers)"):
        st.code(tb, language="text")


def log_and_warn(context: str, exc: Exception) -> None:
    """Log full traceback to console; show st.warning + collapsible details in UI."""
    tb = traceback.format_exc()
    _logger.warning("WARNING loading %s: %s\n%s", context, exc, tb)
    st.warning(f"Could not load **{context}**.")
    with st.expander("Technical details (for developers)"):
        st.code(tb, language="text")
