"""
auth.py — Dashboard authentication & role-based access control
==============================================================

Usage in any page:
    from utils.auth import require_auth, require_admin, get_user, log_action

    require_auth()          # stops page if not logged in
    require_admin()         # stops page if not admin

    user = get_user()       # {"username": "...", "name": "...", "role": "..."}
    log_action("allocation_run", "agent_id=5, count=12")

Login persists across browser refreshes via a signed cookie (30-day expiry).
Requires: pip install extra-streamlit-components
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from typing import Optional

import bcrypt
import streamlit as st

from utils.db import get_conn

logger = logging.getLogger("crm.auth")

# ── Session state keys ─────────────────────────────────────────
_KEY_USER      = "auth_user"       # dict: username, name, role
_KEY_LOGGED_IN = "auth_logged_in"  # bool

# ── Cookie config ──────────────────────────────────────────────
COOKIE_NAME        = "crm_session"
COOKIE_EXPIRY_DAYS = 30
_SECRET = os.getenv("COOKIE_SECRET", "crm-default-secret-change-in-production")


# ── Token helpers ──────────────────────────────────────────────

def _make_token(username: str) -> str:
    """Generate a signed session token: {username}|{expiry_ts}|{hmac}"""
    expiry = str(int(time.time()) + COOKIE_EXPIRY_DAYS * 86400)
    msg    = f"{username}|{expiry}"
    sig    = hmac.new(_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return f"{msg}|{sig}"


def _verify_token(token: str) -> Optional[str]:
    """Returns username if the token is valid and not expired, else None."""
    try:
        parts = token.split("|")
        if len(parts) != 3:
            return None
        username, expiry, sig = parts
        if time.time() > int(expiry):
            return None
        msg      = f"{username}|{expiry}"
        expected = hmac.new(_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        return username
    except Exception:
        return None


# ── Cookie controller (lazy singleton per session) ────────────

def _get_controller():
    """Return the CookieManager for this Streamlit session."""
    if "cookie_controller" not in st.session_state:
        try:
            import extra_streamlit_components as stx
            st.session_state["cookie_controller"] = stx.CookieManager()
        except ImportError:
            st.session_state["cookie_controller"] = None
            logger.warning(
                "extra-streamlit-components not installed — "
                "login will not persist across browser refreshes. "
                "Run: pip install extra-streamlit-components"
            )
    return st.session_state["cookie_controller"]


# ── Password helpers ───────────────────────────────────────────

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ── DB helpers ─────────────────────────────────────────────────

def _fetch_user(username: str) -> Optional[dict]:
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT username, name, password_hash, role, is_active
                FROM dashboard_users
                WHERE LOWER(username) = LOWER(%s)
                LIMIT 1
            """, (username.strip(),))
            row = cur.fetchone()
        if not row:
            logger.warning("_fetch_user: no row found for username=%s", username)
            return None
        return {
            "username":      row[0],
            "name":          row[1],
            "password_hash": row[2],
            "role":          row[3],
            "is_active":     row[4],
        }
    except Exception as e:
        logger.error("_fetch_user failed for username=%s: %s", username, e, exc_info=True)
        return None


def _record_last_login(username: str) -> None:
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE dashboard_users SET last_login = NOW() WHERE username = %s",
                (username,)
            )
        conn.commit()
    except Exception as e:
        logger.warning(f"_record_last_login failed (non-fatal): {e}")


def _set_session(user: dict) -> None:
    """Write user info into session state (called after successful auth)."""
    st.session_state[_KEY_LOGGED_IN] = True
    st.session_state[_KEY_USER]      = {
        "username": user["username"],
        "name":     user["name"],
        "role":     user["role"],
    }


# ── Public session helpers ─────────────────────────────────────

def is_logged_in() -> bool:
    return bool(st.session_state.get(_KEY_LOGGED_IN))


def get_user() -> Optional[dict]:
    return st.session_state.get(_KEY_USER)


def get_role() -> Optional[str]:
    u = get_user()
    return u["role"] if u else None


def is_admin() -> bool:
    return get_role() == "admin"


def logout() -> None:
    st.session_state[_KEY_LOGGED_IN] = False
    st.session_state[_KEY_USER]      = None
    st.session_state["current_page"] = "0_Home.py"
    st.session_state["open_section"] = "Overview"
    # Clear the cookie
    ctrl = _get_controller()
    if ctrl is not None:
        try:
            ctrl.delete(COOKIE_NAME)
        except Exception:
            pass


# ── Cookie-based auto-login ────────────────────────────────────

def init_cookie_auth() -> None:
    """
    Call once at the TOP of app.py, before the login gate.
    If the session is not authenticated but a valid cookie exists,
    auto-logs in the user so they don't see the login form on refresh.
    """
    if is_logged_in():
        return

    ctrl = _get_controller()
    if ctrl is None:
        return

    try:
        token = ctrl.get(COOKIE_NAME)
    except Exception:
        return

    if not token:
        return

    username = _verify_token(token)
    if not username:
        # Token expired or invalid — clear it
        try:
            ctrl.delete(COOKIE_NAME)
        except Exception:
            pass
        return

    # Re-fetch user to get current role/name and verify account is still active
    user = _fetch_user(username)
    if not user or not user["is_active"]:
        try:
            ctrl.delete(COOKIE_NAME)
        except Exception:
            pass
        return

    _set_session(user)


# ── Audit log ──────────────────────────────────────────────────

def log_action(action: str, details: str = "") -> None:
    user = get_user()
    if not user:
        return
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO action_log (username, role, action, details)
                VALUES (%s, %s, %s, %s)
            """, (user["username"], user["role"], action, details))
        conn.commit()
    except Exception as e:
        logger.warning(f"log_action failed (non-fatal): {e}")


# ── Login form ─────────────────────────────────────────────────

def show_login_form() -> None:
    _, col, _ = st.columns([1, 1.2, 1])
    with col:
        st.markdown("## DT Consulting CRM")
        st.markdown("---")
        with st.form("login_form", clear_on_submit=False):
            username  = st.text_input("Username", placeholder="your username")
            password  = st.text_input("Password", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Login", use_container_width=True, type="primary")

        if submitted:
            if not username or not password:
                st.error("Enter both username and password.")
                return

            # Test DB connectivity before attempting login
            try:
                get_conn()
            except Exception as e:
                logger.error("Login DB connection failed: %s", e, exc_info=True)
                st.error("Cannot connect to database. Please check your configuration.")
                return

            user = _fetch_user(username)

            if user is None or not user["is_active"]:
                st.error("Invalid username or password.")
                return

            if not verify_password(password, user["password_hash"]):
                st.error("Invalid username or password.")
                return

            # Success — set session
            _set_session(user)
            _record_last_login(user["username"])

            # Persist via cookie
            ctrl = _get_controller()
            if ctrl is not None:
                try:
                    import datetime
                    ctrl.set(
                        COOKIE_NAME,
                        _make_token(user["username"]),
                        expires_at=datetime.datetime.now() + datetime.timedelta(days=COOKIE_EXPIRY_DAYS),
                    )
                except Exception as e:
                    logger.warning(f"Cookie set failed (non-fatal): {e}")

            st.rerun()


# ── Page guards ────────────────────────────────────────────────

def require_auth() -> None:
    if not is_logged_in():
        show_login_form()
        st.stop()


def require_admin() -> None:
    require_auth()
    if not is_admin():
        st.error("Access denied — admin only.")
        st.info("Contact your administrator if you need access to this page.")
        st.stop()
