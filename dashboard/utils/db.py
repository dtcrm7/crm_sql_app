"""Database helpers for Streamlit dashboard pages.

All functions log the full Python traceback to the console on failure
before re-raising the exception so callers can handle it.
"""

from __future__ import annotations

import logging
import os
import traceback
from pathlib import Path
from typing import Any, Iterable, Optional

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

logger = logging.getLogger("crm.db")

# Ensure a basic handler exists even if the caller never calls basicConfig.
_LOG_LEVEL = os.getenv("CRM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _load_env() -> None:
    """Load .env from project root if present."""
    dashboard_dir = Path(__file__).resolve().parent.parent
    project_root = dashboard_dir.parent
    env_path = project_root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()


def _get_db_config() -> dict[str, Any]:
    """Read DB config from Streamlit secrets first, then env vars."""
    _load_env()

    config = {
        "host":     os.getenv("DB_HOST"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "dbname":   os.getenv("DB_NAME"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

    try:
        import streamlit as st
        secrets = st.secrets
        config["host"]     = secrets.get("DB_HOST",     config["host"])
        config["port"]     = int(secrets.get("DB_PORT", config["port"]))
        config["dbname"]   = secrets.get("DB_NAME",     config["dbname"])
        config["user"]     = secrets.get("DB_USER",     config["user"])
        config["password"] = secrets.get("DB_PASSWORD", config["password"])
    except Exception:
        # Local runs may not have Streamlit secrets configured.
        pass

    missing = [k for k, v in config.items() if v in (None, "")]
    if missing:
        msg = "Missing DB configuration values: " + ", ".join(missing)
        logger.error(msg)
        raise RuntimeError(msg)

    return config


@st.cache_resource(show_spinner=False)
def _persistent_conn() -> psycopg2.extensions.connection:
    """One DB connection per Streamlit server process, reused across all reruns."""
    logger.info("DB: opening persistent connection")
    conn = psycopg2.connect(**_get_db_config())
    conn.autocommit = False
    return conn


def get_conn() -> psycopg2.extensions.connection:
    """Return the cached connection, transparently reconnecting if it closed."""
    conn = _persistent_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.rollback()          # don't leave an idle transaction open
    except Exception:
        logger.warning("DB: connection lost — reconnecting")
        _persistent_conn.clear()
        conn = _persistent_conn()
    return conn


def query_df(sql: str, params: Optional[Any] = None) -> pd.DataFrame:
    """Run a SELECT and return a pandas DataFrame.

    Logs the full traceback to the console on failure, then re-raises.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                columns = [d.name for d in cur.description] if cur.description else []
            conn.rollback()  # keep connection out of an idle transaction state
        return pd.DataFrame(rows, columns=columns)
    except Exception as exc:
        logger.error(
            "query_df() failed.\nSQL (first 400 chars): %.400s\nParams: %s\n%s",
            sql,
            params,
            traceback.format_exc(),
        )
        raise


def execute(sql: str, params: Optional[Iterable[Any]] = None) -> int:
    """Run INSERT/UPDATE/DELETE and return affected row count.

    Logs the full traceback to the console on failure, then re-raises.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rowcount = cur.rowcount
            conn.commit()
        return rowcount
    except Exception as exc:
        logger.error(
            "execute() failed.\nSQL (first 400 chars): %.400s\nParams: %s\n%s",
            sql,
            params,
            traceback.format_exc(),
        )
        raise


def execute_many(sql: str, params: Iterable[Iterable[Any]]) -> int:
    """Run batch INSERT/UPDATE/DELETE and return affected row count.

    Logs the full traceback to the console on failure, then re-raises.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                rowcount = cur.rowcount
            conn.commit()
        return rowcount
    except Exception as exc:
        logger.error(
            "execute_many() failed.\nSQL (first 400 chars): %.400s\n%s",
            sql,
            traceback.format_exc(),
        )
        raise
