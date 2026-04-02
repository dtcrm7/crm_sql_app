"""
8_AI_Query.py — AI Query page for the CRM Dashboard
Ask plain-English questions → AI writes SQL → results shown as a table.

API key is entered once in the sidebar and saved to .ai_shell_config.json.
It reloads automatically on every visit — no setup needed after first time.
"""

import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

# Reuse the dashboard's existing DB connection
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils.db import get_conn  # noqa: E402
from utils.auth import get_user  # noqa: E402

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

CONFIG_FILE = Path(__file__).resolve().parent.parent.parent / ".ai_shell_config.json"

PROVIDERS = {
    "Google Gemini":    {"id": "gemini", "model": "gemini-2.5-flash"},
    "Anthropic Claude": {"id": "claude", "model": "claude-sonnet-4-6"},
}

MAX_ROWS = 200
HISTORY_LIMIT = 100
BLOCKED  = {"insert", "update", "delete", "drop", "create", "alter",
            "truncate", "grant", "revoke", "execute", "call"}

SYSTEM_PROMPT = """\
You are a PostgreSQL expert for crm_db, a two-stage B2B CRM (BD -> MQL -> SQL).

================================================================
SYSTEM CONTEXT (read fully before writing SQL)
================================================================

CORE TABLES:
- contacts: master contact records (campaign, contact_flag, created_at, flag_updated_at, bd_category)
- contact_phones: contact phone numbers (rank, is_invalid, invalidated_at)
- companies: company master
- agents: BD/MQL agents
BD PIPELINE TABLES:
- call_actions: one row per BD call attempt (call_status, current_state, called_at, attempt_number)
- contact_allocations: BD ownership (attempt_number 0-5, closed_at, close_reason)
MQL PIPELINE TABLES:
- mql_allocations: MQL ownership (allocated_at, closed_at, close_reason)
- mql_call_attempts: one row per MQL FU attempt (follow_up_number 1-30, call_status, current_state, called_at)
- mql_analysis: MQL summary/outcome row per allocation
- meetings: meeting lifecycle
IMPORTANT ENUMS / VALUES:

call_actions.call_status (exact):
    'Connected', 'Did not connect', 'Do not Disturb', 'Call back later', 'Invalid Number', 'Referred'

contacts.contact_flag (exact common values):
    'fresh', 'in_progress', 'needs_followup', 'shared_story', 'snapshot_sent',
    'not_interested', 'dnd', 'attempt_3_months', 'invalid_number', 'referred', 'language_issue',
    'mql_in_progress', 'meeting_in_progress', 'mql_qualified', 'mql_rejected'

mql_allocations.close_reason (exact common values):
    'qualified', 'rejected', 'stalled', 'reallocated', 'escalated', 'bd_history'

mql_call_attempts.current_state (common):
    'Escalate', 'Attempt Again', 'Rescheduled', 'Respondent',
    'Interested', 'Snapshot Sent', 'Snapshot Confirmed', 'Dream Snapshot Confirmed',
    'Meeting Requested', 'Meeting Scheduled', 'Meeting Held',
    'Solution Sent', 'Solution Picked', 'Picked Solution',
    'Not interested', 'Do not Disturb', 'Reffered', 'Irrelevant'

FOLLOW-UP LOGIC (critical):
- BD FU depth comes from call_actions.attempt_number (0=New Contact, 1=FU1, ... 5=FU5)
- MQL FU depth comes from mql_call_attempts.follow_up_number (1..30)
- "Fresh connect (FU1)" in BD dashboards uses attempt_number <= 1
- "Follow-up connect (FU2+)" uses attempt_number > 1 (BD) or follow_up_number > 1 (MQL)

BUSINESS TERM MAPPINGS:
- "interested" -> current_state ILIKE '%interested%' and exclude not interested
- "shared story" -> current_state ILIKE '%shared story%' or ILIKE '%story shared%' or contact_flag='shared_story'
- "snapshot sent" -> current_state ILIKE '%snapshot%' or contact_flag='snapshot_sent'
- "true mql" -> mql_call_attempts.current_state IN ('Dream Snapshot Confirmed','Snapshot Confirmed')
- "blocked" -> contact_flag IN ('dnd','invalid_number','referred','language_issue')
- "pool waiting" (MQL) -> contacts in ('shared_story','snapshot_sent') without active mql_allocations

QUEUE / COUNT SEMANTICS (must follow exactly):
- "escalated (historical)" -> COUNT(*) of mql_allocations with close_reason='escalated'
- "reallocated after escalation" -> escalated allocations where a later mql_allocations row exists for same contact + campaign
- "pending escalation now" -> escalated allocations where latest mql state is 'Escalate' and no later mql_allocations row exists for same contact + campaign
- "pending escalation contacts" -> same as above but COUNT(DISTINCT contact_id)
- "3-month ready" -> contact_flag='attempt_3_months' and flag_updated_at <= CURRENT_DATE - INTERVAL '90 days' and no active mql_allocations for campaign
- "all mqls" (marketing) -> contacts with contact_flag in ('shared_story','snapshot_sent','mql_in_progress','mql_qualified','mql_rejected') including both allocated and unallocated

ID / UNIQUENESS RULES:
- Prefer contact_id as entity key across CRM analytics.
- Use allocation_id for allocation lifecycle questions.
- Use COUNT(DISTINCT contact_id) for people-level KPIs.
- Use COUNT(*) for event/allocation history counts.
- If user asks "why mismatch", provide both event count and distinct-contact count in one query.

QUERY STYLE RULES:
- Default to campaign-scoped queries unless user explicitly asks cross-campaign.
- For BD metrics: use call_actions + contacts.
- For MQL metrics: use mql_call_attempts + mql_allocations (+ contacts as needed).
- Do NOT join mql_call_attempts to call_actions by id unless explicitly required and validated.
- Use DISTINCT ON (contact_id) with ORDER BY called_at DESC for latest-contact-state queries.
- Use COUNT(DISTINCT contact_id) for contact-level counts, COUNT(*)/COUNT(id) for attempt-level counts.
- Use ILIKE for free-text current_state matching; use exact equality for strict enum columns.

SAFETY / OUTPUT RULES:
- Return ONLY raw SQL (no markdown, no explanation).
- ONLY SELECT is allowed; never emit DML/DDL.
- Add LIMIT {max_rows} unless user requests aggregate/count or explicitly asks for all rows.
- Use fully qualified/aliased column names to avoid ambiguity.

Database schema:
{schema}
"""


# ─────────────────────────────────────────────────────────────
# CONFIG  (persisted in .ai_shell_config.json)
# ─────────────────────────────────────────────────────────────

def load_config() -> dict | None:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            return None
    return None


def save_config(provider_id: str, api_key: str, model: str) -> None:
    CONFIG_FILE.write_text(json.dumps(
        {"provider": provider_id, "api_key": api_key, "model": model}, indent=2
    ))
    try:
        CONFIG_FILE.chmod(0o600)
    except Exception:
        pass


def mask(key: str) -> str:
    if len(key) <= 10:
        return key[:3] + "•••"
    return key[:6] + "•••" + key[-4:]


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def fetch_schema() -> str:
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT c.table_name, c.column_name, c.data_type,
               c.column_default, c.is_nullable
        FROM   information_schema.columns c
        JOIN   information_schema.tables  t
               ON c.table_name = t.table_name AND c.table_schema = t.table_schema
        WHERE  c.table_schema = 'public'
        ORDER  BY c.table_name, c.ordinal_position;
    """)
    tables: dict[str, list[str]] = {}
    for table, col, dtype, default, nullable in cur.fetchall():
        tables.setdefault(table, []).append(
            f"  {col} {dtype}"
            + (f" DEFAULT {default}" if default else "")
            + ("" if nullable == "YES" else " NOT NULL")
        )
    cur.close()
    conn.close()
    lines = []
    for t, cols in tables.items():
        lines.append(f"TABLE {t}:")
        lines.extend(cols)
        lines.append("")
    return "\n".join(lines)


def run_query(sql: str) -> tuple[pd.DataFrame, float]:
    t0   = time.perf_counter()
    conn = get_conn()
    df   = pd.read_sql_query(sql, conn)
    dur  = time.perf_counter() - t0
    conn.close()
    return df, dur


@st.cache_resource(show_spinner=False)
def _ensure_history_table() -> bool:
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ai_query_history (
                    id            BIGSERIAL PRIMARY KEY,
                    username      VARCHAR(50) NOT NULL,
                    campaign      VARCHAR(80),
                    question      TEXT NOT NULL,
                    sql_query     TEXT,
                    provider      VARCHAR(20),
                    model         VARCHAR(80),
                    is_error      BOOLEAN NOT NULL DEFAULT FALSE,
                    error_text    TEXT,
                    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_ai_query_history_user_time
                ON ai_query_history(username, created_at DESC)
                """
            )
    return True


def _save_history(
    username: str,
    campaign: str,
    question: str,
    sql_query: str | None,
    provider: str,
    model: str,
    error_text: str | None = None,
) -> None:
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ai_query_history
                        (username, campaign, question, sql_query, provider, model, is_error, error_text)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        username,
                        campaign,
                        question,
                        sql_query,
                        provider,
                        model,
                        error_text is not None,
                        error_text,
                    ),
                )
    except Exception:
        # Non-blocking: chat experience should continue even if history write fails.
        pass


def _load_history(username: str, limit: int = HISTORY_LIMIT) -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, campaign, question, sql_query, is_error, error_text, created_at
            FROM ai_query_history
            WHERE username = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (username, limit),
        )
        rows = cur.fetchall()
    conn.rollback()
    return [dict(r) for r in rows]


def _get_history_item(item_id: int, username: str) -> dict | None:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, campaign, question, sql_query, is_error, error_text, created_at
            FROM ai_query_history
            WHERE id = %s AND username = %s
            LIMIT 1
            """,
            (item_id, username),
        )
        row = cur.fetchone()
    conn.rollback()
    return dict(row) if row else None


# ─────────────────────────────────────────────────────────────
# AI
# ─────────────────────────────────────────────────────────────

def ask_ai(question: str, cfg: dict, schema: str) -> str:
    prompt = SYSTEM_PROMPT.format(max_rows=MAX_ROWS, schema=schema)

    if cfg["provider"] == "gemini":
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=cfg["api_key"])
        response = client.models.generate_content(
            model=cfg["model"],
            contents=question,
            config=types.GenerateContentConfig(
                system_instruction=prompt,
                thinking_config=types.ThinkingConfig(thinking_budget=5000),
            ),
        )
        return response.text.strip()

    if cfg["provider"] == "claude":
        import anthropic
        client = anthropic.Anthropic(api_key=cfg["api_key"])
        msg = client.messages.create(
            model=cfg["model"], max_tokens=4096,
            system=prompt,
            thinking={"type": "enabled", "budget_tokens": 3000},
            messages=[{"role": "user", "content": question}],
        )
        # extract only the text block (skip thinking blocks)
        return next(b.text for b in msg.content if b.type == "text").strip()

    raise ValueError(f"Unknown provider: {cfg['provider']!r}")


def _strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* block comments */ so they can't hide DDL."""
    # block comments
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # line comments
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def validate_sql(sql: str) -> str:
    # ── 1. Strip markdown fences the AI sometimes wraps around SQL ──
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:] if lines[-1].strip() != "```" else lines[1:-1])
    sql = sql.strip()

    # ── 2. Strip comments before any checks (comment-hidden DDL attack) ──
    clean = _strip_sql_comments(sql)

    # ── 3. Multi-statement check — only ONE statement allowed ──
    # Split on semicolons; ignore trailing empty parts.
    statements = [s.strip() for s in clean.split(";") if s.strip()]
    if len(statements) > 1:
        raise ValueError(
            f"Multiple statements detected ({len(statements)} found). "
            "Only a single SELECT/WITH statement is allowed. Execution blocked."
        )

    # ── 4. Must start with SELECT or WITH (CTE) ──
    first = clean.split()[0].lower() if clean.split() else ""
    if first not in {"select", "with"}:
        raise ValueError(
            f"Query starts with '{first.upper()}' — only SELECT/WITH is allowed. "
            "Execution blocked."
        )

    # ── 5. Blocked keyword scan (DDL / DML / privilege commands) ──
    for kw in BLOCKED:
        if re.search(rf"\b{kw}\b", clean, re.IGNORECASE):
            raise ValueError(
                f"Blocked keyword '{kw.upper()}' detected inside query. "
                "Only SELECT/WITH read queries are allowed. Execution blocked."
            )

    return sql  # return original (with formatting) — safe to run


def test_key(provider_id: str, api_key: str, model: str) -> tuple[bool, str]:
    """Returns (success, error_message). error_message is '' on success."""
    try:
        if provider_id == "gemini":
            from google import genai
            client = genai.Client(api_key=api_key)
            client.models.generate_content(model=model, contents="reply: ok")
        else:
            import anthropic
            anthropic.Anthropic(api_key=api_key).messages.create(
                model=model, max_tokens=5,
                messages=[{"role": "user", "content": "reply: ok"}]
            )
        return True, ""
    except Exception as e:
        return False, str(e)


# ─────────────────────────────────────────────────────────────
# PAGE SETUP
# ─────────────────────────────────────────────────────────────

# Initialise session state
if "ai_cfg"      not in st.session_state:
    st.session_state.ai_cfg      = load_config()   # None if first visit
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []             # list of message dicts
if "show_sql"    not in st.session_state:
    st.session_state.show_sql    = True
if "ai_mode" not in st.session_state:
    st.session_state.ai_mode = "query"
if "ai_history_selected_id" not in st.session_state:
    st.session_state.ai_history_selected_id = None


# ─────────────────────────────────────────────────────────────
# SIDEBAR — AI Settings
# ─────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("**AI Provider**")

    cfg = st.session_state.ai_cfg

    if cfg:
        cur_label = "Google Gemini" if cfg["provider"] == "gemini" else "Anthropic Claude"
        icon      = "✦" if cfg["provider"] == "gemini" else "◆"
        with st.container(border=True):
            st.markdown(
                f"<div style='font-size:13px; font-weight:600; margin-bottom:2px;'>"
                f"{icon} {cur_label}</div>"
                f"<div style='font-size:11px; color:#8b92a8; font-family:monospace;'>"
                f"{mask(cfg['api_key'])}</div>",
                unsafe_allow_html=True,
            )
        change = st.button("Change key / provider", use_container_width=True)
    else:
        st.caption("No API key saved yet.")
        change = True   # force open the setup form

    if change or not cfg:
        with st.form("ai_setup_form"):
            provider_label = st.selectbox(
                "Provider",
                list(PROVIDERS.keys()),
                index=0 if (not cfg or cfg["provider"] == "gemini") else 1,
                label_visibility="collapsed",
            )

            hint = ("Free key → [aistudio.google.com](https://aistudio.google.com)"
                    if provider_label == "Google Gemini"
                    else "Key → [console.anthropic.com](https://console.anthropic.com)")
            st.caption(hint)

            api_key = st.text_input(
                "API Key",
                value=cfg["api_key"] if cfg else "",
                type="password",
                placeholder="Paste API key here",
                label_visibility="collapsed",
            )

            col1, col2 = st.columns([1, 1])
            with col1:
                submitted = st.form_submit_button("Save", use_container_width=True, type="primary")
            with col2:
                skip_validation = st.form_submit_button("Save (skip test)", use_container_width=True)

            if submitted or skip_validation:
                if not api_key.strip():
                    st.error("API key cannot be empty.")
                else:
                    pinfo = PROVIDERS[provider_label]

                    def _save_and_reload(pinfo, key):
                        save_config(pinfo["id"], key, pinfo["model"])
                        st.session_state.ai_cfg = {
                            "provider": pinfo["id"],
                            "api_key":  key,
                            "model":    pinfo["model"],
                        }
                        st.rerun()

                    if skip_validation:
                        _save_and_reload(pinfo, api_key.strip())
                    else:
                        with st.spinner("Validating…"):
                            ok, err_msg = test_key(pinfo["id"], api_key.strip(), pinfo["model"])
                        if ok:
                            _save_and_reload(pinfo, api_key.strip())
                        else:
                            st.error(f"Invalid key: {err_msg}")
                            st.caption("Use **Save (skip test)** if the key is correct.")

    st.divider()
    st.session_state.show_sql = st.toggle("Show SQL", value=st.session_state.show_sql)

    if st.button("Clear chat", use_container_width=True):
        st.session_state.chat_history = []
        st.rerun()


# ─────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────

st.title("AI Query")
st.caption("Ask anything about your CRM data in plain English.")

user = get_user() or {}
username = user.get("username", "unknown")
campaign = st.session_state.get("campaign", "consulting")

history_ready = True
try:
    _ensure_history_table()
except Exception as e:
    history_ready = False
    st.warning(f"History store is unavailable: {e}")

view_c1, view_c2 = st.columns(2)
with view_c1:
    if st.button("Main query", use_container_width=True, type="primary" if st.session_state.ai_mode == "query" else "secondary"):
        st.session_state.ai_mode = "query"
        st.session_state.ai_history_selected_id = None
        st.rerun()
with view_c2:
    if st.button("History", use_container_width=True, type="primary" if st.session_state.ai_mode == "history" else "secondary"):
        st.session_state.ai_mode = "history"
        st.session_state.ai_history_selected_id = None
        st.rerun()

with st.expander("Definitions and query tips", expanded=False):
    st.markdown(
        "**Core entity rules**  \n"
        "- Contact-level metric: use `COUNT(DISTINCT contact_id)`.  \n"
        "- Event/allocation metric: use `COUNT(*)` on attempts or allocations.  \n"
        "- `call_actions` = BD history, `mql_call_attempts` = MQL history.  \n"
        "\n"
        "**Stage definitions**  \n"
        "- Shared story: state contains shared story or story shared, or contact flag is `shared_story`.  \n"
        "- Snapshot sent: state contains snapshot, or contact flag is `snapshot_sent`.  \n"
        "- True MQL: `mql_call_attempts.current_state` in `Dream Snapshot Confirmed` or `Snapshot Confirmed`.  \n"
        "- Follow-up (FU2+): BD `attempt_number > 1`; MQL `follow_up_number > 1`.  \n"
        "\n"
        "**Queue semantics**  \n"
        "- Escalated historical: all allocations closed with `close_reason='escalated'`.  \n"
        "- Pending escalation now: latest escalated allocation with no later allocation for that contact in same campaign.  \n"
        "- 3-month ready: `attempt_3_months` contacts older than 90 days with no active MQL allocation.  \n"
        "\n"
        "**Marketing tab scope**  \n"
        "- All MQLs includes allocated + unallocated contacts in flags: `shared_story`, `snapshot_sent`, `mql_in_progress`, `mql_qualified`, `mql_rejected`."
    )

cfg = st.session_state.ai_cfg

# ── Not configured yet ──
if not cfg:
    st.info("Enter your API key in the sidebar to get started.")
    st.stop()

# ── Load schema (cached) ──
try:
    schema = fetch_schema()
except Exception as e:
    st.error(f"Could not load DB schema: {e}")
    st.stop()

if st.session_state.ai_mode == "history":
    st.subheader("History")

    if not history_ready:
        st.info("History is currently unavailable for this environment.")
        st.stop()

    selected_id = st.session_state.ai_history_selected_id
    if selected_id is None:
        rows = _load_history(username=username, limit=HISTORY_LIMIT)
        if not rows:
            st.info("No saved history yet. Run a query from Main query first.")
            st.stop()

        st.caption(f"Showing latest {len(rows)} item(s). Click a thread to open and rerun it.")
        for row in rows:
            with st.container(border=True):
                ts = row["created_at"].strftime("%Y-%m-%d %H:%M:%S") if row.get("created_at") else "-"
                st.markdown(f"**{row['question']}**")
                st.caption(f"{ts} · campaign: {row.get('campaign') or '-'}")
                if row.get("sql_query"):
                    with st.expander("Saved SQL", expanded=False):
                        st.code(row["sql_query"], language="sql")
                if row.get("is_error") and row.get("error_text"):
                    st.warning(row["error_text"])
                if st.button("Open thread", key=f"open_hist_{row['id']}", use_container_width=True):
                    st.session_state.ai_history_selected_id = int(row["id"])
                    st.rerun()
        st.stop()

    item = _get_history_item(int(selected_id), username=username)
    if not item:
        st.warning("History item not found.")
        if st.button("Back to history list", use_container_width=True):
            st.session_state.ai_history_selected_id = None
            st.rerun()
        st.stop()

    top_c1, top_c2 = st.columns(2)
    with top_c1:
        if st.button("Back to history list", use_container_width=True):
            st.session_state.ai_history_selected_id = None
            st.rerun()
    with top_c2:
        if st.button("Go to main query", use_container_width=True):
            st.session_state.ai_mode = "query"
            st.session_state.ai_history_selected_id = None
            st.rerun()

    st.markdown("**Input**")
    st.write(item.get("question") or "-")
    st.markdown("**SQL Query**")
    if item.get("sql_query"):
        st.code(item["sql_query"], language="sql")
        with st.spinner("Running saved query..."):
            try:
                df, dur = run_query(item["sql_query"])
                if df.empty:
                    st.info("No rows returned.")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(
                        f"{len(df)} row{'s' if len(df) != 1 else ''} · {dur:.3f} s"
                        + (f" · first {MAX_ROWS} shown" if len(df) == MAX_ROWS else "")
                    )
            except Exception as e:
                st.error(f"Saved SQL failed to run: {e}")
    else:
        st.info("No SQL was saved for this item.")
    st.stop()

# ── Render chat history ──
for msg in st.session_state.chat_history:
    with st.chat_message("user"):
        st.write(msg["question"])

    with st.chat_message("assistant"):
        if msg.get("error"):
            st.error(msg["error"])
        else:
            if st.session_state.show_sql and msg.get("sql"):
                with st.expander("SQL generated", expanded=False):
                    st.code(msg["sql"], language="sql")

            if msg.get("df") is not None:
                df = msg["df"]
                dur = msg.get("duration", 0)
                if df.empty:
                    st.info("No rows returned.")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(f"{len(df)} row{'s' if len(df) != 1 else ''} · {dur:.3f} s"
                               + (f" · first {MAX_ROWS} shown" if len(df) == MAX_ROWS else ""))

# ── Chat input ──
question = st.chat_input("Ask a question about your CRM data…")

if question:
    # Show user message immediately
    with st.chat_message("user"):
        st.write(question)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            try:
                raw_sql = ask_ai(question, cfg, schema)
                sql     = validate_sql(raw_sql)
                df, dur = run_query(sql)

                if st.session_state.show_sql:
                    with st.expander("SQL generated", expanded=False):
                        st.code(sql, language="sql")

                if df.empty:
                    st.info("No rows returned.")
                else:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(f"{len(df)} row{'s' if len(df) != 1 else ''} · {dur:.3f} s"
                               + (f" · first {MAX_ROWS} shown" if len(df) == MAX_ROWS else ""))

                st.session_state.chat_history.append({
                    "question": question,
                    "sql":      sql,
                    "df":       df,
                    "duration": dur,
                    "error":    None,
                })
                _save_history(
                    username=username,
                    campaign=campaign,
                    question=question,
                    sql_query=sql,
                    provider=cfg["provider"],
                    model=cfg["model"],
                    error_text=None,
                )

            except ValueError as e:
                msg = str(e)
                st.error(f"Security block: {msg}")
                if "raw_sql" in dir():
                    with st.expander("Blocked SQL (audit log)", expanded=False):
                        st.code(raw_sql, language="sql")
                st.session_state.chat_history.append(
                    {"question": question, "sql": raw_sql if "raw_sql" in dir() else None,
                     "df": None, "error": f"Security block: {msg}"})
                _save_history(
                    username=username,
                    campaign=campaign,
                    question=question,
                    sql_query=raw_sql if "raw_sql" in dir() else None,
                    provider=cfg["provider"],
                    model=cfg["model"],
                    error_text=f"Security block: {msg}",
                )

            except Exception as e:
                msg = str(e)
                st.error(msg)
                st.session_state.chat_history.append(
                    {"question": question, "sql": None, "df": None, "error": msg})
                _save_history(
                    username=username,
                    campaign=campaign,
                    question=question,
                    sql_query=None,
                    provider=cfg["provider"],
                    model=cfg["model"],
                    error_text=msg,
                )
