"""
ai_query_shell.py — CRM AI Query Shell
Natural language questions → AI generates SQL → runs on PostgreSQL → prints results.

First run: interactive setup asks for your API key and saves it.
Next runs:  loads saved config automatically, goes straight to the query prompt.
Change key: type  !setup  at any time.

Supports: Google Gemini  or  Anthropic Claude
"""

import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

CONFIG_FILE = Path(__file__).parent / ".ai_shell_config.json"

PROVIDERS = {
    "1": {"name": "gemini",  "label": "Google Gemini",    "default_model": "gemini-2.5-flash"},
    "2": {"name": "claude",  "label": "Anthropic Claude", "default_model": "claude-sonnet-4-6"},
}

MAX_ROWS = 100

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "crm_db")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

BLOCKED = {"insert", "update", "delete", "drop", "create", "alter",
           "truncate", "grant", "revoke", "execute", "call"}

# ─────────────────────────────────────────────────────────────
# COLORS
# ─────────────────────────────────────────────────────────────

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
GRAY   = "\033[90m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def c(color, text): return f"{color}{text}{RESET}"
def ok(text):   print(f"  {GREEN}✓{RESET}  {text}")
def err(text):  print(f"  {RED}✗{RESET}  {text}")
def info(text): print(f"  {GRAY}{text}{RESET}")
def rule():     print(f"{CYAN}{'─' * 64}{RESET}")


# ─────────────────────────────────────────────────────────────
# CONFIG FILE  (stores provider + api_key between runs)
# ─────────────────────────────────────────────────────────────

def load_config() -> dict | None:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            return None
    return None


def save_config(provider: str, api_key: str, model: str):
    CONFIG_FILE.write_text(json.dumps({
        "provider": provider,
        "api_key":  api_key,
        "model":    model,
    }, indent=2))
    CONFIG_FILE.chmod(0o600)   # owner-read only — key is sensitive


def mask_key(key: str) -> str:
    """Show first 6 + last 4 chars, mask the middle."""
    if len(key) <= 12:
        return key[:3] + "..." + key[-2:]
    return key[:6] + "..." + key[-4:]


# ─────────────────────────────────────────────────────────────
# API KEY VALIDATION  (test call before saving)
# ─────────────────────────────────────────────────────────────

def validate_gemini_key(api_key: str, model: str) -> bool:
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        m = genai.GenerativeModel(model)
        m.generate_content("reply with: ok")
        return True
    except Exception as e:
        err(f"Gemini key check failed: {e}")
        return False


def validate_claude_key(api_key: str, model: str) -> bool:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        client.messages.create(
            model=model, max_tokens=10,
            messages=[{"role": "user", "content": "reply with: ok"}]
        )
        return True
    except Exception as e:
        err(f"Claude key check failed: {e}")
        return False


def validate_key(provider: str, api_key: str, model: str) -> bool:
    print(f"  {GRAY}Validating key...{RESET}", end=" ", flush=True)
    if provider == "gemini":
        result = validate_gemini_key(api_key, model)
    else:
        result = validate_claude_key(api_key, model)
    if result:
        print(f"{GREEN}OK{RESET}")
    return result


# ─────────────────────────────────────────────────────────────
# SETUP WIZARD
# ─────────────────────────────────────────────────────────────

def setup_wizard(change_key_only: bool = False, current: dict | None = None) -> dict:
    """
    Interactive setup. Returns config dict {provider, api_key, model}.
    change_key_only=True  → keep existing provider, only ask for new key.
    """
    rule()
    print(f"{BOLD}{CYAN}  CRM AI Shell — Setup{RESET}")
    rule()

    # ── Choose provider ──
    if change_key_only and current:
        provider = current["provider"]
        model    = current["model"]
        pname    = "Google Gemini" if provider == "gemini" else "Anthropic Claude"
        print(f"\n  Provider : {c(YELLOW, pname)}  (unchanged)\n")
    else:
        print(f"\n  Choose AI provider:\n")
        for k, v in PROVIDERS.items():
            print(f"    {c(CYAN, k)})  {v['label']}")

        if current:
            cur_num = "1" if current["provider"] == "gemini" else "2"
            print(f"\n  Current: {c(GRAY, PROVIDERS[cur_num]['label'])}  (press Enter to keep)")

        while True:
            choice = input(f"\n  {YELLOW}Enter 1 or 2:{RESET} ").strip()
            if not choice and current:
                provider = current["provider"]
                model    = current["model"]
                break
            if choice in PROVIDERS:
                provider = PROVIDERS[choice]["name"]
                model    = PROVIDERS[choice]["default_model"]
                break
            err("Please enter 1 or 2.")

    # ── Enter API key ──
    key_label = "Gemini API key (from aistudio.google.com)" if provider == "gemini" \
                else "Claude API key (from console.anthropic.com)"

    if current and not change_key_only:
        cur_masked = mask_key(current["api_key"]) if current.get("api_key") else ""
        print(f"\n  Current key: {c(GRAY, cur_masked)}  (press Enter to keep)")

    print(f"\n  {key_label}")

    while True:
        import getpass
        try:
            raw = getpass.getpass(f"  {YELLOW}Paste key (hidden):{RESET} ").strip()
        except Exception:
            raw = input(f"  {YELLOW}Paste key:{RESET} ").strip()

        if not raw:
            if current and current.get("api_key"):
                # keep existing
                raw = current["api_key"]
                ok("Keeping existing key.")
                break
            err("API key cannot be empty.")
            continue

        # Validate
        if validate_key(provider, raw, model):
            ok("Key is valid.")
            break
        else:
            retry = input(f"  {YELLOW}Try a different key? [Y/n]:{RESET} ").strip().lower()
            if retry in ("n", "no"):
                print(f"  {GRAY}Setup cancelled.{RESET}")
                sys.exit(0)

    # ── Save ──
    save_config(provider, raw, model)
    pname = "Google Gemini" if provider == "gemini" else "Anthropic Claude"
    ok(f"Saved to {CONFIG_FILE.name}  →  provider: {pname},  key: {mask_key(raw)}")
    print()

    return {"provider": provider, "api_key": raw, "model": model}


# ─────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def fetch_schema(conn) -> str:
    cur = conn.cursor()
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

    lines = []
    for t, cols in tables.items():
        lines.append(f"TABLE {t}:")
        lines.extend(cols)
        lines.append("")
    return "\n".join(lines)


def execute_sql(conn, sql: str):
    t0 = time.perf_counter()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = cur.fetchmany(MAX_ROWS)
    dur  = time.perf_counter() - t0
    cols = [d.name for d in cur.description] if cur.description else []
    cur.close()
    return cols, [dict(r) for r in rows], dur


# ─────────────────────────────────────────────────────────────
# AI
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a PostgreSQL expert for a B2B CRM database called crm_db.
Convert the user's plain-English question into a single valid SELECT statement.

Rules:
- Return ONLY the raw SQL — no markdown fences, no explanation, no extra text.
- ONLY SELECT is allowed. Never write INSERT, UPDATE, DELETE, DROP, CREATE, ALTER.
- Column and table names are snake_case.
- Limit to {max_rows} rows with LIMIT unless the user asks for more or uses an aggregate.
- Use CURRENT_DATE / CURRENT_TIMESTAMP for date questions.
- If ambiguous, write the most reasonable query.

Database schema:
{schema}
"""


def ask_ai(question: str, cfg: dict, schema: str) -> str:
    prompt = SYSTEM_PROMPT.format(max_rows=MAX_ROWS, schema=schema)

    if cfg["provider"] == "gemini":
        import google.generativeai as genai
        genai.configure(api_key=cfg["api_key"])
        model = genai.GenerativeModel(cfg["model"], system_instruction=prompt)
        return model.generate_content(question).text.strip()

    elif cfg["provider"] == "claude":
        import anthropic
        client = anthropic.Anthropic(api_key=cfg["api_key"])
        msg = client.messages.create(
            model=cfg["model"], max_tokens=1024,
            system=prompt,
            messages=[{"role": "user", "content": question}],
        )
        return msg.content[0].text.strip()

    raise ValueError(f"Unknown provider: {cfg['provider']!r}")


# ─────────────────────────────────────────────────────────────
# SAFETY
# ─────────────────────────────────────────────────────────────

def validate_sql(sql: str) -> str:
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:] if lines[-1].strip() != "```" else lines[1:-1])
    sql = sql.strip()

    first = sql.split()[0].lower() if sql.split() else ""
    if first != "select":
        raise ValueError(f"AI returned a non-SELECT statement (starts with '{first}'). Blocked.")

    for kw in BLOCKED:
        if re.search(rf"\b{kw}\b", sql, re.IGNORECASE):
            raise ValueError(f"Blocked keyword '{kw}' detected. Only SELECT is allowed.")
    return sql


# ─────────────────────────────────────────────────────────────
# DISPLAY
# ─────────────────────────────────────────────────────────────

_show_sql = True   # mutable at runtime via !sql toggle


def print_welcome_banner(cfg: dict, table_count: int):
    pname = "Google Gemini" if cfg["provider"] == "gemini" else "Anthropic Claude"
    rule()
    print(f"{BOLD}{CYAN}  CRM AI Query Shell{RESET}")
    print(f"  {GRAY}AI : {pname} ({cfg['model']})  ·  key: {mask_key(cfg['api_key'])}{RESET}")
    print(f"  {GRAY}DB : {DB_NAME} @ {DB_HOST}:{DB_PORT}  ·  {table_count} tables loaded{RESET}")
    rule()
    print(f"  {DIM}Type a question in plain English and press Enter.{RESET}")
    print(f"  {DIM}Commands: {c(CYAN, '!setup')}  change key/provider   "
          f"{c(CYAN, '!sql')}  toggle SQL display   "
          f"{c(CYAN, '!help')}  all commands{RESET}")
    rule()
    print()


def print_help():
    rule()
    print(f"  {BOLD}Commands{RESET}")
    print(f"  {CYAN}!setup{RESET}       Change API key or switch provider (re-runs wizard)")
    print(f"  {CYAN}!key{RESET}         Change API key only (keep same provider)")
    print(f"  {CYAN}!sql{RESET}         Toggle showing generated SQL  (currently: "
          f"{c(GREEN, 'on') if _show_sql else c(GRAY, 'off')})")
    print(f"  {CYAN}!info{RESET}        Show current config and DB details")
    print(f"  {CYAN}!help{RESET}        Show this help")
    print(f"  {CYAN}exit{RESET}         Quit")
    rule()
    print()


def print_info(cfg: dict, table_count: int):
    pname = "Google Gemini" if cfg["provider"] == "gemini" else "Anthropic Claude"
    rule()
    print(f"  {BOLD}Current config{RESET}")
    print(f"  Provider  : {c(YELLOW, pname)}")
    print(f"  Model     : {cfg['model']}")
    print(f"  API key   : {c(GRAY, mask_key(cfg['api_key']))}")
    print(f"  Config at : {CONFIG_FILE}")
    print()
    print(f"  {BOLD}Database{RESET}")
    print(f"  {DB_NAME} @ {DB_HOST}:{DB_PORT}  ·  {table_count} tables")
    print(f"  Max rows  : {MAX_ROWS}  ·  Show SQL: {c(GREEN,'on') if _show_sql else c(GRAY,'off')}")
    rule()
    print()


def print_results(cols, rows, sql: str, dur: float):
    if _show_sql:
        print(f"  {GRAY}SQL →{RESET}  {DIM}{sql}{RESET}\n")
    if not rows:
        print(f"  {YELLOW}No rows returned.{RESET}")
    else:
        data = [[row[c] for c in cols] for row in rows]
        print(tabulate(data, headers=cols, tablefmt="rounded_outline"))
        capped = f"  {GRAY}(first {MAX_ROWS} rows shown){RESET}" if len(rows) == MAX_ROWS else ""
        print(f"\n  {GRAY}{len(rows)} row{'s' if len(rows) != 1 else ''}  ·  {dur:.3f} s{RESET}{capped}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    global _show_sql

    # ── Load or run setup ──
    cfg = load_config()
    if cfg is None:
        print(f"\n{BOLD}{CYAN}  Welcome to CRM AI Shell{RESET}")
        print(f"  {GRAY}No saved configuration found. Let's set it up.{RESET}\n")
        cfg = setup_wizard()
    else:
        pname = "Google Gemini" if cfg["provider"] == "gemini" else "Anthropic Claude"
        print(f"\n  {GRAY}Loaded config — {pname} · {mask_key(cfg['api_key'])}{RESET}")

    # ── Connect to DB ──
    print(f"  {GRAY}Connecting to {DB_NAME}...{RESET}", end=" ", flush=True)
    try:
        conn = get_db_connection()
        print(f"{GREEN}OK{RESET}")
    except Exception as e:
        print(f"{RED}FAILED{RESET}")
        err(str(e))
        sys.exit(1)

    # ── Load schema ──
    print(f"  {GRAY}Loading schema...{RESET}", end=" ", flush=True)
    schema = fetch_schema(conn)
    table_count = schema.count("TABLE ")
    print(f"{GREEN}{table_count} tables{RESET}")

    print_welcome_banner(cfg, table_count)

    # ── Query loop ──
    try:
        while True:
            try:
                question = input(f"{YELLOW}  >> {RESET}").strip()
            except (EOFError, KeyboardInterrupt):
                print(f"\n  {GRAY}Goodbye.{RESET}\n")
                break

            if not question:
                continue

            q_lower = question.lower()

            # ── Built-in commands ──
            if q_lower in {"exit", "quit", "q"}:
                print(f"  {GRAY}Goodbye.{RESET}\n")
                break

            if q_lower == "!help":
                print_help()
                continue

            if q_lower == "!info":
                print_info(cfg, table_count)
                continue

            if q_lower == "!sql":
                _show_sql = not _show_sql
                state = f"{GREEN}on{RESET}" if _show_sql else f"{GRAY}off{RESET}"
                print(f"  SQL display: {state}\n")
                continue

            if q_lower == "!setup":
                cfg = setup_wizard(change_key_only=False, current=cfg)
                print_welcome_banner(cfg, table_count)
                continue

            if q_lower == "!key":
                cfg = setup_wizard(change_key_only=True, current=cfg)
                pname = "Google Gemini" if cfg["provider"] == "gemini" else "Anthropic Claude"
                ok(f"Now using {pname} · {mask_key(cfg['api_key'])}")
                print()
                continue

            # ── AI → SQL → Results ──
            try:
                print(f"  {GRAY}Thinking...{RESET}", end="\r", flush=True)
                raw_sql = ask_ai(question, cfg, schema)
                sql = validate_sql(raw_sql)
                print(" " * 20, end="\r")   # clear "Thinking..." line

                cols, rows, dur = execute_sql(conn, sql)
                print()
                print_results(cols, rows, sql, dur)
                print()

            except ValueError as e:
                print(f"\n  {RED}Safety block:{RESET} {e}\n")
            except psycopg2.Error as e:
                print(f"\n  {RED}SQL error:{RESET} {e.pgerror or str(e)}\n")
            except Exception as e:
                print(f"\n  {RED}Error:{RESET} {e}\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
