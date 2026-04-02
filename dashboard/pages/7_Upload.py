"""
pages/7_Upload.py — Master Data Upload

Upload a cleaned CSV of new contacts into the normalized database:
  companies  → name, address
  contacts   → source, source_id, name, designation, company_id
  contact_phones  → up to 3 phone numbers per contact
  contact_emails  → up to 3 emails per contact

Duplicate handling (matched by source + source_id):
  - Existing contact → update name, title, company if changed
  - New phones       → added (skipped if already stored, max 3 total)
  - New emails       → added (skipped if already stored, max 3 total)
  - New contact      → fully inserted across all 4 tables
"""

import logging
import traceback

import streamlit as st
import pandas as pd
from utils.db import get_conn
from utils.errors import log_and_show, log_and_warn

logger = logging.getLogger("crm.upload")

campaign = st.session_state.get("campaign", "consulting")
st.title("Master Data Upload")
st.caption(
    f"Uploading into campaign: **{campaign}**. "
    "Data is written to companies → contacts → contact_phones → contact_emails."
)

# ── SOURCE SELECTOR ───────────────────────────────────────────
st.subheader("Step 1 — Select data source")

PREDEFINED_SOURCES = [
    {"label": "RocketReach",        "prefix": "RR", "source": "rocketreach"},
    {"label": "MSME",               "prefix": "MS", "source": "msme"},
    {"label": "Pharma",             "prefix": "PH", "source": "pharma"},
    {"label": "Manual (no prefix)", "prefix": "",   "source": "manual"},
]

dropdown_options = (
    [f"{s['label']}  (prefix: {s['prefix']} | ...)" if s["prefix"] else s["label"]
     for s in PREDEFINED_SOURCES]
    + ["Custom / New source…"]
)

source_label = st.selectbox("Source", dropdown_options)

if source_label == "Custom / New source…":
    col_a, col_b = st.columns(2)
    with col_a:
        custom_prefix = st.text_input(
            "Prefix (e.g. RR)", max_chars=10,
            help="Short prefix that appears before | in your Unique ID column"
        ).strip().upper()
    with col_b:
        custom_source = st.text_input(
            "Source name stored in DB (e.g. rocketreach)", max_chars=50,
            help="Lowercase identifier written to the `source` column in the database"
        ).strip().lower().replace(" ", "_")
    prefix = custom_prefix
    source = custom_source
    if not source:
        st.info("Enter a source name to continue.")
        st.stop()
else:
    idx = dropdown_options.index(source_label)
    sel  = PREDEFINED_SOURCES[idx]
    prefix = sel["prefix"]
    source = sel["source"]

with st.expander("How is the Unique ID stored in the database?"):
    if prefix:
        st.markdown(f"""
- Your CSV has a Unique ID column with values like **`{prefix} | 62078057`**
- The system stores: `source = '{source}'`, `source_id = '62078057'`
- The `{prefix} | ` prefix is **stripped on upload** — it is never stored in the database
- Re-uploading the same CSV is **safe** — existing contacts are updated, not duplicated
        """)
    else:
        st.markdown(f"""
- Whatever value is in the Unique ID column is stored as `source_id` directly
- `source = '{source}'` is set automatically
        """)

st.divider()

# ── CSV FORMAT GUIDE ──────────────────────────────────────────
st.subheader("Step 2 — Upload CSV")

with st.expander("Expected CSV column names (click to expand)"):
    st.markdown("""
| Column name(s) accepted | Required | What it maps to | Notes |
|---|---|---|---|
| `unique_id`, `id`, `source_id` | **Yes** | `contacts.source_id` | Strip prefix: `RR \\| 123` → `123` |
| `person_name`, `name`, `full_name` | **Yes** | `contacts.first_name` + `last_name` | Split on first space |
| `company`, `company_name` | **Yes** | `companies.name` | Matched by normalised name |
| `title`, `designation`, `job_title` | No | `contacts.designation` | |
| `phone`, `phones`, `phone_number`, `phone_no` | No | `contact_phones` | Comma-separated, up to 3 |
| `email`, `emails`, `email_address` | No | `contact_emails` | Comma-separated, up to 3 |
| `address`, `raw_address` | No | `companies.raw_address` | Used if company is new |

Column names are **case-insensitive**. Extra columns are ignored.
    """)

uploaded = st.file_uploader("Choose CSV file", type=["csv"])
if not uploaded:
    st.stop()

# ── PARSE CSV ─────────────────────────────────────────────────
try:
    df = pd.read_csv(uploaded, dtype=str)
    df = df.fillna("")
except Exception as e:
    logger.error("CSV parse failed: %s\n%s", e, traceback.format_exc())
    st.error("Could not read the CSV file. Please check that it is a valid UTF-8 CSV.")
    with st.expander("Technical details"):
        st.code(traceback.format_exc(), language="text")
    st.stop()

# Normalise column names: lowercase, spaces/hyphens → underscore
df.columns = [
    c.strip().lower().replace(" ", "_").replace("-", "_").replace("|", "")
    for c in df.columns
]

# Alias resolution — first match wins
ALIASES: dict[str, list[str]] = {
    "unique_id":   ["unique_id", "id", "source_id", "unique_id___static", "unique_id__static"],
    "person_name": ["person_name", "name", "full_name", "contact_name"],
    "company":     ["company", "company_name", "organisation", "organization"],
    "title":       ["title", "designation", "job_title"],
    "phone":       ["phone", "phones", "phone_number", "phone_numbers", "phone_no"],
    "email":       ["email", "emails", "email_address", "email_addresses"],
    "address":     ["address", "raw_address", "company_address", "location"],
}

def find_col(aliases: list[str]) -> str | None:
    for a in aliases:
        if a in df.columns:
            return a
    return None

col_map = {field: find_col(aliases) for field, aliases in ALIASES.items()}

# Check required columns are present
missing_required = [f for f in ["unique_id", "person_name", "company"] if col_map[f] is None]
if missing_required:
    st.error(
        f"Required column(s) not found: **{missing_required}**\n\n"
        f"Columns detected in your CSV: `{list(df.columns)}`"
    )
    st.stop()

st.success(f"CSV loaded — **{len(df)} rows**")

mapped_cols = {k: v for k, v in col_map.items() if v}
st.caption("Column mapping: " + "  ·  ".join(f"`{v}` → {k}" for k, v in mapped_cols.items()))

# Preview: show only the desired mapped columns with friendly labels
FIELD_LABELS = {
    "unique_id":   "Unique ID",
    "person_name": "Person Name",
    "company":     "Company Name",
    "title":       "Title",
    "phone":       "Phone No",
    "email":       "Email",
    "address":     "Address",
}

st.subheader("Preview (first 5 rows — mapped columns only)")
preview_data = {
    FIELD_LABELS[field]: df[col].head(5)
    for field, col in col_map.items()
    if col and field in FIELD_LABELS
}
if preview_data:
    st.dataframe(pd.DataFrame(preview_data), use_container_width=True, hide_index=True)
else:
    st.dataframe(df.head(5), use_container_width=True, hide_index=True)

st.divider()

# ── PARSE HELPERS ─────────────────────────────────────────────
def get_val(row: pd.Series, field: str) -> str:
    col = col_map.get(field)
    if col and col in row.index:
        return str(row[col]).strip()
    return ""

def parse_source_id(raw: str) -> str:
    """Extract numeric ID: 'RR | 62078057' → '62078057', or return raw."""
    if "|" in raw:
        return raw.split("|", 1)[1].strip()
    return raw.strip()

def split_multi(val: str, max_items: int = 3) -> list[str]:
    """Split comma/semicolon-separated cell into up to max_items non-empty strings."""
    if not val:
        return []
    parts = [p.strip() for p in val.replace(";", ",").split(",") if p.strip()]
    return parts[:max_items]

def split_name(full: str) -> tuple[str, str]:
    parts = full.strip().split(" ", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")

# ── BUILD PLAN ────────────────────────────────────────────────
st.subheader("Step 3 — Upload plan")

# Fetch existing source_ids for this source to classify new vs update
try:
    import psycopg2
    _conn = get_conn()
    with _conn.cursor() as _cur:
        _cur.execute(
            "SELECT source_id FROM contacts WHERE source = %s AND source_id IS NOT NULL",
            (source,)
        )
        existing_ids = {row[0] for row in _cur.fetchall()}
    _conn.close()
except Exception as e:
    logger.error("Could not query existing contacts: %s\n%s", e, traceback.format_exc())
    st.warning("Could not check for existing contacts — all rows will be treated as new.")
    with st.expander("Technical details"):
        st.code(traceback.format_exc(), language="text")
    existing_ids = set()

new_rows:    list[dict] = []
update_rows: list[dict] = []
skip_rows:   list[dict] = []

for _, row in df.iterrows():
    raw_uid   = get_val(row, "unique_id")
    source_id = parse_source_id(raw_uid)
    if not source_id:
        skip_rows.append({"Reason": "No Unique ID", "Row": str(dict(row))[:100]})
        continue

    person_name = get_val(row, "person_name")
    if not person_name:
        skip_rows.append({"Reason": "No name", "Source ID": source_id})
        continue

    company_name = get_val(row, "company")
    if not company_name:
        skip_rows.append({"Reason": "No company", "Source ID": source_id})
        continue

    first, last = split_name(person_name)
    entry = {
        "source_id":   source_id,
        "company":     company_name,
        "address":     get_val(row, "address"),
        "first_name":  first,
        "last_name":   last,
        "title":       get_val(row, "title") or None,
        "phones":      split_multi(get_val(row, "phone")),
        "emails":      split_multi(get_val(row, "email")),
    }
    if source_id in existing_ids:
        update_rows.append(entry)
    else:
        new_rows.append(entry)

c1, c2, c3 = st.columns(3)
c1.metric("New contacts to insert", len(new_rows))
c2.metric("Existing contacts to update", len(update_rows))
c3.metric("Rows skipped (invalid)", len(skip_rows))

if skip_rows:
    with st.expander(f"Skipped rows — {len(skip_rows)}"):
        st.dataframe(pd.DataFrame(skip_rows), use_container_width=True, hide_index=True)

if not new_rows and not update_rows:
    st.info("Nothing to upload after validation.")
    st.stop()

# Show samples
if new_rows:
    with st.expander(f"Sample new contacts (first 5 of {len(new_rows)})"):
        sample = [{
            "Source ID": r["source_id"],
            "Name":      f"{r['first_name']} {r['last_name']}",
            "Company":   r["company"],
            "Title":     r["title"] or "—",
            "Phones":    ", ".join(r["phones"]) or "—",
            "Emails":    ", ".join(r["emails"]) or "—",
        } for r in new_rows[:5]]
        st.dataframe(pd.DataFrame(sample), use_container_width=True, hide_index=True)

if update_rows:
    with st.expander(f"Sample updates (first 5 of {len(update_rows)})"):
        sample = [{
            "Source ID": r["source_id"],
            "Name":      f"{r['first_name']} {r['last_name']}",
            "Company":   r["company"],
            "Phones":    ", ".join(r["phones"]) or "—",
        } for r in update_rows[:5]]
        st.dataframe(pd.DataFrame(sample), use_container_width=True, hide_index=True)

st.divider()

# ── UPLOAD ────────────────────────────────────────────────────
st.subheader("Step 4 — Run upload")
confirmed = st.checkbox("I have reviewed the plan above and want to proceed")
if not confirmed:
    st.info("Check the box above to enable the upload button.")
    st.stop()

if not st.button("Upload to database", type="primary"):
    st.stop()

# ── EXECUTE ───────────────────────────────────────────────────
progress  = st.progress(0, text="Starting…")
inserted  = 0
updated   = 0
phones_added = 0
emails_added = 0
errors: list[str] = []

all_rows = [(entry, False) for entry in new_rows] + [(entry, True) for entry in update_rows]
total    = len(all_rows)

conn = get_conn()
conn.autocommit = False

try:
    with conn.cursor() as cur:
        for i, (entry, is_update) in enumerate(all_rows):
            progress.progress((i + 1) / total, text=f"Processing {i+1}/{total}…")
            try:
                cur.execute("SAVEPOINT row_sp")

                # ── 1. Company: find or insert ───────────────────
                cur.execute(
                    "SELECT id FROM companies WHERE name_normalized = LOWER(TRIM(%s))",
                    (entry["company"],)
                )
                result = cur.fetchone()
                if result:
                    company_id = result[0]
                    # Update raw_address only if we have one and company doesn't yet
                    if entry["address"]:
                        cur.execute(
                            "UPDATE companies SET raw_address = %s "
                            "WHERE id = %s AND (raw_address IS NULL OR raw_address = '')",
                            (entry["address"], company_id)
                        )
                else:
                    cur.execute(
                        "INSERT INTO companies (name, raw_address) VALUES (%s, %s) RETURNING id",
                        (entry["company"], entry["address"] or None)
                    )
                    company_id = cur.fetchone()[0]

                # ── 2. Contact: insert or update ─────────────────
                if is_update:
                    cur.execute("""
                        UPDATE contacts
                        SET first_name  = %s,
                            last_name   = %s,
                            designation = COALESCE(%s, designation),
                            company_id  = %s,
                            updated_at  = NOW()
                        WHERE source = %s AND source_id = %s
                        RETURNING id
                    """, (
                        entry["first_name"], entry["last_name"],
                        entry["title"], company_id,
                        source, entry["source_id"]
                    ))
                    contact_id = cur.fetchone()[0]
                    updated += 1
                else:
                    cur.execute("""
                        INSERT INTO contacts
                            (source, source_id, company_id,
                             first_name, last_name, designation, campaign)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        source, entry["source_id"], company_id,
                        entry["first_name"], entry["last_name"],
                        entry["title"], campaign
                    ))
                    contact_id = cur.fetchone()[0]
                    inserted += 1

                # ── 3. Phones ─────────────────────────────────────
                if entry["phones"]:
                    cur.execute(
                        "SELECT phone_number, rank FROM contact_phones "
                        "WHERE contact_id = %s ORDER BY rank",
                        (contact_id,)
                    )
                    existing_phones = {r[0] for r in cur.fetchall()}
                    current_max_rank = len(existing_phones)

                    for phone in entry["phones"]:
                        if not phone or phone in existing_phones:
                            continue
                        if current_max_rank >= 3:
                            break
                        current_max_rank += 1
                        cur.execute(
                            "INSERT INTO contact_phones (contact_id, phone_number, rank) "
                            "VALUES (%s, %s, %s)",
                            (contact_id, phone, current_max_rank)
                        )
                        existing_phones.add(phone)
                        phones_added += 1

                # ── 4. Emails ─────────────────────────────────────
                if entry["emails"]:
                    cur.execute(
                        "SELECT email, rank FROM contact_emails "
                        "WHERE contact_id = %s ORDER BY rank",
                        (contact_id,)
                    )
                    existing_emails = {r[0] for r in cur.fetchall()}
                    current_max_rank = len(existing_emails)

                    for email in entry["emails"]:
                        if not email or email in existing_emails:
                            continue
                        if current_max_rank >= 3:
                            break
                        current_max_rank += 1
                        cur.execute(
                            "INSERT INTO contact_emails (contact_id, email, rank) "
                            "VALUES (%s, %s, %s)",
                            (contact_id, email, current_max_rank)
                        )
                        existing_emails.add(email)
                        emails_added += 1

                cur.execute("RELEASE SAVEPOINT row_sp")

            except Exception as row_err:
                cur.execute("ROLLBACK TO SAVEPOINT row_sp")
                tb = traceback.format_exc()
                logger.error("Row upload failed | source_id=%s: %s\n%s", entry["source_id"], row_err, tb)
                errors.append(f"ID {entry['source_id']}: {row_err}")

    conn.commit()
    progress.progress(1.0, text="Done!")

    st.success(
        f"**Upload complete.**  "
        f"Inserted: **{inserted}** · Updated: **{updated}** · "
        f"Phones added: **{phones_added}** · Emails added: **{emails_added}**"
    )
    if errors:
        with st.expander(f"{len(errors)} row-level errors"):
            for err in errors:
                st.caption(err)

except Exception as e:
    conn.rollback()
    tb = traceback.format_exc()
    logger.error("Upload transaction failed and was rolled back: %s\n%s", e, tb)
    st.error("Upload failed and was rolled back. No data was written.")
    with st.expander("Technical details (for developers)"):
        st.code(tb, language="text")
finally:
    conn.close()
