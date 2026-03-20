"""
Runtime environment config — reads CURRENT_DATABASE() and CURRENT_SCHEMA()
at startup so the same source code works in any Snowflake account.

Usage in any page:
    from config import DB, STAGE, get_session, get_doc_type_labels, inject_custom_css
    session = get_session()
    session.sql(f"SELECT * FROM {DB}.EXTRACTED_FIELDS").to_pandas()
    stage_path = f"@{STAGE}/{file_name}"
    labels = get_doc_type_labels(session, "INVOICE")
    config = get_doc_type_config(session, "UTILITY_BILL")
    inject_custom_css()  # call once per page for consistent styling
"""

import json
import streamlit as st
from snowflake.snowpark.context import get_active_session


# ── Shared CSS for demo polish ───────────────────────────────────────────────
_CUSTOM_CSS = """
<style>
/* ── KPI metric cards ─────────────────────────────────────── */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #f8fbff 0%, #eef4fb 100%);
    border: 1px solid #d0e3f7;
    border-left: 4px solid #29B5E8;
    border-radius: 8px;
    padding: 16px 20px 12px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
div[data-testid="stMetric"] label {
    color: #5a6577 !important;
    font-size: 0.85rem !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    color: #1a1f36 !important;
    font-weight: 600 !important;
}

/* ── Sidebar branding ─────────────────────────────────────── */
section[data-testid="stSidebar"] > div:first-child {
    padding-top: 1rem;
}
section[data-testid="stSidebar"] .brand-header {
    text-align: center;
    padding: 0.5rem 0 1rem 0;
    border-bottom: 1px solid #e0e4ea;
    margin-bottom: 1rem;
}

/* ── Data tables ──────────────────────────────────────────── */
div[data-testid="stDataFrame"] {
    border: 1px solid #e0e4ea;
    border-radius: 8px;
}

/* ── Page dividers ────────────────────────────────────────── */
hr {
    border-color: #e0e4ea !important;
}

/* ── Buttons ──────────────────────────────────────────────── */
button[kind="primary"] {
    border-radius: 6px !important;
}
</style>
"""


def inject_custom_css():
    """Inject shared CSS once per page. Call at the top of every page."""
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)


def sidebar_branding(customer_name: str | None = None):
    """Render branded sidebar header. Call inside `with st.sidebar:`."""
    subtitle = customer_name or "Document Processing POC"
    st.markdown(
        '<div class="brand-header">'
        '<span style="font-size:1.6rem;">&#10052;</span><br>'
        '<strong style="font-size:1.1rem; color:#29B5E8;">AI_EXTRACT</strong><br>'
        f'<span style="font-size:0.75rem; color:#8a94a6;">{subtitle}</span>'
        '</div>',
        unsafe_allow_html=True,
    )


def _init_session():
    """Get or create a Snowpark session.

    Inside Snowflake (SiS / SPCS): uses get_active_session().
    Local development / testing: falls back to Session.builder with
    the POC_CONNECTION env-var or the 'default' connection name.
    POC_DB / POC_SCHEMA / POC_WH override the connection defaults.
    """
    try:
        return get_active_session()
    except Exception:
        import os
        from snowflake.snowpark import Session
        conn = os.environ.get("POC_CONNECTION", "default")
        sess = Session.builder.config("connection_name", conn).create()
        db = os.environ.get("POC_DB", "AI_EXTRACT_POC")
        sch = os.environ.get("POC_SCHEMA", "DOCUMENTS")
        wh = os.environ.get("POC_WH", "AI_EXTRACT_WH")
        role = os.environ.get("POC_ROLE")
        if role:
            sess.sql(f"USE ROLE {role}").collect()
        sess.sql(f"USE DATABASE {db}").collect()
        sess.sql(f"USE SCHEMA {sch}").collect()
        sess.sql(f"USE WAREHOUSE {wh}").collect()
        return sess


_session = _init_session()
_ctx = _session.sql(
    "SELECT CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch"
).collect()[0]

DB = f"{_ctx['DB']}.{_ctx['SCH']}"
STAGE = f"{_ctx['DB']}.{_ctx['SCH']}.DOCUMENT_STAGE"

# Default labels (INVOICE) used as fallback when config table is missing
_DEFAULT_LABELS = {
    "field_1": "Vendor Name",
    "field_2": "Invoice Number",
    "field_3": "PO Number",
    "field_4": "Invoice Date",
    "field_5": "Due Date",
    "field_6": "Payment Terms",
    "field_7": "Recipient",
    "field_8": "Subtotal",
    "field_9": "Tax Amount",
    "field_10": "Total Amount",
    "sender_label": "Vendor / Sender",
    "amount_label": "Total Amount",
    "date_label": "Invoice Date",
    "reference_label": "Invoice #",
    "secondary_ref_label": "PO #",
}


def get_session():
    """Return the active Snowpark session."""
    return _init_session()


def get_demo_config(session) -> dict:
    """Read optional DEMO_CONFIG table for customer-facing demo settings.

    Returns dict with keys: customer_name, demo_mode, hide_credits.
    Falls back to safe defaults if table doesn't exist.
    """
    defaults = {"customer_name": None, "demo_mode": False, "hide_credits": False}
    try:
        rows = session.sql(
            f"SELECT * FROM {DB}.DEMO_CONFIG LIMIT 1"
        ).collect()
        if rows:
            row = rows[0]
            return {
                "customer_name": _safe_get(row, "CUSTOMER_NAME"),
                "demo_mode": bool(_safe_get(row, "DEMO_MODE", False)),
                "hide_credits": bool(_safe_get(row, "HIDE_CREDITS", False)),
            }
    except Exception:
        pass
    return defaults


def get_doc_type_labels(session, doc_type: str = "INVOICE") -> dict:
    """Fetch UI labels for a document type from DOCUMENT_TYPE_CONFIG.

    Returns a dict mapping field keys to display labels.
    Falls back to _DEFAULT_LABELS if the config table or row is missing.
    """
    try:
        rows = session.sql(
            f"SELECT field_labels FROM {DB}.DOCUMENT_TYPE_CONFIG "
            f"WHERE doc_type = ?",
            params=[doc_type]
        ).collect()
        if rows:
            raw = rows[0]["FIELD_LABELS"]
            return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        pass
    return _DEFAULT_LABELS.copy()


def get_doc_types(session) -> list[str]:
    """Return list of available document types from config table."""
    try:
        rows = session.sql(
            f"SELECT doc_type FROM {DB}.DOCUMENT_TYPE_CONFIG "
            f"WHERE active = TRUE ORDER BY doc_type"
        ).collect()
        return [r["DOC_TYPE"] for r in rows]
    except Exception:
        return ["INVOICE"]


def get_doc_type_config(session, doc_type: str) -> dict | None:
    """Fetch full configuration for a document type.

    Returns a dict with keys: doc_type, display_name, extraction_prompt,
    field_labels, table_extraction_schema, review_fields, active.
    Returns None if not found.
    """
    try:
        rows = session.sql(
            f"SELECT * FROM {DB}.DOCUMENT_TYPE_CONFIG "
            f"WHERE doc_type = ?",
            params=[doc_type]
        ).collect()
        if not rows:
            return None
        row = rows[0]
        result = {
            "doc_type": row["DOC_TYPE"],
            "display_name": row["DISPLAY_NAME"],
            "extraction_prompt": row["EXTRACTION_PROMPT"],
            "field_labels": _parse_variant(_safe_get(row, "FIELD_LABELS")),
            "table_extraction_schema": _parse_variant(_safe_get(row, "TABLE_EXTRACTION_SCHEMA")),
            "review_fields": _parse_variant(_safe_get(row, "REVIEW_FIELDS")),
            "validation_rules": _parse_variant(_safe_get(row, "VALIDATION_RULES")),
            "active": _safe_get(row, "ACTIVE", True),
        }
        return result
    except Exception:
        return None


def get_all_doc_type_configs(session) -> list[dict]:
    """Fetch all document type configurations."""
    try:
        rows = session.sql(
            f"SELECT * FROM {DB}.DOCUMENT_TYPE_CONFIG ORDER BY doc_type"
        ).collect()
        configs = []
        for row in rows:
            configs.append({
                "doc_type": row["DOC_TYPE"],
                "display_name": row["DISPLAY_NAME"],
                "extraction_prompt": row["EXTRACTION_PROMPT"],
                "field_labels": _parse_variant(_safe_get(row, "FIELD_LABELS")),
                "table_extraction_schema": _parse_variant(_safe_get(row, "TABLE_EXTRACTION_SCHEMA")),
                "review_fields": _parse_variant(_safe_get(row, "REVIEW_FIELDS")),
                "validation_rules": _parse_variant(_safe_get(row, "VALIDATION_RULES")),
                "active": _safe_get(row, "ACTIVE", True),
            })
        return configs
    except Exception:
        return []


def get_field_names_from_labels(labels: dict) -> list[str]:
    """Extract ordered field names from a labels dict.

    Returns field keys like ['field_1', 'field_2', ...] sorted by number,
    excluding non-field keys like 'sender_label', 'amount_label', etc.
    """
    field_keys = [k for k in labels if k.startswith("field_")]
    field_keys.sort(key=lambda k: int(k.split("_")[1]))
    return field_keys


def get_raw_extraction_fields(session, record_id: int) -> dict:
    """Fetch raw_extraction VARIANT for a specific record.

    Returns the parsed JSON dict, or empty dict if not available.
    """
    try:
        rows = session.sql(
            f"SELECT raw_extraction FROM {DB}.EXTRACTED_FIELDS "
            f"WHERE record_id = ?",
            params=[record_id]
        ).collect()
        if rows and rows[0]["RAW_EXTRACTION"]:
            return _parse_variant(rows[0]["RAW_EXTRACTION"])
    except Exception:
        pass
    return {}


def get_all_field_values(row_dict: dict, labels: dict) -> dict:
    """Get all field values for a row, merging fixed columns with raw_extraction overflow.

    For fields 1-10, reads from field_1..field_10 columns.
    For fields 11+, reads from raw_extraction VARIANT.
    Returns a dict mapping field_key (e.g. 'field_1') to its value.
    """
    field_keys = get_field_names_from_labels(labels)
    values = {}

    # Parse raw_extraction for overflow fields
    raw = _parse_variant(row_dict.get("RAW_EXTRACTION")) or {}

    # Also need the field-name-to-key mapping from labels
    # labels: {"field_1": "Vendor Name", "field_2": "Invoice Number", ...}
    # We need the reverse: find the extraction field name for each field_key
    # The extraction field names are stored in review_fields.correctable or derived from labels
    # For now, we get them from raw_extraction keys

    for fk in field_keys:
        idx = int(fk.split("_")[1])
        if idx <= 10:
            # Read from fixed column
            values[fk] = row_dict.get(fk.upper(), row_dict.get(fk))
        else:
            # Read from raw_extraction using the extraction field name
            # The label gives us the display name; the extraction field name is typically
            # the snake_case version. We look it up from raw_extraction.
            label = labels.get(fk, "")
            # Try snake_case of label
            ext_name = label.lower().replace(" ", "_")
            val = raw.get(ext_name)
            if val is None and raw:
                # Fallback: try matching by position in raw keys
                # raw_extraction stores all fields by their extraction name;
                # pick the (idx-1)-th key if it exists.
                raw_keys = list(raw.keys())
                pos = idx - 1
                if pos < len(raw_keys):
                    val = raw[raw_keys[pos]]
            values[fk] = val

    return values


def get_field_name_for_key(labels: dict, review_fields: dict | None, field_key: str) -> str | None:
    """Get the extraction field name (e.g. 'vendor_name') for a field key (e.g. 'field_1').

    Uses review_fields.correctable list ordering to map field_key -> extraction name.
    Falls back to snake_case of the label.
    """
    idx = int(field_key.split("_")[1]) - 1  # 0-based
    if review_fields and "correctable" in review_fields:
        correctable = review_fields["correctable"]
        if idx < len(correctable):
            return correctable[idx]
    # Fallback: snake_case of label
    label = labels.get(field_key, "")
    return label.lower().replace(" ", "_") if label else None


def _safe_get(row, key, default=None):
    """Safely get a value from a Snowpark Row (which doesn't support .get())."""
    try:
        return row[key]
    except (KeyError, IndexError):
        return default


def _parse_variant(val) -> dict | None:
    """Parse a Snowflake VARIANT value to a Python dict."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return None
    if isinstance(val, dict):
        return val
    return None
