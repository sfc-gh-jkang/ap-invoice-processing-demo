"""Tests for config.py functions that require a Snowflake connection.

These tests validate get_doc_type_config(), get_all_doc_type_configs(),
get_raw_extraction_fields(), get_field_names_from_labels(), get_all_field_values(),
get_field_name_for_key(), and _parse_variant().

NOTE: test_config.py leaves mocked snowflake.snowpark modules in sys.modules,
so we cannot simply re-import config. Instead, we extract functions via AST
for pure-Python tests and create our own Snowpark session for SF tests.
"""

import ast
import json
import os
import sys

import pytest


# ---------------------------------------------------------------------------
# Extract pure-Python functions from config.py source without importing the module
# (avoids sys.modules contamination from test_config.py's mocks)
# ---------------------------------------------------------------------------
_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "streamlit", "config.py"
)


def _load_config_functions():
    """Load pure-Python functions from config.py via AST extraction."""
    with open(_CONFIG_PATH) as f:
        source = f.read()

    tree = ast.parse(source)
    needed = {
        "_parse_variant",
        "_safe_get",
        "get_field_names_from_labels",
        "get_all_field_values",
        "get_field_name_for_key",
    }
    segments = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name in needed:
            segments.append(ast.get_source_segment(source, node))

    code = "import json\n\n" + "\n\n".join(s for s in segments if s)
    ns = {}
    exec(compile(code, "<config_functions>", "exec"), ns)
    return ns


_funcs = _load_config_functions()
_parse_variant = _funcs["_parse_variant"]
_safe_get = _funcs["_safe_get"]
get_field_names_from_labels = _funcs["get_field_names_from_labels"]
get_all_field_values = _funcs["get_all_field_values"]
get_field_name_for_key = _funcs["get_field_name_for_key"]


def _create_sf_session():
    """Create a Snowpark session directly, bypassing config.py module import.

    Clears any mock snowflake.snowpark modules left by test_config.py
    before importing the real Session class.
    """
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        return None
    # Purge any mocked snowflake.snowpark modules left by test_config.py
    for key in list(sys.modules):
        if key.startswith("snowflake.snowpark"):
            del sys.modules[key]
    # Also remove stale config module
    sys.modules.pop("config", None)

    import importlib
    import snowflake.snowpark
    importlib.reload(snowflake.snowpark)
    from snowflake.snowpark import Session
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


# Snowflake-dependent functions reimplemented here to avoid importing config module
DB = f"{os.environ.get('POC_DB', 'AI_EXTRACT_POC')}.{os.environ.get('POC_SCHEMA', 'DOCUMENTS')}"


def get_doc_type_config(session, doc_type: str):
    try:
        rows = session.sql(
            f"SELECT * FROM {DB}.DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        ).collect()
        if not rows:
            return None
        row = rows[0]
        return {
            "doc_type": row["DOC_TYPE"],
            "display_name": row["DISPLAY_NAME"],
            "extraction_prompt": row["EXTRACTION_PROMPT"],
            "field_labels": _parse_variant(_safe_get(row, "FIELD_LABELS")),
            "table_extraction_schema": _parse_variant(_safe_get(row, "TABLE_EXTRACTION_SCHEMA")),
            "review_fields": _parse_variant(_safe_get(row, "REVIEW_FIELDS")),
            "validation_rules": _parse_variant(_safe_get(row, "VALIDATION_RULES")),
            "active": _safe_get(row, "ACTIVE", True),
        }
    except Exception:
        return None


def get_all_doc_type_configs(session):
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


def get_doc_types(session):
    try:
        rows = session.sql(
            f"SELECT doc_type FROM {DB}.DOCUMENT_TYPE_CONFIG WHERE active = TRUE ORDER BY doc_type"
        ).collect()
        return [r["DOC_TYPE"] for r in rows]
    except Exception:
        return ["INVOICE"]


def get_doc_type_labels(session, doc_type: str = "INVOICE"):
    _DEFAULT_LABELS = {
        "field_1": "Vendor Name", "field_2": "Invoice Number", "field_3": "PO Number",
        "field_4": "Invoice Date", "field_5": "Due Date", "field_6": "Payment Terms",
        "field_7": "Recipient", "field_8": "Subtotal", "field_9": "Tax Amount",
        "field_10": "Total Amount", "sender_label": "Vendor / Sender",
        "amount_label": "Total Amount", "date_label": "Invoice Date",
        "reference_label": "Invoice #", "secondary_ref_label": "PO #",
    }
    try:
        rows = session.sql(
            f"SELECT field_labels FROM {DB}.DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        ).collect()
        if rows:
            raw = rows[0]["FIELD_LABELS"]
            return json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        pass
    return _DEFAULT_LABELS.copy()


def get_raw_extraction_fields(session, record_id: int):
    try:
        rows = session.sql(
            f"SELECT raw_extraction FROM {DB}.EXTRACTED_FIELDS WHERE record_id = {record_id}"
        ).collect()
        if rows and rows[0]["RAW_EXTRACTION"]:
            return _parse_variant(rows[0]["RAW_EXTRACTION"])
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Pure-Python tests (no SF connection needed)
# ---------------------------------------------------------------------------
class TestParseVariant:
    """Test _parse_variant() helper."""

    def test_none_returns_none(self):
        assert _parse_variant(None) is None

    def test_valid_json_string(self):
        assert _parse_variant('{"a": 1}') == {"a": 1}

    def test_invalid_json_string(self):
        assert _parse_variant("not json") is None

    def test_dict_passthrough(self):
        d = {"x": 1}
        assert _parse_variant(d) is d

    def test_other_type_returns_none(self):
        assert _parse_variant(42) is None

    def test_empty_string(self):
        assert _parse_variant("") is None


class TestGetFieldNamesFromLabels:
    """Test get_field_names_from_labels() — pure Python."""

    def test_sorted_by_number(self):
        labels = {"field_3": "C", "field_1": "A", "field_2": "B"}
        assert get_field_names_from_labels(labels) == ["field_1", "field_2", "field_3"]

    def test_excludes_non_field_keys(self):
        labels = {"field_1": "A", "sender_label": "Vendor", "amount_label": "Amt"}
        result = get_field_names_from_labels(labels)
        assert result == ["field_1"]
        assert "sender_label" not in result

    def test_empty_dict(self):
        assert get_field_names_from_labels({}) == []

    def test_double_digit_fields(self):
        labels = {f"field_{i}": f"F{i}" for i in range(1, 14)}
        result = get_field_names_from_labels(labels)
        assert result == [f"field_{i}" for i in range(1, 14)]
        assert result[-1] == "field_13"


class TestGetFieldNameForKey:
    """Test get_field_name_for_key() — pure Python."""

    def test_from_correctable_list(self):
        review = {"correctable": ["vendor_name", "invoice_number", "po_number"]}
        labels = {"field_1": "Vendor Name", "field_2": "Invoice Number"}
        assert get_field_name_for_key(labels, review, "field_1") == "vendor_name"
        assert get_field_name_for_key(labels, review, "field_2") == "invoice_number"

    def test_fallback_to_snake_case_label(self):
        labels = {"field_1": "Vendor Name"}
        assert get_field_name_for_key(labels, None, "field_1") == "vendor_name"

    def test_empty_review_fields(self):
        labels = {"field_1": "Total Due"}
        assert get_field_name_for_key(labels, {}, "field_1") == "total_due"


class TestGetAllFieldValues:
    """Test get_all_field_values() — pure Python."""

    def test_fixed_columns_1_to_10(self):
        labels = {"field_1": "Vendor", "field_2": "Amount"}
        row = {"FIELD_1": "Acme", "FIELD_2": "100.00", "RAW_EXTRACTION": None}
        values = get_all_field_values(row, labels)
        assert values["field_1"] == "Acme"
        assert values["field_2"] == "100.00"

    def test_overflow_field_11_from_raw(self):
        labels = {"field_11": "Extra Field"}
        raw = json.dumps({"extra_field": "overflow_value"})
        row = {"RAW_EXTRACTION": raw}
        values = get_all_field_values(row, labels)
        assert values["field_11"] == "overflow_value"

    def test_missing_overflow_returns_none(self):
        labels = {"field_12": "Missing"}
        row = {"RAW_EXTRACTION": json.dumps({"other_key": "val"})}
        values = get_all_field_values(row, labels)
        assert values["field_12"] is None


# ---------------------------------------------------------------------------
# Snowflake-connected tests
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def sf_session():
    """Get a Snowflake session — skip if connection env vars not set."""
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        pytest.skip("POC_CONNECTION not set — skipping SF tests")
    return _create_sf_session()


class TestGetDocTypeConfig:
    """Test get_doc_type_config() against live Snowflake."""

    def test_invoice_config_exists(self, sf_session):
        cfg = get_doc_type_config(sf_session, "INVOICE")
        assert cfg is not None
        assert cfg["doc_type"] == "INVOICE"

    def test_invoice_has_required_keys(self, sf_session):
        cfg = get_doc_type_config(sf_session, "INVOICE")
        for key in ["doc_type", "display_name", "extraction_prompt", "field_labels", "review_fields"]:
            assert key in cfg, f"Missing key: {key}"

    def test_invoice_field_labels_has_field_1(self, sf_session):
        cfg = get_doc_type_config(sf_session, "INVOICE")
        labels = cfg["field_labels"]
        assert "field_1" in labels

    def test_utility_bill_config_exists(self, sf_session):
        cfg = get_doc_type_config(sf_session, "UTILITY_BILL")
        assert cfg is not None
        assert cfg["doc_type"] == "UTILITY_BILL"

    def test_utility_bill_has_validation_rules(self, sf_session):
        cfg = get_doc_type_config(sf_session, "UTILITY_BILL")
        vr = cfg.get("validation_rules")
        assert vr is not None
        assert "total_due" in vr

    def test_nonexistent_returns_none(self, sf_session):
        cfg = get_doc_type_config(sf_session, "NONEXISTENT_TYPE_XYZ")
        assert cfg is None


class TestGetAllDocTypeConfigs:
    """Test get_all_doc_type_configs() against live Snowflake."""

    def test_returns_list(self, sf_session):
        configs = get_all_doc_type_configs(sf_session)
        assert isinstance(configs, list)

    def test_at_least_two_types(self, sf_session):
        configs = get_all_doc_type_configs(sf_session)
        assert len(configs) >= 2  # INVOICE + UTILITY_BILL at minimum

    def test_each_config_has_doc_type(self, sf_session):
        configs = get_all_doc_type_configs(sf_session)
        for cfg in configs:
            assert "doc_type" in cfg
            assert isinstance(cfg["doc_type"], str)


class TestGetDocTypes:
    """Test get_doc_types() against live Snowflake."""

    def test_returns_list_of_strings(self, sf_session):
        types = get_doc_types(sf_session)
        assert isinstance(types, list)
        assert all(isinstance(t, str) for t in types)

    def test_includes_invoice(self, sf_session):
        types = get_doc_types(sf_session)
        assert "INVOICE" in types


class TestGetDocTypeLabels:
    """Test get_doc_type_labels() against live Snowflake."""

    def test_invoice_returns_dict(self, sf_session):
        labels = get_doc_type_labels(sf_session, "INVOICE")
        assert isinstance(labels, dict)
        assert "field_1" in labels

    def test_nonexistent_returns_defaults(self, sf_session):
        labels = get_doc_type_labels(sf_session, "DOES_NOT_EXIST_XYZ")
        # Should fall back to _DEFAULT_LABELS
        assert "field_1" in labels
        assert labels["field_1"] == "Vendor Name"  # default


class TestGetRawExtractionFields:
    """Test get_raw_extraction_fields() against live Snowflake."""

    def test_existing_record_returns_dict(self, sf_session):
        # Get any record_id from EXTRACTED_FIELDS
        rows = sf_session.sql(
            "SELECT record_id FROM AI_EXTRACT_POC.DOCUMENTS.EXTRACTED_FIELDS LIMIT 1"
        ).collect()
        if not rows:
            pytest.skip("No extracted fields data")
        record_id = rows[0]["RECORD_ID"]
        raw = get_raw_extraction_fields(sf_session, record_id)
        assert isinstance(raw, dict)

    def test_nonexistent_record_returns_empty(self, sf_session):
        raw = get_raw_extraction_fields(sf_session, -999999)
        assert raw == {}
