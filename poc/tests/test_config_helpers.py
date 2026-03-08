"""Unit tests for config.py helper functions.

These tests exercise pure Python logic (no Snowflake connection needed):
- get_field_names_from_labels
- get_all_field_values
- get_field_name_for_key
- _parse_variant
"""
import json
import pytest
import sys
import os

# Add streamlit dir to path so we can import config helpers directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))

# We can't import the full config module (it tries to connect to Snowflake),
# so we extract the pure functions we need to test.
import importlib.util

_config_path = os.path.join(
    os.path.dirname(__file__), "..", "streamlit", "config.py"
)


def _load_pure_functions():
    """Load only the pure helper functions from config.py without triggering
    the module-level Snowflake session initialization."""
    import ast
    import textwrap

    with open(_config_path) as f:
        source = f.read()

    # Parse and extract only the functions we need
    tree = ast.parse(source)
    func_names = {
        "_parse_variant",
        "_safe_get",
        "get_field_names_from_labels",
        "get_all_field_values",
        "get_field_name_for_key",
    }

    func_sources = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in func_names:
            func_sources.append(ast.get_source_segment(source, node))

    # Build a mini-module with just those functions
    code = "import json\n\n" + "\n\n".join(func_sources)
    ns = {}
    exec(compile(code, "<config_helpers>", "exec"), ns)
    return ns


_funcs = _load_pure_functions()
_parse_variant = _funcs["_parse_variant"]
_safe_get = _funcs["_safe_get"]
get_field_names_from_labels = _funcs["get_field_names_from_labels"]
get_all_field_values = _funcs["get_all_field_values"]
get_field_name_for_key = _funcs["get_field_name_for_key"]


# ---------------------------------------------------------------------------
# _parse_variant
# ---------------------------------------------------------------------------
class TestParseVariant:
    def test_none_returns_none(self):
        assert _parse_variant(None) is None

    def test_dict_passes_through(self):
        d = {"a": 1}
        assert _parse_variant(d) == {"a": 1}

    def test_json_string_parsed(self):
        assert _parse_variant('{"x": 42}') == {"x": 42}

    def test_invalid_json_returns_none(self):
        assert _parse_variant("not json") is None

    def test_empty_string_returns_none(self):
        assert _parse_variant("") is None

    def test_numeric_returns_none(self):
        assert _parse_variant(123) is None


# ---------------------------------------------------------------------------
# get_field_names_from_labels
# ---------------------------------------------------------------------------
class TestGetFieldNamesFromLabels:
    def test_basic_ordering(self):
        labels = {
            "field_3": "C",
            "field_1": "A",
            "field_2": "B",
            "sender_label": "Sender",
        }
        result = get_field_names_from_labels(labels)
        assert result == ["field_1", "field_2", "field_3"]

    def test_excludes_non_field_keys(self):
        labels = {
            "field_1": "Name",
            "sender_label": "Sender",
            "amount_label": "Amount",
            "reference_label": "Ref",
        }
        result = get_field_names_from_labels(labels)
        assert result == ["field_1"]

    def test_empty_labels(self):
        assert get_field_names_from_labels({}) == []

    def test_many_fields_sorted(self):
        labels = {f"field_{i}": f"F{i}" for i in range(15, 0, -1)}
        result = get_field_names_from_labels(labels)
        assert result == [f"field_{i}" for i in range(1, 16)]


# ---------------------------------------------------------------------------
# get_all_field_values
# ---------------------------------------------------------------------------
class TestGetAllFieldValues:
    def test_first_10_from_columns(self):
        labels = {f"field_{i}": f"F{i}" for i in range(1, 4)}
        row = {"FIELD_1": "val1", "FIELD_2": "val2", "FIELD_3": "val3"}
        result = get_all_field_values(row, labels)
        assert result["field_1"] == "val1"
        assert result["field_2"] == "val2"
        assert result["field_3"] == "val3"

    def test_overflow_from_raw_extraction(self):
        labels = {
            "field_1": "Name",
            "field_11": "Extra Field",
        }
        raw = json.dumps({"extra_field": "overflow_val"})
        row = {"FIELD_1": "val1", "RAW_EXTRACTION": raw}
        result = get_all_field_values(row, labels)
        assert result["field_1"] == "val1"
        assert result["field_11"] == "overflow_val"

    def test_missing_raw_extraction(self):
        labels = {"field_11": "Extra"}
        row = {}
        result = get_all_field_values(row, labels)
        assert result["field_11"] is None

    def test_case_insensitive_column_lookup(self):
        """Row might have lowercase or uppercase keys."""
        labels = {"field_1": "Name"}
        row = {"field_1": "lower_val"}
        result = get_all_field_values(row, labels)
        assert result["field_1"] == "lower_val"


# ---------------------------------------------------------------------------
# get_field_name_for_key
# ---------------------------------------------------------------------------
class TestGetFieldNameForKey:
    def test_uses_correctable_list(self):
        labels = {"field_1": "Vendor Name", "field_2": "Invoice Number"}
        review = {"correctable": ["vendor_name", "invoice_number"]}
        assert get_field_name_for_key(labels, review, "field_1") == "vendor_name"
        assert get_field_name_for_key(labels, review, "field_2") == "invoice_number"

    def test_fallback_to_snake_case(self):
        labels = {"field_1": "Vendor Name"}
        result = get_field_name_for_key(labels, None, "field_1")
        assert result == "vendor_name"

    def test_fallback_when_correctable_too_short(self):
        labels = {"field_1": "A", "field_3": "Third Field"}
        review = {"correctable": ["a"]}
        # field_3 index is 2, but correctable only has 1 element
        result = get_field_name_for_key(labels, review, "field_3")
        assert result == "third_field"

    def test_empty_label_returns_none(self):
        result = get_field_name_for_key({}, None, "field_99")
        assert result is None

    def test_no_review_fields(self):
        labels = {"field_1": "Due Date"}
        result = get_field_name_for_key(labels, {}, "field_1")
        assert result == "due_date"

    def test_correctable_index_out_of_range(self):
        """If field_key index exceeds correctable length, fall back to snake_case."""
        labels = {"field_5": "Account Number"}
        review = {"correctable": ["vendor_name", "invoice_number"]}
        result = get_field_name_for_key(labels, review, "field_5")
        assert result == "account_number"

    def test_multi_word_label_snake_case(self):
        labels = {"field_1": "Total Amount Due"}
        result = get_field_name_for_key(labels, None, "field_1")
        assert result == "total_amount_due"


# ---------------------------------------------------------------------------
# _safe_get
# ---------------------------------------------------------------------------
class TestSafeGet:
    def test_dict_key_exists(self):
        d = {"A": 1, "B": 2}
        assert _safe_get(d, "A") == 1

    def test_dict_key_missing_returns_default(self):
        d = {"A": 1}
        assert _safe_get(d, "MISSING") is None

    def test_dict_key_missing_custom_default(self):
        d = {"A": 1}
        assert _safe_get(d, "MISSING", "fallback") == "fallback"

    def test_list_index_exists(self):
        lst = ["a", "b", "c"]
        assert _safe_get(lst, 0) == "a"

    def test_list_index_out_of_range(self):
        lst = ["a"]
        assert _safe_get(lst, 5) is None

    def test_list_index_out_of_range_custom_default(self):
        lst = ["a"]
        assert _safe_get(lst, 5, "default") == "default"

    def test_none_value_returned_not_default(self):
        """If the key exists but value is None, return None (not the default)."""
        d = {"A": None}
        assert _safe_get(d, "A", "fallback") is None

    def test_false_value_returned_not_default(self):
        """Boolean False should be returned, not confused with missing."""
        d = {"ACTIVE": False}
        assert _safe_get(d, "ACTIVE", True) is False

    def test_zero_value_returned_not_default(self):
        """Zero should be returned, not confused with missing."""
        d = {"COUNT": 0}
        assert _safe_get(d, "COUNT", 99) == 0


# ---------------------------------------------------------------------------
# _parse_variant — additional edge cases
# ---------------------------------------------------------------------------
class TestParseVariantEdgeCases:
    def test_nested_json(self):
        val = '{"rules": {"total_due": {"required": true}}}'
        result = _parse_variant(val)
        assert result["rules"]["total_due"]["required"] is True

    def test_json_array_string(self):
        """JSON arrays are valid JSON — _parse_variant returns the parsed list via json.loads."""
        result = _parse_variant('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_boolean_input(self):
        assert _parse_variant(True) is None

    def test_list_input(self):
        assert _parse_variant([1, 2]) is None

    def test_whitespace_string(self):
        assert _parse_variant("   ") is None


# ---------------------------------------------------------------------------
# get_all_field_values — additional edge cases
# ---------------------------------------------------------------------------
class TestGetAllFieldValuesEdgeCases:
    def test_null_raw_extraction_with_overflow_fields(self):
        """Overflow fields should be None when RAW_EXTRACTION is NULL."""
        labels = {"field_1": "Name", "field_11": "Extra"}
        row = {"FIELD_1": "Acme", "RAW_EXTRACTION": None}
        result = get_all_field_values(row, labels)
        assert result["field_1"] == "Acme"
        assert result["field_11"] is None

    def test_mixed_fixed_and_overflow(self):
        """Fields 1-10 from columns, 11+ from raw_extraction."""
        labels = {
            "field_1": "Name",
            "field_10": "Total",
            "field_11": "Extra One",
            "field_12": "Extra Two",
        }
        raw = json.dumps({"extra_one": "v11", "extra_two": "v12"})
        row = {"FIELD_1": "Acme", "FIELD_10": "500.00", "RAW_EXTRACTION": raw}
        result = get_all_field_values(row, labels)
        assert result["field_1"] == "Acme"
        assert result["field_10"] == "500.00"
        assert result["field_11"] == "v11"
        assert result["field_12"] == "v12"

    def test_empty_labels_returns_empty(self):
        row = {"FIELD_1": "val", "RAW_EXTRACTION": None}
        assert get_all_field_values(row, {}) == {}

    def test_raw_extraction_as_dict(self):
        """RAW_EXTRACTION might already be a dict (Snowflake VARIANT parsed)."""
        labels = {"field_11": "Notes"}
        row = {"RAW_EXTRACTION": {"notes": "some text"}}
        result = get_all_field_values(row, labels)
        assert result["field_11"] == "some text"
