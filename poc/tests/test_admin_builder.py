"""Unit tests for _build_config_from_fields() from 4_Admin.py.

Pure Python tests — no Snowflake connection needed.
We import the function directly from the Admin page module.
"""

import importlib.util
import os
import sys

import pytest

# ---------------------------------------------------------------------------
# Import _build_config_from_fields without triggering Streamlit page init
# We extract just the function via importlib to avoid st.set_page_config side effects.
# ---------------------------------------------------------------------------
_ADMIN_PATH = os.path.join(
    os.path.dirname(__file__), "..", "streamlit", "pages", "4_Admin.py"
)


def _load_build_config():
    """Extract _build_config_from_fields and constants from 4_Admin.py source."""
    with open(_ADMIN_PATH) as f:
        source = f.read()

    # Extract only the function and the required constants
    import ast
    tree = ast.parse(source)

    needed_names = {
        "_build_config_from_fields",
        "FIELD_TYPE_OPTIONS",
        "TYPE_MAP",
        "REVERSE_TYPE_MAP",
    }
    segments = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name in needed_names:
            segments.append(ast.get_source_segment(source, node))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in needed_names:
                    segments.append(ast.get_source_segment(source, node))

    code = "\n\n".join(segments)
    ns = {}
    exec(compile(code, "<admin_builder>", "exec"), ns)
    return ns["_build_config_from_fields"]


_build_config_from_fields = _load_build_config()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_field(name, label=None, ftype="Text", correctable=True):
    return {
        "name": name,
        "label": label or name.replace("_", " ").title(),
        "type": ftype,
        "correctable": correctable,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestPromptGeneration:
    """Test that extraction_prompt is correctly built."""

    def test_prompt_contains_all_field_names(self):
        fields = [_make_field("vendor_name"), _make_field("invoice_number")]
        prompt, _, _, _ = _build_config_from_fields("INVOICE", "Invoice", fields)
        assert "vendor_name" in prompt
        assert "invoice_number" in prompt

    def test_prompt_mentions_display_name(self):
        fields = [_make_field("vendor_name")]
        prompt, _, _, _ = _build_config_from_fields("UTILITY_BILL", "Utility Bill", fields)
        assert "utility bill" in prompt.lower()

    def test_prompt_contains_formatting_rules(self):
        fields = [_make_field("total", ftype="Number")]
        prompt, _, _, _ = _build_config_from_fields("INV", "Invoice", fields)
        assert "YYYY-MM-DD" in prompt
        assert "currency symbols" in prompt.lower() or "1234.56" in prompt


class TestFieldLabels:
    """Test that field_labels dict is correctly built."""

    def test_field_labels_sequential_keys(self):
        fields = [_make_field("a"), _make_field("b"), _make_field("c")]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        assert labels["field_1"] == "A"
        assert labels["field_2"] == "B"
        assert labels["field_3"] == "C"

    def test_custom_labels_used(self):
        fields = [_make_field("vendor_name", label="Vendor / Sender")]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        assert labels["field_1"] == "Vendor / Sender"

    def test_sender_label_is_first_field(self):
        fields = [_make_field("vendor_name", label="Vendor")]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        assert labels["sender_label"] == "Vendor"

    def test_amount_label_is_last_number_field(self):
        fields = [
            _make_field("subtotal", ftype="Number"),
            _make_field("total", label="Total Amount", ftype="Number"),
        ]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        # amount_label should be the LAST Number field (total)
        assert labels["amount_label"] == "Total Amount"

    def test_date_label_is_last_date_field(self):
        """date_label iterates all fields, keeping the last Date match."""
        fields = [
            _make_field("vendor_name"),
            _make_field("invoice_date", ftype="Date"),
            _make_field("due_date", ftype="Date"),
        ]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        assert labels["date_label"] == "Due Date"

    def test_reference_label_from_text_fields(self):
        fields = [
            _make_field("vendor_name"),  # text[0] → sender_label
            _make_field("invoice_number"),  # text[1] → reference_label
            _make_field("po_number"),  # text[2] → secondary_ref_label
        ]
        _, labels, _, _ = _build_config_from_fields("T", "Test", fields)
        assert labels["reference_label"] == "Invoice Number"
        assert labels["secondary_ref_label"] == "Po Number"


class TestReviewFields:
    """Test that review_fields dict is correctly built."""

    def test_correctable_list(self):
        fields = [
            _make_field("a", correctable=True),
            _make_field("b", correctable=False),
            _make_field("c", correctable=True),
        ]
        _, _, review, _ = _build_config_from_fields("T", "Test", fields)
        assert review["correctable"] == ["a", "c"]

    def test_types_mapping(self):
        fields = [
            _make_field("vendor", ftype="Text"),
            _make_field("amount", ftype="Number"),
            _make_field("date", ftype="Date"),
        ]
        _, _, review, _ = _build_config_from_fields("T", "Test", fields)
        assert review["types"]["vendor"] == "VARCHAR"
        assert review["types"]["amount"] == "NUMBER"
        assert review["types"]["date"] == "DATE"

    def test_all_correctable_by_default(self):
        fields = [_make_field("a"), _make_field("b")]
        _, _, review, _ = _build_config_from_fields("T", "Test", fields)
        assert review["correctable"] == ["a", "b"]


class TestTableSchema:
    """Test table_extraction_schema generation."""

    def test_no_table_columns_returns_none(self):
        fields = [_make_field("a")]
        _, _, _, schema = _build_config_from_fields("T", "Test", fields)
        assert schema is None

    def test_empty_table_columns_returns_none(self):
        fields = [_make_field("a")]
        _, _, _, schema = _build_config_from_fields("T", "Test", fields, table_columns=[])
        # Empty list is falsy, so should be None
        assert schema is None

    def test_table_columns_builds_schema(self):
        fields = [_make_field("a")]
        table_cols = [
            {"name": "Description", "description": "Product name"},
            {"name": "Qty", "description": "Quantity ordered"},
        ]
        _, _, _, schema = _build_config_from_fields("T", "Test", fields, table_columns=table_cols)
        assert schema is not None
        assert schema["columns"] == ["Description", "Qty"]
        assert schema["descriptions"] == ["Product name", "Quantity ordered"]

    def test_table_column_no_description_uses_name(self):
        fields = [_make_field("a")]
        table_cols = [{"name": "Amount"}]
        _, _, _, schema = _build_config_from_fields("T", "Test", fields, table_columns=table_cols)
        assert schema["descriptions"] == ["Amount"]
