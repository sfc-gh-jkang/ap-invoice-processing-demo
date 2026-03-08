"""Unit tests for the _apply_validation_rules() function inside SP_EXTRACT_BY_DOC_TYPE.

Pure Python tests — no Snowflake connection needed.
Extracts _apply_validation_rules() from 06_automate.sql via AST.
"""

import ast
import os

import pytest


# ---------------------------------------------------------------------------
# Extract _apply_validation_rules from the SQL file
# ---------------------------------------------------------------------------
_SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "sql", "06_automate.sql"
)


def _load_functions():
    """Extract _apply_validation_rules() and _normalize() from the SP body."""
    with open(_SQL_PATH) as f:
        content = f.read()

    sp_start = content.find("CREATE OR REPLACE PROCEDURE SP_EXTRACT_BY_DOC_TYPE")
    first_dd = content.find("$$", sp_start)
    second_dd = content.find("$$", first_dd + 2)
    python_body = content[first_dd + 2 : second_dd]

    tree = ast.parse(python_body)
    func_names = {"_apply_validation_rules", "_normalize"}
    func_sources = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in func_names:
            func_sources.append(ast.get_source_segment(python_body, node))

    code = "import re\nfrom datetime import datetime\n\n" + "\n\n".join(func_sources)
    ns = {}
    exec(compile(code, "<validation_rules>", "exec"), ns)
    return ns["_apply_validation_rules"]


_apply_validation_rules = _load_functions()


# ---------------------------------------------------------------------------
# Empty / None rules
# ---------------------------------------------------------------------------
class TestEmptyRules:
    def test_none_rules_returns_empty(self):
        assert _apply_validation_rules({"x": "1"}, {"x": "NUMBER"}, None) == []

    def test_empty_dict_returns_empty(self):
        assert _apply_validation_rules({"x": "1"}, {"x": "NUMBER"}, {}) == []

    def test_non_dict_rule_skipped(self):
        assert _apply_validation_rules({"x": "1"}, {"x": "NUMBER"}, {"x": "bad"}) == []


# ---------------------------------------------------------------------------
# Required rule
# ---------------------------------------------------------------------------
class TestRequiredRule:
    def test_required_present_passes(self):
        warnings = _apply_validation_rules(
            {"name": "Acme Corp"},
            {"name": "VARCHAR"},
            {"name": {"required": True}},
        )
        assert len(warnings) == 0

    def test_required_none_fails(self):
        warnings = _apply_validation_rules(
            {"name": None},
            {"name": "VARCHAR"},
            {"name": {"required": True}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "required"

    def test_required_empty_string_fails(self):
        warnings = _apply_validation_rules(
            {"name": ""},
            {"name": "VARCHAR"},
            {"name": {"required": True}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "required"

    def test_required_zero_number_passes(self):
        """Zero is a valid number; should not fail required."""
        warnings = _apply_validation_rules(
            {"amount": "0"},
            {"amount": "NUMBER"},
            {"amount": {"required": True}},
        )
        # '0' with NUMBER type should NOT trigger required failure
        assert all(w["rule"] != "required" for w in warnings)

    def test_not_required_none_passes(self):
        """When required is False, None should not trigger a warning."""
        warnings = _apply_validation_rules(
            {"name": None},
            {"name": "VARCHAR"},
            {"name": {"required": False}},
        )
        assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Numeric range rules
# ---------------------------------------------------------------------------
class TestNumericRangeRules:
    def test_within_range_passes(self):
        warnings = _apply_validation_rules(
            {"amount": "50.0"},
            {"amount": "NUMBER"},
            {"amount": {"min": 0, "max": 100}},
        )
        assert len(warnings) == 0

    def test_below_min_fails(self):
        warnings = _apply_validation_rules(
            {"amount": "-5"},
            {"amount": "NUMBER"},
            {"amount": {"min": 0}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "min"

    def test_above_max_fails(self):
        warnings = _apply_validation_rules(
            {"amount": "200000"},
            {"amount": "NUMBER"},
            {"amount": {"max": 100000}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "max"

    def test_at_boundary_min_passes(self):
        warnings = _apply_validation_rules(
            {"amount": "0"},
            {"amount": "NUMBER"},
            {"amount": {"min": 0}},
        )
        assert len(warnings) == 0

    def test_at_boundary_max_passes(self):
        warnings = _apply_validation_rules(
            {"amount": "100000"},
            {"amount": "NUMBER"},
            {"amount": {"max": 100000}},
        )
        assert len(warnings) == 0

    def test_non_numeric_value_skipped(self):
        """Non-numeric values should not raise, just skip range check."""
        warnings = _apply_validation_rules(
            {"amount": "not-a-number"},
            {"amount": "NUMBER"},
            {"amount": {"min": 0, "max": 100}},
        )
        # No crash; range check silently skipped
        assert all(w["rule"] not in ("min", "max") for w in warnings)

    def test_varchar_type_skips_range(self):
        """Range rules only apply to NUMBER type fields."""
        warnings = _apply_validation_rules(
            {"name": "500"},
            {"name": "VARCHAR"},
            {"name": {"min": 0, "max": 100}},
        )
        assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Date range rules
# ---------------------------------------------------------------------------
class TestDateRangeRules:
    def test_within_range_passes(self):
        warnings = _apply_validation_rules(
            {"due_date": "2024-06-15"},
            {"due_date": "DATE"},
            {"due_date": {"date_min": "2020-01-01", "date_max": "2030-12-31"}},
        )
        assert len(warnings) == 0

    def test_before_min_fails(self):
        warnings = _apply_validation_rules(
            {"due_date": "2019-12-31"},
            {"due_date": "DATE"},
            {"due_date": {"date_min": "2020-01-01"}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "date_min"

    def test_after_max_fails(self):
        warnings = _apply_validation_rules(
            {"due_date": "2031-01-01"},
            {"due_date": "DATE"},
            {"due_date": {"date_max": "2030-12-31"}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "date_max"

    def test_non_date_value_skipped(self):
        """Non-parseable dates should not raise."""
        warnings = _apply_validation_rules(
            {"due_date": "not-a-date"},
            {"due_date": "DATE"},
            {"due_date": {"date_min": "2020-01-01"}},
        )
        assert all(w["rule"] != "date_min" for w in warnings)

    def test_varchar_type_skips_date_range(self):
        """Date range rules only apply to DATE type fields."""
        warnings = _apply_validation_rules(
            {"name": "2019-01-01"},
            {"name": "VARCHAR"},
            {"name": {"date_min": "2020-01-01"}},
        )
        assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Pattern rules
# ---------------------------------------------------------------------------
class TestPatternRules:
    def test_matching_pattern_passes(self):
        warnings = _apply_validation_rules(
            {"account": "12-3456-7890-12"},
            {"account": "VARCHAR"},
            {"account": {"pattern": r"\d{2}-\d{4}-\d{4}-\d{2}"}},
        )
        assert len(warnings) == 0

    def test_non_matching_pattern_fails(self):
        warnings = _apply_validation_rules(
            {"account": "INVALID"},
            {"account": "VARCHAR"},
            {"account": {"pattern": r"\d{2}-\d{4}-\d{4}-\d{2}"}},
        )
        assert len(warnings) == 1
        assert warnings[0]["rule"] == "pattern"

    def test_none_value_skips_pattern(self):
        warnings = _apply_validation_rules(
            {"account": None},
            {"account": "VARCHAR"},
            {"account": {"pattern": r"\d+"}},
        )
        assert all(w["rule"] != "pattern" for w in warnings)


# ---------------------------------------------------------------------------
# Multiple rules per field
# ---------------------------------------------------------------------------
class TestMultipleRules:
    def test_multiple_rules_all_pass(self):
        warnings = _apply_validation_rules(
            {"total": "500.00"},
            {"total": "NUMBER"},
            {"total": {"required": True, "min": 0, "max": 100000}},
        )
        assert len(warnings) == 0

    def test_multiple_rules_multiple_failures(self):
        """A value can fail multiple rules at once."""
        warnings = _apply_validation_rules(
            {"total": "-5"},
            {"total": "NUMBER"},
            {"total": {"min": 0, "max": 100000}},
        )
        rules = [w["rule"] for w in warnings]
        assert "min" in rules

    def test_multiple_fields_independent(self):
        """Rules on different fields are evaluated independently."""
        warnings = _apply_validation_rules(
            {"total": "50", "date": "2019-01-01"},
            {"total": "NUMBER", "date": "DATE"},
            {
                "total": {"min": 0, "max": 100},
                "date": {"date_min": "2020-01-01"},
            },
        )
        assert len(warnings) == 1
        assert warnings[0]["field"] == "date"


# ---------------------------------------------------------------------------
# Warning structure
# ---------------------------------------------------------------------------
class TestWarningStructure:
    def test_warning_has_required_keys(self):
        warnings = _apply_validation_rules(
            {"x": None},
            {"x": "VARCHAR"},
            {"x": {"required": True}},
        )
        assert len(warnings) == 1
        w = warnings[0]
        assert "field" in w
        assert "rule" in w
        assert "message" in w
        assert w["field"] == "x"
        assert w["rule"] == "required"
        assert isinstance(w["message"], str)
