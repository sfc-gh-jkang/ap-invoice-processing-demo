"""Unit tests for the _normalize() function inside SP_EXTRACT_BY_DOC_TYPE.

These are pure Python tests — no Snowflake connection needed.
We extract _normalize() from 06_automate.sql via AST to avoid running the SP.
"""

import ast
import os
import re
import textwrap

import pytest


# ---------------------------------------------------------------------------
# Extract _normalize from the SQL file (same AST trick as test_config_helpers)
# ---------------------------------------------------------------------------
_SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "sql", "06_automate.sql"
)


def _load_normalize():
    """Extract _normalize() source from the $$ Python body in 06_automate.sql."""
    with open(_SQL_PATH) as f:
        content = f.read()

    # Find the Python body between $$ markers for SP_EXTRACT_BY_DOC_TYPE
    sp_start = content.find("CREATE OR REPLACE PROCEDURE SP_EXTRACT_BY_DOC_TYPE")
    first_dd = content.find("$$", sp_start)
    second_dd = content.find("$$", first_dd + 2)
    python_body = content[first_dd + 2 : second_dd]

    # Parse and extract _normalize
    tree = ast.parse(python_body)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_normalize":
            func_source = ast.get_source_segment(python_body, node)
            # Build a mini-module with imports + function
            code = "import re\nfrom datetime import datetime\n\n" + func_source
            ns = {}
            exec(compile(code, "<normalize>", "exec"), ns)
            return ns["_normalize"]

    raise RuntimeError("Could not find _normalize in 06_automate.sql")


_normalize = _load_normalize()


# ---------------------------------------------------------------------------
# DATE normalization
# ---------------------------------------------------------------------------
class TestNormalizeDates:
    """Test _normalize() with field_type='DATE'."""

    def test_iso_format_passthrough(self):
        assert _normalize("2024-01-15", "DATE") == "2024-01-15"

    def test_us_format_slash(self):
        assert _normalize("01/15/2024", "DATE") == "2024-01-15"

    def test_us_format_dash(self):
        assert _normalize("01-15-2024", "DATE") == "2024-01-15"

    def test_european_format(self):
        assert _normalize("15/01/2024", "DATE") == "2024-01-15"

    def test_long_month_name(self):
        assert _normalize("January 15, 2024", "DATE") == "2024-01-15"

    def test_short_month_name(self):
        assert _normalize("Jan 15, 2024", "DATE") == "2024-01-15"

    def test_long_month_no_comma(self):
        assert _normalize("January 15 2024", "DATE") == "2024-01-15"

    def test_short_month_no_comma(self):
        assert _normalize("Jan 15 2024", "DATE") == "2024-01-15"

    def test_day_month_year_long(self):
        assert _normalize("15 January 2024", "DATE") == "2024-01-15"

    def test_day_month_year_short(self):
        assert _normalize("15 Jan 2024", "DATE") == "2024-01-15"

    def test_year_slash_month_day(self):
        assert _normalize("2024/01/15", "DATE") == "2024-01-15"

    def test_ordinal_suffix_st(self):
        assert _normalize("January 1st, 2024", "DATE") == "2024-01-01"

    def test_ordinal_suffix_nd(self):
        assert _normalize("January 2nd, 2024", "DATE") == "2024-01-02"

    def test_ordinal_suffix_rd(self):
        assert _normalize("January 3rd, 2024", "DATE") == "2024-01-03"

    def test_ordinal_suffix_th(self):
        assert _normalize("January 15th, 2024", "DATE") == "2024-01-15"

    def test_invalid_date_returns_raw(self):
        result = _normalize("not-a-date", "DATE")
        assert result == "not-a-date"

    def test_none_returns_none(self):
        assert _normalize(None, "DATE") is None

    def test_empty_string_returns_none(self):
        assert _normalize("", "DATE") is None

    def test_na_returns_none(self):
        assert _normalize("n/a", "DATE") is None

    def test_null_string_returns_none(self):
        assert _normalize("null", "DATE") is None

    def test_none_string_returns_none(self):
        assert _normalize("None", "DATE") is None

    def test_whitespace_stripped(self):
        assert _normalize("  2024-01-15  ", "DATE") == "2024-01-15"


# ---------------------------------------------------------------------------
# NUMBER normalization
# ---------------------------------------------------------------------------
class TestNormalizeNumbers:
    """Test _normalize() with field_type='NUMBER'."""

    def test_plain_number(self):
        assert _normalize("123.45", "NUMBER") == "123.45"

    def test_dollar_sign_stripped(self):
        assert _normalize("$123.45", "NUMBER") == "123.45"

    def test_comma_stripped(self):
        assert _normalize("1,234.56", "NUMBER") == "1234.56"

    def test_dollar_and_comma(self):
        assert _normalize("$1,234.56", "NUMBER") == "1234.56"

    def test_kwh_unit_stripped(self):
        assert _normalize("898 kWh", "NUMBER") == "898"

    def test_kw_unit_stripped(self):
        assert _normalize("45.2 kW", "NUMBER") == "45.2"

    def test_percent_stripped(self):
        assert _normalize("85%", "NUMBER") == "85"

    def test_negative_number(self):
        assert _normalize("-12.34", "NUMBER") == "-12.34"

    def test_integer(self):
        assert _normalize("42", "NUMBER") == "42"

    def test_zero(self):
        assert _normalize("0", "NUMBER") == "0"

    def test_none_returns_zero(self):
        assert _normalize(None, "NUMBER") == "0"

    def test_empty_returns_zero(self):
        assert _normalize("", "NUMBER") == "0"

    def test_na_returns_zero(self):
        assert _normalize("n/a", "NUMBER") == "0"

    def test_null_string_returns_zero(self):
        assert _normalize("null", "NUMBER") == "0"

    def test_none_string_returns_zero(self):
        assert _normalize("None", "NUMBER") == "0"

    def test_just_dot_returns_zero(self):
        assert _normalize(".", "NUMBER") == "0"

    def test_just_dash_returns_zero(self):
        assert _normalize("-", "NUMBER") == "0"

    def test_whitespace_stripped(self):
        assert _normalize("  123.45  ", "NUMBER") == "123.45"


# ---------------------------------------------------------------------------
# VARCHAR normalization
# ---------------------------------------------------------------------------
class TestNormalizeVarchar:
    """Test _normalize() with field_type='VARCHAR'."""

    def test_plain_string(self):
        assert _normalize("hello", "VARCHAR") == "hello"

    def test_whitespace_stripped(self):
        assert _normalize("  hello world  ", "VARCHAR") == "hello world"

    def test_none_returns_none(self):
        assert _normalize(None, "VARCHAR") is None

    def test_empty_returns_none(self):
        assert _normalize("", "VARCHAR") is None

    def test_na_returns_none(self):
        assert _normalize("n/a", "VARCHAR") is None

    def test_null_string_returns_none(self):
        assert _normalize("null", "VARCHAR") is None

    def test_none_string_returns_none(self):
        assert _normalize("None", "VARCHAR") is None

    def test_preserves_special_chars(self):
        assert _normalize("PSE&G", "VARCHAR") == "PSE&G"

    def test_numeric_string_stays_string(self):
        assert _normalize("12345", "VARCHAR") == "12345"
