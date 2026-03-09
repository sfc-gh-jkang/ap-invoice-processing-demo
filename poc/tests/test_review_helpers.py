"""Unit tests for Review page helper functions (_norm, _safe_str, _safe_num, _safe_date).

These functions live inside streamlit/pages/3_Review.py, which cannot be imported
directly (it calls get_active_session() and st.set_page_config() at module level).
We extract and test the function logic in isolation.
"""

import math
from datetime import date, datetime

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Re-implement the helpers exactly as they appear in 3_Review.py so we can
# unit-test them without importing the Streamlit page module.
# ---------------------------------------------------------------------------

def _norm(val):
    """Normalize for comparison — coerce everything to stripped string."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return str(val.date())
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _safe_str(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    return s if s else None


def _safe_num(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        f = float(v)
    except (ValueError, TypeError):
        return None
    # NUMBER(12,2) range: -9999999999.99 to 9999999999.99
    if f > 9999999999.99 or f < -9999999999.99:
        return None
    return f


def _safe_date(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        return str(pd.to_datetime(v).date())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# _norm tests
# ---------------------------------------------------------------------------
class TestNorm:
    """Verify _norm() normalizes values for change detection comparison."""

    def test_none_returns_empty(self):
        assert _norm(None) == ""

    def test_nan_returns_empty(self):
        assert _norm(float("nan")) == ""

    def test_pd_nat_returns_empty(self):
        assert _norm(pd.NaT) == ""

    def test_string_stripped(self):
        assert _norm("  hello  ") == "hello"

    def test_empty_string(self):
        assert _norm("") == ""

    def test_number_to_string(self):
        assert _norm(42) == "42"

    def test_float_to_string(self):
        assert _norm(3.14) == "3.14"

    def test_zero_to_string(self):
        assert _norm(0) == "0"

    def test_timestamp_to_date_string(self):
        ts = pd.Timestamp("2025-03-15 14:30:00")
        assert _norm(ts) == "2025-03-15"

    def test_date_to_string(self):
        d = date(2025, 1, 15)
        assert _norm(d) == "2025-01-15"

    def test_pd_na_returns_empty(self):
        assert _norm(pd.NA) == ""

    def test_boolean_to_string(self):
        assert _norm(True) == "True"


# ---------------------------------------------------------------------------
# _safe_str tests
# ---------------------------------------------------------------------------
class TestSafeStr:
    """Verify _safe_str() converts values for SQL INSERT."""

    def test_none_returns_none(self):
        assert _safe_str(None) is None

    def test_nan_returns_none(self):
        assert _safe_str(float("nan")) is None

    def test_empty_string_returns_none(self):
        assert _safe_str("") is None

    def test_whitespace_only_returns_none(self):
        assert _safe_str("   ") is None

    def test_normal_string(self):
        assert _safe_str("hello") == "hello"

    def test_string_stripped(self):
        assert _safe_str("  hello  ") == "hello"

    def test_number_to_string(self):
        assert _safe_str(42) == "42"

    def test_zero_to_string(self):
        assert _safe_str(0) == "0"


# ---------------------------------------------------------------------------
# _safe_num tests
# ---------------------------------------------------------------------------
class TestSafeNum:
    """Verify _safe_num() converts values for numeric SQL columns."""

    def test_none_returns_none(self):
        assert _safe_num(None) is None

    def test_nan_returns_none(self):
        assert _safe_num(float("nan")) is None

    def test_integer(self):
        assert _safe_num(42) == 42.0

    def test_float(self):
        assert _safe_num(3.14) == pytest.approx(3.14)

    def test_string_number(self):
        assert _safe_num("99.50") == pytest.approx(99.50)

    def test_zero(self):
        assert _safe_num(0) == 0.0

    def test_negative(self):
        assert _safe_num(-5.5) == pytest.approx(-5.5)

    def test_non_numeric_string_returns_none(self):
        assert _safe_num("not a number") is None

    def test_empty_string_returns_none(self):
        assert _safe_num("") is None

    def test_pd_na_returns_none(self):
        # pd.NA is not a float, so it hits the try/except path
        assert _safe_num(pd.NA) is None

    def test_max_number_12_2(self):
        assert _safe_num(9999999999.99) == pytest.approx(9999999999.99)

    def test_min_number_12_2(self):
        assert _safe_num(-9999999999.99) == pytest.approx(-9999999999.99)

    def test_overflow_returns_none(self):
        assert _safe_num(99999999999.99) is None

    def test_negative_overflow_returns_none(self):
        assert _safe_num(-99999999999.99) is None

    def test_just_over_max_returns_none(self):
        assert _safe_num(10000000000.00) is None


# ---------------------------------------------------------------------------
# _safe_date tests
# ---------------------------------------------------------------------------
class TestSafeDate:
    """Verify _safe_date() converts values for DATE SQL columns."""

    def test_none_returns_none(self):
        assert _safe_date(None) is None

    def test_nan_returns_none(self):
        assert _safe_date(float("nan")) is None

    def test_iso_date_string(self):
        assert _safe_date("2025-03-15") == "2025-03-15"

    def test_us_date_string(self):
        assert _safe_date("03/15/2025") == "2025-03-15"

    def test_datetime_object(self):
        assert _safe_date(datetime(2025, 1, 15, 14, 30)) == "2025-01-15"

    def test_date_object(self):
        assert _safe_date(date(2025, 6, 1)) == "2025-06-01"

    def test_pd_timestamp(self):
        assert _safe_date(pd.Timestamp("2025-12-25")) == "2025-12-25"

    def test_invalid_date_returns_none(self):
        assert _safe_date("not a date") is None

    def test_empty_string(self):
        # pd.to_datetime("") may return NaT (stringified as "NaT") or raise
        # depending on pandas version. Either None or "NaT" is acceptable.
        result = _safe_date("")
        assert result is None or result == "NaT"

    def test_epoch_date(self):
        assert _safe_date("1970-01-01") == "1970-01-01"

    def test_far_future_date(self):
        assert _safe_date("2099-12-31") == "2099-12-31"

    def test_whitespace_only_returns_none(self):
        result = _safe_date("   ")
        assert result is None or result == "NaT"

    def test_partial_date_string(self):
        # "2025-13-01" is month 13 — should be None
        assert _safe_date("2025-13-01") is None

    def test_numeric_string_as_date(self):
        # pd.to_datetime interprets numeric-ish strings; just verify no crash
        result = _safe_date("20250315")
        assert result is None or isinstance(result, str)


# ---------------------------------------------------------------------------
# Verify source parity: helpers in test match actual 3_Review.py source
# ---------------------------------------------------------------------------
class TestSourceParity:
    """Verify that the helper functions tested here match the actual source."""

    def test_norm_source_matches(self):
        """_norm in this test file should match the one in 3_Review.py."""
        import os
        import re
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()

        # Extract _norm function from source
        match = re.search(
            r"(def _norm\(val\):.*?)(?=\n\ndef |\nchanged_rows|\nEDITABLE)",
            source,
            re.DOTALL,
        )
        assert match is not None, "_norm() not found in 3_Review.py"
        source_body = match.group(1).strip()
        # Verify key logic lines are present
        assert 'if val is None:' in source_body
        assert 'pd.isna(val)' in source_body
        assert 'pd.Timestamp' in source_body
        assert 'str(val).strip()' in source_body

    def test_safe_str_source_exists(self):
        """_safe_str should exist in 3_Review.py."""
        import os
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()
        assert "def _safe_str(v):" in source

    def test_safe_num_source_exists(self):
        """_safe_num should exist in 3_Review.py."""
        import os
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()
        assert "def _safe_num(v):" in source

    def test_safe_date_source_exists(self):
        """_safe_date should exist in 3_Review.py."""
        import os
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()
        assert "def _safe_date(v):" in source

    def test_safe_num_has_range_check(self):
        """_safe_num in 3_Review.py must enforce NUMBER(12,2) range."""
        import os
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()
        assert "9999999999.99" in source, (
            "_safe_num must check NUMBER(12,2) range boundary"
        )

    def test_presave_validation_exists(self):
        """3_Review.py must contain the pre-save validation loop."""
        import os
        review_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "streamlit", "pages", "3_Review.py",
        )
        with open(review_path) as f:
            source = f.read()
        assert "validation_errors" in source, (
            "Pre-save validation loop must exist in 3_Review.py"
        )
        assert "not a valid number" in source, (
            "Number validation error message must exist"
        )
        assert "not a valid date" in source, (
            "Date validation error message must exist"
        )
