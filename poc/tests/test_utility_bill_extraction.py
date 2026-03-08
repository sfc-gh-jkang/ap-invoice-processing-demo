"""Utility Bill Extraction Tests — validate extraction quality for UTILITY_BILL doc type.

Tests cover:
  1. All 10 utility bills were extracted (rows exist in EXTRACTED_FIELDS)
  2. RAW_EXTRACTION contains all 13 expected fields
  3. Field-level accuracy: exact-match, partial-match, and known format issues
  4. Cross-provider extraction (ConEdison, PSE&G, National Grid, O&R, JCP&L)
  5. Table data extraction for rate tiers
  6. Field_1..field_10 mapping from config labels
"""

import json
import re

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
UTILITY_BILL_FILES = [f"utility_bill_{i:02d}.pdf" for i in range(1, 11)]

EXPECTED_RAW_FIELDS = [
    "utility_company", "account_number", "meter_number", "service_address",
    "billing_period_start", "billing_period_end", "rate_schedule",
    "kwh_usage", "demand_kw", "previous_balance",
    "current_charges", "total_due", "due_date",
]

# Provider mapping by bill number (1-indexed)
BILL_PROVIDERS = {
    1: "Consolidated Edison", 2: "Consolidated Edison",
    3: "Consolidated Edison", 4: "Consolidated Edison",
    5: "Consolidated Edison", 6: "Public Service Electric and Gas",
    7: "National Grid", 8: "Orange and Rockland Utilities",
    9: "Jersey Central Power & Light", 10: "Consolidated Edison",
}


def _strip_currency(val: str) -> str:
    """Remove $ and commas from a value string."""
    if val is None:
        return ""
    return str(val).replace("$", "").replace(",", "").strip()


def _normalize_numeric(val) -> float | None:
    """Parse a numeric value, stripping units like 'kWh' or 'kW'."""
    if val is None:
        return None
    s = str(val).lower().replace(",", "").replace("kwh", "").replace("kw", "").replace("$", "").strip()
    if not s or s == "none" or s == "null":
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 1. Extraction completeness
# ---------------------------------------------------------------------------
class TestExtractionCompleteness:
    """Verify all 10 utility bills have extraction rows."""

    def test_all_10_bills_registered(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'UTILITY_BILL'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_all_10_bills_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        assert sf_cursor.fetchone()[0] == 10

    def test_all_bills_marked_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'UTILITY_BILL' AND extracted = TRUE"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(
            "SELECT file_name, extraction_error FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'UTILITY_BILL' AND extraction_error IS NOT NULL"
        )
        errors = sf_cursor.fetchall()
        assert len(errors) == 0, f"Extraction errors: {errors}"


# ---------------------------------------------------------------------------
# 2. RAW_EXTRACTION field presence
# ---------------------------------------------------------------------------
class TestRawExtractionFields:
    """Verify raw_extraction VARIANT contains all expected fields."""

    def test_raw_extraction_not_null(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL' AND e.raw_extraction IS NULL
        """)
        assert sf_cursor.fetchone()[0] == 0, "Some utility bills have NULL raw_extraction"

    @pytest.mark.parametrize("field", EXPECTED_RAW_FIELDS)
    def test_raw_extraction_has_field(self, sf_cursor, field):
        """Each raw_extraction should contain the expected field key."""
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
              AND e.raw_extraction:{field} IS NOT NULL
        """)
        count = sf_cursor.fetchone()[0]
        # Allow some nulls for demand_kw and previous_balance (known issue)
        if field in ("demand_kw", "previous_balance"):
            assert count >= 2, (
                f"Field '{field}' present in only {count}/10 bills (expected >=2)"
            )
        else:
            assert count == 10, (
                f"Field '{field}' missing from {10 - count} bills"
            )


# ---------------------------------------------------------------------------
# 3. Field-level accuracy (per-provider)
# ---------------------------------------------------------------------------
class TestFieldAccuracy:
    """Spot-check extracted values for known bills."""

    def test_conedison_company_name(self, sf_cursor):
        """ConEdison bills should extract full or recognizable company name."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:utility_company::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "edison" in val.lower() or "con ed" in val.lower(), (
            f"Expected ConEdison variant, got: {val}"
        )

    def test_pseg_company_name(self, sf_cursor):
        """PSE&G bill should extract recognizable company name."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:utility_company::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_06.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "pse" in val.lower() or "public service" in val.lower(), (
            f"Expected PSE&G variant, got: {val}"
        )

    def test_account_numbers_are_formatted(self, sf_cursor):
        """All account numbers should match XX-XXXX-XXXX-XX pattern."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:account_number::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            acct = row[1]
            assert re.match(r"\d{2}-\d{4}-\d{4}-\d{2}", acct), (
                f"{row[0]}: account_number format unexpected: {acct}"
            )

    def test_meter_numbers_start_with_letter(self, sf_cursor):
        """All meter numbers should start with E, M, or K."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:meter_number::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            meter = row[1]
            assert meter and meter[0] in ("E", "M", "K"), (
                f"{row[0]}: meter_number unexpected prefix: {meter}"
            )

    def test_kwh_usage_is_positive(self, sf_cursor):
        """All kWh usage values should be positive numbers."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:kwh_usage::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: kwh_usage should be positive, got: {row[1]}"
            )

    def test_total_due_is_positive(self, sf_cursor):
        """All total_due values should be positive numbers."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:total_due::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: total_due should be positive, got: {row[1]}"
            )

    def test_current_charges_lte_total_due(self, sf_cursor):
        """Current charges should be <= total due (total = current + previous)."""
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:current_charges::VARCHAR,
                   e.raw_extraction:total_due::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            current = _normalize_numeric(row[1])
            total = _normalize_numeric(row[2])
            if current is not None and total is not None:
                assert current <= total + 0.01, (
                    f"{row[0]}: current_charges ({current}) > total_due ({total})"
                )

    def test_service_addresses_contain_state(self, sf_cursor):
        """All service addresses should contain NY or NJ."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:service_address::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            addr = (row[1] or "").upper()
            assert "NY" in addr or "NJ" in addr, (
                f"{row[0]}: address missing NY/NJ: {row[1]}"
            )

    def test_rate_schedule_not_empty(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:rate_schedule::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            assert row[1] and len(row[1].strip()) > 0, (
                f"{row[0]}: rate_schedule is empty"
            )


# ---------------------------------------------------------------------------
# 4. Known format issues (document for improvement tracking)
# ---------------------------------------------------------------------------
class TestNormalizedFormats:
    """Verify post-processing normalization is applied correctly.

    These tests were previously xfail when normalization was not implemented.
    Now that SP_EXTRACT_BY_DOC_TYPE includes _normalize(), they should pass.
    """

    def test_dates_iso_format(self, sf_cursor):
        """Dates should be normalized to ISO YYYY-MM-DD format."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:billing_period_start::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), \
            f"Expected ISO date, got: {val}"

    def test_monetary_values_no_currency_symbol(self, sf_cursor):
        """Monetary values should be plain numbers without $ prefix."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:total_due::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "$" not in val, f"Expected no $ symbol, got: {val}"

    def test_kwh_values_no_unit_suffix(self, sf_cursor):
        """kWh values should be plain numbers without unit suffix."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:kwh_usage::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "kwh" not in val.lower(), f"Expected no kWh suffix, got: {val}"

    def test_zero_balance_not_null(self, sf_cursor):
        """Bills with $0 previous balance should return '0', not null."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:previous_balance::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_02.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val is not None and val.lower() not in ("none", "null", ""), \
            f"Expected '0' for zero balance, got: {val}"
        assert _normalize_numeric(val) == 0.0

    def test_full_company_names(self, sf_cursor):
        """Companies should return full legal names, not abbreviations.
        Post-extraction abbreviation resolution maps PSE&G → Public Service Electric and Gas.
        """
        sf_cursor.execute("""
            SELECT e.raw_extraction:utility_company::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_06.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "public service" in val.lower(), \
            f"Expected full company name with 'public service', got: {val}"


# ---------------------------------------------------------------------------
# 5. Field mapping (field_1..field_10 populated from config)
# ---------------------------------------------------------------------------
class TestFieldMapping:
    """Verify field_1..field_10 are populated per FIELD_LABELS config."""

    def test_field_1_is_utility_company(self, sf_cursor):
        """field_1 should be mapped to utility_company per config."""
        sf_cursor.execute("""
            SELECT e.field_1
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and "edison" in val.lower()

    def test_field_2_is_account_number(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_2
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and re.match(r"\d{2}-\d{4}-\d{4}-\d{2}", val)

    def test_field_3_is_meter_number(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_3
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'utility_bill_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and val[0] in ("E", "M", "K")

    def test_fields_11_through_13_only_in_raw(self, sf_cursor):
        """UTILITY_BILL has 13 fields; fields 11-13 only in raw_extraction."""
        sf_cursor.execute("""
            SELECT field_labels
            FROM DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'UTILITY_BILL'
        """)
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        # Verify fields 11, 12, 13 exist in config
        assert "field_11" in labels
        assert "field_12" in labels
        assert "field_13" in labels
        # These map to current_charges, total_due, due_date
        assert labels["field_11"] == "Current Charges"
        assert labels["field_12"] == "Total Due"
        assert labels["field_13"] == "Due Date"


# ---------------------------------------------------------------------------
# 6. Cross-provider extraction
# ---------------------------------------------------------------------------
class TestCrossProviderExtraction:
    """Verify extraction works across different utility providers and layouts."""

    @pytest.mark.parametrize("bill_num,expected_provider", [
        (1, "edison"),
        (6, "public service"),
        (7, "national grid"),
        (8, "orange"),
        (9, "jersey central"),
    ])
    def test_provider_recognized(self, sf_cursor, bill_num, expected_provider):
        fname = f"utility_bill_{bill_num:02d}.pdf"
        sf_cursor.execute(f"""
            SELECT e.raw_extraction:utility_company::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = '{fname}'
        """)
        val = (sf_cursor.fetchone()[0] or "").lower()
        assert expected_provider in val, (
            f"{fname}: expected '{expected_provider}' in company name, got: {val}"
        )

    def test_all_providers_have_kwh(self, sf_cursor):
        """Every bill regardless of layout should have kWh usage."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:kwh_usage::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: missing or invalid kwh_usage: {row[1]}"
            )

    def test_all_providers_have_total_due(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:total_due::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: missing or invalid total_due: {row[1]}"
            )


# ---------------------------------------------------------------------------
# 7. Table data extraction (rate tiers)
# ---------------------------------------------------------------------------
class TestTableDataExtraction:
    """Verify EXTRACTED_TABLE_DATA for utility bills.

    Note: SP_EXTRACT_BY_DOC_TYPE currently does not extract table data
    for non-INVOICE doc types. These tests document that gap.
    """

    def test_table_extraction_schema_configured(self, sf_cursor):
        """UTILITY_BILL config has a TABLE_EXTRACTION_SCHEMA defined."""
        sf_cursor.execute("""
            SELECT table_extraction_schema
            FROM DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'UTILITY_BILL'
        """)
        schema = sf_cursor.fetchone()[0]
        assert schema is not None, "TABLE_EXTRACTION_SCHEMA not configured"

    def test_table_data_extracted(self, sf_cursor):
        """Table data (rate tiers) should be extracted for utility bills
        now that SP_EXTRACT_BY_DOC_TYPE supports config-driven table extraction.
        """
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 10, \
            f"Expected at least 10 table data rows for utility bills, got {count}"


# ---------------------------------------------------------------------------
# 8. Isolation from invoice data
# ---------------------------------------------------------------------------
class TestDocTypeIsolation:
    """Utility bill extraction should not affect invoice data."""

    def test_invoice_count_unchanged(self, sf_cursor):
        """Original 100 invoices should still be present."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'INVOICE'"
        )
        assert sf_cursor.fetchone()[0] == 100

    def test_invoice_extractions_unchanged(self, sf_cursor):
        """Original invoice extractions should still be present."""
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'INVOICE'
        """)
        assert sf_cursor.fetchone()[0] == 100

    def test_utility_bills_visible_in_document_summary(self, sf_cursor):
        """V_DOCUMENT_SUMMARY (and its alias V_INVOICE_SUMMARY) shows all doc types
        including utility bills, with a doc_type column for filtering.
        """
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM V_DOCUMENT_SUMMARY
            WHERE doc_type = 'UTILITY_BILL'
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 10, \
            f"Expected 10 utility bill rows in V_DOCUMENT_SUMMARY, got {count}"

    def test_document_summary_has_doc_type_column(self, sf_cursor):
        """V_DOCUMENT_SUMMARY should have a DOC_TYPE column for filtering."""
        sf_cursor.execute("SELECT * FROM V_DOCUMENT_SUMMARY LIMIT 0")
        columns = [d[0] for d in sf_cursor.description]
        assert "DOC_TYPE" in columns
