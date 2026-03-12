"""Lease Extraction Tests — validate extraction quality for LEASE doc type.

Tests cover:
  1. All 10 leases were extracted (rows exist in EXTRACTED_FIELDS)
  2. RAW_EXTRACTION contains all 12 expected fields
  3. Field-level accuracy: landlord names, lease numbers, dates, rent amounts
  4. Rent schedule table data extraction
  5. Field_1..field_12 mapping from config labels
  6. Isolation from other doc types
"""

import json
import re

import pytest


pytestmark = pytest.mark.sql


@pytest.fixture(autouse=True, scope="session")
def _skip_if_no_leases(sf_cursor):
    """Skip all tests in this module if no LEASE data exists."""
    sf_cursor.execute(
        "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'LEASE'"
    )
    count = sf_cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No LEASE data in deployment — skipping lease tests")


LEASE_FILES = [f"lease_{i:02d}.pdf" for i in range(1, 11)]

EXPECTED_RAW_FIELDS = [
    "landlord_name", "tenant_name", "lease_number", "property_address",
    "lease_start_date", "lease_end_date", "lease_term_months",
    "monthly_rent", "security_deposit", "payment_due_day",
    "late_fee", "total_lease_value",
]

LANDLORD_MAPPING = {
    1: "apex commercial",
    2: "harbor point",
    3: "pinnacle asset",
    4: "greenfield development",
    5: "metro plaza",
    6: "sunbelt property",
    7: "pacific rim",
    8: "crossroads realty",
    9: "brightstone capital",
    10: "riverstone retail",
}


def _normalize_numeric(val) -> float | None:
    if val is None:
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    if not s or s.lower() in ("none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


class TestExtractionCompleteness:
    """Verify all 10 leases have extraction rows."""

    def test_all_10_leases_registered(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'LEASE'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_all_10_leases_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        assert sf_cursor.fetchone()[0] == 10

    def test_all_leases_marked_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'LEASE' AND extracted = TRUE"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(
            "SELECT file_name, extraction_error FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'LEASE' AND extraction_error IS NOT NULL"
        )
        errors = sf_cursor.fetchall()
        assert len(errors) == 0, f"Extraction errors: {errors}"


class TestRawExtractionFields:
    """Verify raw_extraction VARIANT contains all expected fields."""

    def test_raw_extraction_not_null(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE' AND e.raw_extraction IS NULL
        """)
        assert sf_cursor.fetchone()[0] == 0

    @pytest.mark.parametrize("field", EXPECTED_RAW_FIELDS)
    def test_raw_extraction_has_field(self, sf_cursor, field):
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
              AND e.raw_extraction:{field} IS NOT NULL
        """)
        count = sf_cursor.fetchone()[0]
        if field in ("late_fee", "payment_due_day"):
            assert count >= 5, (
                f"Field '{field}' present in only {count}/10 leases (expected >=5)"
            )
        else:
            assert count >= 8, (
                f"Field '{field}' present in only {count}/10 leases (expected >=8)"
            )


class TestFieldAccuracy:
    """Spot-check extracted values for known leases."""

    def test_lease_numbers_format(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:lease_number::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = row[1] or ""
            assert "LSE" in val.upper(), (
                f"{row[0]}: lease_number should contain 'LSE', got: {val}"
            )

    def test_monthly_rent_is_positive(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:monthly_rent::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: monthly_rent should be positive, got: {row[1]}"
            )

    def test_security_deposit_is_nonnegative(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:security_deposit::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            if val is not None:
                assert val >= 0, (
                    f"{row[0]}: security_deposit should be >= 0, got: {row[1]}"
                )

    def test_total_lease_value_positive(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:total_lease_value::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: total_lease_value should be positive, got: {row[1]}"
            )

    def test_total_value_gte_monthly_rent(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:monthly_rent::VARCHAR,
                   e.raw_extraction:total_lease_value::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        checks = 0
        for row in sf_cursor.fetchall():
            rent = _normalize_numeric(row[1])
            total = _normalize_numeric(row[2])
            if rent is not None and total is not None:
                assert total >= rent, (
                    f"{row[0]}: total ({total}) < monthly_rent ({rent})"
                )
                checks += 1
        assert checks >= 8, f"Only verified {checks} leases"

    def test_lease_start_date_before_end(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:lease_start_date::VARCHAR,
                   e.raw_extraction:lease_end_date::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            start = row[1] or ""
            end = row[2] or ""
            if (start and end
                    and re.match(r"\d{4}-\d{2}-\d{2}", start)
                    and re.match(r"\d{4}-\d{2}-\d{2}", end)):
                assert start < end, (
                    f"{row[0]}: start ({start}) >= end ({end})"
                )

    def test_dates_iso_format(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.raw_extraction:lease_start_date::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'lease_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), f"Expected ISO date, got: {val}"

    def test_lease_term_months_positive(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:lease_term_months::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            if val is not None:
                assert val > 0 and val <= 600, (
                    f"{row[0]}: lease_term_months should be 1-600, got: {val}"
                )

    def test_payment_due_day_range(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:payment_due_day::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            if val is not None:
                assert 1 <= val <= 31, (
                    f"{row[0]}: payment_due_day should be 1-31, got: {val}"
                )


class TestLandlordRecognition:
    """Verify landlord names are extracted correctly."""

    @pytest.mark.parametrize("lease_num,expected_landlord", [
        (1, "apex commercial"),
        (2, "harbor point"),
        (3, "pinnacle"),
        (4, "greenfield"),
        (5, "metro plaza"),
    ])
    def test_landlord_recognized(self, sf_cursor, lease_num, expected_landlord):
        fname = f"lease_{lease_num:02d}.pdf"
        sf_cursor.execute(f"""
            SELECT e.raw_extraction:landlord_name::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = '{fname}'
        """)
        val = (sf_cursor.fetchone()[0] or "").lower()
        assert expected_landlord in val, (
            f"{fname}: expected '{expected_landlord}' in landlord_name, got: {val}"
        )

    def test_tenant_names_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'LEASE'
              AND e.raw_extraction:tenant_name IS NOT NULL
              AND e.raw_extraction:tenant_name::VARCHAR != ''
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 8, f"Only {count}/10 leases have tenant_name"


class TestRentScheduleTableData:
    """Verify EXTRACTED_TABLE_DATA for lease rent schedules."""

    def test_table_extraction_schema_configured(self, sf_cursor):
        sf_cursor.execute("""
            SELECT table_extraction_schema
            FROM DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'LEASE'
        """)
        schema = sf_cursor.fetchone()[0]
        assert schema is not None

    def test_rent_schedule_data_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'LEASE'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 10, f"Expected >=10 rent schedule rows, got {count}"


class TestFieldMapping:
    """Verify field_1..field_12 are populated per LEASE config."""

    def test_field_1_is_landlord_name(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_1
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'lease_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and len(val) > 2

    def test_field_2_is_tenant_name(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_2
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'lease_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and len(val) > 2

    def test_field_3_is_lease_number(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_3
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'lease_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and "LSE" in val.upper()

    def test_field_8_is_monthly_rent(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_8
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'lease_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val is not None and float(val) > 0


class TestDocTypeIsolation:
    """Lease extraction should not affect other doc types."""

    def test_invoice_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'INVOICE'"
        )
        assert sf_cursor.fetchone()[0] == 100

    def test_contract_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'CONTRACT'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_utility_bill_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'UTILITY_BILL'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_leases_visible_in_document_summary(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM V_DOCUMENT_SUMMARY
            WHERE doc_type = 'LEASE'
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 10


class TestLeaseDocTypeConfig:
    """Verify LEASE config in DOCUMENT_TYPE_CONFIG."""

    def test_lease_config_exists(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        assert sf_cursor.fetchone()[0] == 1

    def test_lease_is_active(self, sf_cursor):
        sf_cursor.execute(
            "SELECT active FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        assert sf_cursor.fetchone()[0] is True

    def test_lease_has_12_field_keys(self, sf_cursor):
        sf_cursor.execute(
            "SELECT field_labels FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        field_keys = [k for k in labels if k.startswith("field_")]
        assert len(field_keys) == 12, (
            f"LEASE should have 12 field_* keys, got {len(field_keys)}: {field_keys}"
        )

    def test_lease_prompt_has_all_fields(self, sf_cursor):
        sf_cursor.execute(
            "SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        prompt = sf_cursor.fetchone()[0]
        for field in EXPECTED_RAW_FIELDS:
            assert field in prompt, f"LEASE prompt missing '{field}'"

    def test_lease_review_fields_complete(self, sf_cursor):
        sf_cursor.execute(
            "SELECT review_fields FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        raw = sf_cursor.fetchone()[0]
        rf = json.loads(raw) if isinstance(raw, str) else raw
        assert len(rf["correctable"]) == 12
        for field in EXPECTED_RAW_FIELDS:
            assert field in rf["correctable"], f"Missing correctable field: {field}"

    def test_lease_validation_rules(self, sf_cursor):
        sf_cursor.execute(
            "SELECT validation_rules FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'LEASE'"
        )
        raw = sf_cursor.fetchone()[0]
        vr = json.loads(raw) if isinstance(raw, str) else raw
        assert "monthly_rent" in vr
        assert vr["monthly_rent"]["required"] is True
        assert "landlord_name" in vr
        assert "tenant_name" in vr
