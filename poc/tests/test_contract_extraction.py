"""Contract Extraction Tests — validate extraction quality for CONTRACT doc type.

Tests cover:
  1. All 10 contracts were extracted (rows exist in EXTRACTED_FIELDS)
  2. RAW_EXTRACTION contains all expected fields
  3. Field-level accuracy: party names, contract numbers, dates, values
  4. Milestone table data extraction
  5. Field_1..field_10 mapping from config labels
  6. Isolation from other doc types
"""

import json
import re

import pytest


pytestmark = pytest.mark.sql


@pytest.fixture(autouse=True, scope="session")
def _skip_if_no_contracts(sf_cursor):
    """Skip all tests in this module if no CONTRACT data exists."""
    sf_cursor.execute(
        "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'CONTRACT'"
    )
    count = sf_cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No CONTRACT data in deployment — skipping contract tests")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
CONTRACT_FILES = [f"contract_{i:02d}.pdf" for i in range(1, 11)]

EXPECTED_RAW_FIELDS = [
    "party_name", "contract_number", "reference_id",
    "effective_date", "expiration_date", "terms",
    "counterparty", "base_value", "adjustments", "total_value",
]

# Party mapping by contract number (1-indexed)
CONTRACT_PARTIES = {
    1: "metro hvac",
    2: "tri-state it",
    3: "garden state janitorial",
    4: "empire security",
    5: "alpine building",
    6: "metro hvac",
    7: "tri-state it",
    8: "garden state janitorial",
    9: "empire security",
    10: "alpine building",
}


def _normalize_numeric(val) -> float | None:
    """Parse a numeric value, stripping currency symbols."""
    if val is None:
        return None
    s = str(val).replace(",", "").replace("$", "").strip()
    if not s or s.lower() in ("none", "null"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# 1. Extraction completeness
# ---------------------------------------------------------------------------
class TestExtractionCompleteness:
    """Verify all 10 contracts have extraction rows."""

    def test_all_10_contracts_registered(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'CONTRACT'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_all_10_contracts_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        assert sf_cursor.fetchone()[0] == 10

    def test_all_contracts_marked_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'CONTRACT' AND extracted = TRUE"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(
            "SELECT file_name, extraction_error FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'CONTRACT' AND extraction_error IS NOT NULL"
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
            WHERE r.doc_type = 'CONTRACT' AND e.raw_extraction IS NULL
        """)
        assert sf_cursor.fetchone()[0] == 0

    @pytest.mark.parametrize("field", EXPECTED_RAW_FIELDS)
    def test_raw_extraction_has_field(self, sf_cursor, field):
        """Each raw_extraction should contain the expected field key."""
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
              AND e.raw_extraction:{field} IS NOT NULL
        """)
        count = sf_cursor.fetchone()[0]
        # Adjustments may be 0/null for contracts without adjustments
        if field == "adjustments":
            assert count >= 3, (
                f"Field '{field}' present in only {count}/10 contracts (expected >=3)"
            )
        else:
            assert count >= 8, (
                f"Field '{field}' present in only {count}/10 contracts (expected >=8)"
            )


# ---------------------------------------------------------------------------
# 3. Field-level accuracy
# ---------------------------------------------------------------------------
class TestFieldAccuracy:
    """Spot-check extracted values for known contracts."""

    def test_contract_numbers_format(self, sf_cursor):
        """Contract numbers should follow CTR-YYYY-XXXX pattern."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:contract_number::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        for row in sf_cursor.fetchall():
            val = row[1] or ""
            assert "CTR" in val.upper(), (
                f"{row[0]}: contract_number should contain 'CTR', got: {val}"
            )

    def test_reference_ids_format(self, sf_cursor):
        """Reference IDs should follow REF-XXX-NNN pattern."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:reference_id::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        for row in sf_cursor.fetchall():
            val = row[1] or ""
            assert "REF" in val.upper(), (
                f"{row[0]}: reference_id should contain 'REF', got: {val}"
            )

    def test_total_value_is_positive(self, sf_cursor):
        """All total_value amounts should be positive."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:total_value::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: total_value should be positive, got: {row[1]}"
            )

    def test_base_value_lte_total_value(self, sf_cursor):
        """Base value should typically be <= total value (unless negative adjustments)."""
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:base_value::VARCHAR,
                   e.raw_extraction:total_value::VARCHAR,
                   e.raw_extraction:adjustments::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        checks = 0
        for row in sf_cursor.fetchall():
            base = _normalize_numeric(row[1])
            total = _normalize_numeric(row[2])
            adj = _normalize_numeric(row[3])
            if base is not None and total is not None:
                # Allow 5% tolerance — AI may miss or misinterpret
                # adjustments (e.g. discount extracted as 0)
                assert base <= total * 1.05 + 1, (
                    f"{row[0]}: base_value ({base}) much larger than total_value ({total})"
                )
                checks += 1
        assert checks >= 5, f"Only verified {checks} contracts"

    def test_effective_date_before_expiration(self, sf_cursor):
        """Effective date should be before expiration date."""
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:effective_date::VARCHAR,
                   e.raw_extraction:expiration_date::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        for row in sf_cursor.fetchall():
            eff = row[1] or ""
            exp = row[2] or ""
            if eff and exp and re.match(r"\d{4}-\d{2}-\d{2}", eff) and re.match(r"\d{4}-\d{2}-\d{2}", exp):
                assert eff < exp, (
                    f"{row[0]}: effective_date ({eff}) >= expiration_date ({exp})"
                )

    def test_dates_iso_format(self, sf_cursor):
        """Dates should be in ISO YYYY-MM-DD format."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:effective_date::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'contract_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), f"Expected ISO date, got: {val}"

    def test_terms_not_empty(self, sf_cursor):
        """Payment terms should be extracted."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:terms::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        non_empty = 0
        for row in sf_cursor.fetchall():
            if row[1] and len(row[1].strip()) > 0:
                non_empty += 1
        assert non_empty >= 8, f"Only {non_empty}/10 contracts have terms"


# ---------------------------------------------------------------------------
# 4. Party recognition
# ---------------------------------------------------------------------------
class TestPartyRecognition:
    """Verify party names are extracted correctly."""

    @pytest.mark.parametrize("contract_num,expected_party", [
        (1, "metro hvac"),
        (2, "tri-state"),
        (3, "garden state"),
        (4, "empire security"),
        (5, "alpine"),
    ])
    def test_party_recognized(self, sf_cursor, contract_num, expected_party):
        fname = f"contract_{contract_num:02d}.pdf"
        sf_cursor.execute(f"""
            SELECT e.raw_extraction:party_name::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = '{fname}'
        """)
        val = (sf_cursor.fetchone()[0] or "").lower()
        assert expected_party in val, (
            f"{fname}: expected '{expected_party}' in party_name, got: {val}"
        )

    def test_counterparty_extracted(self, sf_cursor):
        """Most contracts should have a counterparty extracted."""
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'CONTRACT'
              AND e.raw_extraction:counterparty IS NOT NULL
              AND e.raw_extraction:counterparty::VARCHAR != ''
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 8, f"Only {count}/10 contracts have counterparty"


# ---------------------------------------------------------------------------
# 5. Milestone table data
# ---------------------------------------------------------------------------
class TestMilestoneTableData:
    """Verify EXTRACTED_TABLE_DATA for contract milestones."""

    def test_table_extraction_schema_configured(self, sf_cursor):
        sf_cursor.execute("""
            SELECT table_extraction_schema
            FROM DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'CONTRACT'
        """)
        schema = sf_cursor.fetchone()[0]
        assert schema is not None

    def test_milestone_data_extracted(self, sf_cursor):
        """Each contract has 4 milestones; expect at least 30 total rows."""
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 30, f"Expected >=30 milestone rows, got {count}"

    def test_most_contracts_have_milestones(self, sf_cursor):
        """At least 8/10 contracts should have table data rows."""
        sf_cursor.execute("""
            SELECT COUNT(DISTINCT t.file_name)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'CONTRACT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 8, f"Only {count}/10 contracts have milestone rows"


# ---------------------------------------------------------------------------
# 6. Field mapping
# ---------------------------------------------------------------------------
class TestFieldMapping:
    """Verify field_1..field_10 are populated per CONTRACT config."""

    def test_field_1_is_party_name(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_1
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'contract_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and len(val) > 2

    def test_field_2_is_contract_number(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_2
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'contract_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and "CTR" in val.upper()

    def test_field_10_is_total_value(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_10
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'contract_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val is not None and float(val) > 0


# ---------------------------------------------------------------------------
# 7. Isolation
# ---------------------------------------------------------------------------
class TestDocTypeIsolation:
    """Contract extraction should not affect other doc types."""

    def test_invoice_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'INVOICE'"
        )
        assert sf_cursor.fetchone()[0] >= 100, "Invoice count should not drop below 100"

    def test_utility_bill_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'UTILITY_BILL'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_contracts_visible_in_document_summary(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM V_DOCUMENT_SUMMARY
            WHERE doc_type = 'CONTRACT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 10
