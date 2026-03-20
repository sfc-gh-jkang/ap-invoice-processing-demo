"""Receipt Extraction Tests — validate extraction quality for RECEIPT doc type.

Tests cover:
  1. All 10 receipts were extracted (rows exist in EXTRACTED_FIELDS)
  2. RAW_EXTRACTION contains all expected fields
  3. Field-level accuracy: merchant names, amounts, payment methods
  4. Line item table data extraction
  5. Field_1..field_10 mapping from config labels
  6. Isolation from other doc types
"""

import json
import re

import pytest


pytestmark = pytest.mark.sql


@pytest.fixture(autouse=True, scope="session")
def _skip_if_no_receipts(sf_cursor):
    """Skip all tests in this module if no RECEIPT data exists."""
    sf_cursor.execute(
        "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'RECEIPT'"
    )
    count = sf_cursor.fetchone()[0]
    if count == 0:
        pytest.skip("No RECEIPT data in deployment — skipping receipt tests")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
RECEIPT_FILES = [f"receipt_{i:02d}.pdf" for i in range(1, 11)]

EXPECTED_RAW_FIELDS = [
    "merchant_name", "receipt_number", "transaction_id",
    "purchase_date", "return_by_date", "payment_method",
    "subtotal", "tax_amount", "total_paid",
]

KNOWN_MERCHANTS = [
    "quickstop", "garden state mart", "7-eleven", "wawa", "hudson news",
]


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
    """Verify all 10 receipts have extraction rows."""

    def test_all_10_receipts_registered(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'RECEIPT'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_all_10_receipts_extracted(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        assert sf_cursor.fetchone()[0] == 10

    def test_all_receipts_marked_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'RECEIPT' AND extracted = TRUE"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(
            "SELECT file_name, extraction_error FROM RAW_DOCUMENTS "
            "WHERE doc_type = 'RECEIPT' AND extraction_error IS NOT NULL"
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
            WHERE r.doc_type = 'RECEIPT' AND e.raw_extraction IS NULL
        """)
        assert sf_cursor.fetchone()[0] == 0

    @pytest.mark.parametrize("field", EXPECTED_RAW_FIELDS)
    def test_raw_extraction_has_field(self, sf_cursor, field):
        """Each raw_extraction should contain the expected field key."""
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
              AND e.raw_extraction:{field} IS NOT NULL
        """)
        count = sf_cursor.fetchone()[0]
        # buyer and return_by_date may not always be extracted
        if field == "return_by_date":
            assert count >= 5, (
                f"Field '{field}' present in only {count}/10 receipts (expected >=5)"
            )
        else:
            assert count >= 8, (
                f"Field '{field}' present in only {count}/10 receipts (expected >=8)"
            )


# ---------------------------------------------------------------------------
# 3. Field-level accuracy
# ---------------------------------------------------------------------------
class TestFieldAccuracy:
    """Spot-check extracted values for known receipts."""

    def test_receipt_numbers_format(self, sf_cursor):
        """Receipt numbers should follow RMMDD-XXXX pattern."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:receipt_number::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        matched = 0
        for row in sf_cursor.fetchall():
            val = row[1] or ""
            if re.match(r"R\d{4}-\d{4}", val):
                matched += 1
        assert matched >= 8, f"Only {matched}/10 receipts have correct receipt_number format"

    def test_transaction_ids_format(self, sf_cursor):
        """Transaction IDs should follow TXN followed by digits."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:transaction_id::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        matched = 0
        for row in sf_cursor.fetchall():
            val = row[1] or ""
            if "TXN" in val.upper():
                matched += 1
        assert matched >= 8, f"Only {matched}/10 receipts have TXN-format transaction_id"

    def test_total_paid_is_positive(self, sf_cursor):
        """All total_paid amounts should be positive."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:total_paid::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            assert val is not None and val > 0, (
                f"{row[0]}: total_paid should be positive, got: {row[1]}"
            )

    def test_subtotal_lte_total_paid(self, sf_cursor):
        """Subtotal should be <= total_paid (total = subtotal + tax)."""
        sf_cursor.execute("""
            SELECT e.file_name,
                   e.raw_extraction:subtotal::VARCHAR,
                   e.raw_extraction:total_paid::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        checks = 0
        for row in sf_cursor.fetchall():
            sub = _normalize_numeric(row[1])
            total = _normalize_numeric(row[2])
            if sub is not None and total is not None:
                assert sub <= total + 0.01, (
                    f"{row[0]}: subtotal ({sub}) > total_paid ({total})"
                )
                checks += 1
        assert checks >= 8, f"Only verified {checks} receipts"

    def test_tax_amount_is_non_negative(self, sf_cursor):
        """Tax amounts should be >= 0."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:tax_amount::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        for row in sf_cursor.fetchall():
            val = _normalize_numeric(row[1])
            if val is not None:
                assert val >= 0, (
                    f"{row[0]}: tax_amount should be >= 0, got: {row[1]}"
                )

    def test_payment_method_recognized(self, sf_cursor):
        """Payment methods should be one of the known types."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:payment_method::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        known = ["visa", "mastercard", "amex", "cash", "debit", "apple pay"]
        recognized = 0
        for row in sf_cursor.fetchall():
            val = (row[1] or "").lower()
            if any(k in val for k in known):
                recognized += 1
        assert recognized >= 8, f"Only {recognized}/10 receipts have recognized payment method"

    def test_purchase_date_iso_format(self, sf_cursor):
        """Purchase dates should be in ISO YYYY-MM-DD format."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:purchase_date::VARCHAR
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'receipt_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert re.match(r"\d{4}-\d{2}-\d{2}", val), f"Expected ISO date, got: {val}"


# ---------------------------------------------------------------------------
# 4. Merchant recognition
# ---------------------------------------------------------------------------
class TestMerchantRecognition:
    """Verify merchant names are extracted correctly."""

    def test_merchants_extracted(self, sf_cursor):
        """All receipts should have a non-empty merchant_name."""
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:merchant_name::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        for row in sf_cursor.fetchall():
            assert row[1] and len(row[1].strip()) > 0, (
                f"{row[0]}: merchant_name is empty"
            )

    def test_known_merchants_recognized(self, sf_cursor):
        """At least 80% of receipts should have a recognized merchant."""
        sf_cursor.execute("""
            SELECT e.raw_extraction:merchant_name::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        recognized = 0
        for row in sf_cursor.fetchall():
            val = (row[0] or "").lower()
            if any(m in val for m in KNOWN_MERCHANTS):
                recognized += 1
        assert recognized >= 8, f"Only {recognized}/10 receipts have recognized merchant"


# ---------------------------------------------------------------------------
# 5. Line item table data
# ---------------------------------------------------------------------------
class TestLineItemTableData:
    """Verify EXTRACTED_TABLE_DATA for receipt line items."""

    def test_table_extraction_schema_configured(self, sf_cursor):
        sf_cursor.execute("""
            SELECT table_extraction_schema
            FROM DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'RECEIPT'
        """)
        schema = sf_cursor.fetchone()[0]
        assert schema is not None

    def test_line_items_extracted(self, sf_cursor):
        """Receipts have 2-8 items each; expect at least 20 total rows."""
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 20, f"Expected >=20 line item rows, got {count}"

    def test_most_receipts_have_line_items(self, sf_cursor):
        """At least 8/10 receipts should have table data rows."""
        sf_cursor.execute("""
            SELECT COUNT(DISTINCT t.file_name)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'RECEIPT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count >= 8, f"Only {count}/10 receipts have line item rows"


# ---------------------------------------------------------------------------
# 6. Field mapping
# ---------------------------------------------------------------------------
class TestFieldMapping:
    """Verify field_1..field_10 are populated per RECEIPT config."""

    def test_field_1_is_merchant_name(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_1
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'receipt_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and len(val) > 2

    def test_field_2_is_receipt_number(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_2
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'receipt_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val and "R" in val.upper()

    def test_field_10_is_total_paid(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.field_10
            FROM EXTRACTED_FIELDS e
            WHERE e.file_name = 'receipt_01.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert val is not None and float(val) > 0


# ---------------------------------------------------------------------------
# 7. Isolation
# ---------------------------------------------------------------------------
class TestDocTypeIsolation:
    """Receipt extraction should not affect other doc types."""

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

    def test_contract_count_unchanged(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE doc_type = 'CONTRACT'"
        )
        assert sf_cursor.fetchone()[0] == 10

    def test_receipts_visible_in_document_summary(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM V_DOCUMENT_SUMMARY
            WHERE doc_type = 'RECEIPT'
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 10
