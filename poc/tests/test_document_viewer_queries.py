"""Tests for Document Viewer page (1_Document_Viewer.py) SQL queries.

Validates filtering, detail drill-down, and dynamic field rendering queries.
Does NOT render the Streamlit UI.
"""

import json
import os
import re

import pytest


pytestmark = pytest.mark.sql

CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{POC_DB}.{POC_SCHEMA}"


@pytest.fixture(scope="session")
def sf_cursor():
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {POC_ROLE}")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# 1. Document listing with filters
# ---------------------------------------------------------------------------
class TestDocumentListing:
    """Document Viewer: main listing query with filter options."""

    BASE_SQL = """
        SELECT
            r.file_name,
            r.doc_type,
            e.field_1 AS sender,
            e.field_8 AS amount,
            e.field_4 AS invoice_date,
            e.field_5 AS due_date,
            e.status,
            r.extracted_at
        FROM {fq}.EXTRACTED_FIELDS e
        JOIN {fq}.RAW_DOCUMENTS r ON r.file_name = e.file_name
    """

    def test_unfiltered_returns_all(self, sf_cursor):
        sf_cursor.execute(self.BASE_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        assert len(rows) >= 100, f"Expected >= 100 docs, got {len(rows)}"

    def test_filter_by_doc_type(self, sf_cursor):
        sql = self.BASE_SQL.format(fq=FQ) + " WHERE r.doc_type = %s"
        sf_cursor.execute(sql, ("INVOICE",))
        rows = sf_cursor.fetchall()
        assert len(rows) >= 100, f"Expected >= 100 invoices, got {len(rows)}"

    def test_filter_by_utility_bill(self, sf_cursor):
        sql = self.BASE_SQL.format(fq=FQ) + " WHERE r.doc_type = %s"
        sf_cursor.execute(sql, ("UTILITY_BILL",))
        rows = sf_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("No UTILITY_BILL data in deployment")
        assert len(rows) == 10

    def test_filter_by_sender(self, sf_cursor):
        sql = self.BASE_SQL.format(fq=FQ) + " WHERE e.field_1 LIKE %s"
        sf_cursor.execute(sql, ("%Edison%",))
        rows = sf_cursor.fetchall()
        # Edison may be a utility company (UTILITY_BILL) or not present in invoice-only deployments
        if len(rows) == 0:
            # Verify there is at least some sender-based filtering working
            sf_cursor.execute(
                self.BASE_SQL.format(fq=FQ) + " WHERE e.field_1 IS NOT NULL LIMIT 1"
            )
            fallback = sf_cursor.fetchall()
            assert len(fallback) >= 1, "No extracted documents have field_1 populated"
            pytest.skip("No Edison docs found — expected only in multi-doc deployments")
        assert len(rows) >= 1, "Should find at least 1 Edison doc"

    def test_filter_by_status(self, sf_cursor):
        sql = self.BASE_SQL.format(fq=FQ) + " WHERE e.status = %s"
        sf_cursor.execute(sql, ("EXTRACTED",))
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1


# ---------------------------------------------------------------------------
# 2. Document detail drill-down
# ---------------------------------------------------------------------------
class TestDocumentDetail:
    """Document Viewer: detail view for a single document."""

    def _get_test_file(self, sf_cursor):
        """Return a file_name to test with, preferring utility_bill_01.pdf."""
        sf_cursor.execute(f"""
            SELECT file_name FROM {FQ}.EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        row = sf_cursor.fetchone()
        if row:
            return row[0]
        # Fall back to first available file
        sf_cursor.execute(f"SELECT file_name FROM {FQ}.EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        assert row is not None, "No extracted files found"
        return row[0]

    def test_detail_by_filename(self, sf_cursor):
        fname = self._get_test_file(sf_cursor)
        sf_cursor.execute(f"""
            SELECT
                e.file_name,
                e.raw_extraction,
                e.field_1, e.field_2, e.field_3, e.field_4, e.field_5,
                e.field_6, e.field_7, e.field_8, e.field_9, e.field_10,
                e.status
            FROM {FQ}.EXTRACTED_FIELDS e
            WHERE e.file_name = %s
        """, (fname,))
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, f"Should find exactly 1 row for {fname}"

    def test_raw_extraction_is_valid_json(self, sf_cursor):
        fname = self._get_test_file(sf_cursor)
        sf_cursor.execute(f"""
            SELECT raw_extraction::VARCHAR
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE file_name = %s
            AND raw_extraction IS NOT NULL
        """, (fname,))
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip(f"No raw_extraction for {fname}")
        parsed = json.loads(row[0])
        assert isinstance(parsed, dict)
        assert len(parsed) > 0

    def test_detail_returns_all_fields(self, sf_cursor):
        fname = self._get_test_file(sf_cursor)
        sf_cursor.execute(f"""
            SELECT *
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE file_name = %s
        """, (fname,))
        cols = [d[0] for d in sf_cursor.description]
        expected = ["FILE_NAME", "FIELD_1", "FIELD_2", "FIELD_3", "RAW_EXTRACTION", "STATUS"]
        for c in expected:
            assert c in cols, f"Missing column {c}"


# ---------------------------------------------------------------------------
# 3. Dynamic field rendering (raw_extraction variant access)
# ---------------------------------------------------------------------------
class TestDynamicFieldRendering:
    """Document Viewer: extracting fields from raw_extraction VARIANT."""

    def test_variant_field_access(self, sf_cursor):
        """Can access individual fields from raw_extraction VARIANT."""
        # Use utility_bill if available, otherwise fall back to any invoice
        sf_cursor.execute(f"""
            SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        has_ub = sf_cursor.fetchone()[0] > 0
        if has_ub:
            sf_cursor.execute(f"""
                SELECT
                    raw_extraction:utility_company::VARCHAR,
                    raw_extraction:account_number::VARCHAR,
                    raw_extraction:total_due::VARCHAR
                FROM {FQ}.EXTRACTED_FIELDS
                WHERE file_name = 'utility_bill_01.pdf'
            """)
            row = sf_cursor.fetchone()
            assert row[0] is not None, "utility_company should not be null"
            assert row[1] is not None, "account_number should not be null"
            assert row[2] is not None, "total_due should not be null"
        else:
            # Fall back: verify variant access works — extract any key from raw_extraction
            sf_cursor.execute(f"""
                SELECT raw_extraction::VARCHAR
                FROM {FQ}.EXTRACTED_FIELDS
                WHERE raw_extraction IS NOT NULL
                LIMIT 1
            """)
            row = sf_cursor.fetchone()
            if row is None:
                pytest.skip("No documents with raw_extraction populated")
            parsed = json.loads(row[0])
            assert isinstance(parsed, dict), "raw_extraction should be a JSON object"
            assert len(parsed) > 0, "raw_extraction should have at least one key"

    def test_confidence_field_access(self, sf_cursor):
        """Can access confidence scores from raw_extraction VARIANT."""
        sf_cursor.execute(f"""
            SELECT file_name,
                   raw_extraction:_confidence::VARCHAR
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE raw_extraction:_confidence IS NOT NULL
            LIMIT 1
        """)
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No documents with _confidence scores found")


# ---------------------------------------------------------------------------
# 4. Line items for a document
# ---------------------------------------------------------------------------
class TestDocumentLineItems:
    """Document Viewer: line items from EXTRACTED_TABLE_DATA."""

    def test_line_items_for_invoice(self, sf_cursor):
        """Invoices should have line item data."""
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM {FQ}.EXTRACTED_TABLE_DATA t
            JOIN {FQ}.RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'INVOICE'
        """)
        count = sf_cursor.fetchone()[0]
        assert count > 0, "Invoices should have line items"

    def test_line_items_ordered_by_line_number(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT line_number
            FROM {FQ}.EXTRACTED_TABLE_DATA
            WHERE file_name = (
                SELECT file_name FROM {FQ}.EXTRACTED_TABLE_DATA LIMIT 1
            )
            ORDER BY line_number
        """)
        nums = [r[0] for r in sf_cursor.fetchall()]
        assert nums == sorted(nums), "Line items should be ordered"


# ---------------------------------------------------------------------------
# 5. V_DOCUMENT_SUMMARY view (used for filter dropdowns)
# ---------------------------------------------------------------------------
class TestDocumentSummaryFilters:
    """Document Viewer uses V_DOCUMENT_SUMMARY for filter data."""

    def test_distinct_senders(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT DISTINCT VENDOR_NAME
            FROM {FQ}.V_DOCUMENT_SUMMARY
            WHERE VENDOR_NAME IS NOT NULL
            ORDER BY VENDOR_NAME
        """)
        senders = sf_cursor.fetchall()
        assert len(senders) >= 2, "Should have multiple senders for filtering"

    def test_distinct_statuses(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT DISTINCT EXTRACTION_STATUS
            FROM {FQ}.V_DOCUMENT_SUMMARY
            ORDER BY EXTRACTION_STATUS
        """)
        statuses = [r[0] for r in sf_cursor.fetchall()]
        assert len(statuses) >= 1
        assert "EXTRACTED" in statuses
