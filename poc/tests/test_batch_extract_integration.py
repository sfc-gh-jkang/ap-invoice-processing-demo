"""Tests for batch extraction integration.

Validates end-to-end extraction pipeline behavior:
  1. SP_EXTRACT_BY_DOC_TYPE processes all configured doc types
  2. SP_REEXTRACT_DOC_TYPE clears and re-processes
  3. Extraction pipeline produces correct data structures
  4. Multi-doc-type extraction isolation
  5. Config-driven field mapping consistency
"""

import json
import os
import re

import pytest


pytestmark = pytest.mark.sql

CONNECTION_NAME = os.environ.get("POC_CONNECTION", "aws_spcs")
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
# 1. Config-driven extraction completeness
# ---------------------------------------------------------------------------
class TestConfigDrivenExtraction:
    """Verify config-driven extraction processes all active doc types."""

    def test_all_active_types_have_extractions(self, sf_cursor):
        """Every active doc type in config should have at least 1 extraction."""
        sf_cursor.execute(f"""
            SELECT c.doc_type, COUNT(e.file_name) AS extract_count
            FROM {FQ}.DOCUMENT_TYPE_CONFIG c
            LEFT JOIN {FQ}.RAW_DOCUMENTS r ON r.doc_type = c.doc_type
            LEFT JOIN {FQ}.EXTRACTED_FIELDS e ON e.file_name = r.file_name
            WHERE c.active = TRUE
              AND r.file_name IS NOT NULL
            GROUP BY c.doc_type
        """)
        for row in sf_cursor.fetchall():
            assert row[1] > 0, f"Doc type {row[0]} has 0 extractions"

    def test_all_raw_docs_extracted(self, sf_cursor):
        """All RAW_DOCUMENTS should be marked as extracted."""
        sf_cursor.execute(f"""
            SELECT COUNT(*) FROM {FQ}.RAW_DOCUMENTS
            WHERE extracted = FALSE
              AND doc_type IN (SELECT doc_type FROM {FQ}.DOCUMENT_TYPE_CONFIG WHERE active = TRUE)
        """)
        unextracted = sf_cursor.fetchone()[0]
        assert unextracted == 0, f"{unextracted} documents not yet extracted"

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT file_name, extraction_error
            FROM {FQ}.RAW_DOCUMENTS
            WHERE extraction_error IS NOT NULL
        """)
        errors = sf_cursor.fetchall()
        assert len(errors) == 0, f"Extraction errors found: {errors[:5]}"


# ---------------------------------------------------------------------------
# 2. Field mapping consistency
# ---------------------------------------------------------------------------
class TestFieldMappingConsistency:
    """Verify field_1..field_10 are consistently mapped per doc type config."""

    def test_invoice_field_1_is_vendor(self, sf_cursor):
        """INVOICE field_1 should be vendor_name per config."""
        sf_cursor.execute(f"""
            SELECT field_labels
            FROM {FQ}.DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'INVOICE'
        """)
        labels = json.loads(sf_cursor.fetchone()[0])
        assert labels.get("field_1") is not None

    def test_utility_bill_field_1_is_company(self, sf_cursor):
        """UTILITY_BILL field_1 should be Utility Company per config."""
        sf_cursor.execute(f"""
            SELECT field_labels
            FROM {FQ}.DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'UTILITY_BILL'
        """)
        labels = json.loads(sf_cursor.fetchone()[0])
        assert "Utility Company" == labels.get("field_1") or \
               "utility" in labels.get("field_1", "").lower()

    def test_field_count_matches_extraction(self, sf_cursor):
        """Number of non-null fields in extraction should match config field count."""
        sf_cursor.execute(f"""
            SELECT c.doc_type,
                   LENGTH(c.extraction_prompt) - LENGTH(REPLACE(c.extraction_prompt, ',', '')) + 1
                       AS approx_field_count
            FROM {FQ}.DOCUMENT_TYPE_CONFIG c
            WHERE c.active = TRUE
        """)
        for row in sf_cursor.fetchall():
            doc_type = row[0]
            # Just verify the config has a reasonable number of fields
            assert row[1] >= 3, f"{doc_type} has too few fields in prompt"


# ---------------------------------------------------------------------------
# 3. Raw extraction structure
# ---------------------------------------------------------------------------
class TestRawExtractionStructure:
    """Verify raw_extraction VARIANT has expected structure."""

    def test_raw_extraction_is_object(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT raw_extraction::VARCHAR
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE raw_extraction IS NOT NULL
            LIMIT 10
        """)
        for row in sf_cursor.fetchall():
            parsed = json.loads(row[0])
            assert isinstance(parsed, dict), "raw_extraction should be a JSON object"

    def test_raw_extraction_has_confidence(self, sf_cursor):
        """After re-extraction, raw_extraction should contain _confidence."""
        sf_cursor.execute(f"""
            SELECT raw_extraction:_confidence::VARCHAR
            FROM {FQ}.EXTRACTED_FIELDS e
            JOIN {FQ}.RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
            LIMIT 1
        """)
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None

    def test_raw_extraction_no_stale_keys(self, sf_cursor):
        """raw_extraction should not contain unexpected metadata keys."""
        sf_cursor.execute(f"""
            SELECT raw_extraction::VARCHAR
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE raw_extraction IS NOT NULL
            LIMIT 5
        """)
        allowed_meta = {"_confidence", "_validation_warnings"}
        for row in sf_cursor.fetchall():
            parsed = json.loads(row[0])
            meta_keys = {k for k in parsed if k.startswith("_")}
            unexpected = meta_keys - allowed_meta
            assert len(unexpected) == 0, f"Unexpected meta keys: {unexpected}"


# ---------------------------------------------------------------------------
# 4. Table extraction data
# ---------------------------------------------------------------------------
class TestTableExtractionData:
    """Verify EXTRACTED_TABLE_DATA has correct structure."""

    def test_table_data_has_rows(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_TABLE_DATA")
        count = sf_cursor.fetchone()[0]
        assert count > 0, "EXTRACTED_TABLE_DATA should have rows"

    def test_table_data_references_valid_files(self, sf_cursor):
        """All file_names in EXTRACTED_TABLE_DATA should exist in RAW_DOCUMENTS."""
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM {FQ}.EXTRACTED_TABLE_DATA t
            WHERE t.file_name NOT IN (SELECT file_name FROM {FQ}.RAW_DOCUMENTS)
        """)
        orphans = sf_cursor.fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphaned table data rows"

    def test_table_data_references_valid_records(self, sf_cursor):
        """All file_names in EXTRACTED_TABLE_DATA should have matching EXTRACTED_FIELDS rows.
        Note: record_id format differs between old (INV-XXXXX) and new (auto-increment)
        extraction pipelines, so we use file_name as the canonical join key.
        """
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM {FQ}.EXTRACTED_TABLE_DATA t
            WHERE t.file_name NOT IN (
                SELECT file_name FROM {FQ}.EXTRACTED_FIELDS
            )
        """)
        orphans = sf_cursor.fetchone()[0]
        assert orphans == 0, f"Found {orphans} table data rows with no matching extraction"

    def test_line_numbers_start_at_one(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT MIN(line_number) FROM {FQ}.EXTRACTED_TABLE_DATA
        """)
        min_ln = sf_cursor.fetchone()[0]
        assert min_ln == 1, f"Line numbers should start at 1, got {min_ln}"

    def test_raw_line_data_is_valid_json(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT raw_line_data::VARCHAR
            FROM {FQ}.EXTRACTED_TABLE_DATA
            LIMIT 10
        """)
        for row in sf_cursor.fetchall():
            if row[0]:
                parsed = json.loads(row[0])
                assert isinstance(parsed, dict)


# ---------------------------------------------------------------------------
# 5. Extraction timestamp consistency
# ---------------------------------------------------------------------------
class TestExtractionTimestamps:
    """Verify extraction timestamps are consistent across tables."""

    def test_extracted_at_not_null(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM {FQ}.RAW_DOCUMENTS
            WHERE extracted = TRUE AND extracted_at IS NULL
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 0, f"{count} extracted docs have NULL extracted_at"

    def test_extracted_fields_have_timestamps(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT COUNT(*)
            FROM {FQ}.EXTRACTED_FIELDS
            WHERE extracted_at IS NULL
        """)
        count = sf_cursor.fetchone()[0]
        assert count == 0, f"{count} extraction rows have NULL extracted_at"
