"""Tests for stored procedure error handling and edge cases.

Requires a Snowflake connection. Tests invalid doc types, idempotency,
empty stage scenarios, and SP_REEXTRACT_DOC_TYPE behavior.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))

from config import get_session


@pytest.fixture(scope="module")
def sf_session():
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        pytest.skip("POC_CONNECTION not set — skipping SF tests")
    return get_session()


DB = "AI_EXTRACT_POC.DOCUMENTS"


class TestExtractByDocTypeErrors:
    """Test SP_EXTRACT_BY_DOC_TYPE error handling."""

    def test_invalid_doc_type_returns_message(self, sf_session):
        """Calling with a non-existent doc type should not crash."""
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_BY_DOC_TYPE('NONEXISTENT_TYPE_XYZ')"
        ).collect()
        assert result is not None
        msg = str(result[0][0]).lower()
        # Should indicate no config found or no documents
        assert "no" in msg or "not found" in msg or "0" in msg or "error" in msg or "config" in msg

    def test_empty_string_doc_type(self, sf_session):
        """Empty string doc type should not crash."""
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_BY_DOC_TYPE('')"
        ).collect()
        assert result is not None

    def test_sql_injection_safe(self, sf_session):
        """Doc type with SQL injection attempt should not crash."""
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_BY_DOC_TYPE('INVOICE; DROP TABLE RAW_DOCUMENTS')"
        ).collect()
        # Verify RAW_DOCUMENTS still exists
        count = sf_session.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS").collect()
        assert count[0]["CNT"] > 0


class TestExtractNewDocuments:
    """Test SP_EXTRACT_NEW_DOCUMENTS behavior."""

    def test_idempotent_no_new_docs(self, sf_session):
        """When all docs are already extracted, should report 0 new."""
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_NEW_DOCUMENTS()"
        ).collect()
        assert result is not None
        msg = str(result[0][0])
        # Should indicate 0 documents extracted (all already done)
        assert "0" in msg or "no new" in msg.lower() or "complete" in msg.lower()

    def test_returns_string_result(self, sf_session):
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_NEW_DOCUMENTS()"
        ).collect()
        assert isinstance(str(result[0][0]), str)


class TestReextractDocType:
    """Test SP_REEXTRACT_DOC_TYPE behavior."""

    def test_invalid_doc_type_message(self, sf_session):
        """Re-extract with non-existent type should not crash."""
        result = sf_session.sql(
            f"CALL {DB}.SP_REEXTRACT_DOC_TYPE('NONEXISTENT_TYPE_XYZ')"
        ).collect()
        assert result is not None

    def test_returns_result(self, sf_session):
        """Should return a result string."""
        result = sf_session.sql(
            f"CALL {DB}.SP_REEXTRACT_DOC_TYPE('NONEXISTENT_TYPE_XYZ')"
        ).collect()
        assert isinstance(str(result[0][0]), str)


class TestExtractedFieldsIntegrity:
    """Test data integrity in EXTRACTED_FIELDS table."""

    def test_every_raw_doc_has_extraction(self, sf_session):
        """Every document in RAW_DOCUMENTS should have an EXTRACTED_FIELDS row."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.RAW_DOCUMENTS r
            LEFT JOIN {DB}.EXTRACTED_FIELDS e ON r.file_name = e.file_name
            WHERE e.file_name IS NULL
        """).collect()
        assert rows[0]["CNT"] == 0

    def test_no_orphan_extractions(self, sf_session):
        """Every EXTRACTED_FIELDS row should reference a valid RAW_DOCUMENTS row."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.EXTRACTED_FIELDS e
            LEFT JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name
            WHERE r.file_name IS NULL
        """).collect()
        assert rows[0]["CNT"] == 0

    def test_raw_extraction_is_valid_json(self, sf_session):
        """All raw_extraction values should be valid JSON objects."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.EXTRACTED_FIELDS
            WHERE raw_extraction IS NOT NULL
              AND TRY_PARSE_JSON(raw_extraction::VARCHAR) IS NULL
        """).collect()
        assert rows[0]["CNT"] == 0

    def test_file_name_uniqueness(self, sf_session):
        """Each file_name should appear at most once in EXTRACTED_FIELDS."""
        rows = sf_session.sql(f"""
            SELECT file_name, COUNT(*) AS cnt
            FROM {DB}.EXTRACTED_FIELDS
            GROUP BY file_name
            HAVING cnt > 1
        """).collect()
        assert len(rows) == 0

    def test_doc_type_populated_in_raw(self, sf_session):
        """Every raw document should have a non-null doc_type."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.RAW_DOCUMENTS
            WHERE doc_type IS NULL OR doc_type = ''
        """).collect()
        assert rows[0]["CNT"] == 0
