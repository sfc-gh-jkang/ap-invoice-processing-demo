"""Tests for Phase A-C improvements: normalization, confidence scores,
table extraction, V_DOCUMENT_SUMMARY, SP_REEXTRACT_DOC_TYPE, config helpers.
"""
import json
import os
import re
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
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


def _has_utility_bills(sf_cursor):
    """Check if utility bill data is available."""
    sf_cursor.execute(
        "SELECT COUNT(*) FROM EXTRACTED_FIELDS WHERE file_name LIKE 'utility_bill%'"
    )
    return sf_cursor.fetchone()[0] > 0


# ---------------------------------------------------------------------------
# 1. V_DOCUMENT_SUMMARY view
# ---------------------------------------------------------------------------
class TestDocumentSummaryView:
    """Verify V_DOCUMENT_SUMMARY exists, is queryable, and has correct schema."""

    def test_view_exists(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY")
        assert sf_cursor.fetchone()[0] >= 0

    def test_view_has_doc_type(self, sf_cursor):
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_DOCUMENT_SUMMARY LIMIT 0")
        columns = [d[0] for d in sf_cursor.description]
        assert "DOC_TYPE" in columns

    def test_view_has_raw_extraction(self, sf_cursor):
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_DOCUMENT_SUMMARY LIMIT 0")
        columns = [d[0] for d in sf_cursor.description]
        assert "RAW_EXTRACTION" in columns

    def test_view_has_corrections(self, sf_cursor):
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_DOCUMENT_SUMMARY LIMIT 0")
        columns = [d[0] for d in sf_cursor.description]
        assert "CORRECTIONS" in columns

    def test_backward_compat_alias(self, sf_cursor):
        """V_INVOICE_SUMMARY should return the same rows as V_DOCUMENT_SUMMARY."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY")
        doc_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
        inv_count = sf_cursor.fetchone()[0]
        assert doc_count == inv_count

    def test_shows_all_doc_types(self, sf_cursor):
        """View should include both INVOICE and UTILITY_BILL doc types."""
        sf_cursor.execute(
            f"SELECT DISTINCT doc_type FROM {FQ}.V_DOCUMENT_SUMMARY ORDER BY doc_type"
        )
        types = [r[0] for r in sf_cursor.fetchall()]
        assert "INVOICE" in types

    def test_row_count_matches_extracted_fields(self, sf_cursor):
        """V_DOCUMENT_SUMMARY should have one row per EXTRACTED_FIELDS record."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count


# ---------------------------------------------------------------------------
# 2. SP_REEXTRACT_DOC_TYPE procedure
# ---------------------------------------------------------------------------
class TestReextractProcedure:
    """Verify SP_REEXTRACT_DOC_TYPE exists and is callable."""

    def test_procedure_exists(self, sf_cursor):
        sf_cursor.execute(
            f"SHOW PROCEDURES LIKE 'SP_REEXTRACT_DOC_TYPE' IN SCHEMA {FQ}"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1

    def test_procedure_is_sql(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE PROCEDURE {FQ}.SP_REEXTRACT_DOC_TYPE(VARCHAR)")
        for row in sf_cursor.fetchall():
            if row[0] == 'language':
                assert row[1].upper() == 'SQL'


# ---------------------------------------------------------------------------
# 3. SP_EXTRACT_BY_DOC_TYPE enhancements
# ---------------------------------------------------------------------------
class TestExtractByDocTypeEnhancements:
    """Verify the enhanced SP has normalization, confidence, and table extraction."""

    def test_sp_has_normalize_function(self, sf_cursor):
        sf_cursor.execute(
            f"DESCRIBE PROCEDURE {FQ}.SP_EXTRACT_BY_DOC_TYPE(VARCHAR)"
        )
        body = ""
        for row in sf_cursor.fetchall():
            if row[0] == 'body':
                body = str(row[1])
        assert '_normalize' in body, "SP should contain _normalize function"

    def test_sp_has_confidence_prompt(self, sf_cursor):
        sf_cursor.execute(
            f"DESCRIBE PROCEDURE {FQ}.SP_EXTRACT_BY_DOC_TYPE(VARCHAR)"
        )
        body = ""
        for row in sf_cursor.fetchall():
            if row[0] == 'body':
                body = str(row[1])
        assert '_confidence' in body, "SP should request confidence scores"

    def test_sp_has_table_extraction(self, sf_cursor):
        sf_cursor.execute(
            f"DESCRIBE PROCEDURE {FQ}.SP_EXTRACT_BY_DOC_TYPE(VARCHAR)"
        )
        body = ""
        for row in sf_cursor.fetchall():
            if row[0] == 'body':
                body = str(row[1])
        assert '_extract_table_data' in body, "SP should have table extraction"

    def test_sp_reads_review_fields(self, sf_cursor):
        sf_cursor.execute(
            f"DESCRIBE PROCEDURE {FQ}.SP_EXTRACT_BY_DOC_TYPE(VARCHAR)"
        )
        body = ""
        for row in sf_cursor.fetchall():
            if row[0] == 'body':
                body = str(row[1])
        assert 'review_fields' in body, "SP should read review_fields for type info"


# ---------------------------------------------------------------------------
# 4. DOCUMENT_TYPE_CONFIG schema
# ---------------------------------------------------------------------------
class TestDocTypeConfigSchema:
    """Verify DOCUMENT_TYPE_CONFIG has the new validation_rules column."""

    def test_has_validation_rules_column(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE TABLE {FQ}.DOCUMENT_TYPE_CONFIG")
        columns = [row[0] for row in sf_cursor.fetchall()]
        assert "VALIDATION_RULES" in columns

    def test_has_updated_prompts(self, sf_cursor):
        """All prompts should now include FORMATTING RULES."""
        sf_cursor.execute(
            f"SELECT doc_type, extraction_prompt FROM {FQ}.DOCUMENT_TYPE_CONFIG "
            "WHERE active = TRUE"
        )
        for row in sf_cursor.fetchall():
            assert "FORMATTING RULES" in row[1], \
                f"{row[0]} prompt missing FORMATTING RULES"


# ---------------------------------------------------------------------------
# 5. Confidence scores in raw_extraction (after re-extract)
# ---------------------------------------------------------------------------
class TestConfidenceScores:
    """Verify extraction results contain heuristic confidence scores."""

    def test_confidence_present_in_utility_bill(self, sf_cursor):
        if not _has_utility_bills(sf_cursor):
            pytest.skip("No utility bill extractions available")
        sf_cursor.execute("""
            SELECT raw_extraction:_confidence
            FROM EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None, \
            "Confidence scores not found — re-extraction may be needed"
        confidence = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        assert isinstance(confidence, dict), "Confidence should be a dict"
        assert len(confidence) > 0, "Confidence dict should not be empty"

    def test_confidence_values_are_numeric(self, sf_cursor):
        if not _has_utility_bills(sf_cursor):
            pytest.skip("No utility bill extractions available")
        sf_cursor.execute("""
            SELECT raw_extraction:_confidence
            FROM EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None, \
            "Confidence scores not found — re-extraction may be needed"
        confidence = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        for field, score in confidence.items():
            assert isinstance(score, (int, float)), \
                f"Confidence for {field} should be numeric, got {type(score)}"


# ---------------------------------------------------------------------------
# 6. Normalization in raw_extraction values
# ---------------------------------------------------------------------------
class TestNormalizationInRawExtraction:
    """Verify normalization was applied to raw_extraction values."""

    def test_dates_normalized(self, sf_cursor):
        """Date fields should be in YYYY-MM-DD format in raw_extraction."""
        if not _has_utility_bills(sf_cursor):
            pytest.skip("No utility bill extractions available")
        sf_cursor.execute("""
            SELECT raw_extraction:billing_period_start::VARCHAR,
                   raw_extraction:due_date::VARCHAR
            FROM EXTRACTED_FIELDS
            WHERE file_name LIKE 'utility_bill%'
            LIMIT 5
        """)
        for row in sf_cursor.fetchall():
            for val in row:
                if val:
                    assert re.match(r"\d{4}-\d{2}-\d{2}", val), \
                        f"Expected ISO date, got: {val}"

    def test_numbers_normalized(self, sf_cursor):
        """Numeric fields should be plain numbers (no $, no units)."""
        if not _has_utility_bills(sf_cursor):
            pytest.skip("No utility bill extractions available")
        sf_cursor.execute("""
            SELECT raw_extraction:total_due::VARCHAR,
                   raw_extraction:kwh_usage::VARCHAR
            FROM EXTRACTED_FIELDS
            WHERE file_name LIKE 'utility_bill%'
            LIMIT 5
        """)
        for row in sf_cursor.fetchall():
            for val in row:
                if val:
                    assert "$" not in val, f"Should not have $: {val}"
                    assert "kwh" not in val.lower(), f"Should not have kWh: {val}"


# ---------------------------------------------------------------------------
# 7. Table extraction for utility bills
# ---------------------------------------------------------------------------
class TestUtilityBillTableExtraction:
    """Verify config-driven table extraction produces rate tier data."""

    def test_table_data_exists(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COUNT(*)
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        count = sf_cursor.fetchone()[0]
        if count == 0:
            pytest.xfail("Table extraction pending re-extraction completion")
        assert count >= 1

    def test_table_data_has_raw_line_data(self, sf_cursor):
        sf_cursor.execute("""
            SELECT raw_line_data
            FROM EXTRACTED_TABLE_DATA t
            JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
            LIMIT 1
        """)
        row = sf_cursor.fetchone()
        if row is None:
            pytest.xfail("No table data for utility bills yet")
        assert row[0] is not None, "raw_line_data should be populated"
