"""Tests for Dashboard page (0_Dashboard.py) SQL queries and KPI logic.

Validates that all queries used by the Dashboard page return expected
schema and non-null results. Does NOT render the Streamlit UI.
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
# 1. KPI query
# ---------------------------------------------------------------------------
class TestKPIQuery:
    """Dashboard main KPI query: total docs, total amount, unique senders, overdue."""

    KPI_SQL = """
        SELECT
            COUNT(DISTINCT e.file_name)                   AS total_documents,
            COALESCE(SUM(e.field_8), 0)                   AS total_amount,
            COUNT(DISTINCT e.field_1)                     AS unique_senders,
            COUNT(CASE WHEN e.field_5 < CURRENT_DATE()
                       THEN 1 END)                        AS overdue_count,
            COALESCE(SUM(CASE WHEN e.field_5 < CURRENT_DATE()
                              THEN e.field_8 END), 0)
                                                          AS overdue_amount
        FROM {fq}.EXTRACTED_FIELDS e
        JOIN {fq}.RAW_DOCUMENTS r ON r.file_name = e.file_name
    """

    def test_kpi_returns_one_row(self, sf_cursor):
        sf_cursor.execute(self.KPI_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        assert len(rows) == 1

    def test_total_documents_positive(self, sf_cursor):
        sf_cursor.execute(self.KPI_SQL.format(fq=FQ))
        row = sf_cursor.fetchone()
        assert row[0] > 0, f"total_documents should be > 0, got {row[0]}"

    def test_total_amount_non_negative(self, sf_cursor):
        sf_cursor.execute(self.KPI_SQL.format(fq=FQ))
        row = sf_cursor.fetchone()
        assert row[1] >= 0, f"total_amount should be >= 0, got {row[1]}"

    def test_unique_senders_positive(self, sf_cursor):
        sf_cursor.execute(self.KPI_SQL.format(fq=FQ))
        row = sf_cursor.fetchone()
        assert row[2] > 0, f"unique_senders should be > 0, got {row[2]}"

    def test_kpi_with_doc_type_filter(self, sf_cursor):
        sql = self.KPI_SQL.format(fq=FQ) + " WHERE r.doc_type = %s"
        sf_cursor.execute(sql, ("INVOICE",))
        row = sf_cursor.fetchone()
        assert row[0] > 0, "INVOICE filter should return docs"

    def test_kpi_with_utility_filter(self, sf_cursor):
        sql = self.KPI_SQL.format(fq=FQ) + " WHERE r.doc_type = %s"
        sf_cursor.execute(sql, ("UTILITY_BILL",))
        row = sf_cursor.fetchone()
        assert row[0] == 10, f"Expected 10 utility bills, got {row[0]}"


# ---------------------------------------------------------------------------
# 2. Extraction status query (V_EXTRACTION_STATUS)
# ---------------------------------------------------------------------------
class TestExtractionStatus:
    """Dashboard pipeline progress query."""

    def test_v_extraction_status_exists(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT * FROM {FQ}.V_EXTRACTION_STATUS
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1

    def test_status_has_expected_columns(self, sf_cursor):
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_EXTRACTION_STATUS LIMIT 0")
        cols = [d[0] for d in sf_cursor.description]
        for expected in ("TOTAL_FILES", "EXTRACTED_FILES", "PENDING_FILES", "FAILED_FILES"):
            assert expected in cols, f"Missing column {expected} in V_EXTRACTION_STATUS"

    def test_status_totals_match(self, sf_cursor):
        """TOTAL_FILES should equal EXTRACTED_FILES + PENDING_FILES for the overall row."""
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_EXTRACTION_STATUS")
        cols = [d[0] for d in sf_cursor.description]
        for row in sf_cursor.fetchall():
            row_dict = dict(zip(cols, row))
            total = row_dict["TOTAL_FILES"]
            extracted = row_dict["EXTRACTED_FILES"]
            pending = row_dict["PENDING_FILES"]
            assert total == extracted + pending, \
                f"{total} != {extracted} + {pending}"


# ---------------------------------------------------------------------------
# 3. Recent documents query
# ---------------------------------------------------------------------------
class TestRecentDocuments:
    """Dashboard recent documents listing."""

    def test_recent_docs_query(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT
                r.file_name,
                r.doc_type,
                e.field_1 AS sender,
                e.field_8 AS amount,
                e.status,
                r.extracted_at
            FROM {FQ}.EXTRACTED_FIELDS e
            JOIN {FQ}.RAW_DOCUMENTS r ON r.file_name = e.file_name
            ORDER BY r.extracted_at DESC NULLS LAST
            LIMIT 15
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) > 0, "Recent docs query should return results"
        assert len(rows) <= 15, "Should be limited to 15"

    def test_recent_docs_have_file_names(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT r.file_name
            FROM {FQ}.EXTRACTED_FIELDS e
            JOIN {FQ}.RAW_DOCUMENTS r ON r.file_name = e.file_name
            ORDER BY r.extracted_at DESC NULLS LAST
            LIMIT 15
        """)
        for row in sf_cursor.fetchall():
            assert row[0] is not None and len(row[0]) > 0


# ---------------------------------------------------------------------------
# 4. Doc type filter
# ---------------------------------------------------------------------------
class TestDocTypeFilter:
    """Dashboard doc type filter dropdown data."""

    def test_distinct_doc_types(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT DISTINCT doc_type
            FROM {FQ}.RAW_DOCUMENTS
            ORDER BY doc_type
        """)
        types = [r[0] for r in sf_cursor.fetchall()]
        assert "INVOICE" in types
        assert "UTILITY_BILL" in types
