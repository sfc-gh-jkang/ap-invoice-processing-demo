"""Tests for Analytics page (2_Analytics.py) SQL queries.

Validates that all queries used by the Analytics page return expected
schema and reasonable data. Does NOT render the Streamlit UI.
"""

import json
import os

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
# 1. Amount by sender (top 15 bar chart)
# ---------------------------------------------------------------------------
class TestAmountBySender:
    """Analytics: Amount by sender (top 15)."""

    SENDER_SQL = """
        SELECT
            e.field_1 AS sender,
            SUM(e.field_8) AS total_amount
        FROM {fq}.EXTRACTED_FIELDS e
        JOIN {fq}.RAW_DOCUMENTS r ON r.file_name = e.file_name
        WHERE e.field_1 IS NOT NULL
        GROUP BY e.field_1
        ORDER BY total_amount DESC
        LIMIT 15
    """

    def test_returns_rows(self, sf_cursor):
        sf_cursor.execute(self.SENDER_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        assert len(rows) > 0, "Amount by sender should return results"

    def test_amounts_non_negative(self, sf_cursor):
        sf_cursor.execute(self.SENDER_SQL.format(fq=FQ))
        for row in sf_cursor.fetchall():
            if row[1] is not None:
                assert row[1] >= 0, f"Negative amount for sender {row[0]}: {row[1]}"

    def test_max_15_results(self, sf_cursor):
        sf_cursor.execute(self.SENDER_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        assert len(rows) <= 15

    def test_with_doc_type_filter(self, sf_cursor):
        sql = """
            SELECT e.field_1 AS sender, SUM(e.field_8) AS total
            FROM {fq}.EXTRACTED_FIELDS e
            JOIN {fq}.RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE e.field_1 IS NOT NULL AND r.doc_type = %s
            GROUP BY e.field_1
            ORDER BY total DESC
            LIMIT 15
        """.format(fq=FQ)
        sf_cursor.execute(sql, ("INVOICE",))
        rows = sf_cursor.fetchall()
        assert len(rows) > 0


# ---------------------------------------------------------------------------
# 2. Monthly trend (area chart)
# ---------------------------------------------------------------------------
class TestMonthlyTrend:
    """Analytics: Monthly amount trend."""

    TREND_SQL = """
        SELECT
            DATE_TRUNC('MONTH', e.field_4) AS month,
            SUM(e.field_8)                 AS total_amount,
            COUNT(*)                       AS doc_count
        FROM {fq}.EXTRACTED_FIELDS e
        JOIN {fq}.RAW_DOCUMENTS r ON r.file_name = e.file_name
        WHERE e.field_4 IS NOT NULL
        GROUP BY DATE_TRUNC('MONTH', e.field_4)
        ORDER BY month
    """

    def test_trend_returns_rows(self, sf_cursor):
        sf_cursor.execute(self.TREND_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "Monthly trend should return at least 1 month"

    def test_trend_months_are_ordered(self, sf_cursor):
        sf_cursor.execute(self.TREND_SQL.format(fq=FQ))
        rows = sf_cursor.fetchall()
        months = [r[0] for r in rows if r[0] is not None]
        assert months == sorted(months), "Months should be in ascending order"


# ---------------------------------------------------------------------------
# 3. Aging distribution (bar chart)
# ---------------------------------------------------------------------------
class TestAgingDistribution:
    """Analytics: Aging buckets from V_DOCUMENT_LEDGER or computed query."""

    def test_aging_query_runs(self, sf_cursor):
        """Aging bucket query should execute without error."""
        sf_cursor.execute(f"""
            SELECT
                CASE
                    WHEN e.field_5 IS NULL THEN 'Unknown'
                    WHEN e.field_5 >= CURRENT_DATE() THEN 'Current'
                    WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 30 THEN '1-30 Days'
                    WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 60 THEN '31-60 Days'
                    WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 90 THEN '61-90 Days'
                    ELSE '90+ Days'
                END AS aging_bucket,
                COUNT(*) AS doc_count,
                SUM(e.field_8) AS total_amount
            FROM {FQ}.EXTRACTED_FIELDS e
            GROUP BY aging_bucket
            ORDER BY aging_bucket
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "Aging should return at least 1 bucket"

    def test_aging_all_docs_accounted(self, sf_cursor):
        """Sum of all aging bucket counts should match total extraction count."""
        sf_cursor.execute(f"""
            SELECT SUM(cnt) FROM (
                SELECT COUNT(*) AS cnt
                FROM {FQ}.EXTRACTED_FIELDS e
                JOIN {FQ}.RAW_DOCUMENTS r ON r.file_name = e.file_name
                GROUP BY
                    CASE
                        WHEN e.field_5 IS NULL THEN 'Unknown'
                        WHEN e.field_5 >= CURRENT_DATE() THEN 'Current'
                        WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 30 THEN '1-30'
                        WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 60 THEN '31-60'
                        WHEN DATEDIFF('day', e.field_5, CURRENT_DATE()) <= 90 THEN '61-90'
                        ELSE '90+'
                    END
            )
        """)
        bucket_total = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
        total = sf_cursor.fetchone()[0]
        assert bucket_total == total


# ---------------------------------------------------------------------------
# 4. Top line items
# ---------------------------------------------------------------------------
class TestTopLineItems:
    """Analytics: Top line items by amount."""

    def test_top_line_items_query(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT
                t.file_name,
                t.col_1 AS description,
                t.col_5 AS amount
            FROM {FQ}.EXTRACTED_TABLE_DATA t
            WHERE t.col_5 IS NOT NULL
            ORDER BY t.col_5 DESC
            LIMIT 20
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) > 0, "Top line items should return results"
        assert len(rows) <= 20

    def test_line_items_have_file_names(self, sf_cursor):
        sf_cursor.execute(f"""
            SELECT t.file_name
            FROM {FQ}.EXTRACTED_TABLE_DATA t
            WHERE t.col_5 IS NOT NULL
            ORDER BY t.col_5 DESC
            LIMIT 20
        """)
        for row in sf_cursor.fetchall():
            assert row[0] is not None and len(row[0]) > 0
