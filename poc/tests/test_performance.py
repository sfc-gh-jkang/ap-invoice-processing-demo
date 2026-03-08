"""Performance regression tests.

Time critical queries against the POC database and assert they complete
under reasonable thresholds. These are canary tests — if they start failing,
it signals a schema or data-volume regression.

Marked @pytest.mark.slow so they can be skipped in fast feedback loops.
"""

import time

import pytest

pytestmark = [pytest.mark.sql, pytest.mark.slow]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"

# Thresholds in seconds — generous enough to accommodate network latency
# but tight enough to catch catastrophic regressions.
FAST_QUERY_THRESHOLD = 5.0    # Simple selects, small tables
VIEW_QUERY_THRESHOLD = 10.0   # View queries with joins / window functions


def _time_query(cursor, sql):
    """Execute a query and return elapsed wall-clock seconds."""
    start = time.perf_counter()
    cursor.execute(sql)
    _ = cursor.fetchall()
    return time.perf_counter() - start


class TestViewPerformance:
    """Verify core views return in acceptable time."""

    def test_v_invoice_summary_speed(self, sf_cursor):
        """V_INVOICE_SUMMARY (main review page query) should be fast."""
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_INVOICE_SUMMARY took {elapsed:.2f}s "
            f"(threshold: {VIEW_QUERY_THRESHOLD}s)"
        )

    def test_v_extraction_status_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_EXTRACTION_STATUS",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_EXTRACTION_STATUS took {elapsed:.2f}s"
        )

    def test_v_document_ledger_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_DOCUMENT_LEDGER",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_DOCUMENT_LEDGER took {elapsed:.2f}s"
        )

    def test_v_summary_by_vendor_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_SUMMARY_BY_VENDOR",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_SUMMARY_BY_VENDOR took {elapsed:.2f}s"
        )

    def test_v_monthly_trend_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_MONTHLY_TREND",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_MONTHLY_TREND took {elapsed:.2f}s"
        )

    def test_v_top_line_items_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_TOP_LINE_ITEMS",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_TOP_LINE_ITEMS took {elapsed:.2f}s"
        )

    def test_v_aging_summary_speed(self, sf_cursor):
        elapsed = _time_query(
            sf_cursor,
            f"SELECT * FROM {FQ}.V_AGING_SUMMARY",
        )
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_AGING_SUMMARY took {elapsed:.2f}s"
        )


class TestTableScanPerformance:
    """Verify raw table scans are fast (small dataset, ~100 rows)."""

    TABLES = [
        "RAW_DOCUMENTS",
        "EXTRACTED_FIELDS",
        "EXTRACTED_TABLE_DATA",
        "INVOICE_REVIEW",
    ]

    @pytest.mark.parametrize("table", TABLES)
    def test_table_count_speed(self, sf_cursor, table):
        """COUNT(*) on each table should be near-instant."""
        elapsed = _time_query(
            sf_cursor,
            f"SELECT COUNT(*) FROM {FQ}.{table}",
        )
        assert elapsed < FAST_QUERY_THRESHOLD, (
            f"COUNT(*) on {table} took {elapsed:.2f}s"
        )


class TestJoinPerformance:
    """Verify key join patterns used by the app are fast."""

    def test_invoice_summary_join_pattern(self, sf_cursor):
        """The ROW_NUMBER join pattern used by V_INVOICE_SUMMARY should be fast."""
        # Use the view itself to test the actual join pattern end-to-end
        sql = f"""
            SELECT *
            FROM {FQ}.V_INVOICE_SUMMARY
            ORDER BY RECORD_ID
        """
        elapsed = _time_query(sf_cursor, sql)
        assert elapsed < VIEW_QUERY_THRESHOLD, (
            f"V_INVOICE_SUMMARY ordered scan took {elapsed:.2f}s"
        )
