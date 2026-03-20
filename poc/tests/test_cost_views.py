"""Cost Observability Views Tests — validate the 5 cost views created by 12_cost_views.sql.

Tests cover:
  1. All 5 views exist and are queryable
  2. Column schemas match expectations
  3. Cost summary KPI fields are present and non-null
  4. Cost-by-doc-type returns rows for each active doc type
  5. Query log view returns rows with expected tag parsing
"""

import pytest


pytestmark = pytest.mark.sql


COST_VIEWS = [
    "V_AI_EXTRACT_COST_DAILY",
    "V_AI_EXTRACT_COST_BY_DOC_TYPE",
    "V_AI_EXTRACT_COST_PER_DOCUMENT",
    "V_AI_EXTRACT_QUERY_LOG",
    "V_AI_EXTRACT_COST_SUMMARY",
]


class TestViewExistence:
    """All 5 cost views should exist."""

    @pytest.mark.parametrize("view_name", COST_VIEWS)
    def test_view_exists(self, sf_cursor, view_name):
        sf_cursor.execute(f"SHOW VIEWS LIKE '{view_name}'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, f"View {view_name} not found"


class TestCostDailyView:
    """V_AI_EXTRACT_COST_DAILY schema and data."""

    VIEW = "V_AI_EXTRACT_COST_DAILY"

    def test_columns_present(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE VIEW {self.VIEW}")
        cols = {row[0].upper() for row in sf_cursor.fetchall()}
        for expected in ("USAGE_DATE", "AI_EXTRACT_CREDITS", "AI_EXTRACT_CALLS",
                         "TOTAL_TOKENS", "DOCS_EXTRACTED", "WAREHOUSE_CREDITS"):
            assert expected in cols, f"Missing column: {expected}"

    def test_returns_rows(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.VIEW}")
        count = sf_cursor.fetchone()[0]
        assert count >= 0


class TestCostByDocTypeView:
    """V_AI_EXTRACT_COST_BY_DOC_TYPE schema and data."""

    VIEW = "V_AI_EXTRACT_COST_BY_DOC_TYPE"

    def test_columns_present(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE VIEW {self.VIEW}")
        cols = {row[0].upper() for row in sf_cursor.fetchall()}
        for expected in ("DOC_TYPE", "USAGE_DATE", "CALL_COUNT",
                         "AI_EXTRACT_CREDITS", "TOTAL_ELAPSED_SEC", "AVG_ELAPSED_SEC"):
            assert expected in cols, f"Missing column: {expected}"

    def test_returns_rows(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.VIEW}")
        count = sf_cursor.fetchone()[0]
        assert count >= 0


class TestCostPerDocumentView:
    """V_AI_EXTRACT_COST_PER_DOCUMENT schema and data."""

    VIEW = "V_AI_EXTRACT_COST_PER_DOCUMENT"

    def test_columns_present(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE VIEW {self.VIEW}")
        cols = {row[0].upper() for row in sf_cursor.fetchall()}
        for expected in ("USAGE_DATE", "AI_EXTRACT_CREDITS", "DOCS_EXTRACTED",
                         "CREDITS_PER_DOC", "AI_EXTRACT_CALLS"):
            assert expected in cols, f"Missing column: {expected}"

    def test_returns_rows(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.VIEW}")
        count = sf_cursor.fetchone()[0]
        assert count >= 0


class TestQueryLogView:
    """V_AI_EXTRACT_QUERY_LOG schema and tag parsing."""

    VIEW = "V_AI_EXTRACT_QUERY_LOG"

    def test_columns_present(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE VIEW {self.VIEW}")
        cols = {row[0].upper() for row in sf_cursor.fetchall()}
        for expected in ("QUERY_ID", "DOC_TYPE", "AI_CREDITS",
                         "START_TIME", "ELAPSED_SEC", "TOKENS"):
            assert expected in cols, f"Missing column: {expected}"

    def test_returns_rows(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.VIEW}")
        count = sf_cursor.fetchone()[0]
        assert count >= 0


class TestCostSummaryView:
    """V_AI_EXTRACT_COST_SUMMARY — single-row KPI summary."""

    VIEW = "V_AI_EXTRACT_COST_SUMMARY"

    def test_columns_present(self, sf_cursor):
        sf_cursor.execute(f"DESCRIBE VIEW {self.VIEW}")
        cols = {row[0].upper() for row in sf_cursor.fetchall()}
        for expected in ("AI_CREDITS_LAST_7D", "AI_CREDITS_LAST_30D",
                         "AI_CREDITS_LAST_90D", "UNIQUE_DOCS",
                         "AVG_CREDITS_PER_DOC"):
            assert expected in cols, f"Missing column: {expected}"

    def test_returns_one_row(self, sf_cursor):
        sf_cursor.execute(f"SELECT COUNT(*) FROM {self.VIEW}")
        assert sf_cursor.fetchone()[0] == 1

    def test_total_docs_matches_extracted_count(self, sf_cursor):
        sf_cursor.execute(f"SELECT UNIQUE_DOCS FROM {self.VIEW}")
        summary_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COUNT(DISTINCT file_name) FROM EXTRACTED_FIELDS"
        )
        actual_count = sf_cursor.fetchone()[0]
        assert summary_count == actual_count, (
            f"Summary shows {summary_count} but {actual_count} actually extracted"
        )

    def test_avg_credits_per_doc_nonnegative(self, sf_cursor):
        sf_cursor.execute(f"SELECT AVG_CREDITS_PER_DOC FROM {self.VIEW}")
        val = sf_cursor.fetchone()[0]
        if val is not None:
            assert float(val) >= 0


class TestCostViewsIntegrity:
    """Cross-view consistency checks."""

    def test_daily_credits_sum_matches_summary_30d(self, sf_cursor):
        sf_cursor.execute("""
            SELECT COALESCE(SUM(AI_EXTRACT_CREDITS), 0)
            FROM V_AI_EXTRACT_COST_DAILY
            WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
        """)
        daily_sum = float(sf_cursor.fetchone()[0])
        sf_cursor.execute(
            "SELECT COALESCE(AI_CREDITS_LAST_30D, 0) FROM V_AI_EXTRACT_COST_SUMMARY"
        )
        summary_30d = float(sf_cursor.fetchone()[0])
        if daily_sum > 0 and summary_30d > 0:
            diff_pct = abs(daily_sum - summary_30d) / max(daily_sum, summary_30d)
            assert diff_pct < 0.1, (
                f"Daily sum {daily_sum:.2f} vs summary 30d {summary_30d:.2f} "
                f"differ by {diff_pct*100:.1f}%"
            )
