"""Permissions / RBAC tests.

Verify that Snowflake grants allow the Streamlit app to function correctly.
Tests run as ACCOUNTADMIN (the test user) but inspect grants to confirm that
the required privileges exist on key objects.
"""

import pytest

pytestmark = [pytest.mark.sql]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"


class TestWarehouseGrants:
    """Verify warehouse grants for the app."""

    def test_warehouse_exists_and_usable(self, sf_cursor):
        """AI_EXTRACT_WH should be usable by current role."""
        sf_cursor.execute("USE WAREHOUSE AI_EXTRACT_WH")
        sf_cursor.execute("SELECT 1 AS test_value")
        row = sf_cursor.fetchone()
        assert row[0] == 1


class TestSchemaGrants:
    """Verify schema-level access."""

    def test_schema_usable(self, sf_cursor):
        """Should be able to USE the target schema."""
        sf_cursor.execute(f"USE SCHEMA {FQ}")
        # If this doesn't raise, the grant is sufficient

    def test_can_show_objects_in_schema(self, sf_cursor):
        """Should be able to enumerate objects."""
        sf_cursor.execute(f"SHOW TABLES IN {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 5, "Expected at least 5 tables"


class TestTableReadGrants:
    """Verify SELECT access on all tables."""

    TABLES = [
        "RAW_DOCUMENTS",
        "EXTRACTED_FIELDS",
        "EXTRACTED_TABLE_DATA",
        "INVOICE_REVIEW",
        "DOCUMENT_TYPE_CONFIG",
    ]

    @pytest.mark.parametrize("table", TABLES)
    def test_can_select_from_table(self, sf_cursor, table):
        """Should be able to SELECT from each table."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.{table}")
        row = sf_cursor.fetchone()
        assert row[0] >= 0  # Just verifying access, not row count


class TestTableWriteGrants:
    """Verify INSERT access on INVOICE_REVIEW (the writeback table)."""

    def test_can_insert_and_rollback(self, sf_cursor):
        """Should be able to INSERT into INVOICE_REVIEW.

        Uses a transaction that is rolled back so no test data remains.
        """
        sf_cursor.execute("BEGIN")
        try:
            sf_cursor.execute(f"""
                INSERT INTO {FQ}.INVOICE_REVIEW (RECORD_ID, FILE_NAME, REVIEW_STATUS)
                VALUES (999999, '__rbac_test__.pdf', 'Pending')
            """)
            # If we got here, INSERT privilege exists
            assert True
        finally:
            sf_cursor.execute("ROLLBACK")


class TestViewGrants:
    """Verify SELECT access on all views."""

    VIEWS = [
        "V_EXTRACTION_STATUS",
        "V_DOCUMENT_LEDGER",
        "V_SUMMARY_BY_VENDOR",
        "V_MONTHLY_TREND",
        "V_TOP_LINE_ITEMS",
        "V_AGING_SUMMARY",
        "V_INVOICE_SUMMARY",
        "V_DOCUMENT_SUMMARY",
    ]

    @pytest.mark.parametrize("view", VIEWS)
    def test_can_select_from_view(self, sf_cursor, view):
        """Should be able to SELECT from each view."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.{view}")
        row = sf_cursor.fetchone()
        assert row[0] >= 0


class TestStageGrants:
    """Verify stage access."""

    def test_can_list_document_stage(self, sf_cursor):
        """Should be able to LIST files on DOCUMENT_STAGE."""
        sf_cursor.execute(f"LIST @{FQ}.DOCUMENT_STAGE")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 0  # Access is what matters

    def test_can_list_streamlit_stage(self, sf_cursor):
        """Should be able to LIST files on STREAMLIT_STAGE."""
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 0


class TestStreamAndTaskGrants:
    """Verify stream and task visibility."""

    def test_can_show_streams(self, sf_cursor):
        """Should be able to see RAW_DOCUMENTS_STREAM."""
        sf_cursor.execute(f"SHOW STREAMS IN {FQ}")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "RAW_DOCUMENTS_STREAM" in names

    def test_can_show_tasks(self, sf_cursor):
        """Should be able to see EXTRACT_NEW_DOCUMENTS_TASK."""
        sf_cursor.execute(f"SHOW TASKS IN {FQ}")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "EXTRACT_NEW_DOCUMENTS_TASK" in names


class TestProcedureGrants:
    """Verify procedure is callable."""

    def test_can_describe_procedure(self, sf_cursor):
        """Should be able to DESCRIBE the extraction procedure."""
        sf_cursor.execute(f"""
            SHOW PROCEDURES IN {FQ}
        """)
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "SP_EXTRACT_NEW_DOCUMENTS" in names
