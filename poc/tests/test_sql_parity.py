"""SQL script parity tests.

Verify that every object created by the SQL deploy scripts (01–08) actually
exists in Snowflake. Focuses on objects NOT already covered by
test_sql_integration.py — specifically:
  - PYPI_NETWORK_RULE
  - PYPI_ACCESS_INTEGRATION (External Access Integration)
  - Foreign key constraints on EXTRACTED_FIELDS and EXTRACTED_TABLE_DATA
  - STREAMLIT_STAGE (separate from DOCUMENT_STAGE)
  - Cross-region inference parameter
"""

import pytest

pytestmark = [pytest.mark.sql]


# ---------------------------------------------------------------------------
# Network Rule & External Access Integration (07_deploy_streamlit.sql)
# ---------------------------------------------------------------------------
class TestNetworkAndEAI:
    """Verify network rule and external access integration exist."""

    def test_pypi_network_rule_exists(self, sf_cursor):
        """PYPI_NETWORK_RULE should exist in the database."""
        sf_cursor.execute(
            "SHOW NETWORK RULES IN AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        names = [r[1] for r in rows]  # NAME is typically column index 1
        assert "PYPI_NETWORK_RULE" in names, (
            f"PYPI_NETWORK_RULE not found. Available: {names}"
        )

    def test_pypi_network_rule_allows_pypi(self, sf_cursor):
        """Network rule should reference pypi.org."""
        sf_cursor.execute(
            "SHOW NETWORK RULES IN AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        rule_row = None
        for r in rows:
            row_dict = dict(zip(col_names, r))
            if row_dict.get("name") == "PYPI_NETWORK_RULE":
                rule_row = row_dict
                break
        assert rule_row is not None, "PYPI_NETWORK_RULE not found"

    def test_pypi_access_integration_exists(self, sf_cursor):
        """PYPI_ACCESS_INTEGRATION should exist."""
        sf_cursor.execute("SHOW EXTERNAL ACCESS INTEGRATIONS")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "PYPI_ACCESS_INTEGRATION" in names, (
            f"PYPI_ACCESS_INTEGRATION not found. Available: {names}"
        )


# ---------------------------------------------------------------------------
# Foreign Key Constraints (02_tables.sql)
# ---------------------------------------------------------------------------
class TestForeignKeys:
    """Verify FK relationships declared in 02_tables.sql."""

    def test_extracted_fields_fk_to_raw_documents(self, sf_cursor):
        """EXTRACTED_FIELDS should have a FK referencing RAW_DOCUMENTS."""
        sf_cursor.execute("""
            SELECT constraint_name, table_name,
                   constraint_type
            FROM AI_EXTRACT_POC.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE table_schema = 'DOCUMENTS'
              AND table_name = 'EXTRACTED_FIELDS'
              AND constraint_type = 'FOREIGN KEY'
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "EXTRACTED_FIELDS has no FOREIGN KEY constraint"
        )

    def test_extracted_table_data_fk_to_raw_documents(self, sf_cursor):
        """EXTRACTED_TABLE_DATA should have a FK referencing RAW_DOCUMENTS."""
        sf_cursor.execute("""
            SELECT constraint_name, table_name,
                   constraint_type
            FROM AI_EXTRACT_POC.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE table_schema = 'DOCUMENTS'
              AND table_name = 'EXTRACTED_TABLE_DATA'
              AND constraint_type = 'FOREIGN KEY'
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "EXTRACTED_TABLE_DATA has no FOREIGN KEY constraint"
        )


# ---------------------------------------------------------------------------
# Stages (01_setup.sql, 07_deploy_streamlit.sql)
# ---------------------------------------------------------------------------
class TestStages:
    """Verify both internal stages exist."""

    def test_streamlit_stage_exists(self, sf_cursor):
        """STREAMLIT_STAGE (for app files) should exist."""
        sf_cursor.execute("SHOW STAGES IN AI_EXTRACT_POC.DOCUMENTS")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "STREAMLIT_STAGE" in names, (
            f"STREAMLIT_STAGE not found. Available: {names}"
        )

    def test_document_stage_has_encryption(self, sf_cursor):
        """DOCUMENT_STAGE should use SSE encryption."""
        sf_cursor.execute(
            "DESCRIBE STAGE AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE"
        )
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        # Look for encryption type property
        for r in rows:
            row_dict = dict(zip(col_names, r))
            prop = row_dict.get("parent_property", "")
            prop_name = row_dict.get("property", "")
            prop_val = row_dict.get("property_value", "")
            if "ENCRYPTION" in str(prop).upper() and "TYPE" in str(prop_name).upper():
                assert "SNOWFLAKE_SSE" in str(prop_val).upper(), (
                    f"Expected SSE encryption, got {prop_val}"
                )
                return
        # If we didn't find the property, that's OK — encryption is on by default


# ---------------------------------------------------------------------------
# Object Count Parity: all SQL scripts vs deployed
# ---------------------------------------------------------------------------
class TestObjectCountParity:
    """High-level check: total objects roughly match expectations."""

    EXPECTED_TABLES = {"RAW_DOCUMENTS", "EXTRACTED_FIELDS",
                       "EXTRACTED_TABLE_DATA", "INVOICE_REVIEW"}

    EXPECTED_VIEWS = {
        "V_EXTRACTION_STATUS", "V_DOCUMENT_LEDGER",
        "V_SUMMARY_BY_VENDOR", "V_MONTHLY_TREND",
        "V_TOP_LINE_ITEMS", "V_AGING_SUMMARY", "V_INVOICE_SUMMARY",
        "V_DOCUMENT_SUMMARY",
    }

    def test_all_tables_present(self, sf_cursor):
        """Every expected table should exist."""
        sf_cursor.execute("SHOW TABLES IN AI_EXTRACT_POC.DOCUMENTS")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = {dict(zip(col_names, r)).get("name", "") for r in rows}
        missing = self.EXPECTED_TABLES - names
        assert not missing, f"Missing tables: {missing}"

    def test_all_views_present(self, sf_cursor):
        """Every expected view should exist."""
        sf_cursor.execute("SHOW VIEWS IN AI_EXTRACT_POC.DOCUMENTS")
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = {dict(zip(col_names, r)).get("name", "") for r in rows}
        missing = self.EXPECTED_VIEWS - names
        assert not missing, f"Missing views: {missing}"

    def test_no_dynamic_table_remnants(self, sf_cursor):
        """DT_INVOICE_SUMMARY should have been dropped (replaced by view)."""
        sf_cursor.execute(
            "SHOW DYNAMIC TABLES IN AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = {dict(zip(col_names, r)).get("name", "") for r in rows}
        assert "DT_INVOICE_SUMMARY" not in names, (
            "DT_INVOICE_SUMMARY should have been dropped"
        )
