"""RBAC negative tests — verify hardening grants are applied.

These tests require a Snowflake connection and verify that:
  1. AI_EXTRACT_APP role has proper operational access.
  2. PUBLIC role has NO explicit grants on POC objects (grants-metadata check).
  3. Account-level grants have been revoked from AI_EXTRACT_APP.
  4. Resource monitor is attached to the warehouse.
  5. MANAGED ACCESS is enabled on the schema.

NOTE: We validate via SHOW GRANTS metadata rather than role-switching because
Snowflake's role hierarchy means a session user with ACCOUNTADMIN can still
access objects even after switching to PUBLIC.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))

from config import get_session


@pytest.fixture(scope="module")
def sf_session():
    """Get a Snowflake session — skip if connection env vars not set."""
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        pytest.skip("POC_CONNECTION not set — skipping SF tests")
    return get_session()


@pytest.fixture(scope="module")
def admin_session():
    """Get a session with ACCOUNTADMIN for grants inspection."""
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        pytest.skip("POC_CONNECTION not set — skipping SF tests")
    sess = get_session()
    sess.sql("USE ROLE ACCOUNTADMIN").collect()
    return sess


DB = "AI_EXTRACT_POC.DOCUMENTS"


class TestAppRoleAccess:
    """Verify AI_EXTRACT_APP role has expected access."""

    def test_can_read_raw_documents(self, sf_session):
        rows = sf_session.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS").collect()
        assert rows[0]["CNT"] >= 0

    def test_can_read_extracted_fields(self, sf_session):
        rows = sf_session.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.EXTRACTED_FIELDS").collect()
        assert rows[0]["CNT"] >= 0

    def test_can_read_document_type_config(self, sf_session):
        rows = sf_session.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.DOCUMENT_TYPE_CONFIG").collect()
        assert rows[0]["CNT"] >= 0

    def test_can_read_view(self, sf_session):
        rows = sf_session.sql(f"SELECT COUNT(*) AS cnt FROM {DB}.V_DOCUMENT_SUMMARY").collect()
        assert rows[0]["CNT"] >= 0

    def test_can_call_extract_sp(self, sf_session):
        """Verify SP is callable (with a no-op doc type that won't match anything)."""
        result = sf_session.sql(
            f"CALL {DB}.SP_EXTRACT_BY_DOC_TYPE('_NONEXISTENT_TEST_TYPE')"
        ).collect()
        assert result is not None

    def test_can_read_stage_list(self, sf_session):
        """Verify stage listing works."""
        rows = sf_session.sql(f"LIST @{DB}.DOCUMENT_STAGE").collect()
        assert isinstance(rows, list)


class TestPublicRoleDenied:
    """Verify PUBLIC role has no explicit grants on POC objects.

    Uses SHOW GRANTS metadata to verify hardening rather than role-switching,
    which is unreliable when the session user has ACCOUNTADMIN.
    """

    def test_no_public_grant_on_database(self, admin_session):
        """PUBLIC should have no USAGE grant on AI_EXTRACT_POC database."""
        rows = admin_session.sql("SHOW GRANTS ON DATABASE AI_EXTRACT_POC").collect()
        public_grants = [r for r in rows if r["grantee_name"] == "PUBLIC"]
        assert len(public_grants) == 0, (
            f"PUBLIC has grants on database: {[r['privilege'] for r in public_grants]}"
        )

    def test_no_public_grant_on_schema(self, admin_session):
        """PUBLIC should have no grants on DOCUMENTS schema."""
        rows = admin_session.sql(
            "SHOW GRANTS ON SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        ).collect()
        public_grants = [r for r in rows if r["grantee_name"] == "PUBLIC"]
        assert len(public_grants) == 0, (
            f"PUBLIC has grants on schema: {[r['privilege'] for r in public_grants]}"
        )

    def test_no_public_grant_on_tables(self, admin_session):
        """No individual table should have grants to PUBLIC."""
        tables = ["RAW_DOCUMENTS", "EXTRACTED_FIELDS", "EXTRACTED_TABLE_DATA",
                   "DOCUMENT_TYPE_CONFIG", "INVOICE_REVIEW"]
        for table in tables:
            rows = admin_session.sql(
                f"SHOW GRANTS ON TABLE AI_EXTRACT_POC.DOCUMENTS.{table}"
            ).collect()
            public_grants = [r for r in rows if r["grantee_name"] == "PUBLIC"]
            assert len(public_grants) == 0, (
                f"PUBLIC has grants on {table}: {[r['privilege'] for r in public_grants]}"
            )

    def test_no_public_grant_on_views(self, admin_session):
        """No view should have grants to PUBLIC."""
        views = ["V_DOCUMENT_SUMMARY", "V_INVOICE_SUMMARY"]
        for view in views:
            rows = admin_session.sql(
                f"SHOW GRANTS ON VIEW AI_EXTRACT_POC.DOCUMENTS.{view}"
            ).collect()
            public_grants = [r for r in rows if r["grantee_name"] == "PUBLIC"]
            assert len(public_grants) == 0, (
                f"PUBLIC has grants on {view}: {[r['privilege'] for r in public_grants]}"
            )


class TestAccountLevelGrantsRevoked:
    """Verify AI_EXTRACT_APP has no dangerous account-level privileges."""

    def test_no_create_database_on_account(self, admin_session):
        rows = admin_session.sql("SHOW GRANTS TO ROLE AI_EXTRACT_APP").collect()
        acct_grants = [r for r in rows
                       if r["granted_on"] == "ACCOUNT"
                       and r["privilege"] == "CREATE DATABASE"]
        assert len(acct_grants) == 0, "AI_EXTRACT_APP still has CREATE DATABASE ON ACCOUNT"

    def test_no_create_warehouse_on_account(self, admin_session):
        rows = admin_session.sql("SHOW GRANTS TO ROLE AI_EXTRACT_APP").collect()
        acct_grants = [r for r in rows
                       if r["granted_on"] == "ACCOUNT"
                       and r["privilege"] == "CREATE WAREHOUSE"]
        assert len(acct_grants) == 0, "AI_EXTRACT_APP still has CREATE WAREHOUSE ON ACCOUNT"

    def test_no_bind_service_endpoint(self, admin_session):
        rows = admin_session.sql("SHOW GRANTS TO ROLE AI_EXTRACT_APP").collect()
        acct_grants = [r for r in rows
                       if r["granted_on"] == "ACCOUNT"
                       and r["privilege"] == "BIND SERVICE ENDPOINT"]
        assert len(acct_grants) == 0, "AI_EXTRACT_APP still has BIND SERVICE ENDPOINT"

    def test_only_execute_task_on_account(self, admin_session):
        """The only account-level grant should be EXECUTE TASK (for automation)."""
        rows = admin_session.sql("SHOW GRANTS TO ROLE AI_EXTRACT_APP").collect()
        acct_grants = [r["privilege"] for r in rows if r["granted_on"] == "ACCOUNT"]
        assert acct_grants == ["EXECUTE TASK"] or acct_grants == [], (
            f"Unexpected account-level grants: {acct_grants}"
        )


class TestResourceMonitor:
    """Verify resource monitor is attached to warehouse."""

    def test_resource_monitor_exists(self, admin_session):
        rows = admin_session.sql("SHOW RESOURCE MONITORS LIKE 'AI_EXTRACT_MONITOR'").collect()
        assert len(rows) >= 1, "AI_EXTRACT_MONITOR does not exist"

    def test_warehouse_has_resource_monitor(self, admin_session):
        rows = admin_session.sql("SHOW WAREHOUSES LIKE 'AI_EXTRACT_WH'").collect()
        assert len(rows) == 1
        monitor = rows[0]["resource_monitor"]
        assert monitor == "AI_EXTRACT_MONITOR", (
            f"Warehouse monitor is '{monitor}', expected 'AI_EXTRACT_MONITOR'"
        )


class TestManagedAccess:
    """Verify schema uses MANAGED ACCESS."""

    def test_schema_is_managed(self, admin_session):
        rows = admin_session.sql(
            "SHOW SCHEMAS LIKE 'DOCUMENTS' IN DATABASE AI_EXTRACT_POC"
        ).collect()
        assert len(rows) == 1
        opts = rows[0]["options"]
        assert "MANAGED ACCESS" in opts, f"Schema options: {opts}"
