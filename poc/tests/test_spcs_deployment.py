"""SPCS / Streamlit deployment validation tests.

Validates that all deployment prerequisites and artifacts exist in Snowflake:
  1. STREAMLIT_STAGE exists and contains all expected files
  2. Compute pool exists and is configured
  3. Streamlit app object exists
  4. Network rule and EAI exist
  5. Required SQL objects (SPs, views, tables) exist
  6. deploy_poc.sh script references all Streamlit pages
"""

import json
import os
import re
import subprocess

import pytest


pytestmark = pytest.mark.sql


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


@pytest.fixture(scope="session")
def admin_cursor():
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# 1. Stage and file validation
# ---------------------------------------------------------------------------
class TestStreamlitStage:
    """Verify STREAMLIT_STAGE exists and has all required files."""

    def test_stage_exists(self, sf_cursor):
        sf_cursor.execute(f"SHOW STAGES LIKE 'STREAMLIT_STAGE' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "STREAMLIT_STAGE not found"

    def test_stage_has_main_file(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/streamlit_app.py")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "streamlit_app.py not found in stage"

    def test_stage_has_config(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/config.py")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "config.py not found in stage"

    def test_stage_has_environment_yml(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/environment.yml")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "environment.yml not found in stage"

    @pytest.mark.parametrize("page", [
        "0_Dashboard.py",
        "1_Document_Viewer.py",
        "2_Analytics.py",
        "3_Review.py",
        "4_Admin.py",
    ])
    def test_stage_has_page(self, sf_cursor, page):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/pages/{page}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"pages/{page} not found in stage"


# ---------------------------------------------------------------------------
# 2. Compute pool
# ---------------------------------------------------------------------------
class TestComputePool:
    """Verify the compute pool for Container Runtime."""

    def test_pool_exists(self, admin_cursor):
        admin_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1, "AI_EXTRACT_POC_POOL not found"

    def test_pool_instance_family(self, admin_cursor):
        admin_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1
        cols = [d[0] for d in admin_cursor.description]
        if "instance_family" in cols:
            idx = cols.index("instance_family")
            assert rows[0][idx] == "CPU_X64_XS"


# ---------------------------------------------------------------------------
# 3. Streamlit app object
# ---------------------------------------------------------------------------
class TestStreamlitApp:
    """Verify the Streamlit app was created correctly."""

    def test_streamlit_exists(self, sf_cursor):
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "AI_EXTRACT_DASHBOARD Streamlit not found"

    def test_streamlit_uses_container_runtime(self, sf_cursor):
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1
        # Check runtime column if available
        cols = [d[0] for d in sf_cursor.description]
        row_dict = dict(zip(cols, rows[0]))
        # Runtime name may appear in various columns depending on SF version
        row_str = str(row_dict)
        # Container runtime implies compute_pool is set
        if "compute_pool" in row_dict and row_dict["compute_pool"]:
            assert "AI_EXTRACT_POC_POOL" in str(row_dict["compute_pool"]).upper()


# ---------------------------------------------------------------------------
# 4. Network access (EAI)
# ---------------------------------------------------------------------------
class TestExternalAccess:
    """Verify PyPI access integration exists for Container Runtime."""

    def test_pypi_network_rule_exists(self, admin_cursor):
        admin_cursor.execute("SHOW NETWORK RULES LIKE 'PYPI_NETWORK_RULE'")
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1, "PYPI_NETWORK_RULE not found"

    def test_pypi_access_integration_exists(self, admin_cursor):
        admin_cursor.execute(
            "SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'PYPI_ACCESS_INTEGRATION'"
        )
        rows = admin_cursor.fetchall()
        assert len(rows) >= 1, "PYPI_ACCESS_INTEGRATION not found"


# ---------------------------------------------------------------------------
# 5. Required SQL objects
# ---------------------------------------------------------------------------
class TestRequiredSQLObjects:
    """Verify all required tables, views, stages, and procedures exist."""

    REQUIRED_TABLES = [
        "RAW_DOCUMENTS",
        "EXTRACTED_FIELDS",
        "EXTRACTED_TABLE_DATA",
        "DOCUMENT_TYPE_CONFIG",
        "INVOICE_REVIEW",
    ]

    REQUIRED_VIEWS = [
        "V_DOCUMENT_SUMMARY",
        "V_INVOICE_SUMMARY",
    ]

    REQUIRED_STAGES = [
        "DOCUMENT_STAGE",
        "STREAMLIT_STAGE",
    ]

    REQUIRED_PROCEDURES = [
        "SP_EXTRACT_NEW_DOCUMENTS",
        "SP_EXTRACT_BY_DOC_TYPE",
        "SP_REEXTRACT_DOC_TYPE",
    ]

    @pytest.mark.parametrize("table", REQUIRED_TABLES)
    def test_table_exists(self, sf_cursor, table):
        sf_cursor.execute(f"SHOW TABLES LIKE '{table}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Table {table} not found"

    @pytest.mark.parametrize("view", REQUIRED_VIEWS)
    def test_view_exists(self, sf_cursor, view):
        sf_cursor.execute(f"SHOW VIEWS LIKE '{view}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"View {view} not found"

    @pytest.mark.parametrize("stage", REQUIRED_STAGES)
    def test_stage_exists(self, sf_cursor, stage):
        sf_cursor.execute(f"SHOW STAGES LIKE '{stage}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Stage {stage} not found"

    @pytest.mark.parametrize("proc", REQUIRED_PROCEDURES)
    def test_procedure_exists(self, sf_cursor, proc):
        sf_cursor.execute(f"SHOW PROCEDURES LIKE '{proc}' IN SCHEMA {FQ}")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, f"Procedure {proc} not found"


# ---------------------------------------------------------------------------
# 6. Deploy script completeness (local file check, no SF connection needed)
# ---------------------------------------------------------------------------
class TestDeployScriptCompleteness:
    """Verify deploy_poc.sh references all required files."""

    DEPLOY_SCRIPT = os.path.join(
        os.path.dirname(__file__), "..", "deploy_poc.sh"
    )

    EXPECTED_PAGES = [
        "0_Dashboard.py",
        "1_Document_Viewer.py",
        "2_Analytics.py",
        "3_Review.py",
        "4_Admin.py",
    ]

    def test_deploy_script_exists(self):
        assert os.path.exists(self.DEPLOY_SCRIPT), "deploy_poc.sh not found"

    def test_deploy_script_is_executable(self):
        assert os.access(self.DEPLOY_SCRIPT, os.X_OK), \
            "deploy_poc.sh should be executable"

    def test_deploy_script_uploads_all_pages(self):
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        for page in self.EXPECTED_PAGES:
            assert page in content, \
                f"deploy_poc.sh missing PUT for {page}"

    def test_deploy_script_references_all_sql_files(self):
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        expected_sql = [
            "01_setup.sql",
            "02_tables.sql",
            "05_views.sql",
            "06_automate.sql",
            "07_deploy_streamlit.sql",
        ]
        for sql_file in expected_sql:
            assert sql_file in content, \
                f"deploy_poc.sh missing reference to {sql_file}"

    def test_deploy_script_has_validation_step(self):
        """Deploy script should include a post-deployment validation check."""
        with open(self.DEPLOY_SCRIPT) as f:
            content = f.read()
        assert "validat" in content.lower() or "verify" in content.lower() or \
               "check" in content.lower(), \
            "deploy_poc.sh should include deployment validation"


# ---------------------------------------------------------------------------
# 7. Environment.yml validation
# ---------------------------------------------------------------------------
class TestEnvironmentYml:
    """Verify environment.yml has required packages."""

    ENV_YML = os.path.join(
        os.path.dirname(__file__), "..", "streamlit", "environment.yml"
    )

    def test_environment_yml_exists(self):
        assert os.path.exists(self.ENV_YML), "environment.yml not found"

    def test_has_streamlit_dependency(self):
        with open(self.ENV_YML) as f:
            content = f.read()
        assert "streamlit" in content.lower(), "environment.yml missing streamlit"
