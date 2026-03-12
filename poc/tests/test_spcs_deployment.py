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

    def test_stage_has_pyproject_toml(self, sf_cursor):
        sf_cursor.execute(f"LIST @{FQ}.STREAMLIT_STAGE/pyproject.toml")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "pyproject.toml not found in stage. "
            "Container Runtime requires pyproject.toml for dependency installation. "
            "Upload it: PUT file://streamlit/pyproject.toml @STREAMLIT_STAGE/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
        )

    @pytest.mark.parametrize("page", [
        "0_Dashboard.py",
        "1_Document_Viewer.py",
        "2_Analytics.py",
        "3_Review.py",
        "4_Admin.py",
        "5_Cost.py",
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
    """Verify the Streamlit app was created correctly and is runnable."""

    @pytest.fixture(scope="class")
    def streamlit_desc(self, sf_cursor):
        """DESCRIBE STREAMLIT and return as a dict."""
        sf_cursor.execute(
            f"DESCRIBE STREAMLIT {FQ}.AI_EXTRACT_DASHBOARD"
        )
        rows = sf_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("AI_EXTRACT_DASHBOARD not deployed")
        cols = [d[0].lower() for d in sf_cursor.description]
        return dict(zip(cols, rows[0]))

    def test_streamlit_exists(self, sf_cursor):
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "AI_EXTRACT_DASHBOARD Streamlit not found. "
            "Run 07_deploy_streamlit.sql to create it."
        )

    def test_live_version_active(self, streamlit_desc):
        """App must have a live version — without this it shows an error page."""
        live_uri = streamlit_desc.get("live_version_location_uri", "")
        assert live_uri and live_uri.strip(), (
            "Streamlit has no live version. The app will not load. "
            "Run: ALTER STREAMLIT AI_EXTRACT_DASHBOARD ADD LIVE VERSION FROM LAST;"
        )

    def test_uses_from_not_root_location(self, streamlit_desc):
        """Container runtime does not support ROOT_LOCATION — must use FROM."""
        # FROM-based apps have a version source location; ROOT_LOCATION apps don't
        source_uri = streamlit_desc.get(
            "default_version_source_location_uri", ""
        )
        assert source_uri and "STREAMLIT_STAGE" in source_uri.upper(), (
            "Streamlit was not created with FROM syntax. "
            "ROOT_LOCATION is not supported for container runtimes. "
            "Recreate with: CREATE OR REPLACE STREAMLIT ... FROM '@...STREAMLIT_STAGE'"
        )

    def test_container_runtime(self, streamlit_desc):
        """Must use container runtime, not warehouse runtime."""
        runtime = streamlit_desc.get("runtime_name", "")
        assert "CONTAINER" in runtime.upper(), (
            f"Expected container runtime, got '{runtime}'. "
            "Set RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'"
        )

    def test_compute_pool_set(self, streamlit_desc):
        """Container runtime requires a compute pool."""
        pool = streamlit_desc.get("compute_pool", "")
        assert pool and "AI_EXTRACT_POC_POOL" in pool.upper(), (
            f"Compute pool not set or wrong: '{pool}'. "
            "Container runtime requires COMPUTE_POOL = AI_EXTRACT_POC_POOL"
        )

    def test_eai_attached(self, streamlit_desc):
        """PyPI EAI must be attached for pip install to work."""
        eai = str(streamlit_desc.get("external_access_integrations", ""))
        assert "PYPI_ACCESS_INTEGRATION" in eai.upper(), (
            f"PYPI_ACCESS_INTEGRATION not attached to Streamlit app. "
            "Container runtime cannot install packages without it. "
            "Add: EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION)"
        )

    def test_main_file_correct(self, streamlit_desc):
        main = streamlit_desc.get("main_file", "")
        assert main == "streamlit_app.py", (
            f"Main file is '{main}', expected 'streamlit_app.py'"
        )

    def test_query_warehouse_set(self, streamlit_desc):
        wh = streamlit_desc.get("query_warehouse", "")
        assert wh and "AI_EXTRACT" in wh.upper(), (
            f"Query warehouse not set or wrong: '{wh}'. "
            "Set QUERY_WAREHOUSE = AI_EXTRACT_WH"
        )

    def test_url_id_assigned(self, sf_cursor):
        """App must have a URL ID for Snowsight access."""
        sf_cursor.execute("SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1
        cols = [d[0].lower() for d in sf_cursor.description]
        row_dict = dict(zip(cols, rows[0]))
        url_id = row_dict.get("url_id", "")
        assert url_id and len(url_id) > 0, (
            "Streamlit has no url_id — app is not accessible in Snowsight"
        )


# ---------------------------------------------------------------------------
# 4. Network access (EAI)
# ---------------------------------------------------------------------------
class TestExternalAccess:
    """Verify PyPI access integration exists for Container Runtime."""

    def test_pypi_network_rule_exists(self, admin_cursor):
        admin_cursor.execute("SHOW NETWORK RULES LIKE 'PYPI_NETWORK_RULE'")
        rows = admin_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("PYPI_NETWORK_RULE not deployed (requires ACCOUNTADMIN + 07_deploy_streamlit.sql)")

    def test_pypi_access_integration_exists(self, admin_cursor):
        admin_cursor.execute(
            "SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'PYPI_ACCESS_INTEGRATION'"
        )
        rows = admin_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("PYPI_ACCESS_INTEGRATION not deployed")


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
        "LINE_ITEM_REVIEW",
    ]

    REQUIRED_VIEWS = [
        "V_DOCUMENT_SUMMARY",
        "V_INVOICE_SUMMARY",
        "V_DOCUMENT_LEDGER",
        "V_EXTRACTION_STATUS",
        "V_LINE_ITEM_DETAIL",
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
# 7. pyproject.toml local file validation (Container Runtime)
# ---------------------------------------------------------------------------
class TestPyprojectTomlLocal:
    """Verify pyproject.toml exists locally (environment.yml was removed)."""

    PYPROJECT = os.path.join(
        os.path.dirname(__file__), "..", "streamlit", "pyproject.toml"
    )

    def test_pyproject_toml_exists(self):
        assert os.path.exists(self.PYPROJECT), (
            "pyproject.toml not found. Container Runtime requires pyproject.toml."
        )

    def test_no_environment_yml(self):
        env_yml = os.path.join(
            os.path.dirname(__file__), "..", "streamlit", "environment.yml"
        )
        assert not os.path.exists(env_yml), (
            "environment.yml still exists — it was replaced by pyproject.toml "
            "for Container Runtime. Delete it to avoid confusion."
        )


# ---------------------------------------------------------------------------
# 8. pyproject.toml validation (required for Container Runtime)
# ---------------------------------------------------------------------------
class TestPyprojectToml:
    """Container Runtime requires pyproject.toml for dependency installation."""

    PYPROJECT = os.path.join(
        os.path.dirname(__file__), "..", "streamlit", "pyproject.toml"
    )

    def test_pyproject_toml_exists(self):
        assert os.path.exists(self.PYPROJECT), (
            "pyproject.toml not found in streamlit/ directory. "
            "Container Runtime requires pyproject.toml (not just environment.yml)."
        )

    def test_has_project_section(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "[project]" in content, (
            "pyproject.toml missing [project] section"
        )

    def test_has_streamlit_dependency(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "streamlit" in content.lower(), (
            "pyproject.toml missing streamlit dependency"
        )

    def test_has_python_version(self):
        with open(self.PYPROJECT) as f:
            content = f.read()
        assert "requires-python" in content, (
            "pyproject.toml missing requires-python. "
            "Add: requires-python = \">=3.11\""
        )

    def test_has_snowpark_dependency(self):
        """pyproject.toml must pin snowflake-snowpark-python.

        When pyproject.toml exists on stage, Container Runtime uses it
        instead of the built-in default packages.  Without an explicit
        snowflake-snowpark-python entry the runtime pulls the latest
        version from PyPI, which may have stricter SQL validation
        (e.g. rejecting PARSE_JSON(?) inside VALUES clauses).
        """
        with open(self.PYPROJECT) as f:
            content = f.read().lower()
        assert "snowflake-snowpark-python" in content, (
            "pyproject.toml missing snowflake-snowpark-python dependency. "
            "Container Runtime overrides built-in packages when pyproject.toml "
            "is present — omitting Snowpark causes import failures."
        )


# ---------------------------------------------------------------------------
# 9. DDL linting — catch deployment-breaking syntax before it ships
# ---------------------------------------------------------------------------
class TestStreamlitDDLLinting:
    """Lint 07_deploy_streamlit.sql for known-bad patterns."""

    DDL_FILE = os.path.join(
        os.path.dirname(__file__), "..", "sql", "07_deploy_streamlit.sql"
    )

    def test_ddl_file_exists(self):
        assert os.path.exists(self.DDL_FILE), "07_deploy_streamlit.sql not found"

    def test_no_root_location(self):
        """ROOT_LOCATION is legacy and not supported for Container Runtime."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        # Only check uncommented lines
        active_lines = [
            line for line in content.splitlines()
            if not line.strip().startswith("--")
        ]
        active_sql = "\n".join(active_lines)
        assert "ROOT_LOCATION" not in active_sql, (
            "07_deploy_streamlit.sql contains ROOT_LOCATION. "
            "This is not supported for container runtimes and will cause "
            "'ROOT_LOCATION stages are not supported for vNext applications'. "
            "Replace with: FROM '@...STREAMLIT_STAGE'"
        )

    def test_has_from_clause(self):
        """CREATE STREAMLIT must use FROM for versioned stage."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "FROM '@" in content or "FROM  '@" in content, (
            "07_deploy_streamlit.sql missing FROM clause in CREATE STREAMLIT. "
            "Container runtime requires FROM '@stage_path' syntax."
        )

    def test_has_add_live_version(self):
        """FROM-based apps require ADD LIVE VERSION to activate."""
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "ADD LIVE VERSION" in content.upper(), (
            "07_deploy_streamlit.sql missing ALTER STREAMLIT ... ADD LIVE VERSION FROM LAST. "
            "Without this, the app is created but never activated — users see an error page."
        )

    def test_has_container_runtime(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "SYSTEM$ST_CONTAINER_RUNTIME" in content, (
            "07_deploy_streamlit.sql missing RUNTIME_NAME for container runtime"
        )

    def test_has_compute_pool(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "COMPUTE_POOL" in content, (
            "07_deploy_streamlit.sql missing COMPUTE_POOL (required for container runtime)"
        )

    def test_has_eai(self):
        with open(self.DDL_FILE) as f:
            content = f.read()
        assert "PYPI_ACCESS_INTEGRATION" in content, (
            "07_deploy_streamlit.sql missing EXTERNAL_ACCESS_INTEGRATIONS for PyPI"
        )

    def test_deploy_script_uploads_pyproject(self):
        """deploy_poc.sh must upload pyproject.toml to stage."""
        deploy_script = os.path.join(
            os.path.dirname(__file__), "..", "deploy_poc.sh"
        )
        with open(deploy_script) as f:
            content = f.read()
        assert "pyproject.toml" in content, (
            "deploy_poc.sh does not upload pyproject.toml to STREAMLIT_STAGE. "
            "Container Runtime requires it for dependency installation."
        )


# ---------------------------------------------------------------------------
# 10. View ordering integrity — NOORDER autoincrement regression prevention
# ---------------------------------------------------------------------------
class TestViewOrderingIntegrity:
    """Verify V_DOCUMENT_SUMMARY uses reviewed_at (not review_id) for ordering.

    INVOICE_REVIEW uses AUTOINCREMENT NOORDER, which does NOT guarantee
    monotonically increasing IDs. The view must sort by reviewed_at DESC
    to reliably pick the latest review row.
    """

    WRITEBACK_DDL = os.path.join(
        os.path.dirname(__file__), "..", "sql", "08_writeback.sql"
    )

    def test_live_view_uses_reviewed_at(self, sf_cursor):
        """The deployed V_DOCUMENT_SUMMARY must ORDER BY reviewed_at DESC."""
        sf_cursor.execute(
            f"SELECT GET_DDL('VIEW', '{FQ}.V_DOCUMENT_SUMMARY') AS ddl"
        )
        ddl = sf_cursor.fetchone()[0].upper()
        assert "ORDER BY REVIEWED_AT DESC" in ddl, (
            "V_DOCUMENT_SUMMARY sorts by review_id instead of reviewed_at. "
            "NOORDER autoincrement means review_id is NOT monotonic — "
            "status updates will appear to not persist."
        )

    def test_live_view_does_not_use_review_id_order(self, sf_cursor):
        """Ensure review_id DESC is not used for ordering in the view."""
        sf_cursor.execute(
            f"SELECT GET_DDL('VIEW', '{FQ}.V_DOCUMENT_SUMMARY') AS ddl"
        )
        ddl = sf_cursor.fetchone()[0].upper()
        assert "ORDER BY REVIEW_ID DESC" not in ddl, (
            "V_DOCUMENT_SUMMARY still uses ORDER BY review_id DESC. "
            "This is unreliable with NOORDER autoincrement. "
            "Change to ORDER BY reviewed_at DESC."
        )

    def test_writeback_ddl_uses_reviewed_at(self):
        """08_writeback.sql must use reviewed_at for QUALIFY ordering."""
        with open(self.WRITEBACK_DDL) as f:
            content = f.read()
        assert "order by reviewed_at desc" in content.lower().replace("  ", " "), (
            "08_writeback.sql QUALIFY clause uses review_id instead of reviewed_at"
        )

    def test_writeback_ddl_no_review_id_order(self):
        """08_writeback.sql must NOT have ORDER BY review_id DESC in QUALIFY."""
        with open(self.WRITEBACK_DDL) as f:
            content = f.read()
        # Only check non-comment lines
        active_lines = [
            line for line in content.splitlines()
            if not line.strip().startswith("--")
        ]
        active_sql = "\n".join(active_lines).lower()
        assert "order by review_id desc" not in active_sql, (
            "08_writeback.sql still has ORDER BY review_id DESC in active SQL. "
            "NOORDER autoincrement makes this unreliable."
        )

    def test_review_status_persists_after_update(self, sf_cursor):
        """Insert two reviews for the same record; view must show the later one."""
        # Find a valid record_id to test with
        sf_cursor.execute(
            f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if not row:
            pytest.skip("No extracted records to test with")
        rid, fname = row[0], row[1]

        tag = "__pytest_noorder_check__"

        # Insert CORRECTED first
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', %s)",
            (rid, fname, f"{tag}_1"),
        )

        # Brief pause so reviewed_at is strictly later
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")

        # Insert APPROVED second — this should be the winner
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{tag}_2"),
        )

        # View must show APPROVED (the most recent by reviewed_at)
        sf_cursor.execute(
            f"SELECT review_status FROM {FQ}.V_DOCUMENT_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        view_row = sf_cursor.fetchone()
        assert view_row is not None, "Record not found in V_DOCUMENT_SUMMARY"
        assert view_row[0] == "APPROVED", (
            f"View shows '{view_row[0]}' instead of 'APPROVED'. "
            "The view is not picking the latest review by reviewed_at."
        )

        # Cleanup test rows
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{tag}%'"
        )


# ---------------------------------------------------------------------------
# 11. Streamlit page SQL validation — every page's queries run without error
# ---------------------------------------------------------------------------
class TestStreamlitPageSQL:
    """Run the core SQL query from every Streamlit page against live objects.

    Not a full UI test, but validates that every SQL statement compiles and
    executes against the current schema. Catches missing views, wrong column
    names, and broken joins before users ever hit the page.
    """

    def test_home_extraction_status(self, sf_cursor):
        """streamlit_app.py: V_EXTRACTION_STATUS query."""
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_EXTRACTION_STATUS")
        assert sf_cursor.fetchone() is not None

    def test_home_extraction_summary(self, sf_cursor):
        """streamlit_app.py: extraction summary counts."""
        sf_cursor.execute(f"""
            SELECT
                (SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS) AS documents,
                (SELECT COUNT(*) FROM {FQ}.EXTRACTED_TABLE_DATA) AS line_items,
                (SELECT COUNT(DISTINCT field_1) FROM {FQ}.EXTRACTED_FIELDS
                 WHERE field_1 IS NOT NULL) AS unique_senders
        """)
        row = sf_cursor.fetchone()
        assert row is not None

    def test_dashboard_kpi_query(self, sf_cursor):
        """0_Dashboard.py: KPI aggregate query."""
        sf_cursor.execute(f"""
            SELECT
                COUNT(*) AS total_documents,
                SUM(ef.field_10) AS total_amount,
                COUNT(DISTINCT ef.field_1) AS unique_senders,
                COUNT(CASE WHEN ef.field_5 IS NOT NULL
                            AND ef.field_5 < CURRENT_DATE() THEN 1 END) AS overdue_count
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        """)
        assert sf_cursor.fetchone() is not None

    def test_dashboard_recent_documents(self, sf_cursor):
        """0_Dashboard.py: recent documents query."""
        sf_cursor.execute(f"""
            SELECT
                ef.field_2 AS document_number,
                ef.field_1 AS sender,
                ef.field_4 AS document_date,
                ef.field_10 AS total_amount,
                ef.status,
                ef.extracted_at
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            ORDER BY ef.extracted_at DESC NULLS LAST
            LIMIT 15
        """)
        # May return 0 rows if no data, but should not error
        sf_cursor.fetchall()

    def test_viewer_sender_list(self, sf_cursor):
        """1_Document_Viewer.py: distinct sender query."""
        sf_cursor.execute(f"""
            SELECT DISTINCT ef.field_1 AS sender
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            WHERE ef.field_1 IS NOT NULL
            ORDER BY sender
        """)
        sf_cursor.fetchall()

    def test_viewer_document_ledger(self, sf_cursor):
        """1_Document_Viewer.py: main document list query."""
        sf_cursor.execute(f"""
            SELECT
                ef.record_id, ef.file_name,
                ef.field_1 AS sender, ef.field_2 AS document_number,
                ef.field_4 AS document_date, ef.field_10 AS total_amount,
                ef.status
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            ORDER BY ef.field_4 DESC NULLS LAST
        """)
        sf_cursor.fetchall()

    def test_viewer_aging_buckets(self, sf_cursor):
        """1_Document_Viewer.py / 2_Analytics.py: V_DOCUMENT_LEDGER aging query."""
        sf_cursor.execute(f"""
            SELECT
                aging_bucket, COUNT(*) AS document_count,
                SUM(total_amount) AS total_amount, sort_order
            FROM (
                SELECT total_amount,
                    CASE
                        WHEN due_date IS NULL THEN 'N/A'
                        WHEN due_date >= CURRENT_DATE() THEN 'Current'
                        WHEN DATEDIFF('day', due_date, CURRENT_DATE()) <= 30 THEN '1-30 Days'
                        ELSE '30+ Days'
                    END AS aging_bucket,
                    CASE
                        WHEN due_date IS NULL THEN 99
                        WHEN due_date >= CURRENT_DATE() THEN 0
                        ELSE 1
                    END AS sort_order
                FROM {FQ}.V_DOCUMENT_LEDGER
            ) sub
            GROUP BY aging_bucket, sort_order
            ORDER BY sort_order
        """)
        sf_cursor.fetchall()

    def test_viewer_line_items(self, sf_cursor):
        """1_Document_Viewer.py: line items query."""
        sf_cursor.execute(f"""
            SELECT line_number, col_1, col_2, col_3, col_4, col_5
            FROM {FQ}.EXTRACTED_TABLE_DATA
            ORDER BY line_number
            LIMIT 10
        """)
        sf_cursor.fetchall()

    def test_viewer_line_item_detail_view(self, sf_cursor):
        """1_Document_Viewer.py: V_LINE_ITEM_DETAIL corrected line items query."""
        sf_cursor.execute(f"""
            SELECT line_id, line_number, description, category,
                   quantity, unit_price, line_total
            FROM {FQ}.V_LINE_ITEM_DETAIL
            ORDER BY line_number
            LIMIT 10
        """)
        sf_cursor.fetchall()

    def test_analytics_vendor_summary(self, sf_cursor):
        """2_Analytics.py: vendor spend summary."""
        sf_cursor.execute(f"""
            SELECT
                ef.field_1 AS vendor_name,
                COUNT(*) AS document_count,
                SUM(ef.field_10) AS total_amount
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            WHERE ef.field_1 IS NOT NULL
            GROUP BY ef.field_1
            ORDER BY total_amount DESC
            LIMIT 15
        """)
        sf_cursor.fetchall()

    def test_analytics_monthly_trend(self, sf_cursor):
        """2_Analytics.py: monthly trend query."""
        sf_cursor.execute(f"""
            SELECT
                DATE_TRUNC('month', ef.field_4) AS month,
                COUNT(*) AS document_count,
                SUM(ef.field_10) AS total_amount
            FROM {FQ}.EXTRACTED_FIELDS ef
                JOIN {FQ}.RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
            WHERE ef.field_4 IS NOT NULL
            GROUP BY DATE_TRUNC('month', ef.field_4)
            ORDER BY month
        """)
        sf_cursor.fetchall()

    def test_analytics_top_items(self, sf_cursor):
        """2_Analytics.py: top line items query."""
        sf_cursor.execute(f"""
            SELECT
                etd.col_1 AS item_description,
                etd.col_2 AS category,
                COUNT(*) AS appearance_count,
                SUM(etd.col_5) AS total_spend
            FROM {FQ}.EXTRACTED_TABLE_DATA etd
                JOIN {FQ}.RAW_DOCUMENTS rd ON etd.file_name = rd.file_name
            WHERE etd.col_1 IS NOT NULL
            GROUP BY etd.col_1, etd.col_2
            ORDER BY total_spend DESC
            LIMIT 20
        """)
        sf_cursor.fetchall()

    def test_review_document_summary(self, sf_cursor):
        """3_Review.py: V_DOCUMENT_SUMMARY query."""
        sf_cursor.execute(f"""
            SELECT * FROM {FQ}.V_DOCUMENT_SUMMARY
            ORDER BY record_id DESC
            LIMIT 5
        """)
        sf_cursor.fetchall()

    def test_review_vendor_filter(self, sf_cursor):
        """3_Review.py: distinct vendor_name from V_DOCUMENT_SUMMARY."""
        sf_cursor.execute(f"""
            SELECT DISTINCT vendor_name
            FROM {FQ}.V_DOCUMENT_SUMMARY
            WHERE vendor_name IS NOT NULL
            ORDER BY vendor_name
        """)
        sf_cursor.fetchall()

    def test_admin_config_list(self, sf_cursor):
        """4_Admin.py: DOCUMENT_TYPE_CONFIG listing."""
        sf_cursor.execute(
            f"SELECT * FROM {FQ}.DOCUMENT_TYPE_CONFIG ORDER BY doc_type"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "DOCUMENT_TYPE_CONFIG is empty — at least one doc type must be configured"
        )


# ---------------------------------------------------------------------------
# 12. Streamlit write SQL validation — no PARSE_JSON in VALUES, INSERT SELECT
# ---------------------------------------------------------------------------
class TestStreamlitWriteSQL:
    """Validate that write SQL in Streamlit pages uses safe patterns.

    PARSE_JSON(?) is not allowed inside VALUES clauses. All inserts with
    function calls must use INSERT INTO ... SELECT instead.
    """

    STREAMLIT_DIR = os.path.join(os.path.dirname(__file__), "..", "streamlit")

    def test_review_insert_uses_select(self):
        """3_Review.py must use INSERT ... SELECT, not INSERT ... VALUES with functions."""
        review_py = os.path.join(self.STREAMLIT_DIR, "pages", "3_Review.py")
        with open(review_py) as f:
            content = f.read()
        # Find the INSERT INTO INVOICE_REVIEW block
        assert "INSERT INTO" in content, "3_Review.py has no INSERT statement"
        # Should use SELECT after the column list, not VALUES
        # Look for the pattern: closing paren of column list followed by SELECT
        assert ") SELECT" in content or ")SELECT" in content or ") SELECT" in content.replace("\n", " "), (
            "3_Review.py INSERT INTO INVOICE_REVIEW uses VALUES instead of SELECT. "
            "PARSE_JSON(?) is not allowed inside VALUES clauses."
        )

    def test_admin_insert_uses_select(self):
        """4_Admin.py must use INSERT ... SELECT, not INSERT ... VALUES with functions."""
        admin_py = os.path.join(self.STREAMLIT_DIR, "pages", "4_Admin.py")
        with open(admin_py) as f:
            content = f.read()
        assert "INSERT INTO" in content, "4_Admin.py has no INSERT statement"
        assert ") SELECT" in content or ")SELECT" in content or ") SELECT" in content.replace("\n", " "), (
            "4_Admin.py INSERT INTO DOCUMENT_TYPE_CONFIG uses VALUES instead of SELECT. "
            "PARSE_JSON(?) is not allowed inside VALUES clauses."
        )

    def test_no_parse_json_in_values(self):
        """No Streamlit .py file should have PARSE_JSON inside a VALUES clause."""
        import glob as globmod
        pattern = os.path.join(self.STREAMLIT_DIR, "**", "*.py")
        for filepath in globmod.glob(pattern, recursive=True):
            with open(filepath) as f:
                content = f.read()
            # Normalize whitespace for matching
            flat = " ".join(content.split())
            assert "VALUES" not in flat or "PARSE_JSON" not in flat.split("VALUES")[-1].split(")")[0] if "VALUES" in flat else True, (
                f"{os.path.basename(filepath)} has PARSE_JSON inside a VALUES clause. "
                "Use INSERT ... SELECT instead of INSERT ... VALUES for function calls."
            )

    def test_review_insert_compiles_live(self, sf_cursor):
        """INSERT INTO INVOICE_REVIEW ... SELECT ... PARSE_JSON(?) actually executes."""
        # Find a valid record to test with
        sf_cursor.execute(
            f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if not row:
            pytest.skip("No extracted records to test with")
        rid, fname = row[0], row[1]

        tag = "__pytest_insert_compile__"
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW ("
            f"  record_id, file_name, review_status,"
            f"  corrected_vendor_name, reviewer_notes, corrections"
            f") SELECT %s, %s, 'CORRECTED', 'Test', %s, PARSE_JSON(%s)",
            (rid, fname, tag, '{"test": true}'),
        )

        # Verify it was inserted
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (tag,),
        )
        assert sf_cursor.fetchone()[0] == 1, "INSERT ... SELECT ... PARSE_JSON(?) failed"

        # Cleanup
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes = %s",
            (tag,),
        )
