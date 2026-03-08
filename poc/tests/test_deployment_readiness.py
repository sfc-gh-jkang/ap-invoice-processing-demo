"""Deployment Readiness Tests — pre-flight checks a customer hits before anything works.

These verify the infrastructure prerequisites that must be true for the POC
to succeed in any Snowflake account. A failure here means the customer's
environment isn't configured correctly — not that the POC code is wrong.
"""

import pytest


pytestmark = pytest.mark.sql


class TestCortexAvailability:
    """Verify AI_EXTRACT and Cortex are available in this account."""

    def test_ai_extract_is_available(self, sf_cursor):
        """AI_EXTRACT function exists and can be called without error.

        This catches region-not-supported and missing CORTEX_USER grant.
        Uses a tiny inline PDF-like test — if the function is unavailable,
        Snowflake returns a compilation or runtime error immediately.
        """
        try:
            sf_cursor.execute(
                "SELECT SYSTEM$TYPEOF(AI_EXTRACT("
                "  TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),"
                "  {'test': 'What is this document about?'}"
                ")) AS type_check"
            )
            result = sf_cursor.fetchone()[0]
            assert result is not None
        except Exception as e:
            err = str(e).upper()
            # If CORTEX_USER not granted or region not supported, fail clearly
            if "CORTEX_USER" in err:
                pytest.fail(
                    "SNOWFLAKE.CORTEX_USER database role not granted to current role. "
                    "Run: GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>"
                )
            elif "NOT SUPPORTED" in err or "REGION" in err:
                pytest.fail(
                    f"AI_EXTRACT not available in this region. "
                    f"Enable cross-region: ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'. "
                    f"Error: {e}"
                )
            else:
                raise

    def test_cortex_user_role_granted(self, sf_cursor):
        """Current role has SNOWFLAKE.CORTEX_USER database role."""
        sf_cursor.execute("SELECT CURRENT_ROLE()")
        current_role = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SHOW GRANTS OF DATABASE ROLE SNOWFLAKE.CORTEX_USER"
        )
        rows = sf_cursor.fetchall()
        cols = [desc[0] for desc in sf_cursor.description]
        grantee_idx = cols.index("grantee_name")
        granted_roles = [row[grantee_idx] for row in rows]
        assert current_role in granted_roles, (
            f"SNOWFLAKE.CORTEX_USER not granted to {current_role}. "
            f"Granted to: {granted_roles}"
        )


class TestStageEncryption:
    """Verify stage encryption is SNOWFLAKE_SSE (required for AI_EXTRACT)."""

    def test_document_stage_is_snowflake_sse(self, sf_cursor):
        """Stage must use SNOWFLAKE_SSE — client-side encryption will NOT work."""
        sf_cursor.execute("SHOW STAGES LIKE 'DOCUMENT_STAGE'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, "DOCUMENT_STAGE not found"
        cols = [desc[0] for desc in sf_cursor.description]
        type_idx = cols.index("type")
        has_key_idx = cols.index("has_encryption_key")
        stage_type = rows[0][type_idx]
        has_key = rows[0][has_key_idx]
        # SNOWFLAKE_SSE stages show as "INTERNAL NO CSE" with has_encryption_key=N
        # Client-side encrypted stages show as "INTERNAL" with has_encryption_key=Y
        assert "NO CSE" in stage_type.upper() or has_key == "N", (
            f"DOCUMENT_STAGE uses client-side encryption (type={stage_type}, "
            f"has_encryption_key={has_key}). "
            "AI_EXTRACT requires SNOWFLAKE_SSE. Client-side encrypted stages will NOT work. "
            "You must recreate the stage: DROP STAGE DOCUMENT_STAGE; "
            "CREATE STAGE DOCUMENT_STAGE DIRECTORY=(ENABLE=TRUE) ENCRYPTION=(TYPE='SNOWFLAKE_SSE')"
        )


class TestStageHasFiles:
    """Verify documents were uploaded successfully."""

    def test_stage_has_at_least_one_file(self, sf_cursor):
        """Stage must have files for extraction to work."""
        sf_cursor.execute("SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE)")
        count = sf_cursor.fetchone()[0]
        assert count >= 1, (
            "DOCUMENT_STAGE is empty. Upload documents before running extraction. "
            "Use: PUT file:///path/to/docs/*.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE"
        )

    def test_stage_files_registered_in_raw_documents(self, sf_cursor):
        """Every staged file should be tracked in RAW_DOCUMENTS."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE) d "
            "WHERE d.RELATIVE_PATH LIKE '%.pdf' "
            "  AND d.RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS)"
        )
        unregistered = sf_cursor.fetchone()[0]
        assert unregistered == 0, (
            f"{unregistered} staged file(s) are not in RAW_DOCUMENTS. "
            f"Re-run 02_tables.sql to register them."
        )


class TestExternalAccess:
    """Verify EAI and network rule for Streamlit SPCS deployment."""

    def test_network_rule_exists(self, sf_cursor):
        """A network rule for PyPI access needed for container runtime pip installs.

        Note: The network rule may live in a different database than the POC.
        SHOW NETWORK RULES only searches the current database, so we also
        check IN ACCOUNT. If the rule can't be found, we skip (not fail)
        since the EAI test is the definitive check.
        """
        try:
            # Try account-wide first
            sf_cursor.execute("SHOW NETWORK RULES IN ACCOUNT")
            rows = sf_cursor.fetchall()
            cols = [desc[0] for desc in sf_cursor.description]
            name_idx = cols.index("name")
            pypi_rules = [r for r in rows if "PYPI" in r[name_idx].upper()]
            assert len(pypi_rules) >= 1, (
                "No PyPI network rule found in account. Required for Streamlit container runtime. "
                "Run 07_deploy_streamlit.sql with ACCOUNTADMIN role."
            )
        except Exception as e:
            err = str(e).lower()
            if "does not exist or not authorized" in err or "unexpected" in err:
                pytest.skip(
                    "Cannot query network rules account-wide — "
                    "check PYPI_ACCESS_INTEGRATION test instead."
                )
            raise

    def test_external_access_integration_exists(self, sf_cursor):
        """PYPI_ACCESS_INTEGRATION needed for container runtime pip installs."""
        try:
            sf_cursor.execute(
                "SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'PYPI_ACCESS_INTEGRATION'"
            )
            rows = sf_cursor.fetchall()
            assert len(rows) >= 1, (
                "PYPI_ACCESS_INTEGRATION not found. Required for Streamlit container runtime. "
                "Run 07_deploy_streamlit.sql with ACCOUNTADMIN role."
            )
        except Exception as e:
            if "does not exist or not authorized" in str(e).lower():
                pytest.skip(
                    "Cannot query external access integrations — may need ACCOUNTADMIN."
                )
            raise

    def test_compute_pool_is_ready(self, sf_cursor):
        """Compute pool must exist and be in a usable state."""
        sf_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, "AI_EXTRACT_POC_POOL compute pool not found"
        cols = [desc[0] for desc in sf_cursor.description]
        state_idx = cols.index("state")
        state = rows[0][state_idx]
        # SUSPENDED is OK — Streamlit auto-resumes on access.
        # Only FAILED or unknown states are problems.
        valid_states = ("ACTIVE", "IDLE", "STARTING", "SUSPENDED", "STOPPING")
        assert state in valid_states, (
            f"Compute pool state is {state}. Expected one of {valid_states}. "
            f"You may need to recreate the pool."
        )

    def test_compute_pool_instance_family(self, sf_cursor):
        """Verify compute pool uses CPU_X64_XS (cost-efficient for Streamlit)."""
        sf_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = sf_cursor.fetchall()
        cols = [desc[0] for desc in sf_cursor.description]
        family_idx = cols.index("instance_family")
        family = rows[0][family_idx]
        assert family == "CPU_X64_XS", (
            f"Compute pool uses {family}, expected CPU_X64_XS. "
            f"Larger pools cost more and aren't needed for a Streamlit dashboard."
        )


class TestStreamlitDeployment:
    """Verify Streamlit app files are staged and app is accessible."""

    def test_streamlit_stage_has_main_app(self, sf_cursor):
        """streamlit_app.py must be on STREAMLIT_STAGE."""
        sf_cursor.execute("LS @STREAMLIT_STAGE")
        rows = sf_cursor.fetchall()
        file_names = [str(row[0]) for row in rows]
        has_main = any("streamlit_app.py" in f for f in file_names)
        assert has_main, (
            f"streamlit_app.py not found on STREAMLIT_STAGE. "
            f"Files found: {file_names}"
        )

    def test_streamlit_stage_has_config(self, sf_cursor):
        """config.py must be on STREAMLIT_STAGE."""
        sf_cursor.execute("LS @STREAMLIT_STAGE")
        rows = sf_cursor.fetchall()
        file_names = [str(row[0]) for row in rows]
        has_config = any("config.py" in f for f in file_names)
        assert has_config, (
            f"config.py not found on STREAMLIT_STAGE. "
            f"Files found: {file_names}"
        )

    def test_streamlit_stage_has_pages(self, sf_cursor):
        """Page files must be on STREAMLIT_STAGE."""
        sf_cursor.execute("LS @STREAMLIT_STAGE")
        rows = sf_cursor.fetchall()
        file_names = [str(row[0]) for row in rows]
        page_files = [f for f in file_names if "pages/" in f.lower()]
        assert len(page_files) >= 4, (
            f"Expected at least 4 page files in pages/ on STREAMLIT_STAGE, "
            f"found {len(page_files)}: {page_files}"
        )
