-- =============================================================================
-- 07a_streamlit_prereqs.sql — ACCOUNTADMIN prerequisites for Container Runtime
--
-- Creates compute pool, network rule, EAI, and grants required by the
-- Streamlit app.  Must run as ACCOUNTADMIN (or a role with CREATE COMPUTE
-- POOL and CREATE INTEGRATION privileges).
--
-- deploy_poc.sh runs this automatically before 07_deploy_streamlit.sql.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE COMPUTE POOL IF NOT EXISTS AI_EXTRACT_POC_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pool for AI_EXTRACT POC Streamlit app';

CREATE NETWORK RULE IF NOT EXISTS AI_EXTRACT_POC.DOCUMENTS.PYPI_NETWORK_RULE
    TYPE = 'HOST_PORT'
    MODE = 'EGRESS'
    VALUE_LIST = ('pypi.org', 'files.pythonhosted.org');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS PYPI_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (AI_EXTRACT_POC.DOCUMENTS.PYPI_NETWORK_RULE)
    ENABLED = TRUE
    COMMENT = 'Allow pip install from PyPI for Container Runtime';

GRANT USAGE ON INTEGRATION PYPI_ACCESS_INTEGRATION TO ROLE AI_EXTRACT_APP;
GRANT USAGE ON COMPUTE POOL AI_EXTRACT_POC_POOL TO ROLE AI_EXTRACT_APP;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE AI_EXTRACT_APP;
