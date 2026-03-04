-- =============================================================================
-- 07_deploy_streamlit.sql — Deploy the Streamlit Dashboard
--
-- This creates:
--   1. A stage for Streamlit app files
--   2. A compute pool for Container Runtime
--   3. The Streamlit app itself
--
-- Run this AFTER extraction is working (scripts 01-05).
-- =============================================================================

USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Step 1: Create stage for Streamlit app files
-- ---------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit in Snowflake app files';

-- ---------------------------------------------------------------------------
-- Step 2: Upload Streamlit files
-- ---------------------------------------------------------------------------
-- Upload the streamlit/ folder contents from this POC kit.
--
-- OPTION A: Snowflake CLI
--   snow stage copy streamlit/streamlit_app.py @STREAMLIT_STAGE/  --overwrite
--   snow stage copy streamlit/config.py       @STREAMLIT_STAGE/  --overwrite
--   snow stage copy streamlit/environment.yml @STREAMLIT_STAGE/  --overwrite
--   snow stage copy streamlit/pages/          @STREAMLIT_STAGE/pages/ --overwrite
--
-- OPTION B: SnowSQL
--   PUT file://streamlit/streamlit_app.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://streamlit/config.py        @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://streamlit/environment.yml  @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--   PUT file://streamlit/pages/*.py       @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- OPTION C: Snowsight UI
--   Navigate to the STREAMLIT_STAGE and upload files via drag-and-drop.

-- Verify uploads
SELECT * FROM DIRECTORY(@STREAMLIT_STAGE) ORDER BY RELATIVE_PATH;

-- ---------------------------------------------------------------------------
-- Step 3: Create compute pool (required for Container Runtime)
-- ---------------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS AI_EXTRACT_POC_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pool for AI_EXTRACT POC Streamlit app';

-- ---------------------------------------------------------------------------
-- Step 4: Create External Access Integration (required for pip packages)
-- ---------------------------------------------------------------------------
-- Container Runtime needs network access to install packages from PyPI.
-- This requires ACCOUNTADMIN.

USE ROLE ACCOUNTADMIN;

CREATE NETWORK RULE IF NOT EXISTS PYPI_NETWORK_RULE
    TYPE = 'HOST_PORT'
    MODE = 'EGRESS'
    VALUE_LIST = ('pypi.org', 'files.pythonhosted.org');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS PYPI_ACCESS_INTEGRATION
    ALLOWED_NETWORK_RULES = (PYPI_NETWORK_RULE)
    ENABLED = TRUE
    COMMENT = 'Allow pip install from PyPI for Container Runtime';

-- Switch back to your working role (must be the owner of the POC database)
USE ROLE ACCOUNTADMIN;  -- <-- EDIT to the role that owns AI_EXTRACT_POC
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;

-- ---------------------------------------------------------------------------
-- Step 5: Create the Streamlit app
-- ---------------------------------------------------------------------------
CREATE OR REPLACE STREAMLIT AI_EXTRACT_DASHBOARD
    ROOT_LOCATION = '@AI_EXTRACT_POC.DOCUMENTS.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = AI_EXTRACT_WH
    COMPUTE_POOL = AI_EXTRACT_POC_POOL
    EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION)
    RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
    TITLE = 'AI_EXTRACT Document Processing'
    COMMENT = 'Document extraction dashboard powered by Cortex AI_EXTRACT';

-- ---------------------------------------------------------------------------
-- Step 6: Grant access (optional — for other users in your account)
-- ---------------------------------------------------------------------------
-- GRANT USAGE ON STREAMLIT AI_EXTRACT_DASHBOARD TO ROLE <target_role>;

-- ---------------------------------------------------------------------------
-- Open the app
-- ---------------------------------------------------------------------------
-- Navigate to Snowsight > Projects > Streamlit > AI_EXTRACT_DASHBOARD
-- Or use: SHOW STREAMLITS;
SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD';
