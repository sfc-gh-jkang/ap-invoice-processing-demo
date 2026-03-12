-- =============================================================================
-- 07_deploy_streamlit.sql — Deploy the Streamlit Dashboard (AI_EXTRACT_APP)
--
-- This file contains ONLY statements that run under AI_EXTRACT_APP:
--   1. Create stage for Streamlit app files
--   2. Create the Streamlit app (Container Runtime)
--
-- ACCOUNTADMIN prerequisites (compute pool, EAI, grants) are handled
-- separately by 07a_streamlit_prereqs.sql or by deploy_poc.sh.
-- Run this AFTER extraction is working (scripts 01-05).
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
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
-- Step 2: Create the Streamlit app (Container Runtime)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE STREAMLIT AI_EXTRACT_DASHBOARD
    FROM '@AI_EXTRACT_POC.DOCUMENTS.STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = AI_EXTRACT_WH
    COMPUTE_POOL = AI_EXTRACT_POC_POOL
    EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION)
    RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
    TITLE = 'AI_EXTRACT Document Processing'
    COMMENT = 'Document extraction dashboard powered by Cortex AI_EXTRACT';

ALTER STREAMLIT AI_EXTRACT_DASHBOARD ADD LIVE VERSION FROM LAST;

SHOW STREAMLITS LIKE 'AI_EXTRACT_DASHBOARD';
