-- =============================================================================
-- 01_setup.sql — Infrastructure Setup for AI_EXTRACT POC (AI_EXTRACT_APP role)
--
-- Creates database, schema, warehouse, and document stage.
-- Assumes the AI_EXTRACT_APP role already exists with necessary grants
-- (see 01a_setup_prereqs.sql for ACCOUNTADMIN prerequisites).
--
-- deploy_poc.sh runs 01a first, then this file.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;

CREATE DATABASE IF NOT EXISTS AI_EXTRACT_POC;
USE DATABASE AI_EXTRACT_POC;

CREATE SCHEMA IF NOT EXISTS DOCUMENTS;
USE SCHEMA DOCUMENTS;

CREATE WAREHOUSE IF NOT EXISTS AI_EXTRACT_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'AI_EXTRACT POC — X-Small is optimal (larger does not improve AI_EXTRACT performance)';

USE WAREHOUSE AI_EXTRACT_WH;

-- IMPORTANT: SNOWFLAKE_SSE encryption is REQUIRED for AI_EXTRACT.
CREATE STAGE IF NOT EXISTS DOCUMENT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Document stage for AI_EXTRACT POC — SSE encryption required';

ALTER STAGE DOCUMENT_STAGE REFRESH;
SELECT * FROM DIRECTORY(@DOCUMENT_STAGE) ORDER BY LAST_MODIFIED DESC;
