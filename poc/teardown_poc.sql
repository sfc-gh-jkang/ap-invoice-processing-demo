-- =============================================================================
-- teardown_poc.sql — Remove all AI_EXTRACT POC objects
--
-- Run this to clean up after the POC is complete.
-- This drops the database (which removes all tables, views, stages, streams,
-- tasks, and stored procedures), the warehouse, and the compute pool.
-- =============================================================================

-- Suspend the automation task first (avoids errors on drop)
ALTER TASK IF EXISTS AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;

-- Drop the database (removes everything inside: schema, tables, views, stage, stream, task, proc)
DROP DATABASE IF EXISTS AI_EXTRACT_POC;

-- Drop the warehouse
DROP WAREHOUSE IF EXISTS AI_EXTRACT_WH;

-- Drop the compute pool (if the Streamlit dashboard was deployed)
DROP COMPUTE POOL IF EXISTS AI_EXTRACT_POC_POOL;

-- Optionally drop the External Access Integration and Network Rule
-- (only if no other apps use them — check first with SHOW EXTERNAL ACCESS INTEGRATIONS)
-- USE ROLE ACCOUNTADMIN;
-- DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PYPI_ACCESS_INTEGRATION;
-- DROP NETWORK RULE IF EXISTS PYPI_NETWORK_RULE;

-- Drop the dedicated POC role (requires ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS AI_EXTRACT_APP;

-- Verify cleanup
SHOW DATABASES LIKE 'AI_EXTRACT_POC';
SHOW WAREHOUSES LIKE 'AI_EXTRACT_WH';
