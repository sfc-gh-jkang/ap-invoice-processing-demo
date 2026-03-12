-- =============================================================================
-- 01a_setup_prereqs.sql — ACCOUNTADMIN prerequisites for AI_EXTRACT POC
--
-- Creates the POC role, grants Cortex access, and enables cross-region
-- inference.  Must run as ACCOUNTADMIN.
--
-- deploy_poc.sh runs this automatically before 01_setup.sql.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS AI_EXTRACT_APP;
GRANT ROLE AI_EXTRACT_APP TO ROLE SYSADMIN;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE AI_EXTRACT_APP;

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

GRANT CREATE DATABASE ON ACCOUNT TO ROLE AI_EXTRACT_APP;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE AI_EXTRACT_APP;
