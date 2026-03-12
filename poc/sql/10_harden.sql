-- =============================================================================
-- 10_harden.sql — Production Hardening
--
-- Run AFTER all POC objects are created (scripts 01-09).
-- This script:
--   1. Transfers ownership to SYSADMIN so PUBLIC cannot inherit via owner chain
--   2. Enables MANAGED ACCESS on the schema
--   3. Revokes PUBLIC access to POC database/schema
--   4. Revokes overly broad account-level grants from AI_EXTRACT_APP
--   5. Revokes BIND SERVICE ENDPOINT after Streamlit is deployed
--   6. Adds a resource monitor to cap warehouse credit consumption
--   7. Grants back all privileges to AI_EXTRACT_APP
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Must run as ACCOUNTADMIN to modify grants and create resource monitors
-- ---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------------
-- Step 1: Transfer ownership to SYSADMIN
-- ---------------------------------------------------------------------------
-- When AI_EXTRACT_APP owns objects, PUBLIC inherits access through the
-- Snowflake role hierarchy. Transfer ownership to SYSADMIN so that
-- AI_EXTRACT_APP accesses objects via explicit grants only.

GRANT OWNERSHIP ON DATABASE AI_EXTRACT_POC TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ALL STAGES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON WAREHOUSE AI_EXTRACT_WH TO ROLE SYSADMIN COPY CURRENT GRANTS;

-- ---------------------------------------------------------------------------
-- Step 2: Enable MANAGED ACCESS
-- ---------------------------------------------------------------------------
-- Only the schema owner (SYSADMIN) can grant privileges in a managed schema.

USE ROLE SYSADMIN;
ALTER SCHEMA AI_EXTRACT_POC.DOCUMENTS ENABLE MANAGED ACCESS;

-- ---------------------------------------------------------------------------
-- Step 3: Revoke PUBLIC access
-- ---------------------------------------------------------------------------
REVOKE USAGE ON DATABASE AI_EXTRACT_POC FROM ROLE PUBLIC;
REVOKE USAGE ON SCHEMA AI_EXTRACT_POC.DOCUMENTS FROM ROLE PUBLIC;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS FROM ROLE PUBLIC;
REVOKE ALL PRIVILEGES ON ALL VIEWS IN SCHEMA AI_EXTRACT_POC.DOCUMENTS FROM ROLE PUBLIC;

-- Also revoke PUBLIC usage on the warehouse
USE ROLE ACCOUNTADMIN;
REVOKE USAGE ON WAREHOUSE AI_EXTRACT_WH FROM ROLE PUBLIC;

-- Grant ACCOUNT_USAGE access for cost observability views
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE AI_EXTRACT_APP;

-- ---------------------------------------------------------------------------
-- Step 4: Revoke account-level grants from AI_EXTRACT_APP
-- ---------------------------------------------------------------------------
-- These were needed during setup to let AI_EXTRACT_APP create the DB and WH.
-- Now that those objects exist, the role should NOT be able to create more.

REVOKE CREATE DATABASE ON ACCOUNT FROM ROLE AI_EXTRACT_APP;
REVOKE CREATE WAREHOUSE ON ACCOUNT FROM ROLE AI_EXTRACT_APP;

-- ---------------------------------------------------------------------------
-- Step 5: Revoke BIND SERVICE ENDPOINT (if Streamlit is already deployed)
-- ---------------------------------------------------------------------------
REVOKE BIND SERVICE ENDPOINT ON ACCOUNT FROM ROLE AI_EXTRACT_APP;

-- ---------------------------------------------------------------------------
-- Step 6: Create a resource monitor for AI_EXTRACT_WH
-- ---------------------------------------------------------------------------
-- Caps the warehouse at 100 credits/month. Notifies at 75% and 90%,
-- suspends at 100%. Adjust CREDIT_QUOTA for your workload.

CREATE RESOURCE MONITOR IF NOT EXISTS AI_EXTRACT_MONITOR
    WITH
        CREDIT_QUOTA = 100
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 75 PERCENT DO NOTIFY
            ON 90 PERCENT DO NOTIFY
            ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE AI_EXTRACT_WH SET RESOURCE_MONITOR = AI_EXTRACT_MONITOR;

-- ---------------------------------------------------------------------------
-- Step 7: Grant all privileges back to AI_EXTRACT_APP
-- ---------------------------------------------------------------------------
-- SYSADMIN owns the objects; AI_EXTRACT_APP gets full operational access.

USE ROLE SYSADMIN;

ALTER TABLE AI_EXTRACT_POC.DOCUMENTS.EXTRACTED_FIELDS SET DATA_RETENTION_TIME_IN_DAYS = 14;
ALTER TABLE AI_EXTRACT_POC.DOCUMENTS.INVOICE_REVIEW SET DATA_RETENTION_TIME_IN_DAYS = 14;
ALTER TABLE AI_EXTRACT_POC.DOCUMENTS.LINE_ITEM_REVIEW SET DATA_RETENTION_TIME_IN_DAYS = 14;

GRANT ALL PRIVILEGES ON DATABASE AI_EXTRACT_POC TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;

-- Warehouse: grant operational access (ownership is SYSADMIN)
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE AI_EXTRACT_WH TO ROLE AI_EXTRACT_APP;
GRANT MONITOR ON WAREHOUSE AI_EXTRACT_WH TO ROLE AI_EXTRACT_APP;
GRANT MODIFY ON WAREHOUSE AI_EXTRACT_WH TO ROLE AI_EXTRACT_APP;

-- Grant future privileges so new objects are also accessible
USE ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA AI_EXTRACT_POC.DOCUMENTS TO ROLE AI_EXTRACT_APP;

-- ---------------------------------------------------------------------------
-- Step 8: Verify hardening
-- ---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
SHOW GRANTS ON DATABASE AI_EXTRACT_POC;
SHOW GRANTS ON SCHEMA AI_EXTRACT_POC.DOCUMENTS;
SHOW RESOURCE MONITORS LIKE 'AI_EXTRACT_MONITOR';
SHOW GRANTS TO ROLE AI_EXTRACT_APP;
