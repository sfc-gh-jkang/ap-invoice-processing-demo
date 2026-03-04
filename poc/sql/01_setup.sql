-- =============================================================================
-- 01_setup.sql — Infrastructure Setup for AI_EXTRACT POC
--
-- INSTRUCTIONS: Edit the 4 variables below, then run this entire script.
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  CONFIGURE THESE — set to your preferred names                         │
-- └─────────────────────────────────────────────────────────────────────────┘
SET poc_db        = 'AI_EXTRACT_POC';
SET poc_schema    = 'DOCUMENTS';
SET poc_warehouse = 'AI_EXTRACT_WH';
SET poc_stage     = 'DOCUMENT_STAGE';

-- =============================================================================
-- PREREQUISITES CHECKLIST (review before running)
-- =============================================================================
--
-- 1. ACCOUNT REGION — AI_EXTRACT is available in these regions:
--      AWS:   US West 2, US East 1, CA Central 1, EU Central 1, EU West 1,
--             SA East 1, AP Northeast 1, AP Southeast 2
--      Azure: East US 2, West US 2, South Central US, North Europe,
--             West Europe, Central India, Japan East, Southeast Asia,
--             Australia East
--    If your region is not listed, enable cross-region inference:
--      ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
--
-- 2. CORTEX ACCESS — Your role needs the SNOWFLAKE.CORTEX_USER database role.
--    See Step 1 below.
--
-- 3. SUPPORTED FILE TYPES:
--      PDF, PNG, JPEG/JPG, DOCX/DOC, PPTX/PPT, EML, HTML/HTM,
--      TXT/TEXT, TIF/TIFF, BMP, GIF, WEBP, MD
--
-- 4. FILE LIMITS:
--      - Max 125 pages per document
--      - Max 100 MB per file
--      - Client-side encrypted stages are NOT supported (use SNOWFLAKE_SSE)
--
-- 5. EXTRACTION LIMITS (per AI_EXTRACT call):
--      - Max 100 entity extraction questions
--      - Max 10 table extraction questions
--      - 1 table question = 10 entity questions
--        (e.g., 4 table + 60 entity questions in one call)
--
-- 6. COST:
--      - Each page = 970 tokens (PDF, DOCX, TIF)
--      - Each image file = 1 page (970 tokens)
--      - X-SMALL to MEDIUM warehouse recommended (larger doesn't help)
--
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Step 1: Grant Cortex access to your role (requires ACCOUNTADMIN)
-- ---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Replace YOUR_ROLE with the role you'll use for this POC
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN;  -- <-- EDIT THIS ROLE

-- ---------------------------------------------------------------------------
-- Step 2: Create database, schema, warehouse
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS IDENTIFIER($poc_db);
USE DATABASE IDENTIFIER($poc_db);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($poc_schema);
USE SCHEMA IDENTIFIER($poc_schema);

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($poc_warehouse)
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'AI_EXTRACT POC — X-Small is optimal (larger does not improve AI_EXTRACT performance)';

USE WAREHOUSE IDENTIFIER($poc_warehouse);

-- ---------------------------------------------------------------------------
-- Step 3: Create internal stage for your documents
-- ---------------------------------------------------------------------------
-- IMPORTANT: SNOWFLAKE_SSE encryption is REQUIRED for AI_EXTRACT.
-- Client-side encrypted stages (the default) will NOT work.
-- You cannot change encryption after stage creation — must create correctly.

CREATE STAGE IF NOT EXISTS IDENTIFIER($poc_stage)
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Document stage for AI_EXTRACT POC — SSE encryption required';

-- ---------------------------------------------------------------------------
-- Step 4: Upload your documents
-- ---------------------------------------------------------------------------
-- OPTION A: Snowsight UI (easiest)
--   1. Navigate to Data > Databases > AI_EXTRACT_POC > DOCUMENTS > Stages
--   2. Click on DOCUMENT_STAGE
--   3. Click "+ Files" button
--   4. Drag and drop your PDFs / images / documents
--
-- OPTION B: SnowSQL / SQL client
--   PUT file:///path/to/your/documents/*.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE;
--
-- OPTION C: Snowflake CLI
--   snow stage copy /path/to/your/documents/*.pdf @DOCUMENT_STAGE --overwrite
--
-- After uploading, refresh the directory table and verify:
ALTER STAGE DOCUMENT_STAGE REFRESH;
SELECT * FROM DIRECTORY(@DOCUMENT_STAGE) ORDER BY LAST_MODIFIED DESC;
