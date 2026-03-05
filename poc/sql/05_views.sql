-- =============================================================================
-- 05_views.sql — Analytical Views for Dashboard and Reporting
--
-- These views power the Streamlit app and can also be queried directly.
-- CUSTOMIZE: Rename column references to match your EXTRACTED_FIELDS and
-- EXTRACTED_TABLE_DATA columns from 02_tables.sql.
-- =============================================================================

USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- V_EXTRACTION_STATUS: Pipeline monitoring — how many files processed?
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_EXTRACTION_STATUS AS
SELECT
    COUNT(*)                                                      AS total_files,
    SUM(CASE WHEN extracted = TRUE THEN 1 ELSE 0 END)            AS extracted_files,
    SUM(CASE WHEN extracted = FALSE THEN 1 ELSE 0 END)           AS pending_files,
    SUM(CASE WHEN extraction_error IS NOT NULL THEN 1 ELSE 0 END) AS failed_files,
    MAX(extracted_at)                                              AS last_extraction
FROM RAW_DOCUMENTS;

-- ---------------------------------------------------------------------------
-- V_DOCUMENT_LEDGER: Enriched document view with aging buckets
-- ---------------------------------------------------------------------------
-- This view assumes invoice-style documents with dates and amounts.
-- Adjust the aging logic if your documents don't have due dates.

CREATE OR REPLACE VIEW V_DOCUMENT_LEDGER AS
SELECT
    ef.record_id,
    ef.file_name,
    ef.field_1       AS vendor_name,        -- rename to match your field
    ef.field_2       AS document_number,     -- rename to match your field
    ef.field_3       AS reference,           -- rename to match your field
    ef.field_4       AS document_date,       -- rename to match your field
    ef.field_5       AS due_date,            -- rename to match your field
    ef.field_6       AS terms,              -- rename to match your field
    ef.field_7       AS recipient,          -- rename to match your field
    ef.field_8       AS subtotal,           -- rename to match your field
    ef.field_9       AS tax_amount,         -- rename to match your field
    ef.field_10      AS total_amount,       -- rename to match your field
    ef.status,
    ef.extracted_at,
    -- Aging calculation (days past due) — remove if not applicable
    CASE
        WHEN ef.field_5 IS NULL THEN 0
        ELSE GREATEST(DATEDIFF(day, ef.field_5, CURRENT_DATE()), 0)
    END AS days_past_due,
    -- Aging bucket — remove if not applicable
    CASE
        WHEN ef.field_5 IS NULL THEN 'N/A'
        WHEN CURRENT_DATE() <= ef.field_5 THEN 'Current'
        WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 1 AND 30 THEN '1-30 Days'
        WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
        WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
        ELSE '90+ Days'
    END AS aging_bucket
FROM EXTRACTED_FIELDS ef;

-- ---------------------------------------------------------------------------
-- V_SUMMARY_BY_VENDOR: Aggregate metrics grouped by vendor/sender
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SUMMARY_BY_VENDOR AS
SELECT
    field_1                     AS vendor_name,     -- rename to match your grouping field
    COUNT(*)                    AS document_count,
    SUM(field_10)               AS total_amount,    -- rename to match your amount field
    AVG(field_10)               AS avg_amount,
    MIN(field_4)                AS first_document,  -- rename to match your date field
    MAX(field_4)                AS last_document
FROM EXTRACTED_FIELDS
WHERE field_1 IS NOT NULL
GROUP BY field_1
ORDER BY total_amount DESC;

-- ---------------------------------------------------------------------------
-- V_MONTHLY_TREND: Volume and value over time
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_MONTHLY_TREND AS
SELECT
    DATE_TRUNC('month', field_4)    AS month,        -- rename to match your date field
    COUNT(*)                        AS document_count,
    SUM(field_10)                   AS total_amount,  -- rename to match your amount field
    SUM(field_8)                    AS total_subtotal,
    SUM(field_9)                    AS total_tax,
    AVG(field_10)                   AS avg_amount
FROM EXTRACTED_FIELDS
WHERE field_4 IS NOT NULL
GROUP BY DATE_TRUNC('month', field_4)
ORDER BY month;

-- ---------------------------------------------------------------------------
-- V_TOP_LINE_ITEMS: Most common / highest-spend items from table extraction
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_TOP_LINE_ITEMS AS
SELECT
    col_1                   AS item_description,   -- rename to match your column
    col_2                   AS category,            -- rename to match your column
    COUNT(*)                AS appearance_count,
    SUM(col_3)              AS total_quantity,
    AVG(col_4)              AS avg_unit_price,
    SUM(col_5)              AS total_spend
FROM EXTRACTED_TABLE_DATA
WHERE col_1 IS NOT NULL
GROUP BY col_1, col_2
ORDER BY total_spend DESC;

-- ---------------------------------------------------------------------------
-- V_AGING_SUMMARY: Aggregate by aging bucket (invoice-specific)
-- ---------------------------------------------------------------------------
-- Remove this view if your documents don't have due dates.

CREATE OR REPLACE VIEW V_AGING_SUMMARY AS
SELECT
    aging_bucket,
    COUNT(*)                    AS document_count,
    SUM(total_amount)           AS total_amount,
    CASE aging_bucket
        WHEN 'Current'    THEN 1
        WHEN '1-30 Days'  THEN 2
        WHEN '31-60 Days' THEN 3
        WHEN '61-90 Days' THEN 4
        WHEN '90+ Days'   THEN 5
        WHEN 'N/A'        THEN 6
    END AS sort_order
FROM V_DOCUMENT_LEDGER
GROUP BY aging_bucket
ORDER BY sort_order;

-- Verify views
SELECT 'V_EXTRACTION_STATUS' AS view_name, * FROM V_EXTRACTION_STATUS;
SELECT 'V_SUMMARY_BY_VENDOR' AS view_name, COUNT(*) AS row_count FROM V_SUMMARY_BY_VENDOR;
SELECT 'V_MONTHLY_TREND'     AS view_name, COUNT(*) AS row_count FROM V_MONTHLY_TREND;
SELECT 'V_TOP_LINE_ITEMS'    AS view_name, COUNT(*) AS row_count FROM V_TOP_LINE_ITEMS;
