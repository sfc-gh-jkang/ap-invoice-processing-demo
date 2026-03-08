-- =============================================================================
-- 04_batch_extract.sql — Extract Data from ALL Staged Documents
--
-- Run this AFTER you've validated your prompts in 03_test_single_file.sql.
--
-- This script:
--   1. Extracts entity (header) fields from all unprocessed documents
--   2. Extracts table (line-item) data from all unprocessed documents
--   3. Marks files as extracted
--
-- IMPORTANT: Copy the prompts you validated in 03_test_single_file.sql
-- into the AI_EXTRACT calls below. The prompts must match.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Step 1: Entity extraction — header fields from each document
-- ---------------------------------------------------------------------------
-- This runs AI_EXTRACT once per unprocessed file using LATERAL.
-- Each file is read from the stage and the prompt extracts key-value fields.
--
-- CUSTOMIZE: Match the prompt keys to your 03_test_single_file.sql prompts,
-- and map the response fields to your EXTRACTED_FIELDS columns.

INSERT INTO EXTRACTED_FIELDS (
    file_name,
    field_1,        -- vendor_name
    field_2,        -- document_number
    field_3,        -- reference
    field_4,        -- document_date
    field_5,        -- due_date
    field_6,        -- terms
    field_7,        -- recipient
    field_8,        -- subtotal
    field_9,        -- tax
    field_10        -- total
)
SELECT
    r.file_name,
    ext.extraction:response:vendor_name::VARCHAR,
    ext.extraction:response:document_number::VARCHAR,
    ext.extraction:response:reference::VARCHAR,
    TRY_TO_DATE(ext.extraction:response:document_date::VARCHAR),
    TRY_TO_DATE(ext.extraction:response:due_date::VARCHAR),
    ext.extraction:response:terms::VARCHAR,
    ext.extraction:response:recipient::VARCHAR,
    TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:subtotal::VARCHAR, '[^0-9.]', ''), 12, 2),
    TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:tax::VARCHAR, '[^0-9.]', ''), 12, 2),
    TRY_TO_NUMBER(REGEXP_REPLACE(ext.extraction:response:total::VARCHAR, '[^0-9.]', ''), 12, 2)
FROM RAW_DOCUMENTS r,
    LATERAL (
        SELECT
            AI_EXTRACT(
                TO_FILE('@DOCUMENT_STAGE', r.file_name),
                {
                    'vendor_name':    'What is the vendor or company name on this document?',
                    'document_number':'What is the invoice number or document ID?',
                    'reference':      'What is the PO number, reference number, or order number?',
                    'document_date':  'What is the document date or invoice date? Return in YYYY-MM-DD format.',
                    'due_date':       'What is the due date or expiration date? Return in YYYY-MM-DD format.',
                    'terms':          'What are the payment terms or contract terms (e.g., Net 30)?',
                    'recipient':      'Who is this document addressed to? Return name and address.',
                    'subtotal':       'What is the subtotal amount before tax? Return as a number only.',
                    'tax':            'What is the tax amount? Return as a number only.',
                    'total':          'What is the total amount? Return as a number only.'
                }
            ) AS extraction
    ) AS ext
WHERE r.extracted = FALSE
  AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS);

-- Quick check: how many documents were extracted?
SELECT COUNT(*) AS documents_extracted FROM EXTRACTED_FIELDS;
SELECT * FROM EXTRACTED_FIELDS ORDER BY extracted_at DESC LIMIT 10;


-- ---------------------------------------------------------------------------
-- Step 2: Table extraction — line items / tabular data from each document
-- ---------------------------------------------------------------------------
-- This uses the JSON schema format to describe the table structure.
-- LATERAL FLATTEN unnests the parallel arrays into rows.
--
-- IMPORTANT: The FLATTEN WHERE clause (ln.index = pr.index = ...) ensures
-- columns stay aligned. Never remove these conditions.

INSERT INTO EXTRACTED_TABLE_DATA (
    file_name, record_id, line_number,
    col_1,          -- Description
    col_2,          -- Category
    col_3,          -- Qty
    col_4,          -- Unit Price
    col_5           -- Total
)
WITH extracted AS (
    SELECT
        r.file_name,
        ef.field_2 AS record_id,   -- links to parent (e.g., invoice_number)
        AI_EXTRACT(
            file => TO_FILE('@DOCUMENT_STAGE', r.file_name),
            responseFormat => {
                'schema': {
                    'type': 'object',
                    'properties': {
                        'line_items': {
                            'description': 'The table of line items on the document',
                            'type': 'object',
                            'column_ordering': ['Line', 'Description', 'Category', 'Qty', 'Unit Price', 'Total'],
                            'properties': {
                                'Line':       { 'description': 'Line item number',             'type': 'array' },
                                'Description':{ 'description': 'Product or service name',      'type': 'array' },
                                'Category':   { 'description': 'Product category or type',     'type': 'array' },
                                'Qty':        { 'description': 'Quantity',                     'type': 'array' },
                                'Unit Price': { 'description': 'Price per unit in dollars',    'type': 'array' },
                                'Total':      { 'description': 'Line total in dollars',        'type': 'array' }
                            }
                        }
                    }
                }
            }
        ) AS extraction
    FROM RAW_DOCUMENTS r
        JOIN EXTRACTED_FIELDS ef ON r.file_name = ef.file_name
    WHERE r.extracted = FALSE
      AND r.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)
)
SELECT
    e.file_name,
    e.record_id,
    TRY_TO_NUMBER(ln.value::VARCHAR)                                            AS line_number,
    pr.value::VARCHAR                                                           AS col_1,
    ca.value::VARCHAR                                                           AS col_2,
    TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_3,
    TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2)     AS col_4,
    TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2)     AS col_5
FROM extracted e,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line)             ln,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description)      pr,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Category)         ca,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty)              qt,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:"Unit Price")     up,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total)            tl
WHERE ln.index = pr.index
  AND ln.index = ca.index
  AND ln.index = qt.index
  AND ln.index = up.index
  AND ln.index = tl.index;

-- Quick check: how many line items were extracted?
SELECT COUNT(*) AS line_items_extracted FROM EXTRACTED_TABLE_DATA;
SELECT * FROM EXTRACTED_TABLE_DATA ORDER BY file_name, line_number LIMIT 20;


-- ---------------------------------------------------------------------------
-- Step 3: Mark all processed files as extracted
-- ---------------------------------------------------------------------------
UPDATE RAW_DOCUMENTS
SET extracted = TRUE,
    extracted_at = CURRENT_TIMESTAMP()
WHERE extracted = FALSE
  AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

-- Final status check
SELECT
    COUNT(*) AS total_files,
    SUM(CASE WHEN extracted THEN 1 ELSE 0 END) AS extracted,
    SUM(CASE WHEN NOT extracted THEN 1 ELSE 0 END) AS pending
FROM RAW_DOCUMENTS;
