-- =============================================================================
-- 02_tables.sql — Document Tracking and Extraction Results Tables
--
-- These tables store:
--   1. File metadata (what's been staged and processed)
--   2. Entity extraction results (header-level fields from each document)
--   3. Table extraction results (line items / tabular data from each document)
--
-- The fixed columns (field_1..field_10, col_1..col_5) provide backward
-- compatibility. The VARIANT columns (raw_extraction, raw_line_data) store
-- the full AI_EXTRACT JSON response so any document type with any number
-- of fields works without schema changes.
-- =============================================================================

USE ROLE AI_EXTRACT_APP;          -- <-- match your 01_setup.sql role
USE DATABASE AI_EXTRACT_POC;      -- <-- match your 01_setup.sql values
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- RAW_DOCUMENTS: Tracks every file staged for processing
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW_DOCUMENTS (
    file_name         VARCHAR NOT NULL,
    file_path         VARCHAR NOT NULL,
    doc_type          VARCHAR DEFAULT 'INVOICE',   -- INVOICE | CONTRACT | RECEIPT | custom
    staged_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    extracted         BOOLEAN DEFAULT FALSE,
    extracted_at      TIMESTAMP_NTZ,
    extraction_error  VARCHAR,
    CONSTRAINT pk_raw_documents PRIMARY KEY (file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_FIELDS: Entity-level data pulled from each document
-- ---------------------------------------------------------------------------
-- Fixed columns (field_1..field_10) are populated for the first 10 fields.
-- raw_extraction stores the FULL AI_EXTRACT JSON response — any number of
-- fields, any types. New document types with >10 fields use raw_extraction.
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  For new document types, you do NOT need to add columns here.          │
-- │  Just configure DOCUMENT_TYPE_CONFIG and the extraction prompt.        │
-- │  The full response is stored in raw_extraction as VARIANT/JSON.        │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS EXTRACTED_FIELDS (
    record_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name         VARCHAR NOT NULL,

    -- Fixed columns for backward compatibility (first 10 fields)
    field_1           VARCHAR,       -- e.g., vendor_name / party_a / store_name
    field_2           VARCHAR,       -- e.g., invoice_number / contract_id / receipt_number
    field_3           VARCHAR,       -- e.g., po_number / reference_number
    field_4           DATE,          -- e.g., document_date / invoice_date / effective_date
    field_5           DATE,          -- e.g., due_date / expiration_date
    field_6           VARCHAR,       -- e.g., payment_terms / contract_type
    field_7           VARCHAR,       -- e.g., bill_to / ship_to / recipient
    field_8           NUMBER(12,2),  -- e.g., subtotal / base_amount
    field_9           NUMBER(12,2),  -- e.g., tax_amount / discount
    field_10          NUMBER(12,2),  -- e.g., total_amount / contract_value

    -- Full AI_EXTRACT response as JSON — supports any number of fields
    raw_extraction    VARIANT,       -- e.g., {"vendor_name":"Acme","total":1500.00,...}

    -- Metadata
    status            VARCHAR DEFAULT 'EXTRACTED',
    extracted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT fk_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
);

-- ---------------------------------------------------------------------------
-- EXTRACTED_TABLE_DATA: Line items / tabular data from each document
-- ---------------------------------------------------------------------------
-- Fixed columns (col_1..col_5) handle up to 5 table columns.
-- raw_line_data stores the full line item as JSON for flexible schemas.
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  For new document types, you do NOT need to add columns here.          │
-- │  Configure table_extraction_schema in DOCUMENT_TYPE_CONFIG instead.    │
-- │  The full line-item data is stored in raw_line_data as VARIANT/JSON.   │
-- └─────────────────────────────────────────────────────────────────────────┘

CREATE TABLE IF NOT EXISTS EXTRACTED_TABLE_DATA (
    line_id           NUMBER AUTOINCREMENT PRIMARY KEY,
    file_name         VARCHAR NOT NULL,
    record_id         VARCHAR,       -- Links to parent document (e.g., invoice_number)
    line_number       NUMBER,

    -- Fixed columns for backward compatibility (first 5 table columns)
    col_1             VARCHAR,       -- e.g., product_name / procedure_code / milestone
    col_2             VARCHAR,       -- e.g., category / description
    col_3             NUMBER(10,2),  -- e.g., quantity
    col_4             NUMBER(10,2),  -- e.g., unit_price / charge
    col_5             NUMBER(12,2),  -- e.g., line_total / amount

    -- Full line item as JSON — supports any number of table columns
    raw_line_data     VARIANT,       -- e.g., {"description":"Widget","qty":5,"total":50.00}

    CONSTRAINT fk_table_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
);

-- ---------------------------------------------------------------------------
-- Register all staged files into RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
-- Run this AFTER uploading your documents to the stage.
-- Safe to re-run — skips files already registered.

INSERT INTO RAW_DOCUMENTS (file_name, file_path, doc_type, staged_at, extracted)
SELECT
    RELATIVE_PATH                              AS file_name,
    '@DOCUMENT_STAGE/' || RELATIVE_PATH        AS file_path,
    'INVOICE'                                  AS doc_type,   -- <-- change per document type
    CURRENT_TIMESTAMP()                        AS staged_at,
    FALSE                                      AS extracted
FROM DIRECTORY(@DOCUMENT_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf'         -- <-- adjust file extension filter as needed
  AND RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS);

-- Verify registered files
SELECT * FROM RAW_DOCUMENTS ORDER BY staged_at DESC;
