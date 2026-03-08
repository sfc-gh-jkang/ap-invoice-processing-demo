-- =============================================================================
-- 08_writeback.sql — Writeback Table + View for Review Workflow
--
-- Creates:
--   1. INVOICE_REVIEW        — Writeback table for Streamlit user approvals
--                               Legacy corrected_* columns for backward compat.
--                               corrections VARIANT for flexible doc types.
--   2. V_DOCUMENT_SUMMARY    — View joining extracted fields, line items
--                               (aggregated), and latest review status.
--                               Includes raw_extraction for flexible field access.
--                               Instant reads — no lag.
--   3. V_INVOICE_SUMMARY     — Backward-compat alias for V_DOCUMENT_SUMMARY
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- INVOICE_REVIEW: Writeback table for user review decisions
-- ---------------------------------------------------------------------------
-- Each row represents one review action. Multiple reviews per document are
-- allowed (append-only audit trail — the view picks the latest one).
--
-- Legacy corrected_* columns handle the fixed 10-field invoice schema.
-- The corrections VARIANT column handles any document type with any fields.
-- Both can coexist — VARIANT is preferred for new document types.

CREATE TABLE IF NOT EXISTS INVOICE_REVIEW (
    review_id         NUMBER AUTOINCREMENT PRIMARY KEY,
    record_id         NUMBER NOT NULL,          -- FK to EXTRACTED_FIELDS.record_id
    file_name         VARCHAR NOT NULL,
    review_status     VARCHAR NOT NULL,          -- APPROVED | REJECTED | CORRECTED
    corrected_vendor_name    VARCHAR,
    corrected_invoice_number VARCHAR,
    corrected_po_number      VARCHAR,
    corrected_invoice_date   DATE,
    corrected_due_date       DATE,
    corrected_payment_terms  VARCHAR,
    corrected_recipient      VARCHAR,
    corrected_subtotal       NUMBER(12,2),
    corrected_tax_amount     NUMBER(12,2),
    corrected_total          NUMBER(12,2),
    reviewer_notes    VARCHAR,                   -- Free-text comments
    reviewed_by       VARCHAR DEFAULT CURRENT_USER(),
    reviewed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    corrections       VARIANT                    -- JSON: {"field_name": "corrected_value", ...}
);

-- ---------------------------------------------------------------------------
-- V_DOCUMENT_SUMMARY: View — enriched document view (instant, no lag)
-- ---------------------------------------------------------------------------
-- Joins EXTRACTED_FIELDS with aggregated EXTRACTED_TABLE_DATA line items
-- and the most recent INVOICE_REVIEW per document.
-- Includes raw_extraction and doc_type for flexible document type support.

CREATE OR REPLACE VIEW V_DOCUMENT_SUMMARY AS
SELECT
    ef.record_id,
    ef.file_name,
    rd.doc_type,

    -- Best-known values: VARIANT correction > legacy correction > original
    COALESCE(rv.corrections:vendor_name::VARCHAR,    rv.corrected_vendor_name,    ef.field_1)   AS vendor_name,
    COALESCE(rv.corrections:invoice_number::VARCHAR, rv.corrected_invoice_number, ef.field_2)   AS invoice_number,
    COALESCE(rv.corrections:po_number::VARCHAR,      rv.corrected_po_number,      ef.field_3)   AS po_number,
    COALESCE(rv.corrections:invoice_date::DATE,      rv.corrected_invoice_date,   ef.field_4)   AS invoice_date,
    COALESCE(rv.corrections:due_date::DATE,          rv.corrected_due_date,       ef.field_5)   AS due_date,
    COALESCE(rv.corrections:payment_terms::VARCHAR,  rv.corrected_payment_terms,  ef.field_6)   AS payment_terms,
    COALESCE(rv.corrections:recipient::VARCHAR,      rv.corrected_recipient,      ef.field_7)   AS recipient,
    COALESCE(rv.corrections:subtotal::NUMBER(12,2),  rv.corrected_subtotal,       ef.field_8)   AS subtotal,
    COALESCE(rv.corrections:tax_amount::NUMBER(12,2),rv.corrected_tax_amount,     ef.field_9)   AS tax_amount,
    COALESCE(rv.corrections:total_amount::NUMBER(12,2), rv.corrected_total,        ef.field_10)  AS total_amount,

    ef.status                           AS extraction_status,
    ef.extracted_at,

    -- Aggregated line-item metrics (with VARIANT override)
    COALESCE(rv.corrections:line_item_count::NUMBER, li.line_item_count)                      AS line_item_count,
    COALESCE(rv.corrections:computed_line_total::NUMBER(12,2), li.computed_line_total)         AS computed_line_total,

    -- Latest review info (NULL if not yet reviewed)
    rv.review_status,
    rv.reviewer_notes,
    rv.reviewed_by,
    rv.reviewed_at,

    -- Full extraction data for flexible field access
    ef.raw_extraction,
    rv.corrections
FROM EXTRACTED_FIELDS ef

-- Join to RAW_DOCUMENTS for doc_type
JOIN RAW_DOCUMENTS rd
    ON ef.file_name = rd.file_name

-- Aggregate line items per document
LEFT JOIN (
    SELECT
        file_name,
        COUNT(*)        AS line_item_count,
        SUM(col_5)      AS computed_line_total
    FROM EXTRACTED_TABLE_DATA
    GROUP BY file_name
) li
    ON ef.file_name = li.file_name

-- Latest review per document (highest review_id wins)
LEFT JOIN (
    SELECT *
    FROM INVOICE_REVIEW
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY record_id ORDER BY review_id DESC
    ) = 1
) rv
    ON ef.record_id = rv.record_id;

-- Backward-compatible alias
CREATE OR REPLACE VIEW V_INVOICE_SUMMARY AS
SELECT * FROM V_DOCUMENT_SUMMARY;

-- Verify
SELECT 'V_DOCUMENT_SUMMARY' AS object_name, COUNT(*) AS row_count
FROM V_DOCUMENT_SUMMARY;
