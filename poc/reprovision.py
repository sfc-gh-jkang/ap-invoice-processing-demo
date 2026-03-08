"""
Reprovision a Snowflake account for the AI_EXTRACT POC from scratch.

Usage:
    python3 reprovision.py <connection_name>

Example:
    python3 reprovision.py aws_spcs
    python3 reprovision.py azure_spcs
    python3 reprovision.py gcp_spcs

This script:
  1. Tears down all existing POC objects (database, warehouse, role)
  2. Creates the RBAC role + grants
  3. Creates database, schema, warehouse, stages
  4. Uploads 100 PDFs to DOCUMENT_STAGE
  5. Creates tables and registers files
  6. Runs batch entity extraction (AI_EXTRACT)
  7. Runs batch table extraction (AI_EXTRACT)
  8. Marks files as extracted
  9. Creates all views
  10. Creates automation (stream, proc, task)
  11. Creates writeback objects (INVOICE_REVIEW, V_DOCUMENT_SUMMARY, V_INVOICE_SUMMARY alias)
  12. Creates document type config (DOCUMENT_TYPE_CONFIG)
  13. Uploads Streamlit files to STREAMLIT_STAGE
  14. Grants INSERT on INVOICE_REVIEW + READ on stages + FUTURE grants
"""

import os
import sys
import time
import glob as globmod

import snowflake.connector

CONNECTION_NAME = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data", "invoices")
STREAMLIT_DIR = os.path.join(SCRIPT_DIR, "streamlit")


def run(cur, sql, label=None):
    """Execute a SQL statement and print status."""
    if label:
        print(f"  {label}...", end=" ", flush=True)
    try:
        cur.execute(sql)
        if label:
            print("OK")
        return cur
    except Exception as e:
        if label:
            print(f"FAILED: {e}")
        raise


def run_quiet(cur, sql):
    """Execute SQL without printing."""
    cur.execute(sql)
    return cur


def main():
    print(f"\n{'='*70}")
    print(f"  REPROVISIONING: {CONNECTION_NAME}")
    print(f"{'='*70}\n")

    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()

    # -------------------------------------------------------------------------
    # Phase 1: TEARDOWN
    # -------------------------------------------------------------------------
    print("[1/13] TEARDOWN — dropping existing objects")
    run(cur, "USE ROLE ACCOUNTADMIN", "USE ROLE ACCOUNTADMIN")
    run(cur, f"ALTER TASK IF EXISTS {POC_DB}.{POC_SCHEMA}.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND",
        "Suspend task")
    run(cur, f"DROP DATABASE IF EXISTS {POC_DB}", "Drop database")
    run(cur, f"DROP WAREHOUSE IF EXISTS {POC_WH}", "Drop warehouse")
    run(cur, f"DROP COMPUTE POOL IF EXISTS AI_EXTRACT_POC_POOL", "Drop compute pool")
    run(cur, f"DROP ROLE IF EXISTS {POC_ROLE}", "Drop role")
    print()

    # -------------------------------------------------------------------------
    # Phase 2: RBAC ROLE CREATION
    # -------------------------------------------------------------------------
    print("[2/13] RBAC — creating role and Cortex grant")
    run(cur, "USE ROLE ACCOUNTADMIN", "USE ROLE ACCOUNTADMIN")
    run(cur, f"CREATE ROLE IF NOT EXISTS {POC_ROLE}", f"Create role {POC_ROLE}")
    run(cur, f"GRANT ROLE {POC_ROLE} TO ROLE SYSADMIN", "Grant to SYSADMIN")
    run(cur, f"GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE {POC_ROLE}",
        "Grant CORTEX_USER")
    run(cur, "ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'",
        "Enable cross-region")
    run(cur, f"GRANT CREATE DATABASE ON ACCOUNT TO ROLE {POC_ROLE}",
        "Grant CREATE DATABASE")
    run(cur, f"GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE {POC_ROLE}",
        "Grant CREATE WAREHOUSE")
    # Grant role to current user
    cur.execute("SELECT CURRENT_USER()")
    current_user = cur.fetchone()[0]
    run(cur, f"GRANT ROLE {POC_ROLE} TO USER {current_user}",
        f"Grant role to {current_user}")
    print()

    # -------------------------------------------------------------------------
    # Phase 3: INFRASTRUCTURE
    # -------------------------------------------------------------------------
    print("[3/13] INFRASTRUCTURE — database, schema, warehouse, stage")
    run(cur, f"USE ROLE {POC_ROLE}", f"USE ROLE {POC_ROLE}")
    run(cur, f"CREATE DATABASE IF NOT EXISTS {POC_DB}", "Create database")
    run(cur, f"USE DATABASE {POC_DB}", "USE DATABASE")
    run(cur, f"CREATE SCHEMA IF NOT EXISTS {POC_SCHEMA}", "Create schema")
    run(cur, f"USE SCHEMA {POC_SCHEMA}", "USE SCHEMA")
    run(cur, f"""CREATE WAREHOUSE IF NOT EXISTS {POC_WH}
        WAREHOUSE_SIZE = 'X-SMALL'
        AUTO_SUSPEND = 120
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE
        COMMENT = 'AI_EXTRACT POC'""", "Create warehouse")
    run(cur, f"USE WAREHOUSE {POC_WH}", "USE WAREHOUSE")
    run(cur, f"""CREATE STAGE IF NOT EXISTS DOCUMENT_STAGE
        DIRECTORY = (ENABLE = TRUE)
        ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
        COMMENT = 'Document stage — SSE encryption required for AI_EXTRACT'""",
        "Create DOCUMENT_STAGE")
    print()

    # -------------------------------------------------------------------------
    # Phase 4: UPLOAD PDFs
    # -------------------------------------------------------------------------
    print("[4/13] UPLOAD — putting 100 PDFs to DOCUMENT_STAGE")
    pdf_files = sorted(globmod.glob(os.path.join(DATA_DIR, "*.pdf")))
    print(f"  Found {len(pdf_files)} PDF files locally")
    if not pdf_files:
        print("  ERROR: No PDF files found! Aborting.")
        sys.exit(1)

    # Upload in one PUT command using wildcard
    t0 = time.time()
    run(cur, f"PUT 'file://{DATA_DIR}/*.pdf' @DOCUMENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        f"PUT {len(pdf_files)} PDFs")
    run(cur, "ALTER STAGE DOCUMENT_STAGE REFRESH", "Refresh directory")
    cur.execute("SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE)")
    staged_count = cur.fetchone()[0]
    print(f"  Staged files: {staged_count} ({time.time()-t0:.1f}s)")
    print()

    # -------------------------------------------------------------------------
    # Phase 5: CREATE TABLES + REGISTER FILES
    # -------------------------------------------------------------------------
    print("[5/13] TABLES — creating RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA")
    run(cur, """CREATE TABLE IF NOT EXISTS RAW_DOCUMENTS (
        file_name         VARCHAR NOT NULL,
        file_path         VARCHAR NOT NULL,
        staged_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        extracted         BOOLEAN DEFAULT FALSE,
        extracted_at      TIMESTAMP_NTZ,
        extraction_error  VARCHAR,
        doc_type          VARCHAR DEFAULT 'INVOICE',
        CONSTRAINT pk_raw_documents PRIMARY KEY (file_name)
    )""", "Create RAW_DOCUMENTS")

    run(cur, """CREATE TABLE IF NOT EXISTS EXTRACTED_FIELDS (
        record_id         NUMBER AUTOINCREMENT PRIMARY KEY,
        file_name         VARCHAR NOT NULL,
        field_1           VARCHAR,
        field_2           VARCHAR,
        field_3           VARCHAR,
        field_4           DATE,
        field_5           DATE,
        field_6           VARCHAR,
        field_7           VARCHAR,
        field_8           NUMBER(12,2),
        field_9           NUMBER(12,2),
        field_10          NUMBER(12,2),
        status            VARCHAR DEFAULT 'EXTRACTED',
        extracted_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        raw_extraction    VARIANT,
        CONSTRAINT fk_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
    )""", "Create EXTRACTED_FIELDS")

    run(cur, """CREATE TABLE IF NOT EXISTS EXTRACTED_TABLE_DATA (
        line_id           NUMBER AUTOINCREMENT PRIMARY KEY,
        file_name         VARCHAR NOT NULL,
        record_id         VARCHAR,
        line_number       NUMBER,
        col_1             VARCHAR,
        col_2             VARCHAR,
        col_3             NUMBER(10,2),
        col_4             NUMBER(10,2),
        col_5             NUMBER(12,2),
        raw_line_data     VARIANT,
        CONSTRAINT fk_table_raw FOREIGN KEY (file_name) REFERENCES RAW_DOCUMENTS(file_name)
    )""", "Create EXTRACTED_TABLE_DATA")

    # Register files
    run(cur, """INSERT INTO RAW_DOCUMENTS (file_name, file_path, doc_type, staged_at, extracted)
        SELECT
            RELATIVE_PATH AS file_name,
            '@DOCUMENT_STAGE/' || RELATIVE_PATH AS file_path,
            'INVOICE' AS doc_type,
            CURRENT_TIMESTAMP() AS staged_at,
            FALSE AS extracted
        FROM DIRECTORY(@DOCUMENT_STAGE)
        WHERE RELATIVE_PATH LIKE '%.pdf'
          AND RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS)""",
        "Register files in RAW_DOCUMENTS")
    cur.execute("SELECT COUNT(*) FROM RAW_DOCUMENTS")
    print(f"  Registered files: {cur.fetchone()[0]}")
    print()

    # -------------------------------------------------------------------------
    # Phase 6: ENTITY EXTRACTION (AI_EXTRACT)
    # -------------------------------------------------------------------------
    print("[6/13] ENTITY EXTRACTION — running AI_EXTRACT on all files")
    print("  This may take several minutes...")
    t0 = time.time()
    run(cur, """INSERT INTO EXTRACTED_FIELDS (
        file_name, field_1, field_2, field_3, field_4, field_5,
        field_6, field_7, field_8, field_9, field_10
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
      AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS)""",
        "Entity extraction")
    elapsed = time.time() - t0
    cur.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
    print(f"  Extracted fields: {cur.fetchone()[0]} rows ({elapsed:.1f}s)")
    print()

    # -------------------------------------------------------------------------
    # Phase 7: TABLE EXTRACTION (AI_EXTRACT)
    # -------------------------------------------------------------------------
    print("[7/13] TABLE EXTRACTION — extracting line items")
    print("  This may take several minutes...")
    t0 = time.time()
    run(cur, """INSERT INTO EXTRACTED_TABLE_DATA (
        file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5
    )
    WITH extracted AS (
        SELECT
            r.file_name,
            ef.field_2 AS record_id,
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
        TRY_TO_NUMBER(ln.value::VARCHAR) AS line_number,
        pr.value::VARCHAR AS col_1,
        ca.value::VARCHAR AS col_2,
        TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2) AS col_3,
        TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2) AS col_4,
        TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2) AS col_5
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
      AND ln.index = tl.index""",
        "Table extraction")
    elapsed = time.time() - t0
    cur.execute("SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA")
    print(f"  Extracted line items: {cur.fetchone()[0]} rows ({elapsed:.1f}s)")
    print()

    # -------------------------------------------------------------------------
    # Phase 8: MARK FILES AS EXTRACTED
    # -------------------------------------------------------------------------
    print("[8/13] MARK EXTRACTED")
    run(cur, """UPDATE RAW_DOCUMENTS
        SET extracted = TRUE, extracted_at = CURRENT_TIMESTAMP()
        WHERE extracted = FALSE
          AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS)""",
        "Mark files extracted")
    cur.execute("SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE extracted = TRUE")
    print(f"  Extracted: {cur.fetchone()[0]}")
    print()

    # -------------------------------------------------------------------------
    # Phase 9: VIEWS
    # -------------------------------------------------------------------------
    print("[9/13] VIEWS — creating analytical views")
    run(cur, """CREATE OR REPLACE VIEW V_EXTRACTION_STATUS AS
        SELECT
            COUNT(*) AS total_files,
            SUM(CASE WHEN extracted = TRUE THEN 1 ELSE 0 END) AS extracted_files,
            SUM(CASE WHEN extracted = FALSE THEN 1 ELSE 0 END) AS pending_files,
            SUM(CASE WHEN extraction_error IS NOT NULL THEN 1 ELSE 0 END) AS failed_files,
            MAX(extracted_at) AS last_extraction
        FROM RAW_DOCUMENTS""", "V_EXTRACTION_STATUS")

    run(cur, """CREATE OR REPLACE VIEW V_DOCUMENT_LEDGER AS
        SELECT
            ef.record_id, ef.file_name, rd.doc_type,
            ef.field_1 AS vendor_name, ef.field_2 AS document_number,
            ef.field_3 AS reference, ef.field_4 AS document_date,
            ef.field_5 AS due_date, ef.field_6 AS terms,
            ef.field_7 AS recipient, ef.field_8 AS subtotal,
            ef.field_9 AS tax_amount, ef.field_10 AS total_amount,
            ef.status, ef.extracted_at,
            CASE WHEN ef.field_5 IS NULL THEN 0
                 ELSE GREATEST(DATEDIFF(day, ef.field_5, CURRENT_DATE()), 0) END AS days_past_due,
            CASE WHEN ef.field_5 IS NULL THEN 'N/A'
                 WHEN CURRENT_DATE() <= ef.field_5 THEN 'Current'
                 WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 1 AND 30 THEN '1-30 Days'
                 WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 31 AND 60 THEN '31-60 Days'
                 WHEN DATEDIFF(day, ef.field_5, CURRENT_DATE()) BETWEEN 61 AND 90 THEN '61-90 Days'
                 ELSE '90+ Days' END AS aging_bucket
        FROM EXTRACTED_FIELDS ef
            JOIN RAW_DOCUMENTS rd ON ef.file_name = rd.file_name""",
        "V_DOCUMENT_LEDGER")

    run(cur, """CREATE OR REPLACE VIEW V_SUMMARY_BY_VENDOR AS
        SELECT field_1 AS vendor_name, COUNT(*) AS document_count,
            SUM(field_10) AS total_amount, AVG(field_10) AS avg_amount,
            MIN(field_4) AS first_document, MAX(field_4) AS last_document
        FROM EXTRACTED_FIELDS WHERE field_1 IS NOT NULL
        GROUP BY field_1 ORDER BY total_amount DESC""",
        "V_SUMMARY_BY_VENDOR")

    run(cur, """CREATE OR REPLACE VIEW V_MONTHLY_TREND AS
        SELECT DATE_TRUNC('month', field_4) AS month, COUNT(*) AS document_count,
            SUM(field_10) AS total_amount, SUM(field_8) AS total_subtotal,
            SUM(field_9) AS total_tax, AVG(field_10) AS avg_amount
        FROM EXTRACTED_FIELDS WHERE field_4 IS NOT NULL
        GROUP BY DATE_TRUNC('month', field_4) ORDER BY month""",
        "V_MONTHLY_TREND")

    run(cur, """CREATE OR REPLACE VIEW V_TOP_LINE_ITEMS AS
        SELECT col_1 AS item_description, col_2 AS category,
            COUNT(*) AS appearance_count, SUM(col_3) AS total_quantity,
            AVG(col_4) AS avg_unit_price, SUM(col_5) AS total_spend
        FROM EXTRACTED_TABLE_DATA WHERE col_1 IS NOT NULL
        GROUP BY col_1, col_2 ORDER BY total_spend DESC""",
        "V_TOP_LINE_ITEMS")

    run(cur, """CREATE OR REPLACE VIEW V_AGING_SUMMARY AS
        SELECT aging_bucket, COUNT(*) AS document_count,
            SUM(total_amount) AS total_amount,
            CASE aging_bucket
                WHEN 'Current' THEN 1 WHEN '1-30 Days' THEN 2
                WHEN '31-60 Days' THEN 3 WHEN '61-90 Days' THEN 4
                WHEN '90+ Days' THEN 5 WHEN 'N/A' THEN 6 END AS sort_order
        FROM V_DOCUMENT_LEDGER GROUP BY aging_bucket ORDER BY sort_order""",
        "V_AGING_SUMMARY")
    print()

    # -------------------------------------------------------------------------
    # Phase 10: AUTOMATION (stream, proc, task)
    # -------------------------------------------------------------------------
    print("[10/13] AUTOMATION — stream, stored proc, task")
    run(cur, """CREATE STREAM IF NOT EXISTS RAW_DOCUMENTS_STREAM
        ON TABLE RAW_DOCUMENTS APPEND_ONLY = TRUE
        COMMENT = 'Detects newly staged documents'""",
        "Create stream")

    run(cur, """CREATE OR REPLACE PROCEDURE SP_EXTRACT_NEW_DOCUMENTS()
        RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER
    AS
    $$
    BEGIN
        LET files_processed INT := 0;
        INSERT INTO EXTRACTED_FIELDS (
            file_name, field_1, field_2, field_3, field_4, field_5,
            field_6, field_7, field_8, field_9, field_10
        )
        SELECT r.file_name,
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
                SELECT AI_EXTRACT(
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

        INSERT INTO EXTRACTED_TABLE_DATA (
            file_name, record_id, line_number, col_1, col_2, col_3, col_4, col_5
        )
        WITH extracted AS (
            SELECT r.file_name, ef.field_2 AS record_id,
                AI_EXTRACT(
                    file => TO_FILE('@DOCUMENT_STAGE', r.file_name),
                    responseFormat => {
                        'schema': { 'type': 'object', 'properties': {
                            'line_items': {
                                'description': 'The table of line items on the document',
                                'type': 'object',
                                'column_ordering': ['Line', 'Description', 'Category', 'Qty', 'Unit Price', 'Total'],
                                'properties': {
                                    'Line':       { 'description': 'Line item number',          'type': 'array' },
                                    'Description':{ 'description': 'Product or service name',   'type': 'array' },
                                    'Category':   { 'description': 'Product category or type',  'type': 'array' },
                                    'Qty':        { 'description': 'Quantity',                  'type': 'array' },
                                    'Unit Price': { 'description': 'Price per unit in dollars', 'type': 'array' },
                                    'Total':      { 'description': 'Line total in dollars',     'type': 'array' }
                                }
                            }
                        }}
                    }
                ) AS extraction
            FROM RAW_DOCUMENTS r
                JOIN EXTRACTED_FIELDS ef ON r.file_name = ef.file_name
            WHERE r.extracted = FALSE
              AND r.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)
        )
        SELECT e.file_name, e.record_id,
            TRY_TO_NUMBER(ln.value::VARCHAR) AS line_number,
            pr.value::VARCHAR, ca.value::VARCHAR,
            TRY_TO_NUMBER(REGEXP_REPLACE(qt.value::VARCHAR, '[^0-9.]', ''), 10, 2),
            TRY_TO_NUMBER(REGEXP_REPLACE(up.value::VARCHAR, '[^0-9.]', ''), 10, 2),
            TRY_TO_NUMBER(REGEXP_REPLACE(tl.value::VARCHAR, '[^0-9.]', ''), 12, 2)
        FROM extracted e,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line) ln,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description) pr,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Category) ca,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty) qt,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:"Unit Price") up,
            LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total) tl
        WHERE ln.index = pr.index AND ln.index = ca.index
          AND ln.index = qt.index AND ln.index = up.index AND ln.index = tl.index;

        SELECT COUNT(*) INTO :files_processed
        FROM RAW_DOCUMENTS WHERE extracted = FALSE
          AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

        UPDATE RAW_DOCUMENTS
        SET extracted = TRUE, extracted_at = CURRENT_TIMESTAMP()
        WHERE extracted = FALSE
          AND file_name IN (SELECT file_name FROM EXTRACTED_FIELDS);

        RETURN 'Processed ' || :files_processed || ' new document(s)';
    END;
    $$""", "Create stored procedure")

    run(cur, f"""CREATE OR REPLACE TASK EXTRACT_NEW_DOCUMENTS_TASK
        WAREHOUSE = {POC_WH}
        SCHEDULE = '5 MINUTE'
        COMMENT = 'Auto-extract newly staged documents'
        WHEN SYSTEM$STREAM_HAS_DATA('RAW_DOCUMENTS_STREAM')
    AS
        CALL SP_EXTRACT_NEW_DOCUMENTS()""",
        "Create task")
    run(cur, "ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK RESUME", "Resume task")
    print()

    # -------------------------------------------------------------------------
    # Phase 11: WRITEBACK OBJECTS
    # -------------------------------------------------------------------------
    print("[11/13] WRITEBACK — INVOICE_REVIEW table + V_DOCUMENT_SUMMARY view")
    run(cur, """CREATE TABLE IF NOT EXISTS INVOICE_REVIEW (
        review_id                NUMBER AUTOINCREMENT PRIMARY KEY,
        record_id                NUMBER NOT NULL,
        file_name                VARCHAR NOT NULL,
        review_status            VARCHAR NOT NULL,
        corrected_total          NUMBER(12,2),
        reviewer_notes           VARCHAR,
        reviewed_by              VARCHAR DEFAULT CURRENT_USER(),
        reviewed_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        corrected_vendor_name    VARCHAR,
        corrected_invoice_number VARCHAR,
        corrected_po_number      VARCHAR,
        corrected_invoice_date   DATE,
        corrected_due_date       DATE,
        corrected_payment_terms  VARCHAR,
        corrected_recipient      VARCHAR,
        corrected_subtotal       NUMBER(12,2),
        corrected_tax_amount     NUMBER(12,2),
        corrections              VARIANT
    )""", "Create INVOICE_REVIEW")

    run(cur, """CREATE OR REPLACE VIEW V_DOCUMENT_SUMMARY AS
        SELECT
            ef.record_id, ef.file_name, rd.doc_type,
            COALESCE(rv.corrections:vendor_name::VARCHAR,    rv.corrected_vendor_name,    ef.field_1) AS vendor_name,
            COALESCE(rv.corrections:invoice_number::VARCHAR, rv.corrected_invoice_number, ef.field_2) AS invoice_number,
            COALESCE(rv.corrections:po_number::VARCHAR,      rv.corrected_po_number,      ef.field_3) AS po_number,
            COALESCE(rv.corrections:invoice_date::DATE,      rv.corrected_invoice_date,   ef.field_4) AS invoice_date,
            COALESCE(rv.corrections:due_date::DATE,          rv.corrected_due_date,       ef.field_5) AS due_date,
            COALESCE(rv.corrections:payment_terms::VARCHAR,  rv.corrected_payment_terms,  ef.field_6) AS payment_terms,
            COALESCE(rv.corrections:recipient::VARCHAR,      rv.corrected_recipient,      ef.field_7) AS recipient,
            COALESCE(rv.corrections:subtotal::NUMBER(12,2),  rv.corrected_subtotal,       ef.field_8) AS subtotal,
            COALESCE(rv.corrections:tax_amount::NUMBER(12,2),rv.corrected_tax_amount,     ef.field_9) AS tax_amount,
            COALESCE(rv.corrections:total_amount::NUMBER(12,2), rv.corrected_total,       ef.field_10) AS total_amount,
            ef.status AS extraction_status, ef.extracted_at,
            COALESCE(rv.corrections:line_item_count::NUMBER, li.line_item_count) AS line_item_count,
            COALESCE(rv.corrections:computed_line_total::NUMBER(12,2), li.computed_line_total) AS computed_line_total,
            rv.review_status, rv.reviewer_notes, rv.reviewed_by, rv.reviewed_at,
            ef.raw_extraction,
            rv.corrections
        FROM EXTRACTED_FIELDS ef
        JOIN RAW_DOCUMENTS rd ON ef.file_name = rd.file_name
        LEFT JOIN (
            SELECT file_name, COUNT(*) AS line_item_count, SUM(col_5) AS computed_line_total
            FROM EXTRACTED_TABLE_DATA GROUP BY file_name
        ) li ON ef.file_name = li.file_name
        LEFT JOIN (
            SELECT * FROM INVOICE_REVIEW
            QUALIFY ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY review_id DESC) = 1
        ) rv ON ef.record_id = rv.record_id""",
        "Create V_DOCUMENT_SUMMARY")

    # Backward-compatible alias
    run(cur, "CREATE OR REPLACE VIEW V_INVOICE_SUMMARY AS SELECT * FROM V_DOCUMENT_SUMMARY",
        "Create V_INVOICE_SUMMARY alias")
    print()

    # -------------------------------------------------------------------------
    # Phase 12: DOCUMENT TYPE CONFIG
    # -------------------------------------------------------------------------
    print("[12/13] DOCUMENT TYPES — DOCUMENT_TYPE_CONFIG table + seeds")
    run(cur, """CREATE TABLE IF NOT EXISTS DOCUMENT_TYPE_CONFIG (
        doc_type                 VARCHAR NOT NULL PRIMARY KEY,
        display_name             VARCHAR NOT NULL,
        extraction_prompt        VARCHAR,
        field_labels             VARIANT NOT NULL,
        table_extraction_schema  VARIANT,
        review_fields            VARIANT,
        validation_rules         VARIANT,
        active                   BOOLEAN DEFAULT TRUE
    )""", "Create DOCUMENT_TYPE_CONFIG")

    # Seed INVOICE
    run(cur, """MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
        USING (SELECT 'INVOICE' AS doc_type) AS src ON tgt.doc_type = src.doc_type
        WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
        VALUES ('INVOICE', 'Invoice',
            'Extract the following fields from this invoice: vendor_name, invoice_number, po_number, invoice_date, due_date, payment_terms, recipient, subtotal, tax_amount, total_amount. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
            PARSE_JSON('{"field_1":"Vendor Name","field_2":"Invoice Number","field_3":"PO Number","field_4":"Invoice Date","field_5":"Due Date","field_6":"Payment Terms","field_7":"Recipient","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Amount","sender_label":"Vendor / Sender","amount_label":"Total Amount","date_label":"Invoice Date","reference_label":"Invoice #","secondary_ref_label":"PO #"}'),
            PARSE_JSON('{"columns":["Line","Description","Category","Qty","Unit Price","Total"],"descriptions":["Line item number","Product or service name","Product category","Quantity","Price per unit","Line total"]}'),
            PARSE_JSON('{"correctable":["vendor_name","invoice_number","po_number","invoice_date","due_date","payment_terms","recipient","subtotal","tax_amount","total_amount"],"types":{"vendor_name":"VARCHAR","invoice_number":"VARCHAR","po_number":"VARCHAR","invoice_date":"DATE","due_date":"DATE","payment_terms":"VARCHAR","recipient":"VARCHAR","subtotal":"NUMBER","tax_amount":"NUMBER","total_amount":"NUMBER"}}'))""",
        "Seed INVOICE")

    # Seed CONTRACT
    run(cur, """MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
        USING (SELECT 'CONTRACT' AS doc_type) AS src ON tgt.doc_type = src.doc_type
        WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
        VALUES ('CONTRACT', 'Contract',
            'Extract the following fields from this contract: party_name, contract_number, reference_id, effective_date, expiration_date, terms, counterparty, base_value, adjustments, total_value. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
            PARSE_JSON('{"field_1":"Party Name","field_2":"Contract Number","field_3":"Reference ID","field_4":"Effective Date","field_5":"Expiration Date","field_6":"Terms","field_7":"Counterparty","field_8":"Base Value","field_9":"Adjustments","field_10":"Total Value","sender_label":"Party","amount_label":"Total Value","date_label":"Effective Date","reference_label":"Contract #","secondary_ref_label":"Ref ID"}'),
            PARSE_JSON('{"columns":["Milestone","Due Date","Amount","Status"],"descriptions":["Milestone name","Payment due date","Payment amount","Milestone status"]}'),
            PARSE_JSON('{"correctable":["party_name","contract_number","reference_id","effective_date","expiration_date","terms","counterparty","base_value","adjustments","total_value"],"types":{"party_name":"VARCHAR","contract_number":"VARCHAR","reference_id":"VARCHAR","effective_date":"DATE","expiration_date":"DATE","terms":"VARCHAR","counterparty":"VARCHAR","base_value":"NUMBER","adjustments":"NUMBER","total_value":"NUMBER"}}'))""",
        "Seed CONTRACT")

    # Seed RECEIPT
    run(cur, """MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
        USING (SELECT 'RECEIPT' AS doc_type) AS src ON tgt.doc_type = src.doc_type
        WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
        VALUES ('RECEIPT', 'Receipt',
            'Extract the following fields from this receipt: merchant_name, receipt_number, transaction_id, purchase_date, return_by_date, payment_method, buyer, subtotal, tax_amount, total_paid. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units. Return 0 for zero or missing amounts, not null. Return the full legal company or person name, not abbreviations.',
            PARSE_JSON('{"field_1":"Merchant Name","field_2":"Receipt Number","field_3":"Transaction ID","field_4":"Purchase Date","field_5":"Return By Date","field_6":"Payment Method","field_7":"Buyer","field_8":"Subtotal","field_9":"Tax Amount","field_10":"Total Paid","sender_label":"Merchant","amount_label":"Total Paid","date_label":"Purchase Date","reference_label":"Receipt #","secondary_ref_label":"Transaction ID"}'),
            PARSE_JSON('{"columns":["Item","Qty","Price","Total"],"descriptions":["Item purchased","Quantity","Unit price","Line total"]}'),
            PARSE_JSON('{"correctable":["merchant_name","receipt_number","total_paid"],"types":{"merchant_name":"VARCHAR","receipt_number":"VARCHAR","total_paid":"NUMBER"}}'))""",
        "Seed RECEIPT")

    # Seed UTILITY_BILL
    run(cur, """MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt
        USING (SELECT 'UTILITY_BILL' AS doc_type) AS src ON tgt.doc_type = src.doc_type
        WHEN NOT MATCHED THEN INSERT (doc_type, display_name, extraction_prompt, field_labels, table_extraction_schema, review_fields)
        VALUES ('UTILITY_BILL', 'Utility Bill',
            'Extract the following fields from this utility bill: utility_company, account_number, meter_number, service_address, billing_period_start, billing_period_end, rate_schedule, kwh_usage, demand_kw, previous_balance, current_charges, total_due, due_date. FORMATTING RULES: Return all dates in YYYY-MM-DD format. Return all monetary values as plain numbers without currency symbols or commas (e.g. 1234.56 not $1,234.56). Return numeric values without units (e.g. 898 not 898 kWh). Return 0 for zero or missing amounts, not null. Return the full legal company name, not abbreviations (e.g. Public Service Electric and Gas not PSE&G).',
            PARSE_JSON('{"field_1":"Utility Company","field_2":"Account Number","field_3":"Meter Number","field_4":"Service Address","field_5":"Billing Period Start","field_6":"Billing Period End","field_7":"Rate Schedule","field_8":"kWh Usage","field_9":"Demand kW","field_10":"Previous Balance","field_11":"Current Charges","field_12":"Total Due","field_13":"Due Date","sender_label":"Utility Company","amount_label":"Total Due","date_label":"Due Date","reference_label":"Account #","secondary_ref_label":"Meter #"}'),
            PARSE_JSON('{"columns":["Tier","kWh Range","Rate per kWh","Amount"],"descriptions":["Rate tier name","kWh range for this tier","Rate per kWh in dollars","Tier charge amount"]}'),
            PARSE_JSON('{"correctable":["utility_company","account_number","meter_number","kwh_usage","total_due","due_date"],"types":{"utility_company":"VARCHAR","account_number":"VARCHAR","meter_number":"VARCHAR","kwh_usage":"NUMBER","total_due":"NUMBER","due_date":"DATE"}}'))""",
        "Seed UTILITY_BILL")
    print()

    # -------------------------------------------------------------------------
    # Phase 13: STREAMLIT STAGE + RBAC GRANTS
    # -------------------------------------------------------------------------
    print("[13/13] STREAMLIT FILES + RBAC GRANTS")
    run(cur, """CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
        DIRECTORY = (ENABLE = TRUE)
        COMMENT = 'Stage for Streamlit in Snowflake app files'""",
        "Create STREAMLIT_STAGE")

    # Upload Streamlit files
    st_main = os.path.join(STREAMLIT_DIR, "streamlit_app.py")
    st_config = os.path.join(STREAMLIT_DIR, "config.py")
    st_pages_dir = os.path.join(STREAMLIT_DIR, "pages")

    run(cur, f"PUT 'file://{st_main}' @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        "Upload streamlit_app.py")
    run(cur, f"PUT 'file://{st_config}' @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        "Upload config.py")
    run(cur, f"PUT 'file://{st_pages_dir}/*.py' @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
        "Upload pages/*.py")

    run(cur, "ALTER STAGE STREAMLIT_STAGE REFRESH", "Refresh STREAMLIT_STAGE directory")
    cur.execute("SELECT COUNT(*) FROM DIRECTORY(@STREAMLIT_STAGE)")
    print(f"  Streamlit stage files: {cur.fetchone()[0]}")

    # RBAC grants — switch to ACCOUNTADMIN for grant statements
    run(cur, "USE ROLE ACCOUNTADMIN", "USE ROLE ACCOUNTADMIN (for grants)")
    run(cur, f"USE DATABASE {POC_DB}", "USE DATABASE")
    run(cur, f"USE SCHEMA {POC_SCHEMA}", "USE SCHEMA")

    grants = [
        f"GRANT USAGE ON DATABASE {POC_DB} TO ROLE {POC_ROLE}",
        f"GRANT USAGE ON SCHEMA {POC_DB}.{POC_SCHEMA} TO ROLE {POC_ROLE}",
        f"GRANT USAGE ON WAREHOUSE {POC_WH} TO ROLE {POC_ROLE}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {POC_DB}.{POC_SCHEMA} TO ROLE {POC_ROLE}",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {POC_DB}.{POC_SCHEMA} TO ROLE {POC_ROLE}",
        f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {POC_DB}.{POC_SCHEMA} TO ROLE {POC_ROLE}",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {POC_DB}.{POC_SCHEMA} TO ROLE {POC_ROLE}",
        f"GRANT INSERT ON TABLE {POC_DB}.{POC_SCHEMA}.INVOICE_REVIEW TO ROLE {POC_ROLE}",
        f"GRANT INSERT, UPDATE, DELETE ON TABLE {POC_DB}.{POC_SCHEMA}.DOCUMENT_TYPE_CONFIG TO ROLE {POC_ROLE}",
        f"GRANT READ ON STAGE {POC_DB}.{POC_SCHEMA}.DOCUMENT_STAGE TO ROLE {POC_ROLE}",
        f"GRANT READ ON STAGE {POC_DB}.{POC_SCHEMA}.STREAMLIT_STAGE TO ROLE {POC_ROLE}",
        f"GRANT OPERATE ON TASK {POC_DB}.{POC_SCHEMA}.EXTRACT_NEW_DOCUMENTS_TASK TO ROLE {POC_ROLE}",
        f"GRANT USAGE ON PROCEDURE {POC_DB}.{POC_SCHEMA}.SP_EXTRACT_NEW_DOCUMENTS() TO ROLE {POC_ROLE}",
        f"GRANT USAGE ON PROCEDURE {POC_DB}.{POC_SCHEMA}.SP_EXTRACT_BY_DOC_TYPE(VARCHAR) TO ROLE {POC_ROLE}",
        f"GRANT EXECUTE TASK ON ACCOUNT TO ROLE {POC_ROLE}",
    ]
    for g in grants:
        short = g.replace(f"{POC_DB}.{POC_SCHEMA}.", "").replace(f"TO ROLE {POC_ROLE}", "")
        run(cur, g, short.strip())

    print()

    # -------------------------------------------------------------------------
    # FINAL VERIFICATION
    # -------------------------------------------------------------------------
    print("=" * 70)
    print("  VERIFICATION")
    print("=" * 70)
    run(cur, f"USE ROLE {POC_ROLE}", f"USE ROLE {POC_ROLE}")
    run(cur, f"USE DATABASE {POC_DB}", "USE DATABASE")
    run(cur, f"USE SCHEMA {POC_SCHEMA}", "USE SCHEMA")
    run(cur, f"USE WAREHOUSE {POC_WH}", "USE WAREHOUSE")

    checks = [
        ("RAW_DOCUMENTS", "SELECT COUNT(*) FROM RAW_DOCUMENTS"),
        ("EXTRACTED_FIELDS", "SELECT COUNT(*) FROM EXTRACTED_FIELDS"),
        ("EXTRACTED_TABLE_DATA", "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA"),
        ("INVOICE_REVIEW", "SELECT COUNT(*) FROM INVOICE_REVIEW"),
        ("DOCUMENT_TYPE_CONFIG", "SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG"),
        ("V_EXTRACTION_STATUS", "SELECT total_files FROM V_EXTRACTION_STATUS"),
        ("V_DOCUMENT_LEDGER", "SELECT COUNT(*) FROM V_DOCUMENT_LEDGER"),
        ("V_SUMMARY_BY_VENDOR", "SELECT COUNT(*) FROM V_SUMMARY_BY_VENDOR"),
        ("V_MONTHLY_TREND", "SELECT COUNT(*) FROM V_MONTHLY_TREND"),
        ("V_TOP_LINE_ITEMS", "SELECT COUNT(*) FROM V_TOP_LINE_ITEMS"),
        ("V_AGING_SUMMARY", "SELECT COUNT(*) FROM V_AGING_SUMMARY"),
        ("V_DOCUMENT_SUMMARY", "SELECT COUNT(*) FROM V_DOCUMENT_SUMMARY"),
        ("DOCUMENT_STAGE", "SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE)"),
        ("STREAMLIT_STAGE", "SELECT COUNT(*) FROM DIRECTORY(@STREAMLIT_STAGE)"),
    ]
    for name, sql in checks:
        cur.execute(sql)
        val = cur.fetchone()[0]
        print(f"  {name:30s} {val}")

    conn.close()
    print(f"\n  DONE — {CONNECTION_NAME} fully reprovisioned!\n")


if __name__ == "__main__":
    main()
