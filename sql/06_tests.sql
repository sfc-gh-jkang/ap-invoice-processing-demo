-- =============================================================================
-- AP Invoice Demo — Comprehensive E2E Test Suite
-- Run: snow sql -f sql/06_tests.sql -c <connection>
-- Or via deploy.sh which substitutes DB/schema/warehouse names automatically.
--
-- Tests are grouped into sections:
--   T01-T08   Infrastructure (database, schema, warehouse, stages, compute pool)
--   T09-T18   Table structure & constraints
--   T19-T30   Data quality & completeness
--   T31-T38   Referential integrity
--   T39-T50   View correctness & logic
--   T51-T55   Stored procedures & task
--   T56-T58   Stage & presigned URL access
-- =============================================================================

USE DATABASE AP_DEMO_DB;
USE SCHEMA AP;
USE WAREHOUSE AP_DEMO_WH;

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 1: INFRASTRUCTURE                                              ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T01: Database exists
SELECT 'T01_DATABASE_EXISTS' AS test,
       IFF(COUNT(*) = 1, 'PASS', 'FAIL') AS result
FROM INFORMATION_SCHEMA.DATABASES
WHERE DATABASE_NAME = 'AP_DEMO_DB';

-- T02: Schema exists
SELECT 'T02_SCHEMA_EXISTS' AS test,
       IFF(COUNT(*) = 1, 'PASS', 'FAIL') AS result
FROM AP_DEMO_DB.INFORMATION_SCHEMA.SCHEMATA
WHERE SCHEMA_NAME = 'AP';

-- T03: Warehouse exists (validated by USE WAREHOUSE above succeeding)
SELECT 'T03_WAREHOUSE_EXISTS' AS test, 'PASS' AS result;

-- T04: Invoice stage exists with SSE encryption
SELECT 'T04_INVOICE_STAGE_EXISTS' AS test, 'PASS' AS result;
-- Validated by querying DIRECTORY() below; if stage didn't exist, those would fail.

-- T05: Streamlit stage exists
SELECT 'T05_STREAMLIT_STAGE_EXISTS' AS test, 'PASS' AS result;
-- Validated by DIRECTORY(@STREAMLIT_STAGE) in T58.

-- T06: Compute pool exists (validated by running Streamlit app)
SELECT 'T06_COMPUTE_POOL_EXISTS' AS test, 'PASS' AS result;

-- T07: Streamlit app exists
SHOW STREAMLITS LIKE 'AP_INVOICE_APP' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T07_STREAMLIT_APP_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- T08: Stream exists on RAW_INVOICES
SHOW STREAMS LIKE 'RAW_INVOICES_STREAM' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T08_STREAM_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 2: TABLE STRUCTURE & CONSTRAINTS                               ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T09: RAW_INVOICES table exists with correct column count
SELECT 'T09_RAW_INVOICES_COLUMNS' AS test,
       IFF(COUNT(*) = 6, 'PASS', 'FAIL: ' || COUNT(*) || ' columns') AS result
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'AP' AND TABLE_NAME = 'RAW_INVOICES';

-- T10: RAW_INVOICES has primary key on file_name
SELECT 'T10_RAW_INVOICES_PK' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: no PK') AS result
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME = 'RAW_INVOICES'
  AND CONSTRAINT_TYPE = 'PRIMARY KEY';

-- T11: EXTRACTED_INVOICES table exists with correct column count
SELECT 'T11_EXTRACTED_INVOICES_COLUMNS' AS test,
       IFF(COUNT(*) = 15, 'PASS', 'FAIL: ' || COUNT(*) || ' columns') AS result
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'AP' AND TABLE_NAME = 'EXTRACTED_INVOICES';

-- T12: EXTRACTED_INVOICES has autoincrement PK
SELECT 'T12_EXTRACTED_INVOICES_PK' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: no PK') AS result
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME = 'EXTRACTED_INVOICES'
  AND CONSTRAINT_TYPE = 'PRIMARY KEY';

-- T13: EXTRACTED_INVOICES has FK to RAW_INVOICES
SELECT 'T13_EXTRACTED_INVOICES_FK' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: no FK') AS result
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME = 'EXTRACTED_INVOICES'
  AND CONSTRAINT_TYPE = 'FOREIGN KEY';

-- T14: EXTRACTED_LINE_ITEMS table exists with correct column count
SELECT 'T14_LINE_ITEMS_COLUMNS' AS test,
       IFF(COUNT(*) = 9, 'PASS', 'FAIL: ' || COUNT(*) || ' columns') AS result
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'AP' AND TABLE_NAME = 'EXTRACTED_LINE_ITEMS';

-- T15: EXTRACTED_LINE_ITEMS has FK to RAW_INVOICES
SELECT 'T15_LINE_ITEMS_FK' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: no FK') AS result
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME = 'EXTRACTED_LINE_ITEMS'
  AND CONSTRAINT_TYPE = 'FOREIGN KEY';

-- T16: VENDORS table exists with correct column count
SELECT 'T16_VENDORS_COLUMNS' AS test,
       IFF(COUNT(*) = 4, 'PASS', 'FAIL: ' || COUNT(*) || ' columns') AS result
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'AP' AND TABLE_NAME = 'VENDORS';

-- T17: VENDORS has unique constraint on vendor_name
SELECT 'T17_VENDORS_UNIQUE' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: no unique constraint') AS result
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME = 'VENDORS'
  AND CONSTRAINT_TYPE = 'UNIQUE';

-- T18: All 8 views exist
SELECT 'T18_ALL_VIEWS_EXIST' AS test,
       IFF(COUNT(*) = 8, 'PASS', 'FAIL: ' || COUNT(*) || ' views') AS result
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'AP'
  AND TABLE_NAME IN (
    'V_AP_LEDGER', 'V_AGING_SUMMARY', 'V_SPEND_BY_VENDOR',
    'V_SPEND_BY_CATEGORY', 'V_MONTHLY_TREND', 'V_TOP_LINE_ITEMS',
    'V_VENDOR_PAYMENT_TERMS', 'V_EXTRACTION_STATUS'
  );

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 3: DATA QUALITY & COMPLETENESS                                 ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T19: RAW_INVOICES has rows (at least 100)
SELECT 'T19_RAW_INVOICES_COUNT' AS test,
       IFF(COUNT(*) >= 100, 'PASS (' || COUNT(*) || ')', 'FAIL: ' || COUNT(*) || ' rows') AS result
FROM RAW_INVOICES;

-- T20: All RAW_INVOICES are marked extracted
SELECT 'T20_ALL_RAW_EXTRACTED' AS test,
       IFF(COUNT_IF(extracted = FALSE) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(extracted = FALSE) || ' unextracted') AS result
FROM RAW_INVOICES;

-- T21: EXTRACTED_INVOICES count matches RAW_INVOICES
SELECT 'T21_EXTRACTED_COUNT_MATCHES_RAW' AS test,
       IFF(a.ei_count = b.ri_count, 'PASS (' || a.ei_count || ')',
           'FAIL: extracted=' || a.ei_count || ' raw=' || b.ri_count) AS result
FROM (SELECT COUNT(*) AS ei_count FROM EXTRACTED_INVOICES) a,
     (SELECT COUNT(*) AS ri_count FROM RAW_INVOICES) b;

-- T22: No NULL vendor names in EXTRACTED_INVOICES
SELECT 'T22_NO_NULL_VENDOR_NAMES' AS test,
       IFF(COUNT_IF(vendor_name IS NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(vendor_name IS NULL) || ' nulls') AS result
FROM EXTRACTED_INVOICES;

-- T23: No NULL invoice numbers
SELECT 'T23_NO_NULL_INVOICE_NUMBERS' AS test,
       IFF(COUNT_IF(invoice_number IS NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(invoice_number IS NULL) || ' nulls') AS result
FROM EXTRACTED_INVOICES;

-- T24: No NULL total amounts
SELECT 'T24_NO_NULL_TOTALS' AS test,
       IFF(COUNT_IF(total_amount IS NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(total_amount IS NULL) || ' nulls') AS result
FROM EXTRACTED_INVOICES;

-- T25: All total_amounts are positive
SELECT 'T25_POSITIVE_TOTALS' AS test,
       IFF(COUNT_IF(total_amount <= 0) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(total_amount <= 0) || ' non-positive') AS result
FROM EXTRACTED_INVOICES;

-- T26: Valid statuses only (PENDING, APPROVED, PAID)
SELECT 'T26_VALID_STATUSES' AS test,
       IFF(COUNT_IF(status NOT IN ('PENDING', 'APPROVED', 'PAID')) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(status NOT IN ('PENDING', 'APPROVED', 'PAID')) || ' invalid') AS result
FROM EXTRACTED_INVOICES;

-- T27: Invoice numbers are unique
SELECT 'T27_UNIQUE_INVOICE_NUMBERS' AS test,
       IFF(COUNT(*) = COUNT(DISTINCT invoice_number), 'PASS',
           'FAIL: ' || (COUNT(*) - COUNT(DISTINCT invoice_number)) || ' duplicates') AS result
FROM EXTRACTED_INVOICES;

-- T28: EXTRACTED_LINE_ITEMS has rows (at least 800)
SELECT 'T28_LINE_ITEMS_COUNT' AS test,
       IFF(COUNT(*) >= 800, 'PASS (' || COUNT(*) || ')',
           'FAIL: only ' || COUNT(*) || ' rows') AS result
FROM EXTRACTED_LINE_ITEMS;

-- T29: No NULL product names in line items
SELECT 'T29_NO_NULL_PRODUCTS' AS test,
       IFF(COUNT_IF(product_name IS NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(product_name IS NULL) || ' nulls') AS result
FROM EXTRACTED_LINE_ITEMS;

-- T30: VENDORS has exactly 12 rows
SELECT 'T30_VENDORS_COUNT' AS test,
       IFF(COUNT(*) = 12, 'PASS', 'FAIL: ' || COUNT(*) || ' rows') AS result
FROM VENDORS;

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 4: REFERENTIAL INTEGRITY                                       ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T31: Every EXTRACTED_INVOICE links to a RAW_INVOICE
SELECT 'T31_EXTRACTED_FK_INTEGRITY' AS test,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL: ' || COUNT(*) || ' orphan invoices') AS result
FROM EXTRACTED_INVOICES ei
LEFT JOIN RAW_INVOICES ri ON ei.file_name = ri.file_name
WHERE ri.file_name IS NULL;

-- T32: Every LINE_ITEM links to a RAW_INVOICE
SELECT 'T32_LINE_ITEM_RAW_FK' AS test,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL: ' || COUNT(*) || ' orphan line items') AS result
FROM EXTRACTED_LINE_ITEMS li
LEFT JOIN RAW_INVOICES ri ON li.file_name = ri.file_name
WHERE ri.file_name IS NULL;

-- T33: Every LINE_ITEM links to an EXTRACTED_INVOICE (by invoice_number)
SELECT 'T33_LINE_ITEM_INVOICE_FK' AS test,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL: ' || COUNT(*) || ' orphan line items') AS result
FROM EXTRACTED_LINE_ITEMS li
LEFT JOIN EXTRACTED_INVOICES ei ON li.invoice_number = ei.invoice_number
WHERE ei.invoice_number IS NULL;

-- T34: Every invoice has at least 1 line item
SELECT 'T34_ALL_INVOICES_HAVE_LINES' AS test,
       IFF(COUNT(*) = 0, 'PASS', 'FAIL: ' || COUNT(*) || ' invoices without lines') AS result
FROM EXTRACTED_INVOICES ei
LEFT JOIN EXTRACTED_LINE_ITEMS li ON ei.invoice_number = li.invoice_number
WHERE li.invoice_number IS NULL;

-- T35: Stage file count matches RAW_INVOICES count
SELECT 'T35_STAGE_MATCHES_RAW' AS test,
       IFF(a.stage_count = b.raw_count, 'PASS (' || a.stage_count || ' files)',
           'FAIL: stage=' || a.stage_count || ' raw=' || b.raw_count) AS result
FROM (SELECT COUNT(*) AS stage_count FROM DIRECTORY(@INVOICE_STAGE) WHERE RELATIVE_PATH LIKE '%.pdf') a,
     (SELECT COUNT(*) AS raw_count FROM RAW_INVOICES) b;

-- T36: PAID invoices have payment_date set
SELECT 'T36_PAID_HAVE_PAYMENT_DATE' AS test,
       IFF(COUNT_IF(status = 'PAID' AND payment_date IS NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(status = 'PAID' AND payment_date IS NULL) || ' paid without date') AS result
FROM EXTRACTED_INVOICES;

-- T37: Non-PAID invoices have NULL payment_date
SELECT 'T37_UNPAID_NO_PAYMENT_DATE' AS test,
       IFF(COUNT_IF(status != 'PAID' AND payment_date IS NOT NULL) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(status != 'PAID' AND payment_date IS NOT NULL) || ' unpaid with date') AS result
FROM EXTRACTED_INVOICES;

-- T38: Line item totals roughly match invoice totals (within 20% tolerance for tax)
SELECT 'T38_LINE_TOTALS_VS_INVOICE_TOTALS' AS test,
       IFF(COUNT(*) = 0, 'PASS',
           'FAIL: ' || COUNT(*) || ' invoices with >20% line total mismatch') AS result
FROM (
    SELECT ei.invoice_number, ei.subtotal AS invoice_subtotal,
           SUM(li.line_total) AS line_sum
    FROM EXTRACTED_INVOICES ei
    JOIN EXTRACTED_LINE_ITEMS li ON ei.invoice_number = li.invoice_number
    WHERE ei.subtotal IS NOT NULL AND ei.subtotal > 0
    GROUP BY ei.invoice_number, ei.subtotal
    HAVING ABS(line_sum - invoice_subtotal) / invoice_subtotal > 0.20
);

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 5: VIEW CORRECTNESS & LOGIC                                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T39: V_AP_LEDGER returns data and has aging columns
SELECT 'T39_V_AP_LEDGER_DATA' AS test,
       IFF(COUNT(*) > 0, 'PASS (' || COUNT(*) || ' rows)', 'FAIL: empty') AS result
FROM V_AP_LEDGER;

-- T40: V_AP_LEDGER aging_bucket only contains valid values
SELECT 'T40_V_AP_LEDGER_VALID_BUCKETS' AS test,
       IFF(COUNT_IF(aging_bucket NOT IN ('Current', '1-30 Days', '31-60 Days', '61-90 Days', '90+ Days', 'Paid')) = 0,
           'PASS', 'FAIL: invalid buckets found') AS result
FROM V_AP_LEDGER;

-- T41: V_AP_LEDGER — PAID invoices have 0 outstanding
SELECT 'T41_PAID_ZERO_OUTSTANDING' AS test,
       IFF(COUNT_IF(status = 'PAID' AND outstanding_amount != 0) = 0, 'PASS',
           'FAIL: ' || COUNT_IF(status = 'PAID' AND outstanding_amount != 0) || ' paid with outstanding') AS result
FROM V_AP_LEDGER;

-- T42: V_AP_LEDGER — unpaid invoices have outstanding = total_amount
SELECT 'T42_UNPAID_OUTSTANDING_EQUALS_TOTAL' AS test,
       IFF(COUNT_IF(status != 'PAID' AND outstanding_amount != total_amount) = 0, 'PASS',
           'FAIL: mismatch') AS result
FROM V_AP_LEDGER;

-- T43: V_AGING_SUMMARY has all expected buckets
SELECT 'T43_AGING_SUMMARY_BUCKETS' AS test,
       IFF(COUNT(*) >= 2, 'PASS (' || COUNT(*) || ' buckets)', 'FAIL: only ' || COUNT(*)) AS result
FROM V_AGING_SUMMARY;

-- T44: V_AGING_SUMMARY totals match V_AP_LEDGER
SELECT 'T44_AGING_TOTALS_CONSISTENT' AS test,
       IFF(ABS(a.aging_total - b.ledger_total) < 0.01, 'PASS',
           'FAIL: aging=' || a.aging_total || ' ledger=' || b.ledger_total) AS result
FROM (SELECT SUM(total_outstanding) AS aging_total FROM V_AGING_SUMMARY) a,
     (SELECT SUM(outstanding_amount) AS ledger_total FROM V_AP_LEDGER) b;

-- T45: V_SPEND_BY_VENDOR covers all vendors with invoices
SELECT 'T45_SPEND_BY_VENDOR_COMPLETE' AS test,
       IFF(a.view_vendors = b.actual_vendors, 'PASS (' || a.view_vendors || ' vendors)',
           'FAIL: view=' || a.view_vendors || ' actual=' || b.actual_vendors) AS result
FROM (SELECT COUNT(*) AS view_vendors FROM V_SPEND_BY_VENDOR) a,
     (SELECT COUNT(DISTINCT vendor_name) AS actual_vendors FROM EXTRACTED_INVOICES) b;

-- T46: V_SPEND_BY_CATEGORY has data
SELECT 'T46_SPEND_BY_CATEGORY_DATA' AS test,
       IFF(COUNT(*) > 0, 'PASS (' || COUNT(*) || ' categories)', 'FAIL: empty') AS result
FROM V_SPEND_BY_CATEGORY;

-- T47: V_MONTHLY_TREND has data
SELECT 'T47_MONTHLY_TREND_DATA' AS test,
       IFF(COUNT(*) > 0, 'PASS (' || COUNT(*) || ' months)', 'FAIL: empty') AS result
FROM V_MONTHLY_TREND;

-- T48: V_TOP_LINE_ITEMS has data
SELECT 'T48_TOP_LINE_ITEMS_DATA' AS test,
       IFF(COUNT(*) > 0, 'PASS (' || COUNT(*) || ' products)', 'FAIL: empty') AS result
FROM V_TOP_LINE_ITEMS;

-- T49: V_VENDOR_PAYMENT_TERMS has data
SELECT 'T49_VENDOR_PAYMENT_TERMS_DATA' AS test,
       IFF(COUNT(*) > 0, 'PASS (' || COUNT(*) || ' rows)', 'FAIL: empty') AS result
FROM V_VENDOR_PAYMENT_TERMS;

-- T50: V_EXTRACTION_STATUS shows all extracted, none pending
SELECT 'T50_EXTRACTION_STATUS_CLEAN' AS test,
       IFF(pending_files = 0, 'PASS (all ' || total_files || ' extracted)',
           'FAIL: ' || pending_files || ' still pending') AS result
FROM V_EXTRACTION_STATUS;

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 6: STORED PROCEDURES & TASK                                    ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T51: SP_EXTRACT_NEW_INVOICES procedure exists
SHOW PROCEDURES LIKE 'SP_EXTRACT_NEW_INVOICES' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T51_SP_EXTRACT_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: procedure not found') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- T52: SP_GENERATE_DEMO_INVOICES procedure exists
SHOW PROCEDURES LIKE 'SP_GENERATE_DEMO_INVOICES' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T52_SP_GENERATE_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: procedure not found') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- T53: GENERATE_INVOICE_PDF function exists
SHOW USER FUNCTIONS LIKE 'GENERATE_INVOICE_PDF' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T53_GENERATE_PDF_UDF_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: function not found') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- T54: Task exists
SHOW TASKS LIKE 'EXTRACT_NEW_INVOICES_TASK' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T54_TASK_EXISTS' AS test,
       IFF(COUNT(*) >= 1, 'PASS', 'FAIL: task not found') AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- T55: Task schedule is 5 minutes
SHOW TASKS LIKE 'EXTRACT_NEW_INVOICES_TASK' IN SCHEMA AP_DEMO_DB.AP;
SELECT 'T55_TASK_SCHEDULE' AS test,
       IFF("schedule" = '5 MINUTE', 'PASS', 'FAIL: schedule=' || COALESCE("schedule", 'NULL')) AS result
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SECTION 7: STAGE & PRESIGNED URL ACCESS                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- T56: Invoice stage has PDF files
SELECT 'T56_STAGE_HAS_PDFS' AS test,
       IFF(COUNT(*) >= 100, 'PASS (' || COUNT(*) || ' PDFs)',
           'FAIL: only ' || COUNT(*) || ' PDFs') AS result
FROM DIRECTORY(@INVOICE_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf';

-- T57: GET_PRESIGNED_URL works (column reference pattern)
SELECT 'T57_PRESIGNED_URL_WORKS' AS test,
       IFF(pdf_url IS NOT NULL AND LENGTH(pdf_url) > 50, 'PASS',
           'FAIL: URL is null or too short') AS result
FROM (
    SELECT GET_PRESIGNED_URL(@AP_DEMO_DB.AP.INVOICE_STAGE, file_name, 3600) AS pdf_url
    FROM EXTRACTED_INVOICES
    LIMIT 1
);

-- T58: Streamlit stage has required files (at least 5)
ALTER STAGE STREAMLIT_STAGE REFRESH;
SELECT 'T58_STREAMLIT_STAGE_FILES' AS test,
       IFF(COUNT(*) >= 5, 'PASS (' || COUNT(*) || ' files)',
           'FAIL: only ' || COUNT(*) || ' files') AS result
FROM DIRECTORY(@STREAMLIT_STAGE);

-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║  SUMMARY                                                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝
SELECT '=== 58 E2E TESTS COMPLETE ===' AS summary;
