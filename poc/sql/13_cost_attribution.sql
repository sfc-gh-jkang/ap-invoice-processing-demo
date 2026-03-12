-- =============================================================================
-- 13_cost_attribution.sql — Per-PDF Cost Attribution
--
-- Adds PAGE_COUNT and FILE_SIZE_BYTES to RAW_DOCUMENTS, backfills from
-- stage metadata + a Python UDF for page counting, and creates a view
-- that joins billing data to document metadata for cost driver analysis.
--
-- Prerequisites:
--   - 12_cost_views.sql already deployed
--   - pypdfium2 available in packages (already in pyproject.toml)
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- Step 1: Add PAGE_COUNT and FILE_SIZE_BYTES columns to RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
ALTER TABLE RAW_DOCUMENTS ADD COLUMN IF NOT EXISTS PAGE_COUNT INT;
ALTER TABLE RAW_DOCUMENTS ADD COLUMN IF NOT EXISTS FILE_SIZE_BYTES INT;

-- ---------------------------------------------------------------------------
-- Step 2: Backfill FILE_SIZE_BYTES from DIRECTORY() stage metadata
-- ---------------------------------------------------------------------------
MERGE INTO RAW_DOCUMENTS r
USING (
    SELECT RELATIVE_PATH AS file_name, SIZE AS file_size_bytes
    FROM DIRECTORY(@DOCUMENT_STAGE)
) d ON r.file_name = d.file_name
WHEN MATCHED AND r.file_size_bytes IS NULL THEN
    UPDATE SET r.file_size_bytes = d.file_size_bytes;

-- ---------------------------------------------------------------------------
-- Step 3: Python UDF to count PDF pages via pypdfium2
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION UDF_PDF_PAGE_COUNT(file_path VARCHAR)
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pypdfium2')
HANDLER = 'count_pages'
AS $$
from snowflake.snowpark.files import SnowflakeFile
import pypdfium2 as pdfium

def count_pages(file_path):
    try:
        with SnowflakeFile.open(file_path, "rb") as f:
            pdf = pdfium.PdfDocument(f)
            return len(pdf)
    except Exception:
        return None
$$;

-- ---------------------------------------------------------------------------
-- Step 4: Backfill PAGE_COUNT for all documents
-- ---------------------------------------------------------------------------
UPDATE RAW_DOCUMENTS
SET page_count = UDF_PDF_PAGE_COUNT(
    BUILD_SCOPED_FILE_URL(@DOCUMENT_STAGE, file_name)
)
WHERE page_count IS NULL;

-- ---------------------------------------------------------------------------
-- Step 5: Count extracted fields per doc type from config
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DOC_TYPE_FIELD_COUNTS AS
SELECT
    doc_type,
    ARRAY_SIZE(SPLIT(extraction_prompt, ',')) AS field_count
FROM DOCUMENT_TYPE_CONFIG;

-- ---------------------------------------------------------------------------
-- Step 6: Per-PDF Cost Attribution View
-- Joins: CORTEX_AI_FUNCTIONS_USAGE_HISTORY → QUERY_HISTORY → RAW_DOCUMENTS
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_PER_PDF AS
WITH ai_calls AS (
    SELECT
        a.query_id,
        a.start_time,
        a.credits                                    AS ai_credits,
        PARSE_JSON(a.metrics[0]:value)::INT           AS tokens,
        q.query_text,
        q.total_elapsed_time / 1000.0                 AS elapsed_sec
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY a
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON a.query_id = q.query_id
    WHERE a.function_name = 'AI_EXTRACT'
      AND a.start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
),
file_match AS (
    SELECT
        ac.*,
        r.file_name,
        r.doc_type,
        r.page_count,
        r.file_size_bytes
    FROM ai_calls ac
    LEFT JOIN RAW_DOCUMENTS r
        ON ac.query_text ILIKE '%' || r.file_name || '%'
)
SELECT
    fm.query_id,
    fm.start_time,
    fm.file_name,
    fm.doc_type,
    fm.page_count,
    fm.file_size_bytes,
    fm.ai_credits,
    fm.tokens,
    fm.elapsed_sec,
    fc.field_count,
    CASE WHEN fm.page_count > 0
         THEN ROUND(fm.ai_credits / fm.page_count, 6)
         ELSE NULL
    END AS credits_per_page,
    CASE WHEN fm.page_count > 0
         THEN ROUND(fm.tokens::FLOAT / fm.page_count, 0)
         ELSE NULL
    END AS tokens_per_page
FROM file_match fm
LEFT JOIN V_DOC_TYPE_FIELD_COUNTS fc ON fm.doc_type = fc.doc_type
WHERE fm.file_name IS NOT NULL
ORDER BY fm.start_time DESC;

-- ---------------------------------------------------------------------------
-- Step 7: Aggregated cost drivers by doc type
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_DRIVERS AS
SELECT
    doc_type,
    COUNT(*) AS total_calls,
    ROUND(AVG(page_count), 1) AS avg_pages,
    ROUND(AVG(file_size_bytes), 0) AS avg_file_size,
    ROUND(AVG(tokens), 0) AS avg_tokens,
    ROUND(AVG(ai_credits), 6) AS avg_credits,
    ROUND(AVG(credits_per_page), 6) AS avg_credits_per_page,
    ROUND(AVG(tokens_per_page), 0) AS avg_tokens_per_page,
    AVG(field_count) AS fields_extracted,
    ROUND(AVG(elapsed_sec), 1) AS avg_elapsed_sec,
    ROUND(CORR(page_count, tokens), 4) AS page_token_correlation,
    ROUND(CORR(tokens, ai_credits), 4) AS token_credit_correlation
FROM V_AI_EXTRACT_COST_PER_PDF
GROUP BY doc_type
ORDER BY avg_credits DESC;
