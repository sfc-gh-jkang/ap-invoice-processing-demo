-- =============================================================================
-- 12_cost_views.sql — Cost Observability for the AI_EXTRACT Pipeline
--
-- Uses CORTEX_AI_FUNCTIONS_USAGE_HISTORY for accurate per-call AI_EXTRACT
-- credit attribution (token-based billing), NOT warehouse metering.
--
-- All metrics are in CREDITS (Snowflake's native billing unit).
-- USD conversion is handled in the Streamlit UI layer with a configurable rate.
--
-- Creates views:
--   1. V_AI_EXTRACT_COST_DAILY        — Daily AI_EXTRACT credits + warehouse context
--   2. V_AI_EXTRACT_COST_BY_DOC_TYPE  — Credits by document type via query join
--   3. V_AI_EXTRACT_COST_PER_DOCUMENT — Per-document credits (1 call = 1 doc)
--   4. V_AI_EXTRACT_QUERY_LOG         — Recent AI_EXTRACT calls with credits/tokens
--   5. V_AI_EXTRACT_COST_SUMMARY      — KPI summary (7d, 30d, 90d)
--   6. V_AI_EXTRACT_COST_BREAKDOWN    — Full credit breakdown (AI vs WH vs SPCS)
--
-- Prerequisites:
--   GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE AI_EXTRACT_APP;
--   (handled by 10_harden.sql)
-- =============================================================================

USE ROLE AI_EXTRACT_APP;
USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;
USE WAREHOUSE AI_EXTRACT_WH;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_COST_DAILY: Daily AI_EXTRACT credits from first-party billing
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_DAILY AS
WITH ai AS (
    SELECT
        start_time::DATE                         AS usage_date,
        COUNT(*)                                 AS ai_extract_calls,
        SUM(credits)                             AS ai_extract_credits,
        SUM(PARSE_JSON(metrics[0]:value)::INT)   AS total_tokens
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
    WHERE function_name = 'AI_EXTRACT'
      AND start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),
wh AS (
    SELECT
        DATE_TRUNC('day', start_time)::DATE AS usage_date,
        SUM(credits_used)                   AS warehouse_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name = 'AI_EXTRACT_WH'
      AND start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
    GROUP BY 1
),
docs AS (
    SELECT
        extracted_at::DATE AS usage_date,
        COUNT(*)           AS docs_extracted
    FROM EXTRACTED_FIELDS
    WHERE extracted_at IS NOT NULL
    GROUP BY 1
)
SELECT
    COALESCE(ai.usage_date, wh.usage_date, docs.usage_date) AS usage_date,
    COALESCE(ai.ai_extract_credits, 0)                      AS ai_extract_credits,
    COALESCE(ai.ai_extract_calls, 0)                        AS ai_extract_calls,
    COALESCE(ai.total_tokens, 0)                             AS total_tokens,
    COALESCE(docs.docs_extracted, 0)                         AS docs_extracted,
    COALESCE(wh.warehouse_credits, 0)                        AS warehouse_credits
FROM ai
FULL OUTER JOIN wh ON ai.usage_date = wh.usage_date
FULL OUTER JOIN docs ON COALESCE(ai.usage_date, wh.usage_date) = docs.usage_date
ORDER BY 1 DESC;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_COST_BY_DOC_TYPE: Credits by document type
-- Joins AI_EXTRACT calls to QUERY_HISTORY to get doc_type
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_BY_DOC_TYPE AS
WITH ai_calls AS (
    SELECT
        a.query_id,
        a.start_time::DATE                          AS usage_date,
        a.credits                                   AS ai_credits,
        PARSE_JSON(a.metrics[0]:value)::INT          AS tokens,
        COALESCE(
            REGEXP_SUBSTR(q.query_tag, 'doc_type=([A-Za-z_]+)', 1, 1, 'e'),
            UPPER(REGEXP_SUBSTR(q.query_text, 'doc_type\s*=\s*''([A-Za-z_]+)''', 1, 1, 'ie')),
            'UNKNOWN'
        )                                            AS doc_type,
        q.total_elapsed_time / 1000.0                AS elapsed_sec
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY a
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON a.query_id = q.query_id
    WHERE a.function_name = 'AI_EXTRACT'
      AND a.start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
)
SELECT
    doc_type,
    usage_date,
    COUNT(*)                              AS call_count,
    ROUND(SUM(ai_credits), 6)            AS ai_extract_credits,
    SUM(tokens)                           AS total_tokens,
    ROUND(SUM(elapsed_sec), 1)            AS total_elapsed_sec,
    ROUND(AVG(elapsed_sec), 1)            AS avg_elapsed_sec
FROM ai_calls
GROUP BY 1, 2
ORDER BY 2 DESC, 1;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_COST_PER_DOCUMENT: Per-document credits (1 call = 1 doc)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_PER_DOCUMENT AS
SELECT
    usage_date,
    ai_extract_credits,
    ai_extract_calls,
    total_tokens,
    docs_extracted,
    CASE WHEN ai_extract_calls > 0
         THEN ROUND(ai_extract_credits / ai_extract_calls, 6)
         ELSE NULL
    END AS credits_per_doc
FROM V_AI_EXTRACT_COST_DAILY
WHERE ai_extract_calls > 0
ORDER BY usage_date DESC;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_QUERY_LOG: Recent AI_EXTRACT calls with credits and tokens
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_QUERY_LOG AS
SELECT
    a.query_id,
    a.start_time,
    a.credits                                        AS ai_credits,
    PARSE_JSON(a.metrics[0]:value)::INT               AS tokens,
    COALESCE(
        REGEXP_SUBSTR(q.query_tag, 'doc_type=([A-Za-z_]+)', 1, 1, 'e'),
        'UNKNOWN'
    )                                                 AS doc_type,
    q.total_elapsed_time / 1000.0                     AS elapsed_sec,
    q.rows_produced,
    q.query_text
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY a
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q ON a.query_id = q.query_id
WHERE a.function_name = 'AI_EXTRACT'
  AND a.start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY a.start_time DESC
LIMIT 500;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_COST_SUMMARY: KPI summary using AI_EXTRACT first-party billing
-- All values in CREDITS — USD conversion in UI layer
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_SUMMARY AS
WITH ai AS (
    SELECT
        SUM(CASE WHEN start_time >= DATEADD('day',  -7, CURRENT_TIMESTAMP()) THEN credits ELSE 0 END) AS credits_7d,
        SUM(CASE WHEN start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN credits ELSE 0 END) AS credits_30d,
        SUM(credits)                                                                                    AS credits_90d,
        SUM(CASE WHEN start_time >= DATEADD('day',  -7, CURRENT_TIMESTAMP()) THEN PARSE_JSON(metrics[0]:value)::INT ELSE 0 END) AS tokens_7d,
        SUM(CASE WHEN start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN PARSE_JSON(metrics[0]:value)::INT ELSE 0 END) AS tokens_30d,
        SUM(PARSE_JSON(metrics[0]:value)::INT)                                                          AS tokens_90d,
        COUNT(*)                                                                                         AS total_calls
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
    WHERE function_name = 'AI_EXTRACT'
      AND start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
),
wh AS (
    SELECT
        SUM(CASE WHEN start_time >= DATEADD('day',  -7, CURRENT_TIMESTAMP()) THEN credits_used ELSE 0 END) AS wh_credits_7d,
        SUM(CASE WHEN start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN credits_used ELSE 0 END) AS wh_credits_30d,
        SUM(credits_used)                                                                                    AS wh_credits_90d
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE warehouse_name = 'AI_EXTRACT_WH'
      AND start_time >= DATEADD('day', -90, CURRENT_TIMESTAMP())
),
docs AS (
    SELECT COUNT(*) AS total_docs FROM EXTRACTED_FIELDS
)
SELECT
    ROUND(ai.credits_7d, 4)                                                  AS ai_credits_last_7d,
    ROUND(ai.credits_30d, 4)                                                 AS ai_credits_last_30d,
    ROUND(ai.credits_90d, 4)                                                 AS ai_credits_last_90d,
    ai.tokens_7d,
    ai.tokens_30d,
    ai.tokens_90d,
    ai.total_calls,
    docs.total_docs                                                           AS unique_docs,
    CASE WHEN ai.total_calls > 0
         THEN ROUND(ai.credits_90d / ai.total_calls, 6)
         ELSE 0
    END                                                                       AS avg_credits_per_doc,
    CASE WHEN docs.total_docs > 0
         THEN ROUND(ai.credits_90d / docs.total_docs, 6)
         ELSE 0
    END                                                                       AS amortized_credits_per_unique_doc,
    ROUND(wh.wh_credits_7d, 4)                                               AS wh_credits_last_7d,
    ROUND(wh.wh_credits_30d, 4)                                              AS wh_credits_last_30d,
    ROUND(wh.wh_credits_90d, 4)                                              AS wh_credits_last_90d
FROM ai, wh, docs;

-- ---------------------------------------------------------------------------
-- V_AI_EXTRACT_COST_BREAKDOWN: Full credit breakdown by service type
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AI_EXTRACT_COST_BREAKDOWN AS
SELECT
    usage_date,
    service_type,
    ROUND(credits_used, 4)   AS credits_used,
    ROUND(credits_billed, 4) AS credits_billed
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE service_type IN ('AI_SERVICES', 'WAREHOUSE_METERING', 'SNOWPARK_CONTAINER_SERVICES')
  AND usage_date >= DATEADD('day', -90, CURRENT_DATE())
ORDER BY usage_date DESC, service_type;
