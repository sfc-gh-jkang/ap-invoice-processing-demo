-- =============================================================================
-- 11_alerts.sql — Snowflake Alerts for extraction failure monitoring
--
-- Creates an alert that fires when extraction failures exceed a threshold.
-- The alert checks RAW_DOCUMENTS for recent extraction errors and sends
-- an email notification to the configured recipient.
--
-- Prerequisites:
--   - A notification integration (or use the built-in email notification)
--   - EXECUTE ALERT privilege on the role
-- =============================================================================

USE DATABASE AI_EXTRACT_POC;
USE SCHEMA DOCUMENTS;

-- ---------------------------------------------------------------------------
-- 1. Create a view summarizing extraction health (used by the alert)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_EXTRACTION_HEALTH AS
SELECT
    COUNT(*)                                                         AS TOTAL_FILES,
    COUNT_IF(EXTRACTED = TRUE)                                       AS EXTRACTED_OK,
    COUNT_IF(EXTRACTION_ERROR IS NOT NULL)                           AS FAILED_FILES,
    COUNT_IF(EXTRACTED = FALSE AND EXTRACTION_ERROR IS NULL)         AS PENDING_FILES,
    COALESCE(MAX(EXTRACTED_AT), '1970-01-01'::TIMESTAMP_NTZ)        AS LAST_EXTRACTION,
    -- Recent failures: files that failed in the last 24 hours
    COUNT_IF(EXTRACTION_ERROR IS NOT NULL
             AND EXTRACTED_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS RECENT_FAILURES
FROM RAW_DOCUMENTS;

-- ---------------------------------------------------------------------------
-- 2. Create the extraction failure alert
--
-- Fires every 60 minutes. Triggers when there are 3+ failures in the last
-- 24 hours, indicating a systemic extraction problem.
--
-- The alert action inserts a row into an alert history table for tracking.
-- Email notification can be added by configuring a notification integration.
-- ---------------------------------------------------------------------------

-- Alert history table (lightweight audit trail for alert firings)
CREATE TABLE IF NOT EXISTS EXTRACTION_ALERT_HISTORY (
    ALERT_ID       NUMBER AUTOINCREMENT,
    ALERT_NAME     VARCHAR,
    FIRED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FAILED_COUNT   NUMBER,
    TOTAL_FILES    NUMBER,
    MESSAGE        VARCHAR
);

-- The alert itself
CREATE OR REPLACE ALERT EXTRACTION_FAILURE_ALERT
    WAREHOUSE = AI_EXTRACT_WH
    SCHEDULE  = '60 MINUTE'
IF (EXISTS (
    SELECT 1
    FROM V_EXTRACTION_HEALTH
    WHERE RECENT_FAILURES >= 3
))
THEN
    INSERT INTO EXTRACTION_ALERT_HISTORY (ALERT_NAME, FAILED_COUNT, TOTAL_FILES, MESSAGE)
    SELECT
        'EXTRACTION_FAILURE_ALERT',
        RECENT_FAILURES,
        TOTAL_FILES,
        'Extraction failure threshold exceeded: ' || RECENT_FAILURES || ' failures in last 24h out of ' || TOTAL_FILES || ' total files.'
    FROM V_EXTRACTION_HEALTH;

-- Resume the alert so it starts running
ALTER ALERT EXTRACTION_FAILURE_ALERT RESUME;

-- Grant execute alert to the app role
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE AI_EXTRACT_APP;
GRANT MONITOR ON ALERT EXTRACTION_FAILURE_ALERT TO ROLE AI_EXTRACT_APP;

-- ---------------------------------------------------------------------------
-- 3. Create a stored procedure to check extraction health on-demand
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_CHECK_EXTRACTION_HEALTH()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total     NUMBER;
    v_ok        NUMBER;
    v_failed    NUMBER;
    v_pending   NUMBER;
    v_recent    NUMBER;
    v_msg       VARCHAR;
BEGIN
    SELECT TOTAL_FILES, EXTRACTED_OK, FAILED_FILES, PENDING_FILES, RECENT_FAILURES
    INTO :v_total, :v_ok, :v_failed, :v_pending, :v_recent
    FROM V_EXTRACTION_HEALTH;

    v_msg := 'Extraction Health: ' ||
             :v_total || ' total, ' ||
             :v_ok || ' OK, ' ||
             :v_failed || ' failed, ' ||
             :v_pending || ' pending, ' ||
             :v_recent || ' recent failures (24h)';

    IF (:v_recent >= 3) THEN
        v_msg := v_msg || ' — WARNING: failure threshold exceeded!';
    ELSEIF (:v_pending > 0) THEN
        v_msg := v_msg || ' — INFO: documents pending extraction.';
    ELSE
        v_msg := v_msg || ' — HEALTHY';
    END IF;

    RETURN :v_msg;
END;
$$;

GRANT USAGE ON PROCEDURE SP_CHECK_EXTRACTION_HEALTH() TO ROLE AI_EXTRACT_APP;
