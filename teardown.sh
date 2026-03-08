#!/usr/bin/env bash
# teardown.sh — Remove all Snowflake objects created by deploy.sh
# Usage: ./teardown.sh [--connection <name>]
set -euo pipefail

# ---------- Config (override via environment variables) ----------
DB="${AP_DB:-AP_DEMO_DB}"
SCHEMA="${AP_SCHEMA:-AP}"
WH="${AP_WAREHOUSE:-AP_DEMO_WH}"
COMPUTE_POOL="${AP_COMPUTE_POOL:-AP_DEMO_POOL}"
CONNECTION="${AP_CONNECTION:-${1:-default}}"
CONNECTION_FLAG="-c $CONNECTION"

echo "=============================================="
echo " AP Invoice Processing Demo — Teardown"
echo "=============================================="
echo "  Database:     ${DB}"
echo "  Schema:       ${SCHEMA}"
echo "  Warehouse:    ${WH}"
echo "  Compute Pool: ${COMPUTE_POOL}"
echo "  Connection:   ${CONNECTION}"

# ---------- Drop task first (must be done before other objects) ----------
echo ""
echo "[1/5] Dropping task and stream..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    ALTER TASK IF EXISTS ${DB}.${SCHEMA}.EXTRACT_NEW_INVOICES_TASK SUSPEND;
    DROP TASK IF EXISTS ${DB}.${SCHEMA}.EXTRACT_NEW_INVOICES_TASK;
    DROP STREAM IF EXISTS ${DB}.${SCHEMA}.RAW_INVOICES_STREAM;
    DROP PROCEDURE IF EXISTS ${DB}.${SCHEMA}.SP_EXTRACT_NEW_INVOICES();
" || true

# ---------- Drop Streamlit app ----------
echo "[2/5] Dropping Streamlit app..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    DROP STREAMLIT IF EXISTS ${DB}.${SCHEMA}.AP_INVOICE_APP;
" || true

# ---------- Drop database (takes tables, views, stages with it) ----------
echo "[3/5] Dropping database ${DB}..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    DROP DATABASE IF EXISTS ${DB};
"

# ---------- Drop compute pool ----------
echo "[4/5] Dropping compute pool..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    ALTER COMPUTE POOL IF EXISTS ${COMPUTE_POOL} STOP ALL;
" || true
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    DROP COMPUTE POOL IF EXISTS ${COMPUTE_POOL};
" || true

# ---------- Drop warehouse ----------
echo "[5/5] Dropping warehouse..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    DROP WAREHOUSE IF EXISTS ${WH};
" || true

echo ""
echo "=============================================="
echo " Teardown complete."
echo "=============================================="
echo ""
echo "All objects removed:"
echo "  - Database: ${DB}"
echo "  - Warehouse: ${WH}"
echo "  - Compute Pool: ${COMPUTE_POOL}"
echo "  - Task, Stream, Stored Procedure"
echo "  - Streamlit app"
