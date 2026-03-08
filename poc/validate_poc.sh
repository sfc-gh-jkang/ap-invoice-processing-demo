#!/usr/bin/env bash
# =============================================================================
# validate_poc.sh — Standalone health check for the AI_EXTRACT POC
#
# Usage:
#   ./validate_poc.sh                          # uses default connection
#   ./validate_poc.sh --connection my_account   # specify connection
#
# Override object names via environment variables:
#   POC_DB=MY_DB POC_WH=MY_WH ./validate_poc.sh
#
# Exit codes:
#   0 = all checks passed
#   1 = one or more checks failed
# =============================================================================
set -euo pipefail

# ---------- Parse arguments ----------
CONNECTION="default"
for arg in "$@"; do
    case "$arg" in
        --connection=*) CONNECTION="${arg#*=}" ;;
        --connection)   shift_next=1 ;;
        *)
            if [[ "${shift_next:-}" == "1" ]]; then
                CONNECTION="$arg"
                unset shift_next
            fi
            ;;
    esac
done

CONNECTION_FLAG="-c ${CONNECTION}"

# ---------- Configurable names ----------
POC_DB="${POC_DB:-AI_EXTRACT_POC}"
POC_SCHEMA="${POC_SCHEMA:-DOCUMENTS}"
POC_WH="${POC_WH:-AI_EXTRACT_WH}"
POC_POOL="${POC_POOL:-AI_EXTRACT_POC_POOL}"
POC_ROLE="${POC_ROLE:-AI_EXTRACT_APP}"

echo ""
echo "=============================================="
echo "  AI_EXTRACT POC — HEALTH CHECK"
echo "=============================================="
echo "  Database:      ${POC_DB}"
echo "  Schema:        ${POC_SCHEMA}"
echo "  Warehouse:     ${POC_WH}"
echo "  Compute Pool:  ${POC_POOL}"
echo "  Role:          ${POC_ROLE}"
echo "  Connection:    ${CONNECTION}"
echo ""

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass() { echo "  PASS  $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo "  FAIL  $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo "  WARN  $1"; WARN_COUNT=$((WARN_COUNT + 1)); }

# Helper: run SQL and return first numeric result
sql_count() {
    local result
    result=$(snow sql $CONNECTION_FLAG -q "$1" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1 | cut -d: -f2)
    echo "${result:-0}"
}

# ==========================================================================
# 1. Infrastructure checks
# ==========================================================================
echo "--- Infrastructure ---"

# Database exists
DB_EXISTS=$(snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '${POC_DB}'" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1 | cut -d: -f2 || echo "0")
if [[ "$DB_EXISTS" -ge 1 ]]; then pass "Database ${POC_DB}"; else fail "Database ${POC_DB} not found"; fi

# Warehouse exists
WH_EXISTS=$(snow sql $CONNECTION_FLAG -q "SHOW WAREHOUSES LIKE '${POC_WH}'" --format json 2>/dev/null | grep -c "${POC_WH}" || echo "0")
if [[ "$WH_EXISTS" -ge 1 ]]; then pass "Warehouse ${POC_WH}"; else fail "Warehouse ${POC_WH} not found"; fi

# Role exists
ROLE_EXISTS=$(snow sql $CONNECTION_FLAG -q "SHOW ROLES LIKE '${POC_ROLE}'" --format json 2>/dev/null | grep -c "${POC_ROLE}" || echo "0")
if [[ "$ROLE_EXISTS" -ge 1 ]]; then pass "Role ${POC_ROLE}"; else fail "Role ${POC_ROLE} not found"; fi

echo ""

# ==========================================================================
# 2. Table checks
# ==========================================================================
echo "--- Tables ---"

for TABLE in RAW_DOCUMENTS EXTRACTED_FIELDS EXTRACTED_TABLE_DATA DOCUMENT_TYPE_CONFIG INVOICE_REVIEW; do
    COUNT=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.${TABLE}")
    if [[ "$COUNT" -ge 0 ]]; then
        pass "${TABLE} (${COUNT} rows)"
    else
        fail "${TABLE} not found"
    fi
done

echo ""

# ==========================================================================
# 3. View checks
# ==========================================================================
echo "--- Views ---"

for VIEW in V_DOCUMENT_SUMMARY V_INVOICE_SUMMARY V_EXTRACTION_STATUS; do
    COUNT=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.${VIEW}" 2>/dev/null || echo "-1")
    if [[ "$COUNT" -ge 0 ]]; then
        pass "${VIEW} (${COUNT} rows)"
    else
        fail "${VIEW} not found"
    fi
done

echo ""

# ==========================================================================
# 4. Stage checks
# ==========================================================================
echo "--- Stages ---"

STAGE_FILES=$(snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM DIRECTORY(@${POC_DB}.${POC_SCHEMA}.DOCUMENT_STAGE)" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1 | cut -d: -f2 || echo "0")
if [[ "$STAGE_FILES" -ge 1 ]]; then
    pass "DOCUMENT_STAGE (${STAGE_FILES} files)"
else
    warn "DOCUMENT_STAGE is empty"
fi

STREAMLIT_FILES=$(snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM DIRECTORY(@${POC_DB}.${POC_SCHEMA}.STREAMLIT_STAGE)" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1 | cut -d: -f2 || echo "0")
if [[ "$STREAMLIT_FILES" -ge 1 ]]; then
    pass "STREAMLIT_STAGE (${STREAMLIT_FILES} files)"
else
    warn "STREAMLIT_STAGE is empty"
fi

echo ""

# ==========================================================================
# 5. Extraction health
# ==========================================================================
echo "--- Extraction Health ---"

TOTAL=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.RAW_DOCUMENTS")
EXTRACTED=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.RAW_DOCUMENTS WHERE EXTRACTED = TRUE")
PENDING=$((TOTAL - EXTRACTED))
FAILED=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.RAW_DOCUMENTS WHERE EXTRACTION_ERROR IS NOT NULL")

if [[ "$TOTAL" -ge 1 ]]; then pass "Total documents: ${TOTAL}"; else warn "No documents staged"; fi
if [[ "$EXTRACTED" -ge 1 ]]; then pass "Extracted: ${EXTRACTED}"; else warn "No documents extracted"; fi
if [[ "$PENDING" -le 0 ]]; then pass "No pending documents"; else warn "${PENDING} documents pending extraction"; fi
if [[ "$FAILED" -le 0 ]]; then pass "No extraction errors"; else fail "${FAILED} documents have errors"; fi

# Check for NULL raw_extraction (indicates old extraction pipeline)
NULL_RAW=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.EXTRACTED_FIELDS WHERE RAW_EXTRACTION IS NULL")
if [[ "$NULL_RAW" -le 0 ]]; then
    pass "All rows have raw_extraction populated"
else
    warn "${NULL_RAW} rows have NULL raw_extraction (old pipeline)"
fi

echo ""

# ==========================================================================
# 6. Document type config
# ==========================================================================
echo "--- Document Types ---"

DOC_TYPES=$(sql_count "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.DOCUMENT_TYPE_CONFIG WHERE ACTIVE = TRUE")
if [[ "$DOC_TYPES" -ge 1 ]]; then
    pass "Active document types: ${DOC_TYPES}"
else
    fail "No active document types configured"
fi

echo ""

# ==========================================================================
# 7. Automation
# ==========================================================================
echo "--- Automation ---"

TASK_STATE=$(snow sql $CONNECTION_FLAG -q "SELECT STATE FROM ${POC_DB}.INFORMATION_SCHEMA.TASK_HISTORY WHERE TASK_NAME = 'EXTRACT_NEW_DOCUMENTS_TASK' ORDER BY SCHEDULED_TIME DESC LIMIT 1" --format json 2>/dev/null | grep -o '"STATE":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")
PROC_EXISTS=$(snow sql $CONNECTION_FLAG -q "SHOW PROCEDURES LIKE 'SP_EXTRACT_BY_DOC_TYPE' IN ${POC_DB}.${POC_SCHEMA}" --format json 2>/dev/null | grep -c "SP_EXTRACT_BY_DOC_TYPE" || echo "0")

if [[ "$PROC_EXISTS" -ge 1 ]]; then pass "SP_EXTRACT_BY_DOC_TYPE exists"; else fail "Extraction procedure not found"; fi
echo "  INFO  Last task state: ${TASK_STATE}"

echo ""

# ==========================================================================
# Summary
# ==========================================================================
TOTAL_CHECKS=$((PASS_COUNT + FAIL_COUNT + WARN_COUNT))
echo "=============================================="
echo "  Results: ${PASS_COUNT} passed, ${FAIL_COUNT} failed, ${WARN_COUNT} warnings (${TOTAL_CHECKS} checks)"
echo "=============================================="

if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo "  STATUS: UNHEALTHY"
    exit 1
elif [[ "$WARN_COUNT" -gt 0 ]]; then
    echo "  STATUS: DEGRADED (warnings present)"
    exit 0
else
    echo "  STATUS: HEALTHY"
    exit 0
fi
