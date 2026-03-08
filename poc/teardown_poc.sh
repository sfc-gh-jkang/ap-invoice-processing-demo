#!/usr/bin/env bash
# =============================================================================
# teardown_poc.sh — Remove all AI_EXTRACT POC objects from Snowflake
#
# Usage:
#   ./teardown_poc.sh                          # uses default connection
#   ./teardown_poc.sh --connection my_account   # specify connection
#
# Override object names via environment variables:
#   POC_DB=MY_DB POC_WH=MY_WH ./teardown_poc.sh
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

# ---------- Configurable names (match deploy_poc.sh defaults) ----------
POC_DB="${POC_DB:-AI_EXTRACT_POC}"
POC_SCHEMA="${POC_SCHEMA:-DOCUMENTS}"
POC_WH="${POC_WH:-AI_EXTRACT_WH}"
POC_POOL="${POC_POOL:-AI_EXTRACT_POC_POOL}"
POC_ROLE="${POC_ROLE:-AI_EXTRACT_APP}"

echo ""
echo "=============================================="
echo "  AI_EXTRACT POC — TEARDOWN"
echo "=============================================="
echo "  Database:      ${POC_DB}"
echo "  Schema:        ${POC_SCHEMA}"
echo "  Warehouse:     ${POC_WH}"
echo "  Compute Pool:  ${POC_POOL}"
echo "  Role:          ${POC_ROLE}"
echo "  Connection:    ${CONNECTION}"
echo ""

# ---------- Confirm ----------
read -r -p "This will DROP all POC objects. Continue? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""

# ---------- Step 1: Suspend automation ----------
echo "[1/5] Suspending automation task..."
snow sql $CONNECTION_FLAG -q "
    ALTER TASK IF EXISTS ${POC_DB}.${POC_SCHEMA}.EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;
" 2>/dev/null || true
echo "   Done."

# ---------- Step 2: Drop database ----------
echo "[2/5] Dropping database ${POC_DB}..."
snow sql $CONNECTION_FLAG -q "DROP DATABASE IF EXISTS ${POC_DB};" 2>/dev/null
echo "   Done."

# ---------- Step 3: Drop warehouse ----------
echo "[3/5] Dropping warehouse ${POC_WH}..."
snow sql $CONNECTION_FLAG -q "DROP WAREHOUSE IF EXISTS ${POC_WH};" 2>/dev/null
echo "   Done."

# ---------- Step 4: Drop compute pool ----------
echo "[4/5] Dropping compute pool ${POC_POOL}..."
snow sql $CONNECTION_FLAG -q "DROP COMPUTE POOL IF EXISTS ${POC_POOL};" 2>/dev/null || true
echo "   Done."

# ---------- Step 5: Drop role ----------
echo "[5/5] Dropping role ${POC_ROLE}..."
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    DROP ROLE IF EXISTS ${POC_ROLE};
" 2>/dev/null
echo "   Done."

# ---------- Verify ----------
echo ""
echo "Verifying cleanup..."
DB_COUNT=$(snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = '${POC_DB}'" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1 | cut -d: -f2 || echo "0")
WH_COUNT=$(snow sql $CONNECTION_FLAG -q "SHOW WAREHOUSES LIKE '${POC_WH}'" --format json 2>/dev/null | grep -c "${POC_WH}" || echo "0")

if [[ "$DB_COUNT" == "0" ]] && [[ "$WH_COUNT" == "0" ]]; then
    echo "   All POC objects removed successfully."
else
    echo "   WARNING: Some objects may still exist. Check manually."
fi

echo ""
echo "Teardown complete."
