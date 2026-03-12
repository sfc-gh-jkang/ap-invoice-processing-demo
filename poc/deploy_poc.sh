#!/usr/bin/env bash
# deploy_poc.sh — Deploy the AI_EXTRACT POC kit to a Snowflake account
# Usage: ./poc/deploy_poc.sh [--connection <name>] [--skip-extraction]
#        ./poc/deploy_poc.sh [<connection_name>]
#        POC_CONNECTION=<name> ./poc/deploy_poc.sh
set -euo pipefail

# ---------- Parse arguments ----------
_POSITIONAL_CONNECTION=""
SKIP_EXTRACTION="${SKIP_EXTRACTION:-false}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection|-c)
            _POSITIONAL_CONNECTION="$2"
            shift 2
            ;;
        --connection=*)
            _POSITIONAL_CONNECTION="${1#*=}"
            shift
            ;;
        --skip-extraction)
            SKIP_EXTRACTION="true"
            shift
            ;;
        -*)
            echo "Unknown option: $1" >&2
            echo "Usage: ./poc/deploy_poc.sh [--connection <name>] [--skip-extraction]" >&2
            exit 1
            ;;
        *)
            _POSITIONAL_CONNECTION="$1"
            shift
            ;;
    esac
done

# ---------- Config (override via environment variables) ----------
POC_DB="${POC_DB:-AI_EXTRACT_POC}"
POC_SCHEMA="${POC_SCHEMA:-DOCUMENTS}"
POC_WH="${POC_WH:-AI_EXTRACT_WH}"
POC_STAGE="${POC_STAGE:-DOCUMENT_STAGE}"
POC_POOL="${POC_POOL:-AI_EXTRACT_POC_POOL}"
POC_ROLE="${POC_ROLE:-AI_EXTRACT_APP}"
CONNECTION="${POC_CONNECTION:-${_POSITIONAL_CONNECTION:-default}}"
CONNECTION_FLAG="-c $CONNECTION"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------- Helper: run a SQL file with env var substitution ----------
run_sql() {
    local sql_file="$1"
    local tmp_file
    tmp_file=$(mktemp)
    sed \
        -e "s/AI_EXTRACT_APP/${POC_ROLE}/g" \
        -e "s/AI_EXTRACT_POC/${POC_DB}/g" \
        -e "s/AI_EXTRACT_WH/${POC_WH}/g" \
        -e "s/AI_EXTRACT_POC_POOL/${POC_POOL}/g" \
        -e "s/SCHEMA DOCUMENTS;/SCHEMA ${POC_SCHEMA};/g" \
        -e "s/SCHEMA DOCUMENTS /SCHEMA ${POC_SCHEMA} /g" \
        -e "s/\.DOCUMENTS\./\.${POC_SCHEMA}\./g" \
        -e "s/\.DOCUMENTS;/\.${POC_SCHEMA};/g" \
        -e "s/\.DOCUMENTS /\.${POC_SCHEMA} /g" \
        -e "s/'DOCUMENTS'/'${POC_SCHEMA}'/g" \
        "$sql_file" > "$tmp_file"

    if grep -q '\$\$' "$tmp_file"; then
        python3 -c "
import snowflake.connector
conn = snowflake.connector.connect(connection_name='${CONNECTION}')
with open('$tmp_file') as f:
    sql = f.read()
for cur in conn.execute_string(sql, remove_comments=True):
    pass
conn.close()
"
    else
        snow sql $CONNECTION_FLAG -f "$tmp_file"
    fi
    rm -f "$tmp_file"
}

echo "=============================================="
echo " AI_EXTRACT POC Kit — Deploy"
echo "=============================================="
echo "  Database:      ${POC_DB}"
echo "  Schema:        ${POC_SCHEMA}"
echo "  Warehouse:     ${POC_WH}"
echo "  Compute Pool:  ${POC_POOL}"
echo "  Role:          ${POC_ROLE}"
echo "  Connection:    ${CONNECTION}"
echo ""

# ---------- Step 1: Create Snowflake objects ----------
echo "[1/12] Creating database, schema, warehouse, stage..."

run_sql "$SCRIPT_DIR/sql/01_setup.sql"

echo "   Infrastructure created."

# ---------- Step 2: Create tables ----------
echo ""
echo "[2/12] Creating tables (RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA)..."

run_sql "$SCRIPT_DIR/sql/02_tables.sql"

echo "   Tables created."

# ---------- Step 3: Stage sample invoices ----------
echo ""
echo "[3/12] Staging sample PDF invoices..."

# Check multiple locations for sample documents (first match wins)
INVOICE_DIR="$REPO_DIR/data/invoices"
DEMO_INVOICE_DIR="$REPO_DIR/data/demo_invoices"
SAMPLE_DIR="$SCRIPT_DIR/sample_documents"

if [ -d "$INVOICE_DIR" ] && [ "$(ls -A "$INVOICE_DIR" 2>/dev/null)" ]; then
    echo "   Uploading invoices from data/invoices/..."
    snow sql $CONNECTION_FLAG -q "
        USE DATABASE ${POC_DB};
        USE SCHEMA ${POC_SCHEMA};
        PUT file://${INVOICE_DIR}/*.pdf @${POC_STAGE}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE;
    "
elif [ -d "$DEMO_INVOICE_DIR" ] && [ "$(ls -A "$DEMO_INVOICE_DIR" 2>/dev/null)" ]; then
    echo "   Uploading demo invoices from data/demo_invoices/..."
    snow sql $CONNECTION_FLAG -q "
        USE DATABASE ${POC_DB};
        USE SCHEMA ${POC_SCHEMA};
        PUT file://${DEMO_INVOICE_DIR}/*.pdf @${POC_STAGE}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE;
    "
elif [ -d "$SAMPLE_DIR" ] && [ "$(ls -A "$SAMPLE_DIR" 2>/dev/null)" ]; then
    echo "   Uploading sample invoices from poc/sample_documents/..."
    snow sql $CONNECTION_FLAG -q "
        USE DATABASE ${POC_DB};
        USE SCHEMA ${POC_SCHEMA};
        PUT file://${SAMPLE_DIR}/*.pdf @${POC_STAGE}
            AUTO_COMPRESS = FALSE
            OVERWRITE = TRUE;
    "
else
    echo "   WARNING: No sample PDFs found."
    echo "   Generate them: cd poc && python3 generate_sample_docs.py"
    echo "   Or upload your own documents to @${POC_DB}.${POC_SCHEMA}.${POC_STAGE}"
fi

# Upload multi-type docs (contracts, receipts, utility bills) if they exist in sample_documents
MULTI_TYPE_DIR="$SCRIPT_DIR/sample_documents"
MULTI_COUNT=$(ls "$MULTI_TYPE_DIR"/{contract,receipt,utility_bill,lease}_*.pdf 2>/dev/null | wc -l | tr -d ' ')
if [ "$MULTI_COUNT" -gt 0 ]; then
    echo "   Uploading $MULTI_COUNT multi-type documents (contracts, receipts, utility bills, leases)..."
    for prefix in contract receipt utility_bill lease; do
        if ls "$MULTI_TYPE_DIR"/${prefix}_*.pdf 1>/dev/null 2>&1; then
            snow sql $CONNECTION_FLAG -q "
                USE DATABASE ${POC_DB};
                USE SCHEMA ${POC_SCHEMA};
                PUT file://${MULTI_TYPE_DIR}/${prefix}_*.pdf @${POC_STAGE}
                    AUTO_COMPRESS = FALSE
                    OVERWRITE = TRUE;
            "
        fi
    done
fi

# Refresh stage directory
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${POC_DB};
    USE SCHEMA ${POC_SCHEMA};
    ALTER STAGE ${POC_STAGE} REFRESH;
"

# Re-run 02_tables.sql to register newly staged files into RAW_DOCUMENTS
run_sql "$SCRIPT_DIR/sql/02_tables.sql"

echo "   Files staged and registered."

# ---------- Step 4: Batch extraction ----------
if [[ "${SKIP_EXTRACTION}" == "true" ]]; then
    echo ""
    echo "[4/12] Skipping batch extraction (--skip-extraction flag set)."
else
    echo ""
    echo "[4/12] Running batch AI_EXTRACT on all staged documents..."
    echo "   (This may take several minutes depending on document count)"

    run_sql "$SCRIPT_DIR/sql/04_batch_extract.sql"

    echo "   Extraction complete."
fi

# ---------- Step 5: Create views ----------
echo ""
echo "[5/12] Creating analytical views..."

run_sql "$SCRIPT_DIR/sql/05_views.sql"

echo "   Views created."

# ---------- Step 6: Set up automation ----------
echo ""
echo "[6/12] Setting up Stream + Task automation..."

run_sql "$SCRIPT_DIR/sql/06_automate.sql"

echo "   Automation configured."

# ---------- Step 7: Deploy Streamlit app ----------
echo ""
echo "[7/12] Deploying POC Streamlit dashboard..."

# Upload Streamlit files
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${POC_DB};
    USE SCHEMA ${POC_SCHEMA};
    CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
        DIRECTORY = (ENABLE = TRUE)
        COMMENT = 'Stage for Streamlit app files';
    PUT file://${SCRIPT_DIR}/streamlit/streamlit_app.py @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/config.py @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pyproject.toml @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/environment.yml @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/0_Dashboard.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/1_Document_Viewer.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/2_Analytics.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/3_Review.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/4_Admin.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/5_Cost.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
"

# Create compute pool + Streamlit app (uses dollar-quoted blocks handled by run_sql)
run_sql "$SCRIPT_DIR/sql/07_deploy_streamlit.sql"

echo "   Dashboard deployed."

# ---------- Step 8: Create writeback table + review view ----------
echo ""
echo "[8/12] Creating writeback table and review view..."

run_sql "$SCRIPT_DIR/sql/08_writeback.sql"

echo "   Writeback table and review view created."

# ---------- Step 9: Create document type config ----------
echo ""
echo "[9/12] Creating document type configuration..."

run_sql "$SCRIPT_DIR/sql/09_document_types.sql"

echo "   Document type config created."

# ---------- Step 10: Production hardening (optional) ----------
if [[ "${POC_HARDEN:-true}" == "true" ]]; then
    echo ""
    echo "[10/12] Applying production hardening..."

    run_sql "$SCRIPT_DIR/sql/10_harden.sql"

    echo "   Hardening applied (ownership → SYSADMIN, managed access, resource monitor)."
else
    echo ""
    echo "[10/12] Skipping hardening (POC_HARDEN=false)."
fi

# ---------- Step 11: Extraction alerts ----------
echo ""
echo "[11/12] Setting up extraction failure alerts..."

run_sql "$SCRIPT_DIR/sql/11_alerts.sql"

echo "   Alerts configured."

# ---------- Post-deployment validation ----------
echo ""
echo "[Validation] Checking deployment artifacts..."

VALIDATION_PASSED=true

# Check required tables
for TABLE in RAW_DOCUMENTS EXTRACTED_FIELDS EXTRACTED_TABLE_DATA DOCUMENT_TYPE_CONFIG INVOICE_REVIEW; do
    COUNT=$(snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.${TABLE}" --format json 2>/dev/null | grep -o '"c":[0-9]*' | head -1)
    if [ -z "$COUNT" ]; then
        echo "   FAIL: Table $TABLE not found or not queryable"
        VALIDATION_PASSED=false
    else
        echo "   OK: Table $TABLE exists"
    fi
done

# Check required views
for VIEW in V_DOCUMENT_SUMMARY V_INVOICE_SUMMARY V_AI_EXTRACT_COST_SUMMARY; do
    snow sql $CONNECTION_FLAG -q "SELECT 1 FROM ${POC_DB}.${POC_SCHEMA}.${VIEW} LIMIT 0" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   OK: View $VIEW exists"
    else
        echo "   FAIL: View $VIEW not found"
        VALIDATION_PASSED=false
    fi
done

# Check Streamlit stage files
for FILE in streamlit_app.py config.py pyproject.toml environment.yml; do
    snow sql $CONNECTION_FLAG -q "LIST @${POC_DB}.${POC_SCHEMA}.STREAMLIT_STAGE/${FILE}" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   OK: Stage file $FILE uploaded"
    else
        echo "   FAIL: Stage file $FILE missing"
        VALIDATION_PASSED=false
    fi
done

# Check page files
for PAGE in 0_Dashboard.py 1_Document_Viewer.py 2_Analytics.py 3_Review.py 4_Admin.py; do
    snow sql $CONNECTION_FLAG -q "LIST @${POC_DB}.${POC_SCHEMA}.STREAMLIT_STAGE/pages/${PAGE}" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "   OK: Page $PAGE uploaded"
    else
        echo "   FAIL: Page $PAGE missing"
        VALIDATION_PASSED=false
    fi
done

echo ""
echo "=============================================="
if [ "$VALIDATION_PASSED" = true ]; then
    echo " POC Deploy Complete! All checks passed."
else
    echo " POC Deploy Complete (with warnings)."
    echo " Some validation checks failed — review output above."
fi
echo "=============================================="
echo ""
echo "Open Snowsight and navigate to:"
echo "  Streamlit > ${POC_DB}.${POC_SCHEMA}.AI_EXTRACT_DASHBOARD"
echo ""
echo "Verify extraction results:"
echo "  SELECT COUNT(*) FROM ${POC_DB}.${POC_SCHEMA}.EXTRACTED_FIELDS;"
echo "  SELECT COUNT(*) F                                                                                                                               