#!/usr/bin/env bash
# deploy_poc.sh — Deploy the AI_EXTRACT POC kit to a Snowflake account
# Usage: ./poc/deploy_poc.sh [--connection <name>] [--skip-extraction] [--first-run]
#        ./poc/deploy_poc.sh [<connection_name>]
#        POC_CONNECTION=<name> ./poc/deploy_poc.sh
#
# --first-run   Force execution of ACCOUNTADMIN prereqs (01a, 07a, 10_harden).
#               Required on first deploy; skipped automatically on subsequent runs
#               when the connection role is not ACCOUNTADMIN.
set -euo pipefail

# ---------- Parse arguments ----------
_POSITIONAL_CONNECTION=""
SKIP_EXTRACTION="${SKIP_EXTRACTION:-false}"
FIRST_RUN="${FIRST_RUN:-false}"
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
        --first-run)
            FIRST_RUN="true"
            shift
            ;;
        -*)
            echo "Unknown option: $1" >&2
            echo "Usage: ./poc/deploy_poc.sh [--connection <name>] [--skip-extraction] [--first-run]" >&2
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

# ---------- Detect current role ----------
CURRENT_ROLE=$(snow sql $CONNECTION_FLAG -q "SELECT CURRENT_ROLE() AS r" --format json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['R'])" 2>/dev/null || echo "UNKNOWN")
IS_ACCOUNTADMIN=false
if [[ "${CURRENT_ROLE}" == "ACCOUNTADMIN" ]]; then
    IS_ACCOUNTADMIN=true
fi

# ---------- Helper: run a SQL file with env var substitution ----------
_sed_substitute() {
    local sql_file="$1"
    local tmp_file="$2"
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
}

_exec_sql_file() {
    local tmp_file="$1"
    local fatal="${2:-false}"
    python3 -c "
import snowflake.connector, sys, re
conn = snowflake.connector.connect(connection_name='${CONNECTION}')
with open('$tmp_file') as f:
    sql_text = f.read()
cur = conn.cursor()
in_block = False
buf = []
errors = 0
for line in sql_text.splitlines():
    stripped = line.strip()
    if stripped.startswith('--') and not buf:
        continue
    if line.count(chr(36)*2) % 2 == 1:
        in_block = not in_block
    buf.append(line)
    code_part = re.sub(r'--.*', '', stripped).strip()
    if not in_block and code_part.endswith(';'):
        lines_joined = '\n'.join(buf)
        stmt = re.sub(r'--[^\n]*', '', lines_joined)
        stmt = stmt.strip().rstrip(';').strip()
        buf = []
        if not stmt or stmt.startswith('--'):
            continue
        try:
            cur.execute(stmt)
            cur.fetchall()
        except Exception as e:
            errors += 1
            msg = str(e).split('\n')[0]
            print(f'  WARNING: {msg}', file=sys.stderr)
cur.close()
conn.close()
if errors > 0 and '$fatal' == 'true':
    sys.exit(1)
"
}

run_sql() {
    local sql_file="$1"
    local tmp_file
    tmp_file=$(mktemp)
    _sed_substitute "$sql_file" "$tmp_file"
    _exec_sql_file "$tmp_file" "false"
    rm -f "$tmp_file"
}

run_sql_accountadmin() {
    local sql_file="$1"
    if [[ "${IS_ACCOUNTADMIN}" != "true" && "${FIRST_RUN}" != "true" ]]; then
        echo "   SKIP: $(basename "$sql_file") (current role is ${CURRENT_ROLE}, not ACCOUNTADMIN — use --first-run to force)"
        return 0
    fi
    local tmp_file
    tmp_file=$(mktemp)
    _sed_substitute "$sql_file" "$tmp_file"
    _exec_sql_file "$tmp_file" "false"
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
echo "  Current Role:  ${CURRENT_ROLE}"
echo "  First Run:     ${FIRST_RUN}"
echo ""

# ---------- Step 1: Create Snowflake objects ----------
echo "[1/14] Creating database, schema, warehouse, stage..."

run_sql_accountadmin "$SCRIPT_DIR/sql/01a_setup_prereqs.sql"
run_sql "$SCRIPT_DIR/sql/01_setup.sql"

echo "   Infrastructure created."

# ---------- Step 2: Create tables ----------
echo ""
echo "[2/14] Creating tables (RAW_DOCUMENTS, EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA)..."

run_sql "$SCRIPT_DIR/sql/02_tables.sql"

echo "   Tables created."

# ---------- Step 3: Stage sample invoices ----------
echo ""
echo "[3/14] Staging sample PDF invoices..."

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
    echo "[4/14] Skipping batch extraction (--skip-extraction flag set)."
else
    echo ""
    echo "[4/14] Running batch AI_EXTRACT on all staged documents..."
    echo "   (This may take several minutes depending on document count)"

    run_sql "$SCRIPT_DIR/sql/04_batch_extract.sql"

    echo "   Extraction complete."
fi

# ---------- Step 5: Create views ----------
echo ""
echo "[5/14] Creating analytical views..."

run_sql "$SCRIPT_DIR/sql/05_views.sql"

echo "   Views created."

# ---------- Step 6: Set up automation ----------
echo ""
echo "[6/14] Setting up Stream + Task automation..."

run_sql "$SCRIPT_DIR/sql/06_automate.sql"

echo "   Automation configured."

# ---------- Step 7: Deploy Streamlit app ----------
echo ""
echo "[7/14] Deploying POC Streamlit dashboard..."

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

# ACCOUNTADMIN prereqs: compute pool, EAI, grants (warns if insufficient privileges)
run_sql_accountadmin "$SCRIPT_DIR/sql/07a_streamlit_prereqs.sql"

# Create Streamlit app with Container Runtime (runs as AI_EXTRACT_APP)
run_sql "$SCRIPT_DIR/sql/07_deploy_streamlit.sql"

echo "   Dashboard deployed."

# ---------- Step 8: Create writeback table + review view ----------
echo ""
echo "[8/14] Creating writeback table and review view..."

run_sql "$SCRIPT_DIR/sql/08_writeback.sql"

echo "   Writeback table and review view created."

# ---------- Step 9: Create document type config ----------
echo ""
echo "[9/14] Creating document type configuration..."

run_sql "$SCRIPT_DIR/sql/09_document_types.sql"

echo "   Document type config created."

# ---------- Step 10: Production hardening (optional) ----------
if [[ "${POC_HARDEN:-true}" == "true" ]]; then
    echo ""
    echo "[10/14] Applying production hardening..."

    run_sql_accountadmin "$SCRIPT_DIR/sql/10_harden.sql"

    echo "   Hardening applied (ownership → SYSADMIN, managed access, resource monitor)."
else
    echo ""
    echo "[10/14] Skipping hardening (POC_HARDEN=false)."
fi

# ---------- Step 11: Extraction alerts ----------
echo ""
echo "[11/14] Setting up extraction failure alerts..."

run_sql "$SCRIPT_DIR/sql/11_alerts.sql"

echo "   Alerts configured."

# ---------- Step 12: Cost views ----------
echo ""
echo "[12/14] Creating cost summary views..."

run_sql "$SCRIPT_DIR/sql/12_cost_views.sql"

echo "   Cost views created."

# ---------- Step 13: Cost attribution ----------
echo ""
echo "[13/14] Creating per-document cost attribution views..."

run_sql "$SCRIPT_DIR/sql/13_cost_attribution.sql"

echo "   Cost attribution views created."

# ---------- Step 14: Post-deployment validation ----------
echo ""
echo "[Validation] Checking deployment artifacts..."

VALIDATION_PASSED=true

# Check required tables
for TABLE in RAW_DOCUMENTS EXTRACTED_FIELDS EXTRACTED_TABLE_DATA DOCUMENT_TYPE_CONFIG INVOICE_REVIEW; do
    if snow sql $CONNECTION_FLAG -q "SELECT COUNT(*) AS c FROM ${POC_DB}.${POC_SCHEMA}.${TABLE}" --format json 2>/dev/null | grep -qi '"C"'; then
        echo "   OK: Table $TABLE exists"
    else
        echo "   FAIL: Table $TABLE not found or not queryable"
        VALIDATION_PASSED=false
    fi
done

# Check required views
for VIEW in V_DOCUMENT_SUMMARY V_INVOICE_SUMMARY V_AI_EXTRACT_COST_SUMMARY V_AI_EXTRACT_COST_DAILY V_AI_EXTRACT_COST_BY_DOC_TYPE V_AI_EXTRACT_COST_PER_DOCUMENT; do
    if snow sql $CONNECTION_FLAG -q "SELECT 1 FROM ${POC_DB}.${POC_SCHEMA}.${VIEW} LIMIT 0" > /dev/null 2>&1; then
        echo "   OK: View $VIEW exists"
    else
        echo "   FAIL: View $VIEW not found"
        VALIDATION_PASSED=false
    fi
done

# Check Streamlit stage files
for FILE in streamlit_app.py config.py pyproject.toml; do
    if snow sql $CONNECTION_FLAG -q "LIST @${POC_DB}.${POC_SCHEMA}.STREAMLIT_STAGE/${FILE}" > /dev/null 2>&1; then
        echo "   OK: Stage file $FILE uploaded"
    else
        echo "   FAIL: Stage file $FILE missing"
        VALIDATION_PASSED=false
    fi
done

# Check page files
for PAGE in 0_Dashboard.py 1_Document_Viewer.py 2_Analytics.py 3_Review.py 4_Admin.py 5_Cost.py; do
    if snow sql $CONNECTION_FLAG -q "LIST @${POC_DB}.${POC_SCHEMA}.STREAMLIT_STAGE/pages/${PAGE}" > /dev/null 2>&1; then
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
echo "  SELECT COUNT(*) FROM ${POC_DB}.${POC_SCHEMA}.EXTRACTED_TABLE_DATA;"
echo ""