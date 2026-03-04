#!/usr/bin/env bash
# deploy.sh — Full deployment: generate PDFs, create Snowflake objects, stage files, run extraction, deploy Streamlit
# Usage: ./deploy.sh [--connection <name>]
set -euo pipefail

# ---------- Config (override via environment variables) ----------
DB="${AP_DB:-AP_DEMO_DB}"
SCHEMA="${AP_SCHEMA:-AP}"
WH="${AP_WAREHOUSE:-AP_DEMO_WH}"
COMPUTE_POOL="${AP_COMPUTE_POOL:-AP_DEMO_POOL}"
CONNECTION="${AP_CONNECTION:-${1:-aws_spcs}}"
CONNECTION_FLAG="-c $CONNECTION"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ---------- Helper: run a SQL file with env var substitution ----------
# Replaces hardcoded defaults in SQL files with the user's chosen names.
# Files containing $$ (dollar-quoted blocks) use Python connector instead of
# snow sql, which has template rendering issues with $$.
run_sql() {
    local sql_file="$1"
    local tmp_file
    tmp_file=$(mktemp)
    sed \
        -e "s/AP_DEMO_DB/${DB}/g" \
        -e "s/AP_DEMO_WH/${WH}/g" \
        -e "s/AP_DEMO_POOL/${COMPUTE_POOL}/g" \
        -e "s/SCHEMA AP;/SCHEMA ${SCHEMA};/g" \
        -e "s/SCHEMA AP /SCHEMA ${SCHEMA} /g" \
        -e "s/\.AP\./\.${SCHEMA}\./g" \
        -e "s/\.AP;/\.${SCHEMA};/g" \
        -e "s/\.AP /\.${SCHEMA} /g" \
        -e "s/_SCHEMA = 'AP'/_SCHEMA = '${SCHEMA}'/g" \
        -e "s/SCHEMA_NAME = 'AP'/SCHEMA_NAME = '${SCHEMA}'/g" \
        "$sql_file" > "$tmp_file"

    if grep -q '\$\$' "$tmp_file"; then
        # Dollar-quoted blocks break snow sql template rendering — use Python
        python3 -c "
import snowflake.connector
conn = snowflake.connector.connect(connection_name='${CONNECTION}')
with open('$tmp_file') as f:
    sql = f.read()
for cur in conn.execute_string(sql):
    pass
conn.close()
"
    else
        snow sql $CONNECTION_FLAG -f "$tmp_file"
    fi
    rm -f "$tmp_file"
}

echo "=============================================="
echo " AP Invoice Processing Demo — Deploy"
echo "=============================================="
echo "  Database:     ${DB}"
echo "  Schema:       ${SCHEMA}"
echo "  Warehouse:    ${WH}"
echo "  Compute Pool: ${COMPUTE_POOL}"
echo "  Connection:   ${CONNECTION}"

# ---------- Step 1: Generate invoices ----------
echo ""
echo "[1/6] Generating PDF invoices..."

if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Please install Python 3.11+."
    exit 1
fi

# Install reportlab if needed
python3 -c "import reportlab" 2>/dev/null || pip3 install reportlab --quiet

python3 data/generate_invoices.py

echo "   Created 100 invoices in data/invoices/"
echo "   Created 5 demo invoices in data/demo_invoices/"

# ---------- Step 2: Create Snowflake objects ----------
echo ""
echo "[2/6] Creating Snowflake objects (DB, schema, warehouse, stage, compute pool)..."

run_sql sql/01_setup.sql

echo "   Objects created."

# ---------- Step 3: Create tables ----------
echo ""
echo "[3/6] Creating tables and seeding vendor data..."

run_sql sql/02_tables.sql

echo "   Tables created."

# ---------- Step 4: Stage PDF files ----------
echo ""
echo "[4/6] Staging PDF invoices..."

# Stage the 100 initial invoices
echo "   Uploading 100 initial invoices..."
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${DB};
    USE SCHEMA ${SCHEMA};
    PUT file://${SCRIPT_DIR}/data/invoices/*.pdf @INVOICE_STAGE
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE;
"

# Stage the 5 demo invoices (in the same stage, with demo_ prefix)
echo "   Uploading 5 demo invoices..."
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${DB};
    USE SCHEMA ${SCHEMA};
    PUT file://${SCRIPT_DIR}/data/demo_invoices/*.pdf @INVOICE_STAGE
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE;
"

# Refresh stage directory
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${DB};
    USE SCHEMA ${SCHEMA};
    ALTER STAGE INVOICE_STAGE REFRESH;
"

echo "   All files staged."

# ---------- Step 5: Run batch extraction ----------
echo ""
echo "[5/6] Running batch AI_EXTRACT on initial 100 invoices..."
echo "   (This may take several minutes depending on warehouse size)"

run_sql sql/03_extract.sql

echo "   Extraction complete."

# Create task and stream for new files
echo "   Setting up automated extraction task..."
run_sql sql/04_task.sql

# Create analytical views
echo "   Creating analytical views..."
run_sql sql/05_views.sql

echo "   Views created."

# Create invoice generation UDTF + stored proc
echo "   Creating invoice generation UDTF..."
run_sql sql/07_generate_udf.sql

echo "   Invoice generator ready."

# ---------- Step 6: Deploy Streamlit app ----------
echo ""
echo "[6/6] Deploying Streamlit in Snowflake app..."

# Upload Streamlit files to stage
snow sql $CONNECTION_FLAG -q "
    USE DATABASE ${DB};
    USE SCHEMA ${SCHEMA};
    PUT file://${SCRIPT_DIR}/streamlit/streamlit_app.py @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/config.py @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pyproject.toml @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/environment.yml @STREAMLIT_STAGE/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/1_AP_Ledger.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/2_Analytics.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/3_Process_New.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/0_Dashboard.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
    PUT file://${SCRIPT_DIR}/streamlit/pages/4_AI_Extract_Lab.py @STREAMLIT_STAGE/pages/
        AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
"

# Create the Streamlit app on container runtime
snow sql $CONNECTION_FLAG -q "
    USE ROLE ACCOUNTADMIN;
    USE DATABASE ${DB};
    USE SCHEMA ${SCHEMA};

    CREATE OR REPLACE STREAMLIT AP_INVOICE_APP
        FROM '@STREAMLIT_STAGE'
        MAIN_FILE = 'streamlit_app.py'
        RUNTIME_NAME = 'SYSTEM\$ST_CONTAINER_RUNTIME_PY3_11'
        COMPUTE_POOL = ${COMPUTE_POOL}
        QUERY_WAREHOUSE = ${WH}
        EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION)
        TITLE = 'AP Invoice Processing Demo'
        COMMENT = 'Convenience Store AP Invoice Processing Demo';

    GRANT USAGE ON STREAMLIT AP_INVOICE_APP TO ROLE PUBLIC;
"

echo ""
echo "=============================================="
echo " Deploy complete!"
echo "=============================================="
echo ""
echo "Open Snowsight and navigate to:"
echo "  Streamlit > ${DB}.${SCHEMA}.AP_INVOICE_APP"
echo ""
echo "Demo flow:"
echo "  1. Explore the KPI overview (main page)"
echo "  2. Browse the AP Ledger with aging buckets"
echo "  3. View Analytics (vendor spend, trends, categories)"
echo "  4. Go to 'Process New Invoices' page for live extraction demo"
echo ""
echo "The 5 demo invoices are already staged but NOT yet registered."
echo "Use the 'Process New Invoices' page in the app to demo live extraction."
echo ""
