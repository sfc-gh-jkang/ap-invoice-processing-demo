# AI_EXTRACT POC Kit

Extract structured data from your own PDFs, images, and documents using **Snowflake Cortex AI_EXTRACT** — entirely within your Snowflake account. No external services, no API keys, no data leaves Snowflake.

This kit walks you through a complete proof-of-concept:

1. **Stage** your documents in a Snowflake internal stage
2. **Extract** entity fields (header data) and tabular data (line items) using AI
3. **Visualize** results in an interactive Streamlit dashboard
4. **Automate** extraction of new documents via Stream + Task (optional)

**Total setup time: ~30 minutes** (excluding extraction runtime, which depends on document count).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [What You Need Before Starting](#what-you-need-before-starting)
- [Step-by-Step Guide](#step-by-step-guide)
- [Customizing for Your Document Type](#customizing-for-your-document-type)
- [Understanding the Extraction Output](#understanding-the-extraction-output)
- [Deploying the Dashboard](#deploying-the-dashboard)
- [Setting Up Automation](#setting-up-automation)
- [File Structure](#file-structure)
- [Troubleshooting](#troubleshooting)
- [Cost Estimate](#cost-estimate)
- [Cleanup](#cleanup)
- [Validating the Deployment (Tests)](#validating-the-deployment-tests)
- [Using Your Own Documents](#using-your-own-documents)
- [Next Steps](#next-steps)

---

## Prerequisites

### Account Requirements

| Requirement | Detail |
|---|---|
| **Snowflake Edition** | Standard or higher |
| **Account Region** | Must be a [supported region](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-extract#regional-availability), or enable cross-region inference (see below) |
| **Role** | ACCOUNTADMIN for initial setup; SYSADMIN (or custom role) for day-to-day use |
| **Cortex Access** | SNOWFLAKE.CORTEX_USER database role granted to your working role |

**Supported AI_EXTRACT Regions:**

| Cloud | Regions |
|---|---|
| AWS | US West 2 (Oregon), US East 1 (Virginia), CA Central 1, EU Central 1 (Frankfurt), EU West 1 (Ireland), SA East 1, AP Northeast 1 (Tokyo), AP Southeast 2 (Sydney) |
| Azure | East US 2, West US 2, South Central US, North Europe, West Europe, Central India, Japan East, Southeast Asia, Australia East |

If your account is **not** in a supported region, run this as ACCOUNTADMIN:
```sql
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

> **Note:** `01_setup.sql` (and `deploy_poc.sh`) now run this command automatically, so you don't need to do it manually unless running scripts individually.

### Supported File Types

PDF, PNG, JPEG/JPG, DOCX/DOC, PPTX/PPT, EML, HTML/HTM, TXT/TEXT, TIF/TIFF, BMP, GIF, WEBP, MD

### File Limits

| Limit | Value |
|---|---|
| Max pages per document | 125 |
| Max file size | 100 MB |
| Max entity questions per AI_EXTRACT call | 100 |
| Max table questions per AI_EXTRACT call | 10 |
| 1 table question counts as | 10 entity questions |
| Max output per entity question | 512 tokens |
| Max output per table question | 4,096 tokens |

---

## What You Need Before Starting

1. **5-20 sample documents** of the type you want to extract from (invoices, contracts, receipts, medical claims, etc.)
2. **A list of fields** you want to extract from each document. For example:
   - Invoices: vendor name, invoice number, date, due date, total amount, line items
   - Contracts: parties, effective date, expiration date, contract value, governing law
   - Receipts: store name, date, total, payment method, items purchased
3. **Access to Snowsight** (the Snowflake web UI) — all SQL scripts are designed to be run there

---

## Step-by-Step Guide

### Step 1: Create Snowflake Objects

Open **`sql/01_setup.sql`** in a Snowsight worksheet.

**Edit the 4 variables at the top** to your preferred names (or keep the defaults):

```sql
SET poc_db        = 'AI_EXTRACT_POC';     -- database name
SET poc_schema    = 'DOCUMENTS';          -- schema name
SET poc_warehouse = 'AI_EXTRACT_WH';      -- warehouse name
SET poc_stage     = 'DOCUMENT_STAGE';     -- stage name
```

**Edit the role grant** on line 59 — replace `SYSADMIN` with the role you'll use:

```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN;  -- <-- your role here
```

Run the entire script. This creates:
- A database and schema
- An X-SMALL warehouse (optimal for AI_EXTRACT — larger sizes don't improve performance)
- An internal stage with **SNOWFLAKE_SSE encryption** (required for AI_EXTRACT)

> **IMPORTANT:** The stage MUST use `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')`. The default encryption (client-side) will **not** work with AI_EXTRACT, and you **cannot** change encryption after stage creation. Script 01 handles this correctly.

### Step 2: Upload Your Documents

Upload your sample documents to the `DOCUMENT_STAGE`. Choose one method:

**Option A — Snowsight UI (easiest):**
1. Navigate to **Data > Databases > AI_EXTRACT_POC > DOCUMENTS > Stages**
2. Click on **DOCUMENT_STAGE**
3. Click the **"+ Files"** button
4. Drag and drop your files

**Option B — Snowflake CLI:**
```bash
snow stage copy /path/to/your/documents/*.pdf @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE --overwrite
```

**Option C — SnowSQL:**
```sql
PUT file:///path/to/your/documents/*.pdf @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE AUTO_COMPRESS=FALSE;
```

After uploading, refresh the stage directory and verify:
```sql
ALTER STAGE DOCUMENT_STAGE REFRESH;
SELECT * FROM DIRECTORY(@DOCUMENT_STAGE) ORDER BY LAST_MODIFIED DESC;
```

You should see your files listed with their sizes and timestamps.

### Step 3: Create Tables

Open **`sql/02_tables.sql`** in Snowsight and run it.

This creates three tables:

| Table | Purpose |
|---|---|
| `RAW_DOCUMENTS` | Tracks every file staged for processing (file name, path, extraction status) |
| `EXTRACTED_FIELDS` | Stores entity-level data from each document (10 generic fields: `field_1` through `field_10`) |
| `EXTRACTED_TABLE_DATA` | Stores tabular/line-item data from each document (5 generic columns: `col_1` through `col_5`) |

The script also registers all staged files into `RAW_DOCUMENTS` automatically.

> **TIP:** You can rename the generic columns (`field_1`, `col_1`, etc.) to meaningful names for your document type. See [Customizing for Your Document Type](#customizing-for-your-document-type).

### Step 4: Test on ONE File First

This is the most important step. **Do not skip it.**

Open **`sql/03_test_single_file.sql`** in Snowsight.

1. Run the first query to list your staged files
2. Set `$test_file` to one of your file names:
   ```sql
   SET test_file = 'invoice_001.pdf';  -- <-- your actual filename
   ```
3. Run the **entity extraction** query (Step 2 in the script). This sends your document to AI_EXTRACT with 10 questions about header-level fields.

**Review the raw JSON output carefully:**

```json
{
  "response": {
    "vendor_name": "Acme Corp",
    "document_number": "INV-2024-001",
    "document_date": "2024-03-15",
    "total": "1234.56"
  }
}
```

**Check:**
- Are the field values correct (match what's on the document)?
- Are dates in `YYYY-MM-DD` format?
- Are numbers clean (no `$`, commas, or currency symbols)?
- Did any fields return `null` that shouldn't be empty?

4. Run the **table extraction** query (Step 3 in the script). This extracts line items / tabular data.

5. If the output looks wrong, **adjust your prompts** and re-run. Common fixes:
   - Too vague? Be more specific: `'date'` → `'What is the invoice date? Return in YYYY-MM-DD format.'`
   - Numbers have `$`? Add: `'Return as a number only.'`
   - Wrong table found? Add a `description` field to help locate the right table

**Only proceed to batch extraction once you're satisfied with the single-file results.**

### Step 5: Batch Extract All Documents

Open **`sql/04_batch_extract.sql`** in Snowsight.

> **IMPORTANT:** If you modified the prompts in Step 4, copy those same prompts into this script. The prompts in `04_batch_extract.sql` must match what you validated in `03_test_single_file.sql`.

Run the entire script. This:

1. **Entity extraction** — Runs AI_EXTRACT on every unprocessed file, inserting results into `EXTRACTED_FIELDS`
2. **Table extraction** — Runs AI_EXTRACT again for tabular data, inserting into `EXTRACTED_TABLE_DATA`
3. **Marks files as extracted** — Updates `RAW_DOCUMENTS` so files aren't reprocessed

**Runtime:** Depends on document count and page count. A batch of 100 single-page invoices typically completes in 5-10 minutes on an X-SMALL warehouse.

After completion, verify:
```sql
SELECT COUNT(*) AS documents_extracted FROM EXTRACTED_FIELDS;
SELECT COUNT(*) AS line_items_extracted FROM EXTRACTED_TABLE_DATA;
SELECT * FROM EXTRACTED_FIELDS ORDER BY extracted_at DESC LIMIT 10;
```

### Step 6: Create Analytical Views

Open **`sql/05_views.sql`** in Snowsight and run it.

This creates 6 views that power the dashboard and enable ad-hoc analysis:

| View | Purpose |
|---|---|
| `V_EXTRACTION_STATUS` | Pipeline monitoring — total, extracted, pending, failed counts |
| `V_DOCUMENT_LEDGER` | Enriched document view with aging buckets (days past due) |
| `V_SUMMARY_BY_VENDOR` | Aggregates grouped by sender/vendor |
| `V_MONTHLY_TREND` | Volume and value over time |
| `V_TOP_LINE_ITEMS` | Most common / highest-spend items from table extraction |
| `V_AGING_SUMMARY` | Aggregate counts and amounts by aging bucket |

You can query these views directly:
```sql
-- How much do we owe each vendor?
SELECT * FROM V_SUMMARY_BY_VENDOR;

-- What's overdue?
SELECT * FROM V_DOCUMENT_LEDGER WHERE aging_bucket != 'Current' ORDER BY days_past_due DESC;

-- Monthly trend
SELECT * FROM V_MONTHLY_TREND;
```

---

## Customizing for Your Document Type

The default prompts extract **invoice** fields, but the kit works with any document type. Here's how to adapt it:

### 1. Edit Prompts (`sql/03_test_single_file.sql`)

The script includes commented-out examples for contracts and receipts. Replace the invoice prompts with questions relevant to your documents:

**Contract example:**
```sql
SELECT AI_EXTRACT(
    TO_FILE('@DOCUMENT_STAGE', $test_file),
    {
        'party_a':        'Who is the first party or client in this contract?',
        'party_b':        'Who is the second party or service provider?',
        'effective_date': 'What is the effective date? Return in YYYY-MM-DD format.',
        'expiration':     'What is the expiration or end date? Return in YYYY-MM-DD format.',
        'contract_value': 'What is the total contract value? Return as a number only.',
        'governing_law':  'What state or jurisdiction governs this contract?',
        'auto_renew':     'Does this contract auto-renew? Return YES or NO.'
    }
) AS extraction;
```

**Receipt example:**
```sql
SELECT AI_EXTRACT(
    TO_FILE('@DOCUMENT_STAGE', $test_file),
    {
        'store_name':      'What is the store or merchant name?',
        'receipt_number':  'What is the receipt or transaction number?',
        'date':            'What is the transaction date? Return in YYYY-MM-DD format.',
        'total':           'What is the total amount paid? Return as a number only.',
        'payment_method':  'What payment method was used (cash, credit card, etc.)?'
    }
) AS extraction;
```

### 2. Rename Table Columns (`sql/02_tables.sql`)

Map the generic columns to meaningful names:

```
field_1  → vendor_name (or party_a, store_name)
field_2  → invoice_number (or contract_id, receipt_number)
field_3  → po_number (or reference_number)
field_4  → document_date (DATE type)
field_5  → due_date (DATE type)
field_6  → payment_terms (or contract_type)
field_7  → bill_to (or recipient)
field_8  → subtotal (NUMBER 12,2)
field_9  → tax_amount (NUMBER 12,2)
field_10 → total_amount (NUMBER 12,2)
```

### 3. Update Batch Extraction (`sql/04_batch_extract.sql`)

Copy your validated prompts from Step 4 into the `AI_EXTRACT` calls in this script. The prompt keys must match exactly.

### 4. Update View Aliases (`sql/05_views.sql`)

The views reference `field_1` as `vendor_name`, `field_10` as `total_amount`, etc. Update the aliases to match your renamed columns.

### Prompt Engineering Tips

| Tip | Bad | Good |
|---|---|---|
| Be specific about which field | `'date'` | `'What is the invoice date? Return in YYYY-MM-DD format.'` |
| Request clean numbers | `'total'` | `'What is the total amount? Return as a number only.'` |
| Disambiguate tables | `'items'` | `'The table of line items showing product, quantity, and price'` |
| Handle missing fields | (no guidance) | `'What is the PO number? Return NULL if not present.'` |
| Format dates consistently | `'due date'` | `'What is the due date? Return in YYYY-MM-DD format. Return NULL if not present.'` |

---

## Understanding the Extraction Output

### Entity Extraction Response

AI_EXTRACT returns a JSON object with a `response` key:

```json
{
  "response": {
    "vendor_name": "Acme Corp",
    "document_number": "INV-2024-001",
    "document_date": "2024-03-15",
    "due_date": "2024-04-14",
    "total": "1234.56"
  }
}
```

Access individual fields with Snowflake's semi-structured syntax:
```sql
extraction:response:vendor_name::VARCHAR
TRY_TO_DATE(extraction:response:document_date::VARCHAR)
TRY_TO_NUMBER(REGEXP_REPLACE(extraction:response:total::VARCHAR, '[^0-9.]', ''), 12, 2)
```

The `TRY_TO_NUMBER` with `REGEXP_REPLACE` pattern strips any stray `$`, `,`, or whitespace before casting — this prevents failures on slightly messy outputs.

### Table Extraction Response

Table extraction returns parallel arrays:

```json
{
  "response": {
    "line_items": {
      "Line": [1, 2, 3],
      "Description": ["Widget A", "Widget B", "Shipping"],
      "Qty": [10, 5, 1],
      "Unit Price": [9.99, 14.99, 0],
      "Total": [99.90, 74.95, 12.50]
    }
  }
}
```

The `LATERAL FLATTEN` pattern in `04_batch_extract.sql` unnests these arrays into rows. The `WHERE ln.index = pr.index AND ...` clause ensures columns stay aligned:

```sql
FROM extracted e,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Line)        ln,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description) pr,
    LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Qty)         qt
WHERE ln.index = pr.index
  AND ln.index = qt.index;
```

> **Never remove the index alignment conditions.** Without them, you get a cross join of all array elements.

---

## Deploying the Dashboard

The `streamlit/` folder contains a 3-page Streamlit in Snowflake app. Deployment is optional but gives you an interactive way to explore results.

### Prerequisites for Dashboard

- A **compute pool** (created by `07_deploy_streamlit.sql`)
- An **External Access Integration** for PyPI (so Container Runtime can install `plotly` and `pypdfium2`)
- ACCOUNTADMIN role for creating the EAI

### Deploy Steps

1. **Upload Streamlit files** to the Streamlit stage. In Snowsight or via CLI:

   ```bash
   # Snowflake CLI
   snow stage copy streamlit/streamlit_app.py @STREAMLIT_STAGE/ --overwrite
   snow stage copy streamlit/config.py       @STREAMLIT_STAGE/ --overwrite
   snow stage copy streamlit/environment.yml @STREAMLIT_STAGE/ --overwrite
   snow stage copy streamlit/pages/          @STREAMLIT_STAGE/pages/ --overwrite
   ```

2. **Run `sql/07_deploy_streamlit.sql`** in Snowsight. This creates the compute pool, EAI, and Streamlit app.

3. **Edit role references** in `07_deploy_streamlit.sql`:
   - Line 63: `USE ROLE ACCOUNTADMIN;` — needed for creating the EAI
   - Line 76: `USE ROLE ACCOUNTADMIN;` — change to the role that owns `AI_EXTRACT_POC`

4. **Open the app**: Navigate to **Snowsight > Projects > Streamlit > AI_EXTRACT_DASHBOARD**

### Dashboard Pages

| Page | What It Shows |
|---|---|
| **Landing** | Pipeline status (total/extracted/pending/failed), architecture diagram, extraction summary |
| **Dashboard** | KPI cards (total documents, total amount, unique senders, overdue), recent documents table |
| **Document Viewer** | Filter by sender/status, browse documents, drill down to see extracted fields alongside the rendered source PDF |
| **Analytics** | Bar chart by sender, monthly trend area chart, aging distribution, top 20 line items |

---

## Setting Up Automation

After validating that batch extraction works, you can automate processing of newly staged documents.

Open **`sql/06_automate.sql`** in Snowsight and run it. This creates:

| Object | Purpose |
|---|---|
| `RAW_DOCUMENTS_STREAM` | Append-only stream that detects new rows in `RAW_DOCUMENTS` |
| `SP_EXTRACT_NEW_DOCUMENTS` | Stored procedure that runs entity + table extraction on unprocessed files |
| `EXTRACT_NEW_DOCUMENTS_TASK` | Scheduled task that runs every 5 minutes when the stream has data |

**How automation works:**
1. You upload a new document to `DOCUMENT_STAGE`
2. You register it in `RAW_DOCUMENTS` (or the INSERT from `02_tables.sql` does this)
3. The stream detects the new row
4. The task fires within 5 minutes and calls the stored procedure
5. The procedure extracts fields and line items, then marks the file as processed

**To test manually** (without waiting 5 minutes):
```sql
EXECUTE TASK EXTRACT_NEW_DOCUMENTS_TASK;
```

**To pause automation:**
```sql
ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK SUSPEND;
```

---

## File Structure

```
ai_extract_poc/
├── README.md                              # This guide
├── deploy_poc.sh                          # Automated deploy script (optional)
├── teardown_poc.sql                       # Drop all POC objects
├── pyproject.toml                         # Python dependencies (tests + app)
├── sql/
│   ├── 01_setup.sql                       # Database, schema, warehouse, stage
│   ├── 02_tables.sql                      # Document tracking + extraction tables
│   ├── 03_test_single_file.sql            # Test prompts on one file (START HERE)
│   ├── 04_batch_extract.sql               # Extract all staged documents
│   ├── 05_views.sql                       # Analytical views for dashboard
│   ├── 06_automate.sql                    # Stream + Task automation (optional)
│   └── 07_deploy_streamlit.sql            # Deploy the Streamlit dashboard (optional)
├── streamlit/
│   ├── streamlit_app.py                   # Landing page + pipeline overview
│   ├── config.py                          # Dynamic config (zero hardcoded values)
│   ├── environment.yml                    # Container Runtime dependencies
│   └── pages/
│       ├── 0_Dashboard.py                 # KPI cards + recent documents
│       ├── 1_Document_Viewer.py           # Browse, filter, drill-down + PDF viewer
│       └── 2_Analytics.py                 # Charts: by sender, monthly, aging, top items
└── tests/
    ├── test_deployment_readiness.py       # Pre-flight checks (Cortex, encryption, EAI)
    ├── test_sql_integration.py            # All SQL objects exist with correct schema
    ├── test_extraction_pipeline.py        # Live AI_EXTRACT, stored proc, idempotency
    ├── test_data_validation.py            # Data quality, parse failures, edge cases
    └── test_e2e/
        ├── helpers.py                     # Shared Playwright utilities
        ├── conftest.py                    # E2E fixtures + screenshot-on-failure
        ├── test_poc_landing.py            # Landing page tests
        ├── test_poc_dashboard.py          # Dashboard page tests
        ├── test_poc_document_viewer.py    # Document Viewer page tests
        └── test_poc_analytics.py          # Analytics page tests
```

**Run scripts in order: 01 → 02 → (upload files) → 03 → 04 → 05 → (optionally 06, 07)**

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `AI_EXTRACT function not found` | SNOWFLAKE.CORTEX_USER role not granted | `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <your_role>;` (requires ACCOUNTADMIN) |
| `File not found` or `stage not found` | Stage path is wrong, or directory table is stale | Run `ALTER STAGE DOCUMENT_STAGE REFRESH;` then `SELECT * FROM DIRECTORY(@DOCUMENT_STAGE)` to verify |
| `Unsupported encryption type` | Stage uses client-side encryption (the default) | You must create the stage with `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')`. This **cannot** be changed after creation — drop and recreate the stage. |
| Empty extraction results | Prompts too vague, or document is scanned/low-quality | Make prompts more specific. Try a cleaner file. Check raw JSON in `03_test_single_file.sql`. |
| `Region not supported` | Account is in a non-supported region | `ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';` (requires ACCOUNTADMIN) |
| Numbers extracted as strings (`$1,234.56`) | Prompt doesn't request numeric format | Add `'Return as a number only.'` to numeric prompts |
| `FLATTEN returns no rows` | Table extraction returned empty arrays | Check raw JSON in `03_test_single_file.sql` — the model may not have found a table. Add a more specific `description` to help locate it. |
| `Compute pool not ready` | Pool is still provisioning | Wait 1-2 minutes. Run `DESCRIBE COMPUTE POOL AI_EXTRACT_POC_POOL;` to check status. |
| Streamlit app shows errors | Views or tables don't exist yet | Run scripts 01-05 before deploying the Streamlit app |
| `PYPI_ACCESS_INTEGRATION` error | EAI not created or not granted | Run the EAI creation in `07_deploy_streamlit.sql` as ACCOUNTADMIN |
| Duplicate rows in `EXTRACTED_FIELDS` | Script was run twice on same files | The scripts include `NOT IN (SELECT file_name FROM ...)` guards, but if you re-run manually, check for duplicates: `SELECT file_name, COUNT(*) FROM EXTRACTED_FIELDS GROUP BY 1 HAVING COUNT(*) > 1` |

---

## Cost Estimate

AI_EXTRACT pricing is based on Cortex AI tokens:

| Component | Token Cost |
|---|---|
| Each page of a PDF/DOCX/TIFF | 970 tokens |
| Each image file (PNG, JPG, etc.) | 970 tokens (= 1 page) |
| Input prompt tokens | Variable (typically small) |
| Output tokens | Variable |

**Example: 100 single-page invoices with 10 entity questions + 1 table question**
- Input: 100 files x 970 tokens/page = 97,000 tokens
- Entity extraction: ~20 questions worth (10 entity + 1 table x10) per file
- Approximate total: ~200K-300K tokens per run (entity + table extraction)

**Warehouse cost:**
- AI_EXTRACT performance does **not** improve with larger warehouse sizes
- **X-SMALL is recommended** — you pay for less compute while getting the same extraction speed
- The warehouse is only active during extraction queries; auto-suspend handles idle time

---

## Cleanup

To remove all POC objects from your account:

```sql
-- Drop the database (removes everything: schema, tables, views, stage, stream, task, procedure)
DROP DATABASE IF EXISTS AI_EXTRACT_POC;

-- Drop the warehouse
DROP WAREHOUSE IF EXISTS AI_EXTRACT_WH;

-- Drop the compute pool (if you deployed the dashboard)
DROP COMPUTE POOL IF EXISTS AI_EXTRACT_POC_POOL;

-- Optionally remove the EAI and network rule (if no other apps use them)
-- DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PYPI_ACCESS_INTEGRATION;
-- DROP NETWORK RULE IF EXISTS PYPI_NETWORK_RULE;
```

---

## Validating the Deployment (Tests)

The POC includes a test suite (147 tests) that verifies every SQL object, data quality, extraction pipeline, and Streamlit page. Running tests after deployment proves everything works end-to-end.

> **If you only ran the SQL scripts in Snowsight** (steps 1-7), the tests are optional but recommended. They catch issues like missing grants, encryption mismatches, and parse failures that you might not notice manually.

### Local Tool Prerequisites

These tools are only needed for the automated deploy script (`deploy_poc.sh`) and/or running the test suite. If you ran the SQL scripts manually in Snowsight, you only need tools for the tests.

| Tool | Version | What It's For | Install |
|---|---|---|---|
| **Python** | >= 3.11 | Test runner, deploy script fallback | `brew install python@3.12` (macOS) or [python.org](https://www.python.org/downloads/) |
| **uv** | latest | Python package/venv manager (installs deps, runs pytest) | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| **Snowflake CLI** (`snow`) | >= 3.0 | `deploy_poc.sh` uses `snow sql` to run SQL files | `uv tool install snowflake-cli` |
| **Playwright** | (auto-installed) | E2E browser tests for Streamlit pages | `uv run playwright install chromium` (after `uv sync`) |

### Snowflake Connection Config

The tests and deploy script connect to Snowflake using a **named connection** in `~/.snowflake/config.toml`. Create this file if it doesn't exist:

```toml
# ~/.snowflake/config.toml
[connections.my_account]
account = "YOUR_ORG-YOUR_ACCOUNT"
user = "YOUR_USERNAME"
authenticator = "externalbrowser"          # opens browser for SSO login
# OR use password auth:
# authenticator = "snowflake"
# password = "YOUR_PASSWORD"
warehouse = "AI_EXTRACT_WH"
database = "AI_EXTRACT_POC"
schema = "DOCUMENTS"
role = "ACCOUNTADMIN"
```

Then tell the POC which connection to use:

```bash
# Option A: environment variable (recommended)
export POC_CONNECTION=my_account

# Option B: pass to deploy script directly
./poc/deploy_poc.sh --connection my_account
# or shorthand:
./poc/deploy_poc.sh -c my_account
```

The default connection name is `aws_spcs`. Override it with `POC_CONNECTION` env var or the `--connection` / `-c` flag.

### Install Test Dependencies

```bash
cd poc

# Create venv and install all dependencies (test + app)
uv sync --all-groups

# Install Playwright browser (needed for E2E tests only)
uv run playwright install chromium
```

### Streamlit Secrets (E2E Tests Only)

The E2E tests run a local Streamlit server that needs Snowflake credentials. Create `poc/streamlit/.streamlit/secrets.toml`:

```toml
# poc/streamlit/.streamlit/secrets.toml
[connections.snowflake]
account = "YOUR_ORG-YOUR_ACCOUNT"
user = "YOUR_USERNAME"
authenticator = "externalbrowser"
warehouse = "AI_EXTRACT_WH"
database = "AI_EXTRACT_POC"
schema = "DOCUMENTS"
role = "ACCOUNTADMIN"
```

> This file is gitignored and never committed. Each person running E2E tests creates their own.

### Running the Tests

From the `poc/` directory:

**Tier 1 — SQL + Data Quality tests only** (~60 seconds, no browser needed):

```bash
uv run pytest tests/test_sql_integration.py tests/test_data_validation.py tests/test_deployment_readiness.py tests/test_extraction_pipeline.py -v
```

**Tier 2 — Full suite including E2E browser tests** (~4 minutes):

```bash
# Start the local Streamlit server first (in a separate terminal):
cd poc && uv run streamlit run streamlit/streamlit_app.py --server.port 8504 --server.headless true

# Then run non-E2E tests + each E2E file separately (recommended):
cd poc
uv run pytest tests/test_data_validation.py tests/test_deployment_readiness.py tests/test_sql_integration.py tests/test_extraction_pipeline.py -v
uv run pytest tests/test_e2e/test_poc_analytics.py -v
uv run pytest tests/test_e2e/test_poc_dashboard.py -v
uv run pytest tests/test_e2e/test_poc_document_viewer.py -v
uv run pytest tests/test_e2e/test_poc_landing.py -v
```

> **Why run E2E files separately?** Playwright's Chromium process can become unstable after
> 30+ sequential navigations to a Streamlit app (EPIPE errors). Running each file in its own
> pytest invocation gives each file a fresh browser process. The non-E2E tests can all run
> together since they don't use a browser.

> **Why a local server?** The E2E tests use Playwright to drive a real browser against the
> Streamlit app. The SPCS-hosted dashboard sits behind Snowflake authentication, which
> Playwright cannot negotiate. The local server connects to Snowflake via your
> `secrets.toml` credentials and serves the same pages on `localhost:8504`.

**Tier 3 — Clean-room: teardown, redeploy, test** (proves scripts work from scratch):

```bash
# 1. Tear down existing POC
snow sql -c my_account -f poc/teardown_poc.sql

# 2. Redeploy from scratch
./poc/deploy_poc.sh --connection my_account

# 3. Start local Streamlit server (separate terminal, for E2E tests)
cd poc && uv run streamlit run streamlit/streamlit_app.py --server.port 8504 --server.headless true

# 4. Run all tests
cd poc && uv run pytest tests/ -v
```

### What the Tests Verify

| Test File | Count | What It Checks |
|---|---|---|
| `test_deployment_readiness.py` | 12 | Pre-flight: Cortex access, SSE encryption, staged files, EAI, compute pool, Streamlit stage |
| `test_sql_integration.py` | 42 | Every SQL object: database, schema, warehouse, stages, tables, columns, PKs, views, stream, task, stored proc |
| `test_extraction_pipeline.py` | 16 | Live AI_EXTRACT calls (entity + table mode), stored procedure execution, idempotency, LATERAL FLATTEN |
| `test_data_validation.py` | 36 | Data quality: completeness, no NULLs in required fields, amounts > 0, valid dates, no orphans, no duplicates |
| `test_e2e/` (4 files) | 41 | Playwright browser tests: every Streamlit page loads, no exceptions, KPIs show correct values, charts render |

### Cross-Cloud Verification

This kit has been deployed and tested from scratch on all three Snowflake clouds:

| Cloud | Region | Result |
|---|---|---|
| **AWS** | US East 1 (Virginia) | 147/147 pass |
| **Azure** | East US 2 | 147/147 pass |
| **GCP** | US East 4 | 147/147 pass |

GCP and other non-primary regions require cross-region inference, which `01_setup.sql` enables automatically.

### Interpreting Test Failures

The tests are designed to give **actionable error messages**. Examples:

```
FAILED test_deployment_readiness.py::TestCortexAvailability::test_cortex_user_role_granted
  SNOWFLAKE.CORTEX_USER not granted to ACCOUNTADMIN.
  Run: GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;

FAILED test_deployment_readiness.py::TestStageEncryption::test_document_stage_is_snowflake_sse
  DOCUMENT_STAGE uses client-side encryption. AI_EXTRACT requires SNOWFLAKE_SSE.
  You must recreate the stage: DROP STAGE DOCUMENT_STAGE;
  CREATE STAGE DOCUMENT_STAGE DIRECTORY=(ENABLE=TRUE) ENCRYPTION=(TYPE='SNOWFLAKE_SSE')

FAILED test_data_validation.py::TestEdgeCases::test_no_negative_amounts
  3 records have negative amounts — REGEXP_REPLACE may be mishandling currency formatting
```

---

## Using Your Own Documents

Once the POC is deployed and tests pass with the sample invoices, you're ready to swap in your own files. This section walks through the complete process — from uploading your documents to seeing extracted data in the dashboard.

### Overview

| Phase | What You Do | Time |
|---|---|---|
| 1. Prepare | Gather 5-20 sample documents and decide which fields to extract | 10 min |
| 2. Upload | Stage your documents in Snowflake | 5 min |
| 3. Tune prompts | Test AI_EXTRACT on one file and refine your questions | 10-15 min |
| 4. Batch extract | Run extraction across all your documents | 5-10 min |
| 5. Verify | Query the results and check accuracy | 5 min |

### Phase 1: Prepare Your Documents

1. **Collect 5-20 representative files** of the type you want to extract from (invoices, contracts, receipts, claims, etc.). Include a mix of formats and layouts if your documents come from different senders.

2. **Define the fields you need.** Write down 5-10 header-level fields and any tabular data:

   | Document Type | Example Header Fields | Example Table Columns |
   |---|---|---|
   | Invoices | vendor, invoice #, date, due date, PO #, total | line item, qty, unit price, amount |
   | Contracts | parties, effective date, term, value, governing law | milestone, deliverable, date, payment |
   | Receipts | store, date, total, payment method | item, qty, price |
   | Medical claims | patient, provider, date of service, diagnosis, total | procedure code, description, charge |

3. **Supported file types:** PDF, PNG, JPEG/JPG, DOCX/DOC, PPTX/PPT, EML, HTML/HTM, TXT, TIF/TIFF, BMP, GIF, WEBP, MD. Max 125 pages and 100 MB per file.

### Phase 2: Clear Sample Data and Upload Your Files

The POC was deployed with sample invoices. You need to clear those and upload your own files.

**Option A — Start fresh (recommended for clean results):**

```sql
-- Remove sample data from tables (keeps the table structure)
TRUNCATE TABLE EXTRACTED_TABLE_DATA;
TRUNCATE TABLE EXTRACTED_FIELDS;
TRUNCATE TABLE RAW_DOCUMENTS;

-- Remove sample files from stage
REMOVE @DOCUMENT_STAGE;
```

**Option B — Keep sample data and add yours alongside:**

Skip the truncation above. Your documents will be processed alongside the existing samples. This is fine for experimentation but can clutter the dashboard.

**Upload your documents** using any of these methods:

```sql
-- Snowsight UI: Data > Databases > AI_EXTRACT_POC > DOCUMENTS > Stages > DOCUMENT_STAGE > + Files

-- Snowflake CLI:
-- snow stage copy /path/to/your/docs/*.pdf @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE --overwrite

-- SnowSQL:
-- PUT file:///path/to/your/docs/*.pdf @AI_EXTRACT_POC.DOCUMENTS.DOCUMENT_STAGE AUTO_COMPRESS=FALSE;
```

After uploading, refresh the directory and register files:

```sql
ALTER STAGE DOCUMENT_STAGE REFRESH;

-- Verify your files are staged
SELECT * FROM DIRECTORY(@DOCUMENT_STAGE) ORDER BY LAST_MODIFIED DESC;

-- Register new files into RAW_DOCUMENTS
INSERT INTO RAW_DOCUMENTS (file_name, file_path, staged_at)
SELECT
    RELATIVE_PATH,
    '@DOCUMENT_STAGE/' || RELATIVE_PATH,
    CURRENT_TIMESTAMP()
FROM DIRECTORY(@DOCUMENT_STAGE) d
WHERE NOT EXISTS (
    SELECT 1 FROM RAW_DOCUMENTS r WHERE r.file_name = d.RELATIVE_PATH
);

-- Confirm registration
SELECT COUNT(*) AS total_registered,
       COUNT_IF(extracted = FALSE) AS pending_extraction
FROM RAW_DOCUMENTS;
```

### Phase 3: Tune Prompts on a Single File

This is the most important step. Open **`sql/03_test_single_file.sql`** and edit the prompts for your document type.

```sql
-- Set to one of YOUR file names
SET test_file = 'your_document.pdf';

-- Edit the AI_EXTRACT questions to match YOUR fields
SELECT AI_EXTRACT(
    TO_FILE('@DOCUMENT_STAGE', $test_file),
    {
        'field_1': 'What is the vendor or sender name?',
        'field_2': 'What is the document or invoice number?',
        'field_3': 'What is the PO or reference number? Return NULL if not present.',
        'field_4': 'What is the document date? Return in YYYY-MM-DD format.',
        'field_5': 'What is the due date? Return in YYYY-MM-DD format. Return NULL if not present.',
        'field_6': 'What are the payment terms (e.g., Net 30)?',
        'field_7': 'Who is the recipient or bill-to party?',
        'field_8': 'What is the subtotal before tax? Return as a number only.',
        'field_9': 'What is the tax amount? Return as a number only. Return 0 if not present.',
        'field_10': 'What is the total amount due? Return as a number only.'
    }
) AS extraction;
```

**Review the JSON output carefully.** Check:

- Are values correct compared to the source document?
- Are dates in `YYYY-MM-DD` format?
- Are numbers clean (no `$`, commas, or currency symbols)?
- Do missing fields return `NULL` instead of hallucinated values?

Iterate on the prompts until the output is accurate. See [Customizing for Your Document Type](#customizing-for-your-document-type) for prompt engineering tips and examples for contracts, receipts, and other document types.

**Test table extraction too** (if your documents have line items):

```sql
SELECT AI_EXTRACT(
    TO_FILE('@DOCUMENT_STAGE', $test_file),
    {
        'line_items': {
            'col_1': 'What is the item description or product name?',
            'col_2': 'What is the quantity? Return as a number only.',
            'col_3': 'What is the unit price? Return as a number only.',
            'col_4': 'What is the line total? Return as a number only.',
            'col_5': 'What is the item code or SKU? Return NULL if not present.'
        }
    }
) AS extraction;
```

### Phase 4: Batch Extract All Documents

Once single-file results look good, open **`sql/04_batch_extract.sql`**.

> **Copy your tuned prompts** from Phase 3 into this script. The prompts in `04_batch_extract.sql` must match exactly what you validated on a single file.

Run the script. It processes all files in `RAW_DOCUMENTS` where `extracted = FALSE`.

**Monitor progress:**

```sql
-- Check extraction status
SELECT
    COUNT(*) AS total,
    COUNT_IF(extracted) AS extracted,
    COUNT_IF(NOT extracted) AS pending
FROM RAW_DOCUMENTS;

-- Preview extracted data
SELECT * FROM EXTRACTED_FIELDS ORDER BY extracted_at DESC LIMIT 10;
SELECT * FROM EXTRACTED_TABLE_DATA ORDER BY extracted_at DESC LIMIT 20;
```

**Runtime guide:** ~3-6 seconds per single-page document on an X-SMALL warehouse. A batch of 100 single-page documents typically completes in 5-10 minutes.

### Phase 5: Verify Results and Refresh Views

After extraction completes, refresh the analytical views and verify:

```sql
-- Views automatically reflect the latest data (they're views, not materialized)
-- Query them directly:

-- Pipeline status
SELECT * FROM V_EXTRACTION_STATUS;

-- All documents with parsed fields
SELECT * FROM V_DOCUMENT_LEDGER ORDER BY document_date DESC LIMIT 20;

-- Totals by sender/vendor
SELECT * FROM V_SUMMARY_BY_VENDOR ORDER BY total_amount DESC;

-- Monthly trend
SELECT * FROM V_MONTHLY_TREND ORDER BY month DESC;

-- Top line items
SELECT * FROM V_TOP_LINE_ITEMS;
```

**Spot-check accuracy** by comparing a few rows against the source documents:

```sql
-- Pick a specific document and compare against the original
SELECT file_name, field_1 AS vendor, field_2 AS doc_number,
       field_4 AS doc_date, field_10 AS total
FROM EXTRACTED_FIELDS
WHERE file_name = 'your_document.pdf';
```

If the dashboard is deployed (Step 7), open it in Snowsight — it will automatically show your new data.

### Phase 6: Enable Ongoing Automation (Optional)

If you plan to stage new documents over time, enable the automation pipeline so they are extracted automatically:

```sql
-- If not already created, run sql/06_automate.sql first

-- Resume the task (it may be suspended)
ALTER TASK EXTRACT_NEW_DOCUMENTS_TASK RESUME;

-- Upload a new document to test
-- PUT file:///path/to/new_doc.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE;

-- Register it
ALTER STAGE DOCUMENT_STAGE REFRESH;
INSERT INTO RAW_DOCUMENTS (file_name, file_path, staged_at)
SELECT RELATIVE_PATH, '@DOCUMENT_STAGE/' || RELATIVE_PATH, CURRENT_TIMESTAMP()
FROM DIRECTORY(@DOCUMENT_STAGE) d
WHERE NOT EXISTS (SELECT 1 FROM RAW_DOCUMENTS r WHERE r.file_name = d.RELATIVE_PATH);

-- The task will fire within 5 minutes and extract the new document
-- Or trigger it immediately:
EXECUTE TASK EXTRACT_NEW_DOCUMENTS_TASK;

-- Verify
SELECT * FROM EXTRACTED_FIELDS ORDER BY extracted_at DESC LIMIT 5;
```

### Common Issues When Switching to Your Own Documents

| Issue | Symptom | Fix |
|---|---|---|
| Wrong field mapping | `field_1` contains a date instead of vendor name | Edit prompts in `03_test_single_file.sql` and `04_batch_extract.sql` to match your document layout |
| Amounts have `$` or `,` | Numbers stored as strings like `$1,234.56` | Add `'Return as a number only.'` to your prompt, or rely on the `REGEXP_REPLACE` in the views |
| Dates in wrong format | `03/15/2024` instead of `2024-03-15` | Add `'Return in YYYY-MM-DD format.'` to your date prompts |
| Table extraction returns wrong table | Line items come from a summary table instead of the detail table | Add a `description` key: `'description': 'The detailed line item table with product, quantity, and price columns'` |
| Some documents return NULLs for all fields | AI_EXTRACT can't read the document | Check file format is supported, file isn't corrupted, and file size is under 100 MB. Scanned images may need better resolution. |
| Duplicate rows after re-running batch | Batch was run twice without marking files as extracted | The `04_batch_extract.sql` script only processes `extracted = FALSE` rows. If you re-ran it manually, deduplicate: `DELETE FROM EXTRACTED_FIELDS WHERE ...` |

---

## Next Steps

After a successful POC:

1. **Rename columns** — Replace `field_1`...`field_10` and `col_1`...`col_5` with meaningful names
2. **Add error handling** — Wrap extraction in TRY/CATCH to capture failures per document
3. **Scale up** — Stage thousands of documents and run batch extraction
4. **Automate** — Enable the Stream + Task pipeline from `06_automate.sql`
5. **Integrate** — Connect extraction output to your ERP, AP system, or data warehouse via Snowflake data sharing, Snowpipe, or direct table access
6. **Explore the reference demo** — A full-featured invoice processing app with generated test data, additional analytics, and E2E tests: [github.com/sfc-gh-jkang/ap-invoice-processing-demo](https://github.com/sfc-gh-jkang/ap-invoice-processing-demo)

---

*Built with [Snowflake Cortex AI_EXTRACT](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-extract)*
