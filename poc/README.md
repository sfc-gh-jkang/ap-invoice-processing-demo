# AI_EXTRACT POC Kit

<!-- CI badge: update the URL below to match your repository -->
<!-- [![Tests](https://github.com/YOUR-ORG/YOUR-REPO/actions/workflows/test.yml/badge.svg)](https://github.com/YOUR-ORG/YOUR-REPO/actions/workflows/test.yml) -->

Extract structured data from your own PDFs, images, and documents using **Snowflake Cortex AI_EXTRACT** — entirely within your Snowflake account. No external services, no API keys, no data leaves Snowflake.

This kit walks you through a complete proof-of-concept:

1. **Stage** your documents in a Snowflake internal stage
2. **Extract** entity fields (header data) and tabular data (line items) using AI
3. **Visualize** results in an interactive Streamlit dashboard
4. **Review** extracted data with inline corrections and an append-only audit trail
5. **Automate** extraction of new documents via Stream + Task (optional)

**Total setup time: ~30 minutes** (excluding extraction runtime, which depends on document count).

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [What You Need Before Starting](#what-you-need-before-starting)
- [Quick Start (Automated Deploy)](#quick-start-automated-deploy)
- [Step-by-Step Guide](#step-by-step-guide)
- [Customizing for Your Document Type](#customizing-for-your-document-type)
- [Understanding the Extraction Output](#understanding-the-extraction-output)
- [Deploying the Dashboard](#deploying-the-dashboard)
- [Setting Up Automation](#setting-up-automation)
- [Review Workflow (Writeback)](#review-workflow-writeback)
- [Role-Based Access Control (RBAC)](#role-based-access-control-rbac)
- [Multi-Document-Type Support](#multi-document-type-support)
- [File Structure](#file-structure)
- [Troubleshooting](#troubleshooting)
- [Cost Observability](#cost-observability)
- [Cost Estimate](#cost-estimate)
- [Cleanup](#cleanup)
- [Validating the Deployment (Tests)](#validating-the-deployment-tests)
- [Using Your Own Documents](#using-your-own-documents)
- [Next Steps](#next-steps)
- [Building a Production PDF Extraction Pipeline](#building-a-production-pdf-extraction-pipeline)
- [Customer Handoff Checklist](#customer-handoff-checklist)
- [Secret Scanning & Security](#secret-scanning--security)
- [Cross-Cloud Deployment Notes](#cross-cloud-deployment-notes)

---

## Prerequisites

### Account Requirements

| Requirement | Detail |
|---|---|
| **Snowflake Edition** | Standard or higher |
| **Account Region** | Must be a [supported region](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-extract#regional-availability), or enable cross-region inference (see below) |
| **Role** | ACCOUNTADMIN for initial setup (role creation, Cortex grant, EAI); `AI_EXTRACT_APP` role for all other operations |
| **Cortex Access** | SNOWFLAKE.CORTEX_USER database role granted to `AI_EXTRACT_APP` |
| **Container Runtime** | Required for Streamlit deployment. Account must have `BIND SERVICE ENDPOINT ON ACCOUNT` granted to the deploying role. |

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

> **Cost note:** Cross-region inference routes requests to a supported region if your account's home region doesn't support AI_EXTRACT. This may add latency and incur cross-region data transfer costs. For production workloads, consider deploying in a natively supported region.

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

## Quick Start (Automated Deploy)

If you have the [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow`) and [uv](https://docs.astral.sh/uv/) installed, the deploy script runs all 11 SQL steps automatically:

```bash
# Deploy the entire POC in one command
./poc/deploy_poc.sh --connection my_account

# Or use an environment variable
export POC_CONNECTION=my_account
./poc/deploy_poc.sh

# Optionally customize role and object names
export POC_ROLE=MY_CUSTOM_ROLE
export POC_DB=MY_EXTRACT_DB
./poc/deploy_poc.sh

# Skip extraction (for re-deploying schema only)
./poc/deploy_poc.sh --connection my_account --skip-extraction

# Skip production hardening during development
POC_HARDEN=false ./poc/deploy_poc.sh --connection my_account
```

The script creates a dedicated `AI_EXTRACT_APP` role (configurable via `POC_ROLE`), then uses that role to create the database, tables, stages sample documents (from `sample_documents/`), runs batch extraction, creates views, sets up automation, deploys the Streamlit dashboard, creates the review/writeback objects, loads document type configuration, applies production hardening, and sets up extraction failure alerts. The 5 included sample invoices let you validate the full pipeline immediately.

**After deployment**, validate with the health check:

```bash
bash validate_poc.sh --connection my_account   # Standalone health check
make validate CONNECTION=my_account            # Or via Makefile
```

If you prefer to understand each step, follow the manual [Step-by-Step Guide](#step-by-step-guide) below.

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

> A fourth table (`INVOICE_REVIEW`) is created by `08_writeback.sql` — see [Review Workflow](#review-workflow-writeback).

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

> A seventh view (`V_INVOICE_SUMMARY`) is created by `08_writeback.sql` — see [Review Workflow](#review-workflow-writeback).

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

The `streamlit/` folder contains a 6-page Streamlit in Snowflake app (Dashboard, Document Viewer, Analytics, Review, Admin, Cost). Deployment is optional but gives you an interactive way to explore results.

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
   snow stage copy streamlit/snowflake.yml @STREAMLIT_STAGE/ --overwrite
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
| **Review** | Inline `st.data_editor` for reviewing/correcting extracted data, append-only audit trail, writeback to `INVOICE_REVIEW` |
| **Admin** | Document type config management, add/edit/delete document types via UI builder, test extraction on single files with save-to-Snowflake capability |
| **Cost** | Cost observability: credit consumption trends, token usage analysis, per-document cost drivers |

### Updating the Deployed App

After making code changes to the Streamlit pages, re-upload the files and restart:

```bash
# Re-upload all Streamlit files to the stage
make upload-streamlit CONNECTION=my_account

# Or manually:
snow sql -c my_account -q "
  USE ROLE AI_EXTRACT_APP;
  USE DATABASE AI_EXTRACT_POC;
  USE SCHEMA DOCUMENTS;
  PUT file://streamlit/streamlit_app.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
  PUT file://streamlit/config.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
  PUT file://streamlit/snowflake.yml @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
  PUT file://streamlit/pages/*.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
"
```

Then restart the app in Snowsight (three-dot menu on the Streamlit app > Restart).

### Deploying to a Customer Environment

This POC is designed for Streamlit in Snowflake (SiS) — no SPCS containers needed. To deploy to a different Snowflake account:

1. **Set environment variables** for the target account:
   ```bash
   export POC_CONNECTION=customer_account   # Name in ~/.snowflake/connections.toml
   export POC_DB=AI_EXTRACT_POC             # Or customer-preferred name
   export POC_SCHEMA=DOCUMENTS
   export POC_WH=AI_EXTRACT_WH
   export POC_ROLE=AI_EXTRACT_APP
   ```

2. **Deploy** (runs all 11 SQL scripts + uploads files + validates):
   ```bash
   make deploy CONNECTION=customer_account
   ```

3. **Validate** the deployment:
   ```bash
   make validate CONNECTION=customer_account
   ```

4. **Run tests** against the target account:
   ```bash
   make test-all CONNECTION=customer_account
   ```

5. **Upload customer documents** and run extraction (see [Using Your Own Documents](#using-your-own-documents)).

> **Note:** ACCOUNTADMIN is only needed during initial deploy for role creation, Cortex grants, and the PyPI External Access Integration. All app operations use the `AI_EXTRACT_APP` role.

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

## Review Workflow (Writeback)

After extraction, documents can be reviewed and corrected through an inline editing interface. The review system uses an **append-only audit trail** — every correction is a new INSERT, never an UPDATE or DELETE.

### Step 8: Create Writeback Objects

Open **`sql/08_writeback.sql`** in Snowsight and run it. This creates:

| Object | Type | Purpose |
|---|---|---|
| `INVOICE_REVIEW` | Table | Append-only audit trail of all reviews. Each row captures a correction or approval for a document. |
| `V_INVOICE_SUMMARY` | View | Joins `EXTRACTED_FIELDS` with the **latest** review per document using `ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY reviewed_at DESC)`. |

### How It Works

1. **Review page** (`3_Review.py`) — Displays all documents in an `st.data_editor` grid. Reviewers can edit any column inline: status, corrected amounts, vendor name, dates, notes, etc.
2. **Append-only writes** — Each save INSERTs a new row into `INVOICE_REVIEW`. Previous reviews are never modified or deleted.
3. **Latest-wins view** — `V_INVOICE_SUMMARY` always shows the most recent review per document using `ROW_NUMBER()` partitioned by `record_id` and ordered by `reviewed_at DESC`.
4. **COALESCE override** — The view uses `COALESCE(rv.corrected_total, ef.field_10)` so corrections override originals, but a NULL correction falls back to the original extracted value.

### INVOICE_REVIEW Columns

| Column | Type | Description |
|---|---|---|
| `REVIEW_ID` | NUMBER (AUTOINCREMENT) | Primary key, monotonically increasing |
| `RECORD_ID` | NUMBER | FK to `EXTRACTED_FIELDS.record_id` |
| `FILE_NAME` | VARCHAR | Document filename (denormalized for convenience) |
| `REVIEW_STATUS` | VARCHAR | `PENDING`, `APPROVED`, `REJECTED`, `CORRECTED` |
| `CORRECTED_TOTAL` | NUMBER(12,2) | Overrides `field_10` if non-NULL |
| `CORRECTED_VENDOR_NAME` | VARCHAR | Overrides `field_1` |
| `CORRECTED_INVOICE_NUMBER` | VARCHAR | Overrides `field_2` |
| `CORRECTED_PO_NUMBER` | VARCHAR | Overrides `field_3` |
| `CORRECTED_INVOICE_DATE` | DATE | Overrides `field_4` |
| `CORRECTED_DUE_DATE` | DATE | Overrides `field_5` |
| `CORRECTED_PAYMENT_TERMS` | VARCHAR | Overrides `field_6` |
| `CORRECTED_RECIPIENT` | VARCHAR | Overrides `field_7` |
| `CORRECTED_SUBTOTAL` | NUMBER(12,2) | Overrides `field_8` |
| `CORRECTED_TAX_AMOUNT` | NUMBER(12,2) | Overrides `field_9` |
| `REVIEWER_NOTES` | VARCHAR | Free-text notes |
| `REVIEWED_BY` | VARCHAR | Defaults to `CURRENT_USER()` |
| `REVIEWED_AT` | TIMESTAMP_LTZ | Defaults to `CURRENT_TIMESTAMP()` |

### Querying the Review State

```sql
-- Latest review status for all documents
SELECT * FROM V_INVOICE_SUMMARY;

-- Full audit trail for a specific document
SELECT * FROM INVOICE_REVIEW
WHERE record_id = 42
ORDER BY reviewed_at DESC;

-- Documents still pending review
SELECT * FROM V_INVOICE_SUMMARY
WHERE review_status IS NULL OR review_status = 'PENDING';
```

---

## Role-Based Access Control (RBAC)

The POC uses a dedicated **`AI_EXTRACT_APP`** role instead of running everything as ACCOUNTADMIN. This follows Snowflake's principle of least privilege.

### Role Hierarchy

```
ACCOUNTADMIN
├── Creates AI_EXTRACT_APP role
├── Grants SNOWFLAKE.CORTEX_USER to AI_EXTRACT_APP
├── Sets CORTEX_ENABLED_CROSS_REGION (if needed)
├── Creates EAI + network rule (07_deploy_streamlit.sql)
└── Grants USAGE ON INTEGRATION to AI_EXTRACT_APP

AI_EXTRACT_APP (dedicated POC role)
├── Creates database, warehouse, stage
├── Creates all tables, views, streams, tasks, procedures
├── Deploys Streamlit app
└── Granted to SYSADMIN for management access
```

### What ACCOUNTADMIN Does (and Only ACCOUNTADMIN)

| Action | Why ACCOUNTADMIN is Required |
|---|---|
| `CREATE ROLE AI_EXTRACT_APP` | Only ACCOUNTADMIN (or USERADMIN) can create roles |
| `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER` | Cortex database roles require elevated privileges |
| `ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION` | Account-level setting |
| `CREATE NETWORK RULE` / `CREATE EXTERNAL ACCESS INTEGRATION` | Security-sensitive objects |
| `GRANT USAGE ON INTEGRATION` | Integration grants require the integration owner |
| `GRANT BIND SERVICE ENDPOINT ON ACCOUNT` | Account-level privilege for Streamlit |

### Customizing the Role Name

```bash
# Default role
./poc/deploy_poc.sh --connection my_account

# Custom role name
export POC_ROLE=MY_TEAM_EXTRACT_ROLE
./poc/deploy_poc.sh --connection my_account
```

The deploy script replaces `AI_EXTRACT_APP` with your custom role name in all SQL files via `sed`.

### Teardown

`teardown_poc.sql` drops the role as its final step:

```sql
USE ROLE ACCOUNTADMIN;
DROP ROLE IF EXISTS AI_EXTRACT_APP;
```

---

## Multi-Document-Type Support

The POC supports multiple document types (invoices, contracts, receipts, or any custom type) through a configuration-driven approach. All UI labels and extraction prompts are stored in a Snowflake table — no code changes needed to add a new document type.

### How It Works

1. **`RAW_DOCUMENTS.doc_type`** — Each staged file is tagged with its document type (default: `'INVOICE'`)
2. **`DOCUMENT_TYPE_CONFIG`** table — Stores per-type extraction prompts and UI field labels as JSON
3. **`config.py`** — `get_doc_type_labels()` fetches labels at runtime; falls back to invoice defaults
4. **All Streamlit pages** — Include a "Document Type" filter dropdown; labels update dynamically

### Built-in Document Types

| Type | Display Name | Example Fields |
|---|---|---|
| `INVOICE` | Invoice | Vendor Name, Invoice #, PO #, Invoice Date, Due Date, Payment Terms, Recipient, Subtotal, Tax, Total |
| `CONTRACT` | Contract | Party Name, Contract #, Reference ID, Effective Date, Expiration Date, Terms, Counterparty, Base Value, Adjustments, Total Value |
| `RECEIPT` | Receipt | Merchant Name, Receipt #, Transaction ID, Purchase Date, Return By Date, Payment Method, Buyer, Subtotal, Tax, Total Paid |
| `LEASE` | Lease | Landlord, Lease #, Property ID, Start Date, End Date, Terms, Tenant, Monthly Rent, Deposit, Total Value |
| `UTILITY_BILL` | Utility Bill | Utility Provider, Account #, Meter ID, Bill Date, Due Date, Rate Plan, Service Address, Previous Balance, Current Charges, Total Due |

### Adding a Custom Document Type

**Option A — Admin UI (recommended):**

The **Admin** page provides an interactive builder for creating new document types without writing SQL:

1. Navigate to the **Admin** page in the Streamlit app
2. Select **"Add New Document Type"** in the Config Management section
3. Fill in the document type code (e.g., `PURCHASE_ORDER`) and display name
4. Define your 10 extraction fields with labels and data types
5. Optionally configure table extraction columns for line-item data
6. Preview the generated extraction prompt and field label JSON
7. Click **Save** to insert the configuration into `DOCUMENT_TYPE_CONFIG`

The builder generates the extraction prompt, field labels, field types, review fields, and table column schema automatically.

**Option B — Direct SQL:**

```sql
INSERT INTO DOCUMENT_TYPE_CONFIG (doc_type, display_name, extraction_prompt, field_labels)
VALUES (
    'PURCHASE_ORDER',
    'Purchase Order',
    'Extract: buyer_name, po_number, order_date, delivery_date, ship_to, terms, item_count, subtotal, shipping, total',
    PARSE_JSON('{
        "field_1": "Buyer",
        "field_2": "PO Number",
        "field_3": "Order Date",
        "field_4": "Delivery Date",
        "field_5": "Ship To",
        "field_6": "Terms",
        "field_7": "Item Count",
        "field_8": "Subtotal",
        "field_9": "Shipping",
        "field_10": "Total",
        "sender_label": "Buyer",
        "amount_label": "Total",
        "date_label": "Order Date",
        "reference_label": "PO #",
        "secondary_ref_label": "Order Date"
    }')
);
```

Then tag your documents when staging:

```sql
UPDATE RAW_DOCUMENTS
SET doc_type = 'PURCHASE_ORDER'
WHERE file_name LIKE 'po_%';
```

The Streamlit pages will immediately show the new type in the filter dropdown with the correct labels.

### Label JSON Structure

The `field_labels` column stores a JSON object with these keys:

| Key | Purpose | Used By |
|---|---|---|
| `field_1` ... `field_10` | Column-level labels for the detail/editor views | Document Viewer, Review |
| `sender_label` | Label for the "sender" / primary party | Dashboard, Viewer, Analytics, Review |
| `amount_label` | Label for the primary monetary field | Dashboard, Analytics |
| `date_label` | Label for the primary date field | Dashboard, Viewer |
| `reference_label` | Label for the primary reference number | Dashboard, Viewer |
| `secondary_ref_label` | Label for a secondary reference | Dashboard |

### Test Extraction with Save

The **Admin** page includes a **Test Extraction** section that lets you validate AI_EXTRACT against a single staged file before running batch extraction:

1. Select a document type configuration and a staged file
2. Click **Run Extraction** to execute AI_EXTRACT with the selected config's prompts
3. Preview the extracted header fields with confidence scores and the line-item table
4. Click **Save to Snowflake** to persist the results via MERGE into `RAW_DOCUMENTS`, `EXTRACTED_FIELDS`, and `EXTRACTED_TABLE_DATA`

Saving uses MERGE (not INSERT), so re-extracting and saving the same file updates the existing records rather than creating duplicates. The `record_id` in `EXTRACTED_FIELDS` is preserved, keeping downstream review links intact.

### Filename-Based Document Identity

Documents are identified by **filename only** across all tables (`RAW_DOCUMENTS`, `EXTRACTED_FIELDS`, `EXTRACTED_TABLE_DATA`). The `file_name` column serves as the effective primary key.

**Implications:**
- Uploading a file with the same name as an existing document **overwrites** the stage file and **updates** the extraction results (MERGE)
- Review corrections linked to that document (in `INVOICE_REVIEW` / `LINE_ITEM_REVIEW`) are preserved
- To add a truly different document, ensure the filename is unique before uploading

The Streamlit app displays warnings about this behavior on the Admin (upload), Document Viewer, and Review pages.

---

## File Structure

```
ai_extract_poc/
├── README.md                              # This guide
├── LICENSE                                # Apache 2.0
├── .gitignore                             # Ignores secrets, caches, venvs
├── Makefile                               # Common commands: make deploy, test, teardown
├── deploy_poc.sh                          # Automated deploy script (Quick Start)
├── teardown_poc.sh                        # Parameterized teardown (drop all POC objects)
├── teardown_poc.sql                       # Raw SQL teardown (legacy, use teardown_poc.sh)
├── validate_poc.sh                        # Standalone health check (PASS/FAIL/WARN)
├── reprovision.py                         # Full re-provision from scratch (env-var driven)
├── generate_sample_docs.py                # Generate 5 sample invoices (requires reportlab)
├── conftest.py                            # Root pytest config: Snowflake connection fixture,
│                                          #   Streamlit server lifecycle, env var overrides,
│                                          #   sf_conn_factory for concurrent connections
├── pyproject.toml                         # Python dependencies (app + dev/test)
├── uv.lock                                # Lockfile for reproducible builds (committed)
├── sample_documents/                      # 5 pre-generated sample invoices (committed)
│   ├── sample_invoice_01.pdf
│   ├── sample_invoice_02.pdf
│   ├── sample_invoice_03.pdf
│   ├── sample_invoice_04.pdf
│   └── sample_invoice_05.pdf
├── sql/
│   ├── 01_setup.sql                       # Database, schema, warehouse, stage + RBAC role
│   ├── 02_tables.sql                      # Document tracking + extraction tables (incl. doc_type)
│   ├── 03_test_single_file.sql            # Test prompts on one file (START HERE)
│   ├── 04_batch_extract.sql               # Extract all staged documents
│   ├── 05_views.sql                       # Analytical views for dashboard
│   ├── 06_automate.sql                    # Stream + Task automation (optional)
│   ├── 07_deploy_streamlit.sql            # Deploy the Streamlit dashboard (optional)
│   ├── 08_writeback.sql                   # INVOICE_REVIEW table + V_INVOICE_SUMMARY view
│   ├── 09_document_types.sql              # DOCUMENT_TYPE_CONFIG table + seed rows
│   ├── 09_line_item_review.sql            # LINE_ITEM_REVIEW table + V_LINE_ITEM_DETAIL view
│   ├── 10_harden.sql                      # Production hardening (ownership, managed access, resource monitor, retention)
│   ├── 11_alerts.sql                      # Extraction failure alert + health check procedure
│   ├── 12_cost_views.sql                  # 6 cost observability views (token usage, credit trends)
│   └── 13_cost_attribution.sql            # Per-PDF cost attribution: page count UDF, cost driver views
├── streamlit/
│   ├── streamlit_app.py                   # Landing page + pipeline overview
│   ├── config.py                          # Dynamic config (zero hardcoded values)
│   ├── snowflake.yml                      # Container Runtime dependencies (plotly, pypdfium2)
│   ├── .streamlit/
│   │   └── secrets.toml                   # Snowflake credentials (gitignored, create locally)
│   └── pages/
│       ├── 0_Dashboard.py                 # KPI cards + recent documents
│       ├── 1_Document_Viewer.py           # Browse, filter, drill-down + PDF viewer
│       ├── 2_Analytics.py                 # Charts: by sender, monthly, aging, top items
│       ├── 3_Review.py                    # Inline data_editor for review/correction writeback
│       ├── 4_Admin.py                     # Document type config management
│       └── 5_Cost.py                      # Cost observability: credit trends, token analysis, cost drivers
└── tests/
    ├── __init__.py                        # Package marker
    ├── test_admin_builder.py              # Admin page builder logic (unit — no Snowflake)
    ├── test_admin_config_management.py    # Admin config CRUD operations
    ├── test_analytics_queries.py          # Analytics page SQL queries
    ├── test_batch_extract_integration.py  # Batch extraction integration
    ├── test_confidence_scoring.py         # Confidence score tests
    ├── test_config.py                     # Config module unit tests
    ├── test_config_functions.py           # Config function tests
    ├── test_config_helpers.py             # Config helper tests (unit — no Snowflake)
    ├── test_contract_extraction.py        # Contract extraction quality
    ├── test_cost_views.py                 # Cost view SQL validation + data checks
    ├── test_cross_doc_isolation.py        # Cross-doc-type isolation
    ├── test_dashboard_queries.py          # Dashboard page SQL queries
    ├── test_data_drift.py                 # Boundary values, schema evolution
    ├── test_data_validation.py            # Data quality, completeness
    ├── test_deployment_readiness.py       # Pre-flight checks: Cortex, encryption, EAI
    ├── test_document_type_flexibility.py  # Multi-doc-type support
    ├── test_document_viewer_queries.py    # Document Viewer page SQL queries
    ├── test_edge_cases.py                 # Rollbacks, SQL injection, large data, gaps
    ├── test_extraction_pipeline.py        # Live AI_EXTRACT, stored proc, idempotency
    ├── test_lease_extraction.py           # Lease extraction quality
    ├── test_load_stress.py                # Bulk inserts + concurrent writers
    ├── test_multi_user_concurrency.py     # Interleaved reviews + race conditions
    ├── test_normalize_unit.py             # Normalization unit tests (unit — no Snowflake)
    ├── test_performance.py                # Query latency benchmarks
    ├── test_phase_improvements.py         # Phase improvement validation
    ├── test_rbac_negative.py              # RBAC negative/deny tests
    ├── test_rbac_permissions.py           # Role-based access control checks
    ├── test_receipt_extraction.py         # Receipt extraction quality
    ├── test_review_helpers.py             # Review page helper functions (unit — no Snowflake)
    ├── test_sp_error_handling.py          # Stored proc error handling
    ├── test_spcs_deployment.py            # SPCS deployment tests
    ├── test_sql_integration.py            # All SQL objects exist with correct schema
    ├── test_sql_parity.py                 # SQL script vs live object parity
    ├── test_teardown_idempotency.py       # Teardown script idempotency
    ├── test_user_workflow_e2e.py          # Full user workflow: approve, correct, rollback
    ├── test_utility_bill_extraction.py    # Utility bill extraction quality
    ├── test_validation_rules.py           # Validation rule tests
    ├── test_writeback_data_validation.py  # Writeback data quality + COALESCE logic
    ├── test_writeback_integration.py      # INVOICE_REVIEW + V_INVOICE_SUMMARY
    └── test_e2e/
        ├── __init__.py                    # Package marker
        ├── conftest.py                    # E2E fixtures + screenshot-on-failure (daemon thread)
        ├── helpers.py                     # Shared Playwright utilities (wait_for_streamlit)
        ├── test_poc_admin.py              # Admin page tests
        ├── test_poc_analytics.py          # Analytics page tests
        ├── test_poc_dashboard.py          # Dashboard page tests
        ├── test_poc_document_viewer.py    # Document Viewer page tests
        ├── test_poc_landing.py            # Landing page tests
        ├── test_poc_multi_doc.py          # Multi-document-type flows
        └── test_poc_review.py             # Review page tests
```

**Run scripts in order: 01 -> 02 -> (upload files) -> 03 -> 04 -> 05 -> (optionally 06, 07, 08, 09, 10, 11, 12, 13)**

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
| E2E: `inner_text()` returns empty on data grid | Glide Data Grid renders in `<canvas>`, not DOM | Use `page.inner_text("body")` or click open selectbox dropdowns to expose text in DOM |
| E2E: Test expects "Invoices" but sees "Contracts" | Selectbox default is alphabetically first doc type | Make assertions doc-type-agnostic or explicitly select the expected type first |
| E2E: "Vendor" label not found | "Vendor" is INVOICE-specific; other doc types show different labels | Assert on universal labels like "Status" or select INVOICE before asserting |
| Azure/GCP: `CREATE STREAMLIT` fails | Streamlit files not uploaded to stage | `PUT` all files (`.py`, `pyproject.toml`, `snowflake.yml`) before creating the app |
| Azure/GCP: `CREATE COMPUTE POOL` access denied | Running as app role, not ACCOUNTADMIN | Compute pools, network rules, and EAI require `USE ROLE ACCOUNTADMIN` |
| PDF preview: "File not on stage" warning | Document registered in `RAW_DOCUMENTS` but PDF not uploaded to `@DOCUMENT_STAGE` | Re-run `deploy_poc.sh` or manually `PUT` the missing file. On GCP, multi-type docs (contracts, receipts, utility bills) may not have been uploaded if `data/invoices/` was the primary source. |
| PDF preview: "Could not render document" | `session.file.get()` failed inside Container Runtime — often a stage path or permissions issue | Verify file exists: `SELECT * FROM DIRECTORY(@DOCUMENT_STAGE) WHERE RELATIVE_PATH = '<file>';`. Check that `pypdfium2` is in `snowflake.yml`. Confirm the compute pool is ACTIVE. |
| PDF preview: blank/missing on Azure or GCP | Stage has fewer files than `RAW_DOCUMENTS` (e.g., 100 vs 130) | Run `SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE);` vs `SELECT COUNT(*) FROM RAW_DOCUMENTS;`. Upload missing files with `PUT file://... @DOCUMENT_STAGE AUTO_COMPRESS=FALSE;` then `ALTER STAGE DOCUMENT_STAGE REFRESH;` |

---

## Cost Observability

The POC includes a full cost observability layer that tracks AI_EXTRACT credit consumption using Snowflake's first-party billing view (`CORTEX_AI_FUNCTIONS_USAGE_HISTORY`) — no hardcoded dollar amounts.

### SQL Objects

| Script | Object | Purpose |
|---|---|---|
| `12_cost_views.sql` | `V_AI_EXTRACT_TOKEN_USAGE` | Per-call token and credit metrics |
| | `V_AI_EXTRACT_CREDIT_TREND` | Daily credit consumption trend |
| | `V_AI_EXTRACT_CREDIT_SUMMARY` | Aggregate credit stats (total, avg, min, max) |
| | `V_AI_EXTRACT_TOKEN_DISTRIBUTION` | Token count bucketed into ranges |
| | `V_AI_EXTRACT_COST_PER_DOC` | Credits per unique document (total calls / unique docs) |
| | `V_AI_EXTRACT_MODEL_COST` | Credit breakdown by model version |
| `13_cost_attribution.sql` | `UDF_PDF_PAGE_COUNT` | Python UDF — counts pages in a staged PDF via `pypdfium2` |
| | `SP_POPULATE_DOC_METADATA` | Stored proc — backfills `PAGE_COUNT` and `FILE_SIZE_BYTES` for all docs |
| | `V_DOC_TYPE_FIELD_COUNTS` | Field count per document type (from extraction prompt) |
| | `V_AI_EXTRACT_COST_PER_PDF` | Per-PDF cost attribution: joins billing → query history → document |
| | `V_AI_EXTRACT_COST_DRIVERS` | Aggregated cost drivers by doc type with correlations |

### New Columns on RAW_DOCUMENTS

| Column | Type | Source |
|---|---|---|
| `PAGE_COUNT` | INT | Backfilled via `UDF_PDF_PAGE_COUNT` (reads PDF from stage) |
| `FILE_SIZE_BYTES` | INT | Backfilled from `DIRECTORY(@DOCUMENT_STAGE)` |

### Cost Page (Streamlit)

The **Cost** page (`5_Cost.py`) provides:
- Credit consumption trend (daily bar chart)
- Per-document cost metrics (credits/doc)
- Token distribution histogram
- Model cost breakdown
- **Cost Drivers** — per-PDF attribution showing what makes a PDF cost more (page count, tokens, fields extracted, content density)
- Scatter plots: Pages vs Credits, Tokens vs Credits
- Per-PDF detail table

### Key Findings (from 140 sample documents)

| Doc Type | Avg Pages | Avg Tokens | Avg Credits | Credits/Page | Token↔Credit Corr. |
|---|---|---|---|---|---|
| INVOICE | 1.2 | 1,583 | 0.007913 | 0.006688 | 1.0 |
| UTILITY_BILL | 1.0 | 1,335 | 0.006674 | 0.006674 | 1.0 |
| CONTRACT | 1.0 | 1,242 | 0.006211 | 0.006211 | 1.0 |
| LEASE | 1.0 | 1,237 | 0.006183 | 0.006183 | 1.0 |
| RECEIPT | 1.0 | 1,224 | 0.006118 | 0.006118 | 1.0 |

**Token↔Credit correlation = 1.0** across all document types, confirming AI_EXTRACT credits are purely token-based (no fixed per-call overhead). **Page↔Token correlation = 0.9754** for invoices (more pages = proportionally more tokens).

---

## Cost Estimate

AI_EXTRACT pricing is based on Cortex AI tokens:

| Component | Token Cost |
|---|---|
| Each page of a PDF/DOCX/TIFF | 970 tokens |
| Each image file (PNG, JPG, etc.) | 970 tokens (= 1 page) |
| Input prompt tokens | Variable (typically small) |
| Output tokens | Variable |

**Measured cost from this POC** (140 documents across 5 types):

| Metric | Value |
|---|---|
| Average credits per single-page extraction | ~0.006–0.008 |
| Average tokens per single-page extraction | ~1,200–1,600 |
| Credits for 100 single-page invoices (13 fields) | ~0.79 credits |
| Credits for 100 single-page utility bills (19 fields) | ~0.67 credits |
| What drives credit variation | Page count (0.97 corr.), content density, field count |

> **Note:** Credits are reported in Snowflake credits, not USD. Your contract's credit price determines the dollar cost. Use the Cost page in the Streamlit dashboard to see real-time credit consumption from `CORTEX_AI_FUNCTIONS_USAGE_HISTORY`.

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

To remove all POC objects from your account, use the parameterized teardown script:

```bash
# Interactive teardown (prompts for confirmation)
bash teardown_poc.sh --connection my_account

# Or with custom names
POC_DB=MY_DB POC_WH=MY_WH bash teardown_poc.sh --connection my_account

# Or via Makefile
make teardown CONNECTION=my_account
```

The script drops the database, warehouse, compute pool, and role. It prompts for
confirmation before proceeding.

**Manual cleanup** (raw SQL, if the script isn't available):

```sql
DROP DATABASE IF EXISTS AI_EXTRACT_POC;
DROP WAREHOUSE IF EXISTS AI_EXTRACT_WH;
DROP COMPUTE POOL IF EXISTS AI_EXTRACT_POC_POOL;
-- Optionally: DROP EXTERNAL ACCESS INTEGRATION IF EXISTS PYPI_ACCESS_INTEGRATION;
```

---

## Validating the Deployment (Tests)

The POC includes a comprehensive test suite (1,098 tests across 44 files) that verifies every SQL object, data quality, extraction pipeline, writeback workflow, review logic, RBAC permissions, concurrency, confidence scoring, multi-doc-type isolation, all Streamlit pages across all three Snowflake clouds (AWS, Azure, GCP), and a full user workflow E2E test. Running tests after deployment proves everything works end-to-end.

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
# Use ACCOUNTADMIN for initial setup; switch to AI_EXTRACT_APP after deploy
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

The default connection name is `default`. Override it with the `POC_CONNECTION` env var or the `--connection` / `-c` flag. You can also override the database, schema, and warehouse names:

```bash
export POC_CONNECTION=my_account   # Snowflake connection name from config.toml
export POC_DB=AI_EXTRACT_POC       # Database name (default: AI_EXTRACT_POC)
export POC_SCHEMA=DOCUMENTS        # Schema name (default: DOCUMENTS)
export POC_WH=AI_EXTRACT_WH        # Warehouse name (default: AI_EXTRACT_WH)
export POC_ROLE=AI_EXTRACT_APP     # Role name (default: AI_EXTRACT_APP)
```

### Install Test Dependencies

```bash
cd poc

# Create venv and install all dependencies (test + app)
uv sync --all-groups

# Install Playwright browser (needed for E2E tests only)
uv run playwright install chromium
```

> **Note:** `uv.lock` is committed to the repo, so `uv sync` produces a deterministic install matching the exact versions used during development and testing.

### Test Infrastructure (`conftest.py`)

The root-level `conftest.py` provides shared test infrastructure:

- **Snowflake connection fixture** (`sf_conn`, `sf_cursor`) — connects using the `POC_CONNECTION` env var (default `default`), executes `USE ROLE` with `POC_ROLE` (default `AI_EXTRACT_APP`), and sets the active database, schema, and warehouse
- **Connection factory fixture** (`sf_conn_factory`) — returns a callable that creates fresh Snowflake connections, used by load and concurrency tests for true multi-connection parallelism
- **Streamlit server lifecycle** — automatically starts a local Streamlit server on port 8504 when E2E tests are selected, kills stale port holders, and waits for the server to be ready
- **Environment variable configuration** — all names (database, schema, warehouse, role, connection) are configurable via `POC_DB`, `POC_SCHEMA`, `POC_WH`, `POC_ROLE`, and `POC_CONNECTION` env vars

### Streamlit Secrets (E2E Tests Only)

The E2E tests run a local Streamlit server that needs Snowflake credentials. Create `poc/streamlit/.streamlit/secrets.toml`:

```toml
# poc/streamlit/.streamlit/secrets.toml

# Option A: SSO / browser-based login
[connections.snowflake]
account = "YOUR_ORG-YOUR_ACCOUNT"
user = "YOUR_USERNAME"
authenticator = "externalbrowser"
warehouse = "AI_EXTRACT_WH"
database = "AI_EXTRACT_POC"
schema = "DOCUMENTS"
role = "AI_EXTRACT_APP"

# Option B: Programmatic Access Token (PAT) — headless, no browser popup
# [connections.snowflake]
# account = "YOUR_ORG-YOUR_ACCOUNT"
# user = "YOUR_USERNAME"
# authenticator = "programmatic_access_token"
# token = "YOUR_PAT_TOKEN_HERE"
# warehouse = "AI_EXTRACT_WH"
# database = "AI_EXTRACT_POC"
# schema = "DOCUMENTS"
# role = "AI_EXTRACT_APP"
```

> **Important:** When using PAT auth, the key must be `token` (not `password`). Generate a PAT in Snowsight under **User Menu > Preferences > Programmatic Access Tokens**.

> This file is gitignored and never committed. Each person running E2E tests creates their own.

### Running the Tests

From the `poc/` directory:

**Tier 1 — Core SQL + Data Quality tests** (~60 seconds, no browser needed):

```bash
uv run pytest tests/test_config.py tests/test_sql_integration.py tests/test_data_validation.py tests/test_deployment_readiness.py tests/test_extraction_pipeline.py -v
```

**Tier 2 — Writeback + Review tests** (~90 seconds):

```bash
uv run pytest tests/test_writeback_integration.py tests/test_writeback_data_validation.py tests/test_review_helpers.py tests/test_sql_parity.py -v
```

**Tier 3 — Security, Performance, Resilience** (~2-3 minutes):

```bash
uv run pytest tests/test_rbac_permissions.py tests/test_performance.py tests/test_teardown_idempotency.py -v
```

**Tier 4 — Load, Concurrency, Edge Cases** (~3-5 minutes):

```bash
uv run pytest tests/test_load_stress.py tests/test_multi_user_concurrency.py tests/test_data_drift.py tests/test_edge_cases.py -v
```

**Tier 5 — All non-E2E tests at once** (~5-8 minutes):

```bash
uv run pytest tests/ --ignore=tests/test_e2e -v
```

**Tier 6 — Full suite including E2E browser tests** (~10 minutes):

```bash
# Start the local Streamlit server first (in a separate terminal):
cd poc && uv run streamlit run streamlit/streamlit_app.py --server.port 8504 --server.headless true

# Then run non-E2E tests + each E2E file separately (recommended):
cd poc
uv run pytest tests/ --ignore=tests/test_e2e -v
uv run pytest tests/test_e2e/test_poc_landing.py -v
uv run pytest tests/test_e2e/test_poc_dashboard.py -v
uv run pytest tests/test_e2e/test_poc_document_viewer.py -v
uv run pytest tests/test_e2e/test_poc_analytics.py -v
uv run pytest tests/test_e2e/test_poc_review.py -v
uv run pytest tests/test_e2e/test_poc_admin.py -v
uv run pytest tests/test_e2e/test_poc_multi_doc.py -v
```

> **Why run E2E files separately?** Playwright's Chromium process can become unstable after
> 30+ sequential navigations to a Streamlit app (EPIPE errors). Running each file in its own
> pytest invocation gives each file a fresh browser process. The non-E2E tests can all run
> together since they don't use a browser.

> **Why a local server?** The E2E tests use Playwright to drive a real browser against the
> Streamlit app. The SPCS-hosted dashboard sits behind Snowflake authentication, which
> Playwright cannot negotiate. The local server connects to Snowflake via your
> `secrets.toml` credentials and serves the same pages on `localhost:8504`.

**Tier 7 — Clean-room: teardown, redeploy, test** (proves scripts work from scratch):

```bash
# 1. Tear down existing POC
snow sql -c my_account -f poc/teardown_poc.sql

# 2. Redeploy from scratch
./poc/deploy_poc.sh --connection my_account

# 3. Start local Streamlit server (separate terminal, for E2E tests)
cd poc && uv run streamlit run streamlit/streamlit_app.py --server.port 8504 --server.headless true

# 4. Run all tests (E2E files separately — see note above)
cd poc
uv run pytest tests/ --ignore=tests/test_e2e -v
uv run pytest tests/test_e2e/test_poc_landing.py -v
uv run pytest tests/test_e2e/test_poc_dashboard.py -v
uv run pytest tests/test_e2e/test_poc_document_viewer.py -v
uv run pytest tests/test_e2e/test_poc_analytics.py -v
uv run pytest tests/test_e2e/test_poc_review.py -v
uv run pytest tests/test_e2e/test_poc_admin.py -v
uv run pytest tests/test_e2e/test_poc_multi_doc.py -v
```

### What the Tests Verify

| Test File | Count | What It Checks |
|---|---|---|
| `test_admin_builder.py` | 16 | Admin page builder logic: prompt generation, field labels, review fields, table schema |
| `test_admin_config_management.py` | 14 | Admin config CRUD: insert, update, duplicate detection, delete/reinsert, null handling |
| `test_analytics_queries.py` | 10 | Analytics page SQL queries: vendor breakdown, monthly trends, aging, top items |
| `test_batch_extract_integration.py` | 16 | Batch extraction pipeline: stored proc, parallel files, error recovery |
| `test_confidence_scoring.py` | 43 | Confidence score calculations across document types |
| `test_config.py` | 20 | Config module: env var overrides, defaults, connection name resolution |
| `test_config_functions.py` | 31 | Config function tests: label parsing, type mapping, field name extraction |
| `test_config_helpers.py` | 39 | Config helper tests: variant parsing, doc type queries (unit — no Snowflake) |
| `test_contract_extraction.py` | 37 | Contract extraction quality: field accuracy, party names, dates, values |
| `test_cross_doc_isolation.py` | 14 | Cross-doc-type isolation: no data leakage between INVOICE/CONTRACT/RECEIPT |
| `test_dashboard_queries.py` | 12 | Dashboard page SQL queries: KPI cards, recent documents, doc_type filtering |
| `test_data_drift.py` | 13 | Boundary values (Unicode, large strings, max precision), NULL COALESCE, schema evolution |
| `test_data_validation.py` | 36 | Data quality: completeness, no NULLs in required fields, amounts > 0, valid dates |
| `test_deployment_readiness.py` | 12 | Pre-flight: Cortex access, SSE encryption, staged files, EAI, compute pool |
| `test_document_type_flexibility.py` | 65 | Multi-doc-type support: config-driven extraction, label mapping, type switching |
| `test_document_viewer_queries.py` | 14 | Document Viewer page SQL queries: filtering, detail view, PDF rendering |
| `test_edge_cases.py` | 33 | AUTOINCREMENT gaps, transaction rollbacks, SQL injection, empty-table cold start |
| `test_extraction_pipeline.py` | 22 | Live AI_EXTRACT (entity + table mode), stored proc execution, idempotency |
| `test_load_stress.py` | 7 | Bulk inserts (50 sequential), concurrent writers (5 threads) |
| `test_multi_user_concurrency.py` | 7 | Interleaved reviews, simultaneous writes with threading.Barrier, race conditions |
| `test_normalize_unit.py` | 49 | Normalization unit tests: currency parsing, date formats, field cleaning |
| `test_performance.py` | 12 | Query latency benchmarks: views return within thresholds |
| `test_phase_improvements.py` | 21 | Phase improvement validation: all enhancement features working |
| `test_rbac_negative.py` | 17 | RBAC negative/deny tests: unauthorized role cannot access protected objects |
| `test_rbac_permissions.py` | 22 | Role-based access control: table grants, view grants, stage access, procedure execute |
| `test_receipt_extraction.py` | 33 | Receipt extraction quality: store name, total, payment method, line items |
| `test_review_helpers.py` | 55 | Review page helper functions: data loading, save logic, status transitions, range/date validation |
| `test_sp_error_handling.py` | 12 | Stored procedure error handling: invalid inputs, missing files, exception paths |
| `test_spcs_deployment.py` | 80 | SPCS deployment, view ordering, per-page SQL validation, write SQL linting |
| `test_sql_integration.py` | 52 | Every SQL object: tables, columns, PKs, views, stream, task, stored proc |
| `test_sql_parity.py` | 10 | SQL script DDL matches live Snowflake objects |
| `test_teardown_idempotency.py` | 15 | Teardown script is idempotent (safe to run multiple times) |
| `test_user_workflow_e2e.py` | 36 | Full user workflow: approve, correct, re-edit, audit trail, rollback + edge cases (injection, unicode, overflow, boundary values) + TRY_TO_* regression guard |
| `test_utility_bill_extraction.py` | 49 | Utility bill extraction quality: account number, service dates, charges |
| `test_validation_rules.py` | 27 | Validation rule tests: per-doc-type rules, boundary conditions |
| `test_writeback_data_validation.py` | 20 | Writeback data quality, corrected field types, review status values |
| `test_writeback_integration.py` | 19 | INVOICE_REVIEW table operations, V_INVOICE_SUMMARY, COALESCE override |
| `test_e2e/` (7 files) | 108 | Playwright browser tests: all 5 pages + Admin + multi-doc flows + validation, no exceptions |
| **Total** | **~1,098** | **990 non-E2E + 108 E2E across 44 test files** |

### Cross-Cloud Verification

This kit has been deployed and tested from scratch on all three Snowflake clouds. Every account was provisioned with identical infrastructure: database, schema, warehouse, tables (including `INVOICE_REVIEW` and `DOCUMENT_TYPE_CONFIG`), views (including `V_INVOICE_SUMMARY` and `V_DOCUMENT_LEDGER` with `doc_type`), the `AI_EXTRACT_APP` RBAC role with full grants, and all Streamlit files uploaded to `STREAMLIT_STAGE`.

| Cloud | Region | Non-E2E | E2E | Total |
|---|---|---|---|---|
| **AWS** | US East 1 (Virginia) | 990 passed | 106 passed, 2 skipped | **1,098 total** |
| **Azure** | East US 2 | 846 passed, 142 skipped, 2 xfailed | — | **990 total** |
| **GCP** | US Central 1 | 846 passed, 142 skipped, 2 xfailed | — | **990 total** |

GCP and other non-primary regions require cross-region inference, which `01_setup.sql` enables automatically.

#### Cross-Cloud Testing Commands

To run the full test suite against a non-default account, set the `POC_CONNECTION` environment variable along with the database, schema, warehouse, and role:

```bash
cd poc

# Run against a specific connection
POC_CONNECTION=my_other_conn POC_DB=AI_EXTRACT_POC POC_SCHEMA=DOCUMENTS \
  POC_WH=AI_EXTRACT_WH POC_ROLE=AI_EXTRACT_APP \
  uv run pytest tests/ --ignore=tests/test_e2e -v

# E2E tests (starts a local Streamlit server pointed at the target account)
POC_CONNECTION=my_other_conn POC_DB=AI_EXTRACT_POC POC_SCHEMA=DOCUMENTS \
  POC_WH=AI_EXTRACT_WH POC_ROLE=AI_EXTRACT_APP \
  uv run pytest tests/test_e2e/ -v
```

**Shortcut — run all three clouds in one command:**

```bash
make test-cross-cloud                    # uses CLOUDS variable (default: "default")
make test-cross-cloud CLOUDS="conn_a conn_b conn_c"  # test multiple connections
```

This iterates each connection, runs all non-E2E tests, and prints a per-cloud PASSED / FAILED summary.

> **Note:** The `POC_ROLE` env var tells `conftest.py` to `USE ROLE <role>` before running any queries. This ensures tests run with the same least-privilege role used by the app, not ACCOUNTADMIN.

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
6. **Explore the reference demo** — A full-featured invoice processing app with generated test data, additional analytics, and E2E tests (see the root `README.md` in this repository)

---

## Building a Production PDF Extraction Pipeline

This section provides a comprehensive, step-by-step guide for building a fully end-to-end document extraction pipeline on Snowflake — from raw PDFs landing in cloud storage through structured, validated, human-reviewed data ready for downstream consumption.

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE ACCOUNT                                │
│                                                                         │
│  ┌─────────────┐    ┌───────────────┐    ┌────────────────────────────┐ │
│  │ Cloud Storage│    │  Internal     │    │  AI_EXTRACT (Cortex)       │ │
│  │ (S3/Azure/  │───►│  Stage        │───►│  Entity + Table extraction │ │
│  │  GCS)       │    │  (SSE enc.)   │    └────────────┬───────────────┘ │
│  └─────────────┘    └───────────────┘                 │                 │
│        │                    │                          ▼                 │
│        │             ┌──────┴──────┐    ┌────────────────────────────┐  │
│        │             │ RAW_DOCUMENTS│    │  EXTRACTED_FIELDS          │  │
│        │             │ (file        │    │  EXTRACTED_TABLE_DATA      │  │
│        │             │  registry)   │    │  (structured output)       │  │
│        │             └──────┬──────┘    └────────────┬───────────────┘  │
│        │                    │                         │                  │
│  ┌─────┴─────┐       ┌─────┴──────┐    ┌────────────┴───────────────┐  │
│  │ Snowpipe / │       │  Stream    │    │  Analytical Views          │  │
│  │ Auto-ingest│       │  + Task    │    │  (V_DOCUMENT_LEDGER,       │  │
│  │ (optional) │       │  (5-min)   │    │   V_DOCUMENT_SUMMARY,      │  │
│  └────────────┘       └────────────┘    │   V_LINE_ITEM_DETAIL,      │  │
│                                         │   V_EXTRACTION_STATUS)     │  │
│                                         └────────────┬───────────────┘  │
│                                                      │                  │
│  ┌────────────────────────────┐                      │                  │
│  │  DOCUMENT_TYPE_CONFIG      │                      │                  │
│  │  (prompts, labels, types)  │                      │                  │
│  └────────────────────────────┘                      │                  │
│                                                      │                  │
│  ┌────────────────────────────┐    ┌─────────────────┴──────────────┐  │
│  │  INVOICE_REVIEW            │    │  Streamlit Dashboard           │  │
│  │  LINE_ITEM_REVIEW          │◄───│  (review, approve, correct)    │  │
│  │  (append-only audit trails)│    └────────────────────────────────┘  │
│  └────────────────────────────┘                      │                  │
│                                                      │                  │
│  ┌────────────────────────────┐    ┌─────────────────┴──────────────┐  │
│  │  Cost Observability        │    │  Extraction Failure Alert      │  │
│  │  (9 views from billing +   │    │  (fires on 3+ failures/24h)   │  │
│  │   query history)           │    └────────────────────────────────┘  │
│  └────────────────────────────┘                      │                  │
│                                                      │                  │
│                                                      ▼                  │
│                                    ┌────────────────────────────────┐  │
│                                    │  Downstream: ERP, AP system,   │  │
│                                    │  Data Warehouse, Snowflake     │  │
│                                    │  Data Sharing                  │  │
│                                    └────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

### Phase 1: Foundation (Day 1)

**Goal:** Get Snowflake objects created and a single document extracting correctly.

| Step | Script | What It Does |
|------|--------|-------------|
| 1.1 | `01_setup.sql` | Creates dedicated role (`AI_EXTRACT_APP`), database, schema, X-SMALL warehouse, and internal stage with **SNOWFLAKE_SSE** encryption (required for AI_EXTRACT). Enables cross-region inference. |
| 1.2 | `02_tables.sql` | Creates three core tables: `RAW_DOCUMENTS` (file registry), `EXTRACTED_FIELDS` (entity-level data — 10 flexible columns), `EXTRACTED_TABLE_DATA` (line-item data — 5 flexible columns). Both extraction tables include a `raw_extraction` / `raw_line_data` VARIANT column for full fidelity. |
| 1.3 | Upload docs | Stage 5-20 representative sample documents in `DOCUMENT_STAGE`. Use Snowsight drag-and-drop, `snow stage put`, or the `PUT` command. Script 02 auto-registers them. |
| 1.4 | `03_test_single_file.sql` | **Interactive exploration** — run `AI_EXTRACT` on a single file, inspect the raw JSON output, and iterate on your prompt until the extraction matches your expectations. This is the most important step. |

**Key decisions at this phase:**
- What fields do you need? Map them to `field_1`..`field_10` (rename later).
- Does your document have a line-item table? Define its schema for table extraction.
- What data types should each field be? (DATE, NUMBER, VARCHAR)

**Validation checkpoint:** Run `03_test_single_file.sql` against 3-5 different documents. If extraction accuracy is below 90%, refine your prompt before proceeding.

### Phase 2: Batch Extraction (Day 1-2)

**Goal:** Extract all staged documents in bulk.

| Step | Script | What It Does |
|------|--------|-------------|
| 2.1 | `04_batch_extract.sql` | Processes all unextracted files (`extracted = FALSE`) in a single statement using `LATERAL` subqueries with `AI_EXTRACT`. Inserts into both `EXTRACTED_FIELDS` and `EXTRACTED_TABLE_DATA`. Marks files as extracted. |
| 2.2 | `05_views.sql` | Creates 6 analytical views: `V_EXTRACTION_STATUS` (pipeline health), `V_DOCUMENT_LEDGER` (enriched document view with aging), `V_SUMMARY_BY_VENDOR`, `V_MONTHLY_TREND`, `V_TOP_LINE_ITEMS`, `V_AGING_SUMMARY`. |

**Key patterns:**
- The batch script uses `TRY_TO_DATE`, `TRY_TO_NUMBER`, and `REGEXP_REPLACE` to safely parse AI output into typed columns
- Table extraction uses `LATERAL FLATTEN` on parallel arrays with index-aligned `WHERE` to unnest rows
- Views use generic `field_N` references — rename them to meaningful names (`vendor_name`, `invoice_date`, etc.) once your schema is stable

**Validation checkpoint:** Query `V_EXTRACTION_STATUS` — you should see `PENDING_FILES = 0` and `FAILED_FILES = 0`. Spot-check 10 documents in `EXTRACTED_FIELDS` against the source PDFs.

### Phase 3: Automation (Day 2-3)

**Goal:** New documents are automatically extracted within 5 minutes of being staged.

| Step | Script | What It Does |
|------|--------|-------------|
| 3.1 | `06_automate.sql` | Creates the automation layer: an append-only **Stream** on `RAW_DOCUMENTS`, a **Stored Procedure** (`SP_EXTRACT_NEW_DOCUMENTS`) that wraps the batch logic, and a **Task** that fires every 5 minutes when the stream has new data. Also creates `SP_EXTRACT_BY_DOC_TYPE` — a Python SP that reads prompts from config and supports any document type. |

**How the automated pipeline works:**

```
New PDF uploaded to stage
        │
        ▼
INSERT INTO RAW_DOCUMENTS  ──►  Stream detects new row
        │                              │
        ▼                              ▼
  File registered              Task fires (5-min schedule)
                                       │
                                       ▼
                              SP_EXTRACT_NEW_DOCUMENTS()
                                       │
                                       ▼
                              AI_EXTRACT on each new file
                                       │
                                       ▼
                              INSERT into EXTRACTED_FIELDS
                              INSERT into EXTRACTED_TABLE_DATA
                              UPDATE RAW_DOCUMENTS SET extracted = TRUE
```

**To onboard new documents after automation is live:**

```sql
-- 1. Upload
PUT file:///path/to/new_doc.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE;

-- 2. Refresh stage directory
ALTER STAGE DOCUMENT_STAGE REFRESH;

-- 3. Register (idempotent — skips duplicates)
INSERT INTO RAW_DOCUMENTS (file_name, file_path, staged_at)
SELECT RELATIVE_PATH, '@DOCUMENT_STAGE/' || RELATIVE_PATH, CURRENT_TIMESTAMP()
FROM DIRECTORY(@DOCUMENT_STAGE) d
WHERE NOT EXISTS (SELECT 1 FROM RAW_DOCUMENTS r WHERE r.file_name = d.RELATIVE_PATH);

-- 4. Wait for task (max 5 min) or trigger immediately:
EXECUTE TASK EXTRACT_NEW_DOCUMENTS_TASK;
```

**Validation checkpoint:** Upload a new test document, wait 5 minutes, then check `V_EXTRACTION_STATUS` — `PENDING_FILES` should return to 0.

### Phase 4: Multi-Document Types (Day 3-4)

**Goal:** Extract different document types (invoices, contracts, receipts, utility bills) with type-specific prompts and validation.

| Step | Script | What It Does |
|------|--------|-------------|
| 4.1 | `09_document_types.sql` | Creates `DOCUMENT_TYPE_CONFIG` table with per-type configuration: extraction prompts, field labels, table extraction schemas, review field definitions, and validation rules. Seeds 4 types: INVOICE, CONTRACT, RECEIPT, UTILITY_BILL. |
| 4.2 | Use `SP_EXTRACT_BY_DOC_TYPE` | Call `CALL SP_EXTRACT_BY_DOC_TYPE('INVOICE')` to extract all documents of a given type using its configured prompt. |

**Adding a new document type:**

```sql
-- Insert config for your new type
INSERT INTO DOCUMENT_TYPE_CONFIG (doc_type, extraction_prompt, field_labels, active)
VALUES (
    'MEDICAL_CLAIM',
    '{
        "patient_name": "Full name of the patient",
        "provider_name": "Name of the healthcare provider",
        "claim_date": "Date of service in YYYY-MM-DD format",
        "diagnosis_code": "ICD-10 diagnosis code",
        "total_billed": "Total amount billed as a number only",
        "insurance_paid": "Amount paid by insurance as a number only",
        "patient_responsibility": "Amount owed by patient as a number only"
    }',
    '{"field_1":"patient_name","field_2":"provider_name","field_3":"claim_date",
      "field_4":"diagnosis_code","field_5":"total_billed","field_6":"insurance_paid",
      "field_7":"patient_responsibility"}',
    TRUE
);

-- Tag your documents
UPDATE RAW_DOCUMENTS SET doc_type = 'MEDICAL_CLAIM'
WHERE file_name ILIKE '%claim%';

-- Extract
CALL SP_EXTRACT_BY_DOC_TYPE('MEDICAL_CLAIM');
```

**Validation rules** in `DOC_TYPE_CONFIG.VALIDATION_RULES` support:
- `type`: DATE, NUMBER, VARCHAR
- `required`: true/false
- `min` / `max`: numeric range bounds
- `pattern`: regex pattern (e.g., `"^INV-\\d+"` for invoice numbers)
- `date_range_days`: maximum age in days

### Phase 5: Human Review & Corrections (Day 4-5)

**Goal:** Humans can review, approve, reject, or correct extracted data through a Streamlit UI.

| Step | Script | What It Does |
|------|--------|-------------|
| 5.1 | `08_writeback.sql` | Creates `INVOICE_REVIEW` table (append-only audit trail — never UPDATE/DELETE, only INSERT). Creates `V_DOCUMENT_SUMMARY` view that joins extraction data with the latest review per document using `ROW_NUMBER()`. |
| 5.2 | Deploy Streamlit | The Review page provides inline `st.data_editor` editing with writeback to Snowflake. |

**How the review workflow works:**

```
Extracted data appears in Streamlit "Review" page
        │
        ▼
Reviewer edits fields inline (data_editor)
        │
        ▼
Click "Save" → INSERT INTO INVOICE_REVIEW
(original values + corrections + reviewer + timestamp)
        │
        ▼
V_DOCUMENT_SUMMARY automatically shows corrected values
(COALESCE: correction > original)
        │
        ▼
Downstream consumers read V_DOCUMENT_SUMMARY
(always get best-known values)
```

**Key design decisions:**
- **Append-only:** Every review is an INSERT, creating a full audit trail. The view picks the latest review per document via `ROW_NUMBER()`.
- **Corrections stored as VARIANT:** The `corrections` column stores a JSON object of changed fields, supporting any document type without schema changes.
- **Three-tier COALESCE:** View prefers VARIANT correction → legacy correction → original extraction value.

### Phase 6: Dashboard & Deployment (Day 5-6)

**Goal:** Deploy a production Streamlit dashboard accessible via Snowsight.

| Step | Script | What It Does |
|------|--------|-------------|
| 6.1 | `07_deploy_streamlit.sql` | Creates compute pool, external access integration (for pip), and the Streamlit app on Container Runtime. |

**The dashboard includes 5 pages:**

| Page | Purpose |
|------|---------|
| **Landing** | Pipeline status KPIs, extraction summary, architecture diagram |
| **Document Viewer** | Browse documents, view extracted fields alongside rendered PDF, filter by type/date |
| **Analytics** | Charts by vendor, time period, line items, aging buckets |
| **Review** | Inline editing with `st.data_editor`, approve/reject workflow, audit trail |
| **Admin** | Document type configuration, field mappings, validation rules |

**Deployment options:**

| Method | Best For | How |
|--------|----------|-----|
| **SiS (Streamlit in Snowflake)** | Most use cases | `07_deploy_streamlit.sql` or `snow streamlit deploy` |
| **Local Streamlit** | Development and testing | `streamlit run streamlit/streamlit_app.py` |
| **SPCS (Container Services)** | Custom containers, GPU | Only if you need packages not available via pip |

### Phase 7: Production Hardening (Day 6-7)

**Goal:** Lock down security, set cost guardrails, and enable monitoring.

| Step | Script | What It Does |
|------|--------|-------------|
| 7.1 | `10_harden.sql` | Transfers ownership to `SYSADMIN`, enables managed access, revokes `PUBLIC` grants, removes broad account privileges from the POC role, sets up future grants. |
| 7.2 | `11_alerts.sql` | Creates `EXTRACTION_FAILURE_ALERT` that fires when 3+ extractions fail in 24 hours. Logs to `EXTRACTION_ALERT_HISTORY`. Creates `V_EXTRACTION_HEALTH` view and `SP_CHECK_EXTRACTION_HEALTH()` for on-demand monitoring. |

**Security hardening checklist:**

- [ ] Run `10_harden.sql` to lock down privileges
- [ ] Resource monitor is set (100 credits/month default — adjust for your workload)
- [ ] `AI_EXTRACT_APP` role has minimum necessary privileges (no `CREATE DATABASE`, no `CREATE WAREHOUSE`)
- [ ] All access to data goes through `AI_EXTRACT_APP` role, not `ACCOUNTADMIN`
- [ ] Stage uses `SNOWFLAKE_SSE` encryption (verified by script 01)
- [ ] Network rule limits egress to `pypi.org` only (for Streamlit pip installs)

**Cost controls:**

| Control | Where | Default |
|---------|-------|---------|
| Resource monitor | `10_harden.sql` | 100 credits/month, suspend at 100% |
| Warehouse auto-suspend | `01_setup.sql` | 120 seconds |
| Warehouse size | `01_setup.sql` | X-SMALL (larger sizes don't improve AI_EXTRACT performance) |
| Task frequency | `06_automate.sql` | Every 5 minutes (adjustable) |
| Compute pool auto-suspend | `07_deploy_streamlit.sql` | 300 seconds |

### Phase 8: Integration & Scaling

**Goal:** Connect extraction output to downstream systems and handle high volumes.

**Option A — Snowflake Data Sharing:**
```sql
-- Share extracted data with another Snowflake account
CREATE SHARE AI_EXTRACT_SHARE;
GRANT USAGE ON DATABASE AI_EXTRACT_POC TO SHARE AI_EXTRACT_SHARE;
GRANT USAGE ON SCHEMA DOCUMENTS TO SHARE AI_EXTRACT_SHARE;
GRANT SELECT ON VIEW V_DOCUMENT_SUMMARY TO SHARE AI_EXTRACT_SHARE;
```

**Option B — External table / Snowpipe for auto-ingest:**
```sql
-- Auto-ingest from S3 (replaces manual PUT + REFRESH)
CREATE PIPE DOCUMENT_PIPE AUTO_INGEST = TRUE AS
    COPY INTO RAW_DOCUMENTS (file_name, file_path, staged_at)
    FROM (
        SELECT
            METADATA$FILENAME,
            '@DOCUMENT_STAGE/' || METADATA$FILENAME,
            CURRENT_TIMESTAMP()
        FROM @DOCUMENT_STAGE
    );
```

**Option C — Direct query from downstream:**
```sql
-- Any Snowflake user/role with access can query the views
SELECT * FROM AI_EXTRACT_POC.DOCUMENTS.V_DOCUMENT_SUMMARY
WHERE doc_type = 'INVOICE' AND review_status = 'APPROVED';
```

**Scaling considerations:**

| Volume | Approach | Notes |
|--------|----------|-------|
| < 100 docs/day | Default setup | X-SMALL WH, 5-min task |
| 100-1,000 docs/day | Increase task frequency | `ALTER TASK ... SET SCHEDULE = '1 MINUTE'` |
| 1,000-10,000 docs/day | Batch with larger windows | Process in hourly batches to reduce task overhead |
| 10,000+ docs/day | Parallel extraction | Split by doc_type, run multiple SPs concurrently |

### Complete Script Execution Order

For a full deployment from scratch:

```
01_setup.sql          ← ACCOUNTADMIN (creates role, DB, WH, stage)
        │
02_tables.sql         ← AI_EXTRACT_APP (creates tables, registers files)
        │
   [Upload PDFs]      ← Stage your documents
        │
03_test_single_file   ← AI_EXTRACT_APP (interactive — tune prompts)
        │
04_batch_extract.sql  ← AI_EXTRACT_APP (bulk extraction)
        │
05_views.sql          ← AI_EXTRACT_APP (analytical views)
        │
06_automate.sql       ← AI_EXTRACT_APP (stream + task + SPs)
        │
07_deploy_streamlit   ← ACCOUNTADMIN (compute pool, Streamlit app)
        │
08_writeback.sql      ← AI_EXTRACT_APP (review table + summary views)
        │
09_document_types.sql ← AI_EXTRACT_APP (doc type config + seed data)
        │
10_harden.sql         ← ACCOUNTADMIN (lock down, resource monitor)
        │
11_alerts.sql         ← AI_EXTRACT_APP (failure monitoring)
```

Or use the automated deploy script:
```bash
POC_DB=MY_DB POC_WH=MY_WH ./deploy_poc.sh
```

---

## Customer Handoff Checklist

Use this checklist when transferring the POC kit to a customer or deploying in a new account.

### Before Sharing

- [ ] **Update CI badge** — Uncomment and update the badge URL at the top of this README to point to the customer's repository
- [ ] **Update `snowflake.yml`** — Change `query_warehouse` and `compute_pool` if the customer uses different names (only needed if deploying via `snow streamlit deploy`)
- [ ] **Verify connection config** — Ensure the customer has a `default` connection in `~/.snowflake/connections.toml`, or instruct them to set `POC_CONNECTION` to their connection name

### Customer Account Setup

1. **Verify prerequisites** — Standard Edition or higher, supported region (or cross-region enabled), ACCOUNTADMIN access for initial setup
2. **Run `deploy_poc.sh`** — This creates all objects (database, schema, warehouse, role, stage, tables, views, Streamlit app) in one command
3. **Upload sample documents** — Stage 5-20 customer documents in `DOCUMENT_STAGE`
4. **Run single-file extraction** — Use `03_test_single_file.sql` to validate prompts against the customer's document format
5. **Tune prompts** — Edit the `EXTRACTION_PROMPT` and `TABLE_PROMPT` in `DOC_TYPE_CONFIG` to match the customer's document layout
6. **Run batch extraction** — Execute `04_batch_extract.sql` to process all staged documents
7. **Review results** — Open the Streamlit dashboard and review extracted data for accuracy

### Customization Points

| What | Where | How |
|---|---|---|
| Object names (DB, schema, WH) | `deploy_poc.sh` env vars | `POC_DB=MY_DB POC_WH=MY_WH ./deploy_poc.sh` |
| Document types | `sql/09_document_types.sql` | Add/modify rows in `DOC_TYPE_CONFIG` |
| Extraction prompts | `DOC_TYPE_CONFIG.EXTRACTION_PROMPT` | JSON object mapping field names to extraction instructions |
| Validation rules | `DOC_TYPE_CONFIG.VALIDATION_RULES` | JSON object with field-level validation (type, required, range) |
| Streamlit branding | `streamlit/streamlit_app.py` | Edit titles, descriptions, and layout |

### Teardown

To remove all POC objects from the customer account:

```bash
./teardown_poc.sh
```

This drops the database, warehouse, compute pool, and role. The `PYPI_ACCESS_INTEGRATION` is intentionally left in place as other apps may use it.

---

## AI_EXTRACT Lessons Learned

Real-world experience building this POC across 4 document types and 130+ documents revealed patterns that aren't in the docs yet. These lessons apply to any AI_EXTRACT deployment.

### 1. AI Doesn't Always Follow Formatting Instructions

Even with explicit prompt instructions like "return dates in YYYY-MM-DD format", AI_EXTRACT may return:
- Dates as `MM/DD/YYYY`, `Month DD, YYYY`, or mixed formats
- Currency values with `$` prefixes (`$1,234.56` instead of `1234.56`)
- "None" or "N/A" strings instead of `0` for zero-balance fields
- Abbreviated company names (`PSE&G` instead of `Public Service Electric and Gas`)

**Solution:** Always post-process `raw_extraction` VARIANT data after extraction:

```sql
-- Normalize dates from MM/DD/YYYY to YYYY-MM-DD
UPDATE EXTRACTED_FIELDS
SET raw_extraction = OBJECT_INSERT(raw_extraction, 'service_from',
    REGEXP_REPLACE(raw_extraction:service_from::STRING,
        '(\\d{2})/(\\d{2})/(\\d{4})', '\\3-\\1-\\2'), TRUE)
WHERE doc_type = 'UTILITY_BILL'
  AND raw_extraction:service_from::STRING REGEXP '\\d{2}/\\d{2}/\\d{4}';

-- Strip currency symbols
UPDATE EXTRACTED_FIELDS
SET raw_extraction = OBJECT_INSERT(raw_extraction, 'total_amount_due',
    REPLACE(raw_extraction:total_amount_due::STRING, '$', ''), TRUE)
WHERE raw_extraction:total_amount_due::STRING LIKE '$%';

-- Replace "None" strings with "0"
UPDATE EXTRACTED_FIELDS
SET raw_extraction = OBJECT_INSERT(raw_extraction, 'balance_forward',
    '0', TRUE)
WHERE raw_extraction:balance_forward::STRING IN ('None', 'N/A', 'none');
```

**Key pattern:** `OBJECT_INSERT(variant, 'key', new_value, TRUE)` — the `TRUE` flag enables upsert (replace if key exists).

### 2. Confidence Scoring via OBJECT_CONSTRUCT

AI_EXTRACT doesn't return confidence scores natively. Add synthetic per-field confidence metadata to `raw_extraction` for downstream consumption:

```sql
UPDATE EXTRACTED_FIELDS
SET raw_extraction = OBJECT_INSERT(raw_extraction, '_confidence',
    OBJECT_CONSTRUCT(
        'vendor_name',    0.95,
        'invoice_number', 0.90,
        'total_amount',   0.85,
        'invoice_date',   0.88
    ), TRUE)
WHERE doc_type = 'INVOICE';
```

This stores a `_confidence` dictionary alongside the extracted fields, keyed by field name with scores from 0.0 to 1.0. Downstream apps can use these scores to flag low-confidence fields for human review.

### 3. Multi-Document-Type Data Loading

When loading documents for multiple types, register each file with its correct `doc_type` in `RAW_DOCUMENTS`:

```sql
-- Stage files organized by type
PUT file:///path/to/contracts/*.pdf @DOCUMENT_STAGE/contracts/ AUTO_COMPRESS=FALSE;
PUT file:///path/to/receipts/*.pdf @DOCUMENT_STAGE/receipts/ AUTO_COMPRESS=FALSE;

-- Register with correct doc_type
INSERT INTO RAW_DOCUMENTS (file_name, file_path, doc_type)
SELECT 'contract_001.pdf', '@DOCUMENT_STAGE/contracts/contract_001.pdf', 'CONTRACT';
```

The `doc_type` drives which extraction prompt is used (from `DOCUMENT_TYPE_CONFIG`), which labels are applied, and which validation rules are checked.

### 4. Ambiguous Column Names in JOINs

When joining `EXTRACTED_FIELDS` and `RAW_DOCUMENTS` (both have `file_name`), always qualify:

```sql
-- Wrong: "ambiguous column name 'FILE_NAME'"
SELECT file_name, raw_extraction FROM EXTRACTED_FIELDS e JOIN RAW_DOCUMENTS r ...

-- Right: qualify with alias
SELECT e.file_name, e.raw_extraction FROM EXTRACTED_FIELDS e JOIN RAW_DOCUMENTS r ...
```

### 5. Test Data Strategy

- **Don't insert synthetic records** if your tests count rows — it breaks count-based assertions
- **Modify existing records** for edge cases (e.g., NULL out a field on an existing row)
- **Replace `pytest.skip()` with real data** — deploy the data the test needs, then remove the skip
- **Use `autouse` fixtures** to check for required data and skip gracefully only when the data genuinely doesn't exist

### 6. Playwright E2E Testing with Streamlit

- **Glide Data Grid renders in `<canvas>`, not DOM text.** `st.dataframe` and `st.data_editor` use Glide Data Grid which paints cells on an HTML canvas. Playwright's `inner_text()` on the `stDataFrame` container returns an empty string. Workaround: read text from surrounding page chrome (selectboxes, markdown, full `page.inner_text("body")`) instead of the grid element itself. To verify doc-type names exist, click open a selectbox dropdown to expose its options in the DOM.
- **Streamlit selectbox defaults are alphabetical.** When `st.selectbox` is populated from a database query (e.g., configured doc types), the default selection is the first value in sort order — not necessarily the one you expect. Don't hardcode `"Invoices"` in assertions when `"Contracts"` comes first alphabetically. Use doc-type-agnostic patterns like regex `\(\d+ results?\)` instead.
- **Filter-specific labels aren't universal.** A label like "Vendor" only appears when INVOICE is selected; Contracts show "Party" instead. Assert on universal labels (e.g., "Status") or explicitly select the expected doc type before asserting on type-specific content.
- **E2E timing flakes vs real failures — retry once to distinguish.** If a test passes on retry, it's a timing flake (page not fully rendered). If it fails consistently, the test logic is wrong. One quick retry run saves hours of debugging.
- **`wait_for_timeout()` is necessary but fragile.** Streamlit re-renders asynchronously after data loads. A 2-3 second `page.wait_for_timeout()` after navigation helps, but isn't guaranteed. Prefer waiting for specific DOM elements (`page.locator('[data-testid="stDataFrame"]').count() > 0`) before asserting.

### 7. Cross-Cloud Deployment

- **SQL scripts don't upload files.** Running `01_setup.sql` through `08_teardown.sql` creates database objects but does NOT upload Streamlit files to the stage. Each cloud requires explicit `PUT` of all files (`.py`, `pyproject.toml`, `snowflake.yml`) before `CREATE STREAMLIT ... FROM '@STAGE'` will work.
- **ACCOUNTADMIN is required for infrastructure, not the app.** Compute pools, network rules, and External Access Integrations all require `ACCOUNTADMIN`. The app role (`AI_EXTRACT_APP`) can't create them. Pattern: ACCOUNTADMIN creates infra + grants, then switch to app role for `CREATE STREAMLIT`.
- **Test skip counts vary by data volume.** Azure/GCP with 100 documents skip ~142 tests that pass on AWS (130 documents). Skip guards like "at least N rows exist" cause this. The skipped tests aren't broken — they just lack the data to exercise those code paths.
- **AWS runs slower because it runs more tests.** 990 actual executions vs 846 on Azure/GCP. Combined with more rows per query (130 vs 100 docs), AWS takes ~2x the wall-clock time. Per-test speed is comparable across clouds.

---

## Secret Scanning & Security

This project includes automated secret detection to prevent accidental credential leaks.

### Pre-commit Hooks (Local)

Install pre-commit to scan for secrets on every commit:

```bash
pip install pre-commit detect-secrets
pre-commit install
```

The `.pre-commit-config.yaml` includes:
- **detect-secrets** — scans for high-entropy strings, AWS keys, private keys, and other credential patterns
- **detect-private-key** — catches `-----BEGIN * PRIVATE KEY-----` blocks
- **check-added-large-files** — prevents accidental commit of large binaries (>1 MB)

A `.secrets.baseline` file tracks known false positives (e.g., GitHub Actions `${{ secrets.* }}` references). To update the baseline after a legitimate change:

```bash
detect-secrets scan --baseline .secrets.baseline
detect-secrets audit .secrets.baseline
```

### CI/CD Secret Scanning

The GitHub Actions workflow (`.github/workflows/test.yml`) includes a **Secret Scan** job that runs on every push and PR:
1. Validates the `.secrets.baseline` against the current codebase
2. Greps for common secret patterns (AWS access keys, OpenAI keys, private key headers)
3. Fails the build if any unaudited secrets are found

### What's Already Protected

The `.gitignore` files exclude:
- `.env`, `.env.*` — environment variable files
- `credentials.json`, `service_account*.json` — cloud provider credentials
- `*.pem`, `*.key`, `*.p12`, `*.pfx` — certificate/key files
- `.streamlit/secrets.toml` — Streamlit connection secrets
- `.aws/`, `.gcp/` — cloud CLI config directories

---

## Cross-Cloud Deployment Notes

### Stage File Counts

When deploying across AWS, Azure, and GCP, verify that all clouds have the same stage file count:

```sql
SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE);  -- Should match across clouds
SELECT COUNT(*) FROM RAW_DOCUMENTS;                -- Should match stage count
```

**Common mismatch:** `deploy_poc.sh` prioritizes `data/invoices/` (100 invoice PDFs). Multi-type documents (contracts, receipts, utility bills) in `poc/sample_documents/` are uploaded separately. If you deployed before the multi-type upload step was added, run:

```sql
-- Upload missing multi-type docs (run from local machine)
PUT file:///path/to/poc/sample_documents/contract_*.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/poc/sample_documents/receipt_*.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/poc/sample_documents/utility_bill_*.pdf @DOCUMENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
ALTER STAGE DOCUMENT_STAGE REFRESH;
```

### PDF Preview Requirements

The Document Viewer page renders PDFs using `pypdfium2` inside Container Runtime. For it to work:

1. **`pypdfium2`** must be listed in both `snowflake.yml` and `pyproject.toml`
2. **`PYPI_ACCESS_INTEGRATION`** EAI must exist and be granted to the Streamlit app
3. **Compute pool** must be ACTIVE (`DESCRIBE COMPUTE POOL AI_EXTRACT_POC_POOL;`)
4. **File must exist on stage** — the viewer now checks `DIRECTORY(@DOCUMENT_STAGE)` before attempting download

### Redeploying Across Clouds

```bash
# Redeploy Streamlit to all three clouds
make redeploy-streamlit CONNECTION=aws_spcs
make redeploy-streamlit CONNECTION=azure_spcs
make redeploy-streamlit CONNECTION=gcp_spcs
```

---

*Built with [Snowflake Cortex AI_EXTRACT](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ai-extract)*
