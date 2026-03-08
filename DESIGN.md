# Design Document

> Original design specification for the Convenience Store Accounts Payable demo.
> This document consolidates the project's constitution, feature spec, implementation plan, and task breakdown.

---

## Design Principles

### Demo-First Design

This is a customer-facing demo, not a production application. Every design decision optimizes for:

- Visual impact in a 20-minute meeting
- Single-command deployability to any Snowflake account
- Minimal prerequisites (no external services, API keys, or complex config)
- A clear "wow moment" when AI_EXTRACT processes invoices live on stage

### Snowflake-Native Architecture

The demo showcases Snowflake capabilities, not generic Python data processing:

- All data lives in Snowflake tables
- PDF extraction uses `AI_EXTRACT` — no external OCR services
- Scheduling uses Snowflake Tasks + Streams — no Airflow or cron
- Visualization runs in Streamlit in Snowflake on Container Runtime
- Authentication uses Snowflake-native auth (zero app-level auth code)

### End-to-End Pipeline Story

The demo tells a complete story from raw PDF to actionable insight:

1. **Ingest** — PDF invoices land on a Snowflake stage
2. **Extract** — AI_EXTRACT pulls structured data (headers + line items)
3. **Automate** — A Task/Stream pipeline processes new files automatically
4. **Visualize** — Streamlit app shows AP ledger, aging, and spend analytics
5. **Live Demo** — Presenter stages new PDFs and watches extraction happen in real-time

### Realistic Synthetic Data

The demo data must be believable in a customer meeting:

- Real distributor names (McLane, Core-Mark, Coca-Cola, Frito-Lay)
- Real convenience store product names (not "Product A")
- Realistic invoice amounts, payment terms, and date ranges
- 100 invoices for initial load + 5 held back for live demo

### Development Standards

- **SQL excellence** — no `SELECT *`, CTEs over subqueries, comments explaining business logic, maintainable by an SE who isn't a SQL expert
- **Cost awareness** — `CPU_X64_XS` compute pool, X-Small warehouse, stream-gated task (no wasted compute), teardown script removes all objects
- **Simplicity** — if it adds complexity without demo value, skip it; if an SE would be confused by the code, simplify it

---

## Feature Specification

### User Story 1 — AP Ledger Overview (P1)

An SE opens the Streamlit app during a customer meeting with a convenience store chain's finance team. The landing page shows KPI cards: total payables outstanding, invoice count, overdue amount, and average days to pay. The AP Ledger page shows all extracted invoices with aging buckets and drill-down to line items.

**Why P1**: The AP ledger is the core deliverable — it proves that AI_EXTRACT turned unstructured PDFs into a structured, queryable accounts payable system.

**Acceptance criteria**:
1. KPI cards show aggregate AP metrics across all invoices on load
2. AP Ledger table filters by vendor and updates in place
3. Expanding an invoice row shows extracted line items (product, qty, price)

### User Story 2 — Spend Analytics (P1)

The SE navigates to the Analytics page showing spend by vendor (horizontal bar chart), monthly spend trend (line chart), category breakdown (treemap), and aging distribution (stacked bar). The customer sees how Snowflake turns raw invoices into actionable financial insights.

**Why P1**: Analytics is the "so what" — it shows the business value of automated invoice processing beyond just data extraction.

**Acceptance criteria**:
1. Spend by vendor chart shows top vendors ranked by total spend
2. All charts update when date range filter is applied
3. Category breakdown shows product categories (beverages, snacks, tobacco, etc.)

### User Story 3 — Live Extraction Demo (P1)

The SE demonstrates the "magic moment": from the Streamlit app, they stage new PDF invoices, trigger the extraction task, and watch the results appear in real-time. The progress panel shows files being detected and extracted.

**Why P1**: This is the wow moment — live AI processing in front of the customer proves the system works end-to-end, not just on pre-loaded data.

**Acceptance criteria**:
1. Clicking "Stage Demo Invoices" PUTs PDFs to the Snowflake stage and registers them in RAW_INVOICES
2. Clicking "Run Extraction" executes the task with a live progress panel
3. New invoices appear in the AP Ledger and Analytics pages after extraction

### User Story 4 — Automated Pipeline (P2)

A Snowflake Task monitors the stage via a Stream. Any new files added are automatically extracted without manual intervention. The task runs every 5 minutes and only activates when new data exists.

**Acceptance criteria**:
1. Stream detects new files and task processes them within 5 minutes
2. No compute is consumed when there are no new files (stream-gated)

### Edge Cases

- **Malformed PDF** — mark as `EXTRACTION_FAILED` with error message
- **Duplicate invoice** — deduplicate on filename
- **Concurrent task execution** — Snowflake handles task concurrency; only one instance runs

### User Story 5 — AI Extract Lab (P2)

The SE opens the AI Extract Lab page to interactively explore AI_EXTRACT capabilities. Three modes are available: Starter Template (pre-built prompts for invoice headers, line items, general Q&A), Visual Builder (point-and-click entity definition), and Raw JSON Editor. The SE can test extraction against any staged or uploaded PDF and see results immediately.

**Why P2**: The lab lets technically curious customers experiment with AI_EXTRACT prompts beyond the canned demo, extending the conversation into their own use cases.

**Acceptance criteria**:
1. Pre-built prompts available for invoice headers, line items, and general Q&A in Starter Template mode
2. Results display in a structured format when extraction runs against a staged PDF
3. Uploaded PDFs are staged and available for extraction

### User Story 6 — In-Snowflake Invoice Generation (P2)

The SE generates new PDF invoices directly inside Snowflake using a Python UDTF (`GENERATE_INVOICE_PDF`). This eliminates the need for local PDF generation and demonstrates Snowflake as a complete platform — even document creation runs inside the warehouse.

**Why P2**: Generating PDFs inside Snowflake is a strong "platform completeness" talking point and makes the live demo self-contained.

**Acceptance criteria**:
1. Selecting vendors and clicking generate creates new PDF invoices via the UDTF and stages them to @INVOICE_STAGE
2. AI_EXTRACT results on generated invoices match the generated content

---

## Functional Requirements

| ID | Requirement |
|----|-------------|
| FR-001 | Extract vendor name, invoice number, date, due date, subtotal, tax, and total from PDF invoices using AI_EXTRACT |
| FR-002 | Extract line items (product, quantity, unit price, line total) as structured table data |
| FR-003 | Display an AP ledger with aging buckets (current, 1-30, 31-60, 61-90, 90+ days) |
| FR-004 | Provide spend analytics by vendor, category, and time period |
| FR-005 | Support live extraction of new invoices from within the Streamlit app |
| FR-006 | Include a scheduled Task that auto-extracts newly staged PDFs via Stream |
| FR-007 | Generate 100 realistic synthetic invoices + 5 demo invoices for live presentation |
| FR-008 | Deploy with a single `deploy.sh` command |
| FR-009 | Provide an interactive AI_EXTRACT prompt builder (AI Extract Lab) with starter templates, visual builder, and raw JSON editor |
| FR-010 | Generate PDF invoices inside Snowflake via a Python UDTF (no local dependencies for live demo) |
| FR-011 | Support dual-environment deployment via dynamic config (CURRENT_DATABASE/CURRENT_SCHEMA) with zero hardcoded values |
| FR-012 | Include a comprehensive E2E test suite (Playwright) covering all pages |
| FR-013 | Support an append-only review/correction workflow with inline `st.data_editor` and `INVOICE_REVIEW` table |
| FR-014 | Use a dedicated RBAC role (`AI_EXTRACT_APP`) with least-privilege grants — not ACCOUNTADMIN |
| FR-015 | Support multiple document types (invoices, contracts, receipts) via configuration table with per-type prompts and UI labels |
| FR-016 | Use parameterized SQL queries (`params=[]`) throughout the Streamlit app to prevent SQL injection |
| FR-017 | Deploy and validate on all three Snowflake clouds (AWS, Azure, GCP) with identical test results |

### Key Entities

- **Invoice** — vendor, invoice number, date, due date, PO number, subtotal, tax, total, payment terms, status
- **Line Item** — invoice reference, product name, quantity, unit price, line total, category
- **Vendor** — name, normalized name, address, payment terms
- **Raw Invoice** — staged filename, upload timestamp, extraction status

---

## Tech Stack Decisions

### Data Generation

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| PDF creation | reportlab | Pure Python, no external deps, full PDF control |
| Output | 100 initial + 5 demo PDFs | Realistic invoice content |

### SQL Pipeline

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Extraction | AI_EXTRACT | Snowflake-native document AI, no external OCR |
| Automation | Task + Stream | Event-driven, zero compute when idle |
| Storage | Snowflake tables | Structured extraction results + AP ledger |

### Streamlit App

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Runtime | Container Runtime | Shared server, fast load, caching support |
| Charts | Plotly | Interactive, good defaults, widely available |
| Tables | st.dataframe | Native Streamlit, sortable/filterable |
| Layout | st.columns + st.metric | Clean KPI cards, responsive layout |

### Infrastructure

| Choice | Technology | Rationale |
|--------|-----------|-----------|
| Compute pool | CPU_X64_XS | Minimal cost for Streamlit container |
| Warehouse | X-Small | Sufficient for extraction + queries |
| Stage | Internal named stage | Simple, no external storage needed |

---

## Data Flow

```
┌──────────────┐     PUT to stage      ┌─────────────────┐
│  PDF Invoices │ ───────────────────► │ @INVOICE_STAGE   │
│  (100 + 5)   │                       └────────┬────────┘
└──────────────┘                                │
                                                │ AI_EXTRACT
                                                ▼
                                   ┌────────────────────────┐
                                   │  EXTRACTED_INVOICES     │ (headers)
                                   │  EXTRACTED_LINE_ITEMS   │ (details)
                                   └────────────┬───────────┘
                                                │
                              ┌─────────────────┼──────────────────┐
                              ▼                 ▼                  ▼
                     ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                     │  AP Ledger   │  │  Analytics   │  │  Live Demo   │
                     │  (aging,     │  │  (vendor,    │  │  (stage new, │
                     │   status)    │  │   trends)    │  │   extract)   │
                     └──────────────┘  └──────────────┘  └──────────────┘
                              └─────────────────┼──────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │  Streamlit in         │
                                    │  Snowflake (container)│
                                    └───────────────────────┘
```

## Automation Flow

```
New PDF on stage
       │
       ▼
INSERT into RAW_INVOICES (extracted = FALSE)
       │
       ▼
Stream RAW_INVOICES_STREAM detects new rows
       │
       ▼
Task EXTRACT_NEW_INVOICES_TASK fires (every 5 min or on-demand)
       │
       ▼
Stored Proc SP_EXTRACT_NEW_INVOICES:
  - Reads unprocessed files from RAW_INVOICES
  - Runs AI_EXTRACT for headers → EXTRACTED_INVOICES
  - Runs AI_EXTRACT for line items → EXTRACTED_LINE_ITEMS
  - Marks RAW_INVOICES.extracted = TRUE
```

---

## Success Criteria

| ID | Criteria |
|----|----------|
| SC-001 | 100 PDF invoices extracted with >90% field accuracy on header fields |
| SC-002 | AP Ledger displays all invoices with correct aging calculations |
| SC-003 | Live extraction of 5 demo invoices completes within 2 minutes and results appear in the app |
| SC-004 | An SE can deploy the full demo in under 15 minutes |
| SC-005 | The demo tells a complete story (ingest → extract → automate → visualize) in a 20-minute meeting |
| SC-006 | ~421 automated tests pass (non-E2E + E2E) covering SQL objects, data quality, RBAC, concurrency, and every Streamlit page |
| SC-007 | Full test suite passes on all three Snowflake clouds (AWS, Azure, GCP) with zero failures |
| SC-008 | App runs with a least-privilege RBAC role (not ACCOUNTADMIN) on all three clouds |

---

## Implementation Phases

### Phase 1 — Setup
- Project directory structure
- Spec-kit files (constitution, spec, plan, tasks)

### Phase 2 — Data Generation
- `data/generate_invoices.py` — generates 100 initial + 5 demo PDFs
- Run generator to produce PDFs

### Phase 3 — SQL Pipeline
- `sql/01_setup.sql` — DB, schema, warehouse, stage, compute pool
- `sql/02_tables.sql` — RAW_INVOICES, EXTRACTED_INVOICES, EXTRACTED_LINE_ITEMS, VENDORS
- `sql/03_extract.sql` — batch AI_EXTRACT pipeline for initial 100 PDFs
- `sql/04_task.sql` — Stream + stored proc + scheduled task for new files
- `sql/05_views.sql` — 8 analytical views (aging, vendor spend, category, trends)

### Phase 4 — Streamlit App
- `streamlit_app.py` — landing page with architecture diagram and business value
- `pages/0_Dashboard.py` — KPI dashboard (metrics, recent invoices, vendors)
- `pages/1_AP_Ledger.py` — invoice list, aging buckets, drill-down, inline PDF rendering
- `pages/2_Analytics.py` — spend charts and trends (6 Plotly visualizations)
- `pages/3_Process_New.py` — live demo page (UDTF generation, extraction, progress)
- `pages/4_AI_Extract_Lab.py` — interactive AI_EXTRACT prompt builder (3 modes)
- `config.py` — dynamic environment config (CURRENT_DATABASE/CURRENT_SCHEMA)

### Phase 5 — In-Snowflake PDF Generation
- `sql/07_generate_udf.sql` — Python UDTF `GENERATE_INVOICE_PDF` using fpdf
- Wrapper stored procedure for Streamlit integration

### Phase 6 — SQL Validation Tests
- `sql/06_tests.sql` — 58 E2E SQL validation tests

### Phase 7 — Deployment Scripts
- `deploy.sh` — single-command deploy (env var configurable)
- `teardown.sh` — clean removal script (env var configurable)

### Phase 8 — E2E Test Suite (Playwright)
- Original demo app: 146 tests across 8 test files (6 functional + 2 integration)
- POC kit: ~421 tests across 21 test files (16 non-E2E + 5 E2E)
- Session-scoped server, fixtures, page object helpers

### Phase 9 — Documentation & Screenshots
- `scripts/capture_screenshots.py` — async Playwright parallel capture
- 6 screenshots in `docs/` for README
- README, DESIGN.md, LICENSE

### Phase 10 — Dual-Environment Deployment
- Deploy to demo account and Snowhouse with identical source code
- Validate both apps pass all query categories end-to-end

### Phase 11 — Public Sharing Prep
- `sql/08_grants.sql` — re-runnable role grants
- Security audit, genericized scripts, Apache 2.0 license

### Phase 12 — AI_EXTRACT POC Kit
- Standalone `poc/` directory with SQL scripts, Streamlit app, and comprehensive test suite
- Generic column names (`field_1`...`field_10`, `col_1`...`col_5`) for any document type
- Review workflow with append-only `INVOICE_REVIEW` table and `V_INVOICE_SUMMARY` view
- Automated deploy script (`deploy_poc.sh`) with env var configuration

### Phase 13 — RBAC & Security Hardening
- Dedicated `AI_EXTRACT_APP` role with least-privilege grants
- `SNOWFLAKE.CORTEX_USER` database role grant
- Replaced ACCOUNTADMIN usage throughout app and tests
- Parameterized all user-facing SQL queries to prevent SQL injection (replaced f-string interpolation with `params=[]`)
- `POC_ROLE` env var in `conftest.py` for cross-role testing

### Phase 14 — Multi-Document-Type Support
- `DOCUMENT_TYPE_CONFIG` table with per-type extraction prompts and UI labels (VARIANT JSON)
- `RAW_DOCUMENTS.doc_type` column (DEFAULT `'INVOICE'`) tags each file
- Three built-in types: INVOICE, CONTRACT, RECEIPT
- All Streamlit pages wired with document type filter dropdown
- `config.py` exposes `get_doc_type_labels()` and `get_doc_types()` with fallback defaults
- Views (`V_DOCUMENT_LEDGER`, `V_INVOICE_SUMMARY`) include `doc_type` via JOIN to `RAW_DOCUMENTS`

### Phase 15 — Cross-Cloud Validation
- Deployed identical infrastructure to AWS, Azure, and GCP Snowflake accounts
- Setup script provisions: tables, views, RBAC role, grants, stage uploads
- ~421 tests (non-E2E + E2E) pass on all three clouds with zero failures
- Validated RBAC role works identically across clouds
- Documented cross-cloud testing commands and env var configuration
