# Tasks: Convenience Store Accounts Payable Processing

**Input**: Design documents from `.specify/`
**Prerequisites**: plan.md, spec.md
**Updated**: 2026-03-07

## Phase 1: Setup

- [x] T001 Create project directory structure
- [x] T002 Create `.specify/` spec-kit files (constitution, spec, plan, tasks)

## Phase 2: Data Generation

- [x] T003 Create `data/generate_invoices.py` — generates 100 initial + 5 demo PDFs
- [x] T004 Run generator to produce PDFs in `data/invoices/` and `data/demo_invoices/`

## Phase 3: SQL Pipeline

- [x] T005 Create `sql/01_setup.sql` — DB, schema, warehouse, stage, compute pool
- [x] T006 Create `sql/02_tables.sql` — RAW_INVOICES, EXTRACTED_INVOICES, EXTRACTED_LINE_ITEMS, VENDORS
- [x] T007 Create `sql/03_extract.sql` — batch AI_EXTRACT pipeline for initial 100 PDFs
- [x] T008 Create `sql/04_task.sql` — Stream + stored proc + scheduled task for new files
- [x] T009 Create `sql/05_views.sql` — 8 analytical views (aging, vendor spend, category, trends, extraction status)

## Phase 4: Streamlit App

- [x] T010 Create `streamlit/streamlit_app.py` — landing page with architecture diagram, business value, live stats
- [x] T011 Create `streamlit/pages/0_Dashboard.py` — KPI dashboard (metrics, recent invoices, vendors)
- [x] T012 Create `streamlit/pages/1_AP_Ledger.py` — invoice list, aging buckets, drill-down, inline PDF rendering
- [x] T013 Create `streamlit/pages/2_Analytics.py` — spend charts and trends (6 views, Plotly)
- [x] T014 Create `streamlit/pages/3_Process_New.py` — live demo page (UDTF generation, extraction, progress)
- [x] T015 Create `streamlit/pages/4_AI_Extract_Lab.py` — interactive AI_EXTRACT prompt builder (3 modes)
- [x] T016 Create `streamlit/config.py` — dynamic environment config (CURRENT_DATABASE/CURRENT_SCHEMA)
- [x] T017 Create `streamlit/pyproject.toml` and `environment.yml` — dependencies

## Phase 5: In-Snowflake PDF Generation

- [x] T018 Create `sql/07_generate_udf.sql` — Python UDTF `GENERATE_INVOICE_PDF` using fpdf
- [x] T019 Create wrapper stored procedure for Streamlit integration

## Phase 6: SQL Validation Tests

- [x] T020 Create `sql/06_tests.sql` — 58 E2E SQL validation tests (row counts, data integrity, view correctness)

## Phase 7: Deployment Scripts

- [x] T021 Create `deploy.sh` — single-command deploy script (env var configurable)
- [x] T022 Create `teardown.sh` — clean removal script (env var configurable)
- [x] T023 Create `.gitignore`

## Phase 8: E2E Test Suite (Playwright)

- [x] T024 Create `streamlit/tests/conftest.py` — session-scoped server, fixtures, helpers
- [x] T025 Create `streamlit/tests/test_functional/test_landing.py` — landing page tests
- [x] T026 Create `streamlit/tests/test_functional/test_dashboard.py` — KPI dashboard tests
- [x] T027 Create `streamlit/tests/test_functional/test_ap_ledger.py` — AP ledger tests
- [x] T028 Create `streamlit/tests/test_functional/test_analytics.py` — analytics tests
- [x] T029 Create `streamlit/tests/test_functional/test_process_new.py` — process new tests
- [x] T030 Create `streamlit/tests/test_functional/test_ai_extract_lab.py` — AI extract lab tests
- [x] T031 Create `streamlit/tests/test_integration/test_navigation.py` — cross-page navigation tests
- [x] T032 Create `streamlit/tests/test_integration/test_data_pipeline.py` — data consistency tests
- [x] T033 Achieve 146 passing tests, 0 failures

## Phase 9: Documentation & Screenshots

- [x] T034 Create `scripts/capture_screenshots.py` — async Playwright parallel capture
- [x] T035 Capture 6 screenshots in `docs/` (landing, dashboard, ledger, analytics, process new, extract lab)
- [x] T036 Create `README.md` — features, architecture, quick start, project structure, demo flow
- [x] T037 Create `DESIGN.md` — consolidated design document from `.specify/` planning docs

## Phase 10: Dual-Environment Deployment

- [x] T038 Deploy to demo account (AP_DEMO_DB.AP) — all SQL objects + Streamlit app
- [x] T039 Deploy to Snowhouse (TEMP.JKANG_AP) — all SQL objects + Streamlit app on Container Runtime
- [x] T040 Validate both apps pass all query categories end-to-end

## Phase 11: Public Sharing Prep

- [x] T041 Create `sql/08_grants.sql` — re-runnable role grants (workaround for MANAGE GRANTS limitation)
- [x] T042 Create `LICENSE` (Apache 2.0)
- [x] T043 Security audit — remove secrets from tracking, scrub account-specific references
- [x] T044 Genericize deploy/teardown scripts with env var overrides
- [x] T045 Update README for public audience — dual-env docs, generic refs, grants section

## Verification

- [x] T046 All SQL compiles without errors
- [x] T047 `generate_invoices.py` runs and produces PDFs
- [x] T048 Streamlit app has no import errors
- [x] T049 146 Playwright E2E tests pass
- [x] T050 Both demo and Snowhouse apps work end-to-end
- [x] T051 No secrets in git history

## Phase 12: AI_EXTRACT POC Kit

- [x] T052 Create `poc/` standalone POC directory with SQL scripts, Streamlit app, sample docs
- [x] T053 Create generic extraction pipeline (field_1..field_10, col_1..col_5) for any document type
- [x] T054 Create `poc/deploy_poc.sh` — automated deploy script with env var configuration
- [x] T055 Create `poc/teardown_poc.sql` — drop all POC objects
- [x] T056 Create review workflow: `INVOICE_REVIEW` table (append-only audit trail) + `V_INVOICE_SUMMARY` view
- [x] T057 Create inline `st.data_editor` review page with writeback to `INVOICE_REVIEW`
- [x] T058 Create comprehensive non-E2E test suite (16 files, ~350 tests): SQL integration, data validation, extraction pipeline, writeback, review helpers, RBAC, performance, load stress, concurrency, edge cases, data drift, teardown idempotency, deployment readiness, SQL parity, config
- [x] T059 Create Playwright E2E test suite (5 files, ~72 tests): landing, dashboard, document viewer, analytics, review

## Phase 13: RBAC & Security Hardening

- [x] T060 Create `AI_EXTRACT_APP` role with least-privilege grants (USAGE, SELECT, INSERT, READ, FUTURE)
- [x] T061 Grant `SNOWFLAKE.CORTEX_USER` database role to `AI_EXTRACT_APP`
- [x] T062 Replace ACCOUNTADMIN usage in `secrets.toml`, `conftest.py`, and deploy scripts
- [x] T063 Add `POC_ROLE` env var to `conftest.py` for cross-role testing
- [x] T064 Parameterize all user-facing SQL queries (`params=[]`) — eliminate SQL injection via f-strings
- [x] T065 Create `test_rbac_permissions.py` — 20 tests for role-based access control

## Phase 14: Multi-Document-Type Support

- [x] T066 Create `DOCUMENT_TYPE_CONFIG` table with per-type extraction prompts and UI labels (VARIANT JSON)
- [x] T067 Add `doc_type` column to `RAW_DOCUMENTS` (DEFAULT 'INVOICE')
- [x] T068 Seed 3 built-in types: INVOICE, CONTRACT, RECEIPT
- [x] T069 Wire document type filter dropdown into all 4 Streamlit pages (Dashboard, Viewer, Analytics, Review)
- [x] T070 Update `config.py` with `get_doc_type_labels()` and `get_doc_types()` with fallback defaults
- [x] T071 Update `V_DOCUMENT_LEDGER` and `V_INVOICE_SUMMARY` views to include `doc_type` via JOIN

## Phase 15: Cross-Cloud Validation

- [x] T072 Provision Azure account: tables, views, RBAC role, grants, CORTEX_USER, stage uploads
- [x] T073 Provision GCP account: tables, views, RBAC role, grants, CORTEX_USER, stage uploads
- [x] T074 Run full non-E2E test suite against Azure (350 passed, 3 skipped, 0 failed)
- [x] T075 Run full non-E2E test suite against GCP (350 passed, 3 skipped, 0 failed)
- [x] T076 Run full E2E test suite against Azure (71 passed, 1 skipped, 0 failed)
- [x] T077 Run full E2E test suite against GCP (71 passed, 1 skipped, 0 failed)
- [x] T078 Fix cross-cloud issues: missing DOC_TYPE column, missing INVOICE_REVIEW/DOCUMENT_TYPE_CONFIG tables, missing V_INVOICE_SUMMARY view, missing 3_Review.py on STREAMLIT_STAGE
- [x] T079 Document cross-cloud testing commands and results in poc/README.md, CLAUDE.md, README.md, DESIGN.md
- [x] T080 Update all documentation with RBAC, multi-doc-type, and cross-cloud learnings
