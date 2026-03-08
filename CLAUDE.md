# AI_EXTRACT POC — Testing & Development Guide

## Test Architecture

- ~999 tests across 43 files in `poc/tests/` (36 non-E2E + 7 E2E)
- Non-E2E tests connect to Snowflake directly via `snowflake.connector`
- E2E tests use Playwright against a local Streamlit server on port 8504
- `poc/conftest.py` manages Snowflake connection fixtures and Streamlit server lifecycle via double-fork daemon
- All test infrastructure is configurable via environment variables

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `POC_CONNECTION` | `default` | Snowflake connection name from `~/.snowflake/config.toml` |
| `POC_DB` | `AI_EXTRACT_POC` | Database name |
| `POC_SCHEMA` | `DOCUMENTS` | Schema name |
| `POC_WH` | `AI_EXTRACT_WH` | Warehouse name |
| `POC_ROLE` | `AI_EXTRACT_APP` | Role name (USE ROLE before any queries) |

## Running Tests

```bash
cd poc

# All non-E2E tests (uses default connection):
uv run pytest tests/ --ignore=tests/test_e2e -v

# All E2E tests (auto-starts Streamlit server):
uv run pytest tests/test_e2e/ -v

# Full suite:
uv run pytest tests/ -v

# Cross-cloud (override connection):
POC_CONNECTION=my_other_conn POC_DB=AI_EXTRACT_POC POC_SCHEMA=DOCUMENTS \
  POC_WH=AI_EXTRACT_WH POC_ROLE=AI_EXTRACT_APP \
  uv run pytest tests/ --ignore=tests/test_e2e -v

POC_CONNECTION=my_other_conn POC_DB=AI_EXTRACT_POC POC_SCHEMA=DOCUMENTS \
  POC_WH=AI_EXTRACT_WH POC_ROLE=AI_EXTRACT_APP \
  uv run pytest tests/test_e2e/ -v

# Single test:
uv run pytest tests/test_sql_integration.py::TestTables::test_raw_documents_exists -v
```

## Cross-Cloud Test Results

All three Snowflake clouds have been validated with identical infrastructure:

| Cloud | Non-E2E | E2E | Total |
|---|---|---|---|
| **AWS** (US East 1) | 340 passed | 71 passed | **411 passed** |
| **Azure** (East US 2) | 350 passed | 71 passed | **421 passed** |
| **GCP** (US Central 1) | 350 passed | 71 passed | **421 passed** |

## Server Lifecycle (conftest.py)

- `pytest_configure` starts the Streamlit server only when E2E tests are selected
- Server launched via **double-fork** to fully detach from pytest process tree
- If server already healthy on port 8504, reuse it (enables back-to-back runs)
- Server uses the uv venv Python (`poc/.venv/bin/python3`) and passes all `POC_*` env vars
- All `POC_*` env vars are forwarded to the Streamlit process for cross-account testing

## Test File Summary

```
poc/tests/
├── test_admin_builder.py              # Admin page builder logic (unit — no Snowflake)
├── test_admin_config_management.py    # Admin config CRUD operations
├── test_analytics_queries.py          # Analytics page SQL queries
├── test_batch_extract_integration.py  # Batch extraction integration
├── test_confidence_scoring.py         # Confidence score tests
├── test_config.py                     # Config module unit tests
├── test_config_functions.py           # Config function tests
├── test_config_helpers.py             # Config helper tests (unit — no Snowflake)
├── test_contract_extraction.py        # Contract extraction quality
├── test_cross_doc_isolation.py        # Cross-doc-type isolation
├── test_dashboard_queries.py          # Dashboard page SQL queries
├── test_data_drift.py                 # Boundary values, schema evolution
├── test_data_validation.py            # Data quality, completeness
├── test_deployment_readiness.py       # Pre-flight: Cortex, encryption, stages, RBAC
├── test_document_type_flexibility.py  # Multi-doc-type support
├── test_document_viewer_queries.py    # Document Viewer page SQL queries
├── test_edge_cases.py                 # Rollbacks, SQL injection, large data
├── test_extraction_pipeline.py        # Live AI_EXTRACT, stored proc
├── test_load_stress.py                # Bulk inserts, concurrent writers
├── test_multi_user_concurrency.py     # Interleaved reviews, race conditions
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
├── test_utility_bill_extraction.py    # Utility bill extraction quality
├── test_validation_rules.py           # Validation rule tests
├── test_writeback_data_validation.py  # Writeback data quality
├── test_writeback_integration.py      # INVOICE_REVIEW + V_INVOICE_SUMMARY
└── test_e2e/
    ├── conftest.py                    # E2E fixtures + screenshot-on-failure
    ├── helpers.py                     # Shared Playwright utilities
    ├── test_poc_admin.py              # Admin page tests
    ├── test_poc_analytics.py          # Analytics page tests
    ├── test_poc_dashboard.py          # Dashboard page tests
    ├── test_poc_document_viewer.py    # Document Viewer page tests
    ├── test_poc_landing.py            # Landing page tests
    ├── test_poc_multi_doc.py          # Multi-document-type flows
    └── test_poc_review.py             # Review page tests
```

## Known Constraints

- E2E tests require `poc/streamlit/.streamlit/secrets.toml` with Snowflake credentials
- Playwright Chromium can become unstable after ~30+ sequential navigations; run E2E files separately if needed
- Port TIME_WAIT: After SIGKILL, port 8504 needs ~5-10s before rebinding
- `python` command not found on this system; always use `python3` or `.venv/bin/python3`

## RBAC

The POC uses a dedicated `AI_EXTRACT_APP` role (not ACCOUNTADMIN) for all operations:
- USAGE on DATABASE, SCHEMA, WAREHOUSE
- SELECT on ALL TABLES + ALL VIEWS + FUTURE TABLES + FUTURE VIEWS
- INSERT on INVOICE_REVIEW (append-only audit trail)
- READ on DOCUMENT_STAGE + STREAMLIT_STAGE
- DATABASE ROLE SNOWFLAKE.CORTEX_USER
- Granted to ROLE SYSADMIN and relevant users

ACCOUNTADMIN is only needed for: role creation, CORTEX_USER grant, cross-region setting, EAI/network rule creation, BIND SERVICE ENDPOINT.
