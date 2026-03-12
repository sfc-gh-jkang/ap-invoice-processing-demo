# AI_EXTRACT POC Kit — Common Commands
# Usage: make <target> [CONNECTION=my_account]
#
# Override defaults via environment or inline:
#   make deploy CONNECTION=my_account
#   POC_CONNECTION=my_account make test-all

CONNECTION ?= default
POC_DB     ?= AI_EXTRACT_POC
POC_SCHEMA ?= DOCUMENTS
POC_WH     ?= AI_EXTRACT_WH
POC_ROLE   ?= AI_EXTRACT_APP
POC_POOL   ?= AI_EXTRACT_POC_POOL
CONNECTION_FLAG = $(if $(filter default,$(CONNECTION)),,-c $(CONNECTION))

# Common env vars passed to all test targets
TEST_ENV = POC_CONNECTION=$(CONNECTION) POC_DB=$(POC_DB) POC_SCHEMA=$(POC_SCHEMA) \
           POC_WH=$(POC_WH) POC_ROLE=$(POC_ROLE)

.PHONY: help install deploy teardown validate harden test test-all test-unit test-sql test-e2e \
        test-coverage test-cross-cloud lint server upload-streamlit redeploy-streamlit generate-samples clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies (app + dev/test + Playwright)
	uv sync --all-groups
	uv run playwright install chromium

deploy: ## Deploy the full POC to Snowflake
	./deploy_poc.sh --connection $(CONNECTION)

teardown: ## Drop all POC objects from Snowflake
	bash teardown_poc.sh --connection $(CONNECTION)

validate: ## Run health check against deployed POC
	bash validate_poc.sh --connection $(CONNECTION)

harden: ## Apply production hardening to deployed POC
	$(TEST_ENV) snow sql -c $(CONNECTION) -f sql/10_harden.sql

# ---------------------------------------------------------------------------
# Test targets
# ---------------------------------------------------------------------------

test: test-all ## Alias for test-all

test-all: ## Run ALL tests except E2E (SQL + unit + integration)
	$(TEST_ENV) uv run pytest tests/ \
	  --ignore=tests/test_e2e -v

test-unit: ## Run unit tests only (no Snowflake connection needed)
	uv run pytest tests/test_normalize_unit.py \
	  tests/test_review_helpers.py \
	  tests/test_config_helpers.py \
	  tests/test_admin_builder.py -v

UNIT_TEST_FILES = tests/test_normalize_unit.py tests/test_review_helpers.py \
    tests/test_config_helpers.py tests/test_admin_builder.py

test-sql: ## Run all SQL integration tests (requires Snowflake connection)
	$(TEST_ENV) uv run pytest tests/ \
	  --ignore=tests/test_e2e \
	  $(addprefix --ignore=,$(UNIT_TEST_FILES)) \
	  --timeout=120 -v

test-e2e: ## Run ALL E2E browser tests (requires local Streamlit server)
	$(TEST_ENV) uv run pytest tests/test_e2e/ --timeout=120 -v

test-coverage: ## Run tests with coverage report (target: 80%)
	$(TEST_ENV) uv run pytest tests/ \
	  --ignore=tests/test_e2e \
	  --cov=streamlit --cov-report=term-missing --cov-report=html -v

lint: ## Run linting checks
	uv run python -m py_compile streamlit/streamlit_app.py
	uv run python -m py_compile streamlit/config.py
	@for f in streamlit/pages/*.py; do \
	  uv run python -m py_compile "$$f" && echo "  OK: $$f"; \
	done
	@echo "All files compile successfully."

# ---------------------------------------------------------------------------
# Development
# ---------------------------------------------------------------------------

server: ## Start local Streamlit server on port 8504 (for E2E tests)
	uv run streamlit run streamlit/streamlit_app.py \
	  --server.port 8504 --server.headless true --browser.gatherUsageStats false

generate-samples: ## Regenerate the 5 sample invoice PDFs
	uv run python generate_sample_docs.py

clean: ## Remove caches and test artifacts
	rm -rf .pytest_cache __pycache__ tests/__pycache__ tests/test_e2e/__pycache__ \
	  streamlit/__pycache__ streamlit/pages/__pycache__ \
	  screenshots/ htmlcov/ .coverage dist/ build/

# ---------------------------------------------------------------------------
# Deployment
# ---------------------------------------------------------------------------

upload-streamlit: ## Re-upload Streamlit files to STREAMLIT_STAGE (after code changes)
	snow sql $(CONNECTION_FLAG) -q " \
	  USE ROLE $(POC_ROLE); \
	  USE DATABASE $(POC_DB); \
	  USE SCHEMA $(POC_SCHEMA); \
	  PUT file://streamlit/streamlit_app.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/config.py @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/pyproject.toml @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/environment.yml @STREAMLIT_STAGE/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/pages/0_Dashboard.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/pages/1_Document_Viewer.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/pages/2_Analytics.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
	  PUT file://streamlit/pages/3_Review.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
    PUT file://streamlit/pages/4_Admin.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
    PUT file://streamlit/pages/5_Cost.py @STREAMLIT_STAGE/pages/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE; \
  "
	@echo "Streamlit files uploaded. Restart the app in Snowsight to pick up changes."

redeploy-streamlit: upload-streamlit ## Upload files AND recreate Streamlit app with container runtime
	snow sql $(CONNECTION_FLAG) -q " \
	  USE ROLE $(POC_ROLE); \
	  USE DATABASE $(POC_DB); \
	  USE SCHEMA $(POC_SCHEMA); \
	  CREATE OR REPLACE STREAMLIT AI_EXTRACT_DASHBOARD \
	    FROM '@$(POC_DB).$(POC_SCHEMA).STREAMLIT_STAGE' \
	    MAIN_FILE = 'streamlit_app.py' \
	    QUERY_WAREHOUSE = $(POC_WH) \
	    COMPUTE_POOL = $(POC_POOL) \
	    EXTERNAL_ACCESS_INTEGRATIONS = (PYPI_ACCESS_INTEGRATION) \
	    RUNTIME_NAME = 'SYSTEM\$$ST_CONTAINER_RUNTIME_PY3_11' \
	    TITLE = 'AI_EXTRACT Document Processing' \
	    COMMENT = 'Document extraction dashboard powered by Cortex AI_EXTRACT'; \
	  ALTER STREAMLIT AI_EXTRACT_DASHBOARD ADD LIVE VERSION FROM LAST; \
	"
	@echo "Streamlit app recreated with container runtime. Open in Snowsight."

# ---------------------------------------------------------------------------
# Cross-cloud testing
# ---------------------------------------------------------------------------

CLOUDS ?= default

test-cross-cloud: ## Run non-E2E tests against all 3 clouds (AWS, Azure, GCP)
	@for cloud in $(CLOUDS); do \
	  echo ""; \
	  echo "========================================"; \
	  echo "  Testing cloud: $$cloud"; \
	  echo "========================================"; \
	  POC_CONNECTION=$$cloud POC_DB=$(POC_DB) POC_SCHEMA=$(POC_SCHEMA) \
	    POC_WH=$(POC_WH) POC_ROLE=$(POC_ROLE) \
	    uv run pytest tests/ --ignore=tests/test_e2e --timeout=120 -v \
	    && echo "  ✓ $$cloud PASSED" \
	    || echo "  ✗ $$cloud FAILED"; \
	done
