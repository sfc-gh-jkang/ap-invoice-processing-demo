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

# Common env vars passed to all test targets
TEST_ENV = POC_CONNECTION=$(CONNECTION) POC_DB=$(POC_DB) POC_SCHEMA=$(POC_SCHEMA) \
           POC_WH=$(POC_WH) POC_ROLE=$(POC_ROLE)

.PHONY: help install deploy teardown validate harden test test-all test-unit test-sql test-e2e \
        test-coverage lint server generate-samples clean

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
	  tests/test_edge_cases.py -v

test-sql: ## Run SQL integration and pipeline tests
	$(TEST_ENV) uv run pytest \
	  tests/test_deployment_readiness.py \
	  tests/test_sql_integration.py \
	  tests/test_extraction_pipeline.py \
	  tests/test_data_validation.py \
	  tests/test_dashboard_queries.py \
	  tests/test_analytics_queries.py \
	  tests/test_document_viewer_queries.py \
	  tests/test_batch_extract_integration.py \
	  tests/test_spcs_deployment.py \
	  tests/test_rbac_negative.py \
	  tests/test_confidence_scoring.py \
	  tests/test_utility_bill_extraction.py -v

test-e2e: ## Run ALL E2E browser tests (requires local Streamlit server)
	$(TEST_ENV) uv run pytest tests/test_e2e/ \
	  --ignore=tests/test_e2e/test_poc_review.py -v

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
