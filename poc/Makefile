# AI_EXTRACT POC Kit — Common Commands
# Usage: make <target> [CONNECTION=my_account]
#
# Override defaults via environment or inline:
#   make deploy CONNECTION=my_account
#   POC_CONNECTION=my_account make test

CONNECTION ?= default

.PHONY: help install deploy teardown test test-sql test-e2e server generate-samples clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies (app + dev/test + Playwright)
	uv sync --all-groups
	uv run playwright install chromium

deploy: ## Deploy the full POC to Snowflake
	./deploy_poc.sh --connection $(CONNECTION)

teardown: ## Drop all POC objects from Snowflake
	snow sql -c $(CONNECTION) -f teardown_poc.sql

test: test-sql test-e2e ## Run all tests (SQL + E2E)

test-sql: ## Run SQL, data validation, and pipeline tests (~60s)
	POC_CONNECTION=$(CONNECTION) uv run pytest \
		tests/test_deployment_readiness.py \
		tests/test_sql_integration.py \
		tests/test_extraction_pipeline.py \
		tests/test_data_validation.py -v

test-e2e: ## Run E2E browser tests (requires local Streamlit server)
	POC_CONNECTION=$(CONNECTION) uv run pytest tests/test_e2e/test_poc_landing.py -v
	POC_CONNECTION=$(CONNECTION) uv run pytest tests/test_e2e/test_poc_dashboard.py -v
	POC_CONNECTION=$(CONNECTION) uv run pytest tests/test_e2e/test_poc_document_viewer.py -v
	POC_CONNECTION=$(CONNECTION) uv run pytest tests/test_e2e/test_poc_analytics.py -v

server: ## Start local Streamlit server on port 8504 (for E2E tests)
	uv run streamlit run streamlit/streamlit_app.py \
		--server.port 8504 --server.headless true --browser.gatherUsageStats false

generate-samples: ## Regenerate the 5 sample invoice PDFs
	uv run python generate_sample_docs.py

clean: ## Remove caches and test artifacts
	rm -rf .pytest_cache __pycache__ tests/__pycache__ tests/test_e2e/__pycache__ screenshots/
