"""E2E: POC Landing Page — pipeline status, architecture diagram, extraction summary."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, get_metric_value, assert_no_exceptions


pytestmark = pytest.mark.e2e


def _navigate(page, app_url):
    """Navigate to the POC landing page with retry on empty render."""
    for attempt in range(3):
        page.goto(app_url, wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator('[data-testid="stMetric"]').count() > 0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestLandingSmoke:
    """Core landing page smoke tests."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Document Extraction" in title.first.inner_text()

    @pytest.mark.smoke
    def test_pipeline_status_metrics(self, page, app_url):
        _navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 4, f"Expected >=4 metrics, got {metrics.count()}"

    def test_total_files_metric(self, page, app_url):
        _navigate(page, app_url)
        total = get_metric_value(page, "Total Files")
        assert total is not None and total >= 5

    def test_extracted_files_metric(self, page, app_url):
        _navigate(page, app_url)
        extracted = get_metric_value(page, "Successfully Extracted")
        assert extracted is not None and extracted >= 5

    def test_pending_is_zero(self, page, app_url):
        _navigate(page, app_url)
        pending = get_metric_value(page, "Pending")
        assert pending == 0

    def test_failed_is_zero(self, page, app_url):
        _navigate(page, app_url)
        failed = get_metric_value(page, "Failed")
        assert failed == 0


class TestLandingContent:
    """Architecture diagram and extraction summary."""

    def test_graphviz_diagram_renders(self, page, app_url):
        _navigate(page, app_url)
        # Graphviz renders as an SVG inside the page
        svg = page.locator("svg")
        assert svg.count() >= 1, "No SVG (graphviz diagram) found"

    def test_extraction_summary_section(self, page, app_url):
        _navigate(page, app_url)
        # Should have documents extracted, line items, unique senders
        docs_metric = get_metric_value(page, "Documents Extracted")
        assert docs_metric is not None and docs_metric >= 5

    def test_line_items_metric(self, page, app_url):
        _navigate(page, app_url)
        items = get_metric_value(page, "Line Items")
        assert items is not None and items > 0

    def test_unique_senders_metric(self, page, app_url):
        _navigate(page, app_url)
        senders = get_metric_value(page, "Unique Senders")
        assert senders is not None and senders >= 1

    def test_sidebar_has_navigation(self, page, app_url):
        _navigate(page, app_url)
        sidebar = page.locator('[data-testid="stSidebar"]')
        assert sidebar.count() >= 1
        sidebar_text = sidebar.inner_text()
        assert "Dashboard" in sidebar_text
        assert "Document Viewer" in sidebar_text
        assert "Analytics" in sidebar_text
