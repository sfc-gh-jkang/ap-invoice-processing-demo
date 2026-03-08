"""E2E: POC Dashboard — KPI cards, pipeline progress, recent documents."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, get_metric_value, assert_no_exceptions


pytestmark = pytest.mark.e2e

DASHBOARD_PATH = "/Dashboard"


def _navigate(page, app_url):
    """Navigate to the Dashboard page with retry on empty render."""
    for attempt in range(3):
        page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator('[data-testid="stMetric"]').count() > 0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestDashboardSmoke:
    """Core dashboard smoke tests."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Dashboard" in title.first.inner_text()

    @pytest.mark.smoke
    def test_kpi_cards_render(self, page, app_url):
        _navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 4, f"Expected >=4 KPI cards, got {metrics.count()}"


class TestDashboardKPIs:
    """Verify specific KPI values."""

    def test_total_documents_value(self, page, app_url):
        _navigate(page, app_url)
        total = get_metric_value(page, "Total Documents")
        assert total is not None and total >= 5

    def test_total_amount_is_positive(self, page, app_url):
        _navigate(page, app_url)
        amount = get_metric_value(page, "Total Amount")
        assert amount is not None and amount > 0

    def test_unique_senders(self, page, app_url):
        _navigate(page, app_url)
        # Label is dynamic (e.g. "Unique Vendor / Sender") so match prefix
        senders = get_metric_value(page, "Unique")
        assert senders is not None and senders >= 1

    def test_pipeline_progress_section(self, page, app_url):
        _navigate(page, app_url)
        progress = get_metric_value(page, "Pipeline Progress")
        # Pipeline progress renders as "5/5 processed" — we extract the leading digit
        assert progress is not None and progress >= 5


class TestDashboardDocTypeFilter:
    """Verify Document Type filter on Dashboard."""

    def test_doc_type_selectbox_exists(self, page, app_url):
        """Dashboard should have a Document Type selectbox."""
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 1, "No selectbox found — Document Type filter missing"

    def test_doc_type_selectbox_has_all_option(self, page, app_url):
        """Document Type selectbox should include 'ALL' option."""
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        if selectboxes.count() < 1:
            pytest.skip("No selectbox on Dashboard")
        # Click the first selectbox to open dropdown
        selectboxes.first.click()
        page.wait_for_timeout(500)
        options = page.locator('[role="option"]')
        option_texts = [options.nth(i).inner_text() for i in range(options.count())]
        # Close dropdown
        page.keyboard.press("Escape")
        assert any("ALL" in t.upper() for t in option_texts), (
            f"Expected 'ALL' in Document Type options, got: {option_texts}"
        )

    def test_kpis_render_after_doc_type_filter(self, page, app_url):
        """KPI cards should still render when Document Type is set to ALL."""
        _navigate(page, app_url)
        # KPIs should be visible with the default filter
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 4, f"Expected >=4 KPI cards, got {metrics.count()}"


class TestDashboardRecentDocuments:
    """Verify recent documents table."""

    def test_recent_documents_table_exists(self, page, app_url):
        _navigate(page, app_url)
        # Wait specifically for dataframe
        wait_for_streamlit(page, '[data-testid="stDataFrame"]')
        tables = page.locator('[data-testid="stDataFrame"]')
        assert tables.count() >= 1, "No recent documents table found"

    def test_recent_documents_has_rows(self, page, app_url):
        _navigate(page, app_url)
        wait_for_streamlit(page, '[data-testid="stDataFrame"]')
        # Streamlit dataframes render cells as data-testid="stDataFrameResizableContainer"
        table = page.locator('[data-testid="stDataFrame"]').first
        # Check the table has visible content (rows)
        cells = table.locator('[role="gridcell"]')
        assert cells.count() > 0, "Recent documents table has no data rows"
