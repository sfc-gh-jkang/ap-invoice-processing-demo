"""E2E: POC Analytics — charts render (vendor bar, monthly trend, aging, top items)."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

ANALYTICS_PATH = "/Analytics"


def _navigate(page, app_url):
    """Navigate to the Analytics page with retry."""
    for attempt in range(3):
        page.goto(f"{app_url}{ANALYTICS_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        # Check for plotly charts (rendered as iframes or JS containers)
        if page.locator(".js-plotly-plot, iframe").count() > 0:
            # Scroll to bottom so Streamlit renders all lazy sections
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestAnalyticsSmoke:
    """Core analytics page smoke tests."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Analytics" in title.first.inner_text()


class TestAnalyticsCharts:
    """Verify chart sections render."""

    def test_vendor_chart_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Amount by Sender" in page_text, "Vendor chart section not found"

    def test_monthly_trend_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Monthly Trend" in page_text, "Monthly trend section not found"

    def test_aging_distribution_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Aging Distribution" in page_text, "Aging distribution section not found"

    def test_top_items_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Top 20" in page_text, "Top items section not found"

    def test_plotly_charts_render(self, page, app_url):
        """At least one Plotly chart should render as a JS plot."""
        _navigate(page, app_url)
        # Plotly renders either as .js-plotly-plot or inside an iframe
        plotly_plots = page.locator(".js-plotly-plot")
        iframes = page.locator("iframe")
        chart_count = plotly_plots.count() + iframes.count()
        assert chart_count >= 1, (
            f"No Plotly charts found (plots={plotly_plots.count()}, iframes={iframes.count()})"
        )


class TestAnalyticsTable:
    """Verify the top items data table."""

    def test_top_items_table_exists(self, page, app_url):
        _navigate(page, app_url)
        tables = page.locator('[data-testid="stDataFrame"]')
        assert tables.count() >= 1, "No data table found on analytics page"

    def test_top_items_table_has_rows(self, page, app_url):
        _navigate(page, app_url)
        wait_for_streamlit(page, '[data-testid="stDataFrame"]')
        table = page.locator('[data-testid="stDataFrame"]').first
        cells = table.locator('[role="gridcell"]')
        assert cells.count() > 0, "Top items table has no data rows"
