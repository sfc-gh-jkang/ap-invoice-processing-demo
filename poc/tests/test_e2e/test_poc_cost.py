"""E2E: POC Cost page — metrics, charts, cost drivers, confidence section."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

COST_PATH = "/Cost"


def _navigate(page, app_url):
    for attempt in range(3):
        page.goto(f"{app_url}{COST_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator('[data-testid="stMetric"]').count() > 0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestCostSmoke:

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Cost" in title.first.inner_text()


class TestCostMetrics:

    def test_metric_cards_exist(self, page, app_url):
        _navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 4, f"Expected >= 4 metric cards, got {metrics.count()}"

    def test_credits_per_doc_metric(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Credits/Doc" in page_text or "credits/doc" in page_text.lower(), (
            "Credits/Doc metric not found"
        )

    def test_docs_processed_metric(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Docs Processed" in page_text or "docs processed" in page_text.lower(), (
            "Docs Processed metric not found"
        )


class TestCostCharts:

    def test_plotly_charts_render(self, page, app_url):
        _navigate(page, app_url)
        plotly_plots = page.locator(".js-plotly-plot")
        iframes = page.locator("iframe")
        chart_count = plotly_plots.count() + iframes.count()
        assert chart_count >= 1, (
            f"No Plotly charts found (plots={plotly_plots.count()}, iframes={iframes.count()})"
        )

    def test_token_range_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Token Range" in page_text, "Token Range section not found"

    def test_daily_credits_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Daily" in page_text, "Daily credits section not found"


class TestCostDrivers:

    def test_cost_drivers_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Cost Drivers" in page_text, "Cost Drivers section not found"

    def test_cost_drivers_has_table(self, page, app_url):
        _navigate(page, app_url)
        tables = page.locator('[data-testid="stDataFrame"]')
        assert tables.count() >= 1, "No data tables found on Cost page"


class TestCostConfidence:

    def test_confidence_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        has_section = "Confidence" in page_text
        has_fallback = "No confidence data" in page_text
        assert has_section or has_fallback, (
            "Neither confidence section nor fallback message found"
        )

    def test_confidence_metric_or_info(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        has_metric = "Avg Confidence" in page_text
        has_info = "confidence data available" in page_text.lower()
        assert has_metric or has_info, (
            "Expected Avg Confidence metric or info message"
        )


class TestCostInfrastructure:

    def test_infrastructure_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Infrastructure" in page_text, "Infrastructure Credits section not found"

    def test_resource_monitor_section(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        has_monitor = "Resource Monitor" in page_text
        has_info = "No resource monitor" in page_text
        assert has_monitor or has_info, (
            "Resource Monitor section not found"
        )
