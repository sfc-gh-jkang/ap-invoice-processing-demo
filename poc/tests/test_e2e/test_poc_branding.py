"""E2E: CSS theming and sidebar branding — verify polished styling is applied."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Landing page branding
# ---------------------------------------------------------------------------

class TestLandingBranding:
    """Verify CSS theming and sidebar branding on the landing page."""

    def _navigate(self, page, app_url):
        for attempt in range(3):
            page.goto(app_url, wait_until="domcontentloaded", timeout=90_000)
            wait_for_streamlit(page)
            if page.locator('[data-testid="stMetric"]').count() > 0:
                return
            page.wait_for_timeout(2000)
        wait_for_streamlit(page)

    @pytest.mark.smoke
    def test_custom_css_injected(self, page, app_url):
        """The shared CSS block should be present in the DOM."""
        self._navigate(page, app_url)
        # The CSS contains our custom border-left rule for stMetric
        styles = page.locator("style")
        found = False
        for i in range(styles.count()):
            text = styles.nth(i).inner_text()
            if "#29B5E8" in text and "stMetric" in text:
                found = True
                break
        assert found, "Custom CSS with #29B5E8 and stMetric not found in page styles"

    @pytest.mark.smoke
    def test_metric_card_has_border(self, page, app_url):
        """KPI metric cards should have the Snowflake blue left border."""
        self._navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 1, "No metric cards found"
        border = metrics.first.evaluate(
            "el => window.getComputedStyle(el).borderLeftColor"
        )
        # #29B5E8 = rgb(41, 181, 232)
        assert "41" in border and "181" in border and "232" in border, (
            f"Expected Snowflake blue border-left, got: {border}"
        )

    def test_metric_card_has_background(self, page, app_url):
        """KPI metric cards should have the gradient background."""
        self._navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 1
        bg = metrics.first.evaluate(
            "el => window.getComputedStyle(el).backgroundImage"
        )
        assert "gradient" in bg or "linear" in bg, (
            f"Expected gradient background on metric card, got: {bg}"
        )

    def test_metric_label_uppercase(self, page, app_url):
        """KPI metric labels should be uppercase via CSS."""
        self._navigate(page, app_url)
        labels = page.locator('[data-testid="stMetric"] label')
        if labels.count() == 0:
            pytest.skip("No metric labels found")
        transform = labels.first.evaluate(
            "el => window.getComputedStyle(el).textTransform"
        )
        assert transform == "uppercase", (
            f"Expected uppercase text-transform on metric labels, got: {transform}"
        )

    @pytest.mark.smoke
    def test_sidebar_brand_header_exists(self, page, app_url):
        """Sidebar should contain the .brand-header div with AI_EXTRACT text."""
        self._navigate(page, app_url)
        sidebar = page.locator('[data-testid="stSidebar"]')
        if sidebar.count() == 0:
            # Try expanding collapsed sidebar
            expand = page.locator('[data-testid="stSidebarCollapsedControl"]')
            if expand.count() > 0:
                expand.first.click()
                page.wait_for_timeout(1000)
            sidebar = page.locator('[data-testid="stSidebar"]')
        assert sidebar.count() >= 1, "Sidebar not found"
        brand = sidebar.locator(".brand-header")
        assert brand.count() >= 1, "Sidebar .brand-header div not found"
        brand_text = brand.first.inner_text()
        assert "AI_EXTRACT" in brand_text, (
            f"Expected 'AI_EXTRACT' in sidebar brand header, got: {brand_text}"
        )

    def test_sidebar_brand_has_subtitle(self, page, app_url):
        """Brand header should contain 'Document Processing POC' subtitle."""
        self._navigate(page, app_url)
        sidebar = page.locator('[data-testid="stSidebar"]')
        if sidebar.count() == 0:
            expand = page.locator('[data-testid="stSidebarCollapsedControl"]')
            if expand.count() > 0:
                expand.first.click()
                page.wait_for_timeout(1000)
            sidebar = page.locator('[data-testid="stSidebar"]')
        if sidebar.count() == 0:
            pytest.skip("Sidebar not found")
        brand = sidebar.locator(".brand-header")
        if brand.count() == 0:
            pytest.skip("Brand header not found")
        assert "Document Processing POC" in brand.first.inner_text()


# ---------------------------------------------------------------------------
# Dashboard page branding
# ---------------------------------------------------------------------------

class TestDashboardBranding:
    """Verify CSS theming carries over to the Dashboard page."""

    def _navigate(self, page, app_url):
        for attempt in range(3):
            page.goto(f"{app_url}/Dashboard", wait_until="domcontentloaded", timeout=90_000)
            wait_for_streamlit(page)
            if page.locator('[data-testid="stMetric"]').count() > 0:
                return
            page.wait_for_timeout(2000)
        wait_for_streamlit(page)

    def test_dashboard_metric_border(self, page, app_url):
        """Dashboard KPI cards should have the Snowflake blue border."""
        self._navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        assert metrics.count() >= 1
        border = metrics.first.evaluate(
            "el => window.getComputedStyle(el).borderLeftColor"
        )
        assert "41" in border and "181" in border and "232" in border, (
            f"Expected Snowflake blue border on Dashboard metrics, got: {border}"
        )

    def test_dashboard_sidebar_brand(self, page, app_url):
        """Dashboard sidebar should have the brand header."""
        self._navigate(page, app_url)
        sidebar = page.locator('[data-testid="stSidebar"]')
        if sidebar.count() == 0:
            expand = page.locator('[data-testid="stSidebarCollapsedControl"]')
            if expand.count() > 0:
                expand.first.click()
                page.wait_for_timeout(1000)
            sidebar = page.locator('[data-testid="stSidebar"]')
        if sidebar.count() == 0:
            pytest.skip("Sidebar not found")
        brand = sidebar.locator(".brand-header")
        assert brand.count() >= 1, "Brand header missing on Dashboard sidebar"

    def test_dataframe_has_border(self, page, app_url):
        """Data tables on Dashboard should have styled borders."""
        self._navigate(page, app_url)
        wait_for_streamlit(page, '[data-testid="stDataFrame"]')
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            pytest.skip("No data tables on Dashboard")
        border = tables.first.evaluate(
            "el => window.getComputedStyle(el).borderRadius"
        )
        assert "8px" in border, (
            f"Expected 8px border-radius on data tables, got: {border}"
        )


# ---------------------------------------------------------------------------
# Cross-page consistency
# ---------------------------------------------------------------------------

class TestCrossPageBranding:
    """Verify branding is consistent across multiple pages."""

    PAGES = [
        ("Landing", ""),
        ("Analytics", "/Analytics"),
        ("Review", "/Review"),
    ]

    @pytest.mark.slow
    def test_brand_header_on_all_pages(self, page, app_url):
        """Every page should have the sidebar brand header."""
        for name, path in self.PAGES:
            page.goto(f"{app_url}{path}", wait_until="domcontentloaded", timeout=90_000)
            wait_for_streamlit(page)
            page.wait_for_timeout(2000)
            sidebar = page.locator('[data-testid="stSidebar"]')
            if sidebar.count() == 0:
                expand = page.locator('[data-testid="stSidebarCollapsedControl"]')
                if expand.count() > 0:
                    expand.first.click()
                    page.wait_for_timeout(1000)
                sidebar = page.locator('[data-testid="stSidebar"]')
            if sidebar.count() == 0:
                continue  # Some pages may not render sidebar in CI
            brand = sidebar.locator(".brand-header")
            assert brand.count() >= 1, (
                f"Brand header missing on {name} page"
            )
            assert_no_exceptions(page)

    @pytest.mark.slow
    def test_css_present_on_all_pages(self, page, app_url):
        """Custom CSS should be injected on every page."""
        for name, path in self.PAGES:
            page.goto(f"{app_url}{path}", wait_until="domcontentloaded", timeout=90_000)
            wait_for_streamlit(page)
            page.wait_for_timeout(2000)
            styles = page.locator("style")
            found = False
            for i in range(styles.count()):
                text = styles.nth(i).inner_text()
                if "#29B5E8" in text:
                    found = True
                    break
            assert found, f"Custom CSS not found on {name} page"
            assert_no_exceptions(page)
