"""E2E: POC Document Viewer — document list, filters, drill-down detail."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

VIEWER_PATH = "/Document_Viewer"


def _navigate(page, app_url):
    """Navigate to the Document Viewer page with retry."""
    for attempt in range(3):
        page.goto(f"{app_url}{VIEWER_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator('[data-testid="stDataFrame"]').count() > 0:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


class TestDocumentViewerSmoke:
    """Core Document Viewer smoke tests."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Document Viewer" in title.first.inner_text()

    @pytest.mark.smoke
    def test_document_table_loads(self, page, app_url):
        _navigate(page, app_url)
        tables = page.locator('[data-testid="stDataFrame"]')
        assert tables.count() >= 1, "No document table found"


class TestDocumentViewerFilters:
    """Verify filter controls work."""

    def test_sender_filter_exists(self, page, app_url):
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 1, "No filter selectboxes found"

    def test_status_filter_exists(self, page, app_url):
        _navigate(page, app_url)
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        assert selectboxes.count() >= 2, "Expected at least 2 filter selectboxes"

    def test_document_count_label(self, page, app_url):
        _navigate(page, app_url)
        # Should show "Documents (N results)"
        subheaders = page.locator('[data-testid="stMarkdown"] h3, [data-testid="stSubheader"]')
        found = False
        for i in range(subheaders.count()):
            text = subheaders.nth(i).inner_text()
            if "Documents" in text and "results" in text:
                found = True
                break
        # Also check the broader page content
        if not found:
            page_text = page.inner_text("body")
            assert "Documents" in page_text and "results" in page_text


class TestDocumentViewerDetail:
    """Verify document drill-down shows extracted fields."""

    def test_document_detail_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Document Detail" in page_text, "Document Detail section not found"

    def test_select_document_dropdown(self, page, app_url):
        _navigate(page, app_url)
        # There should be a selectbox for choosing a specific document
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        # At least 3: sender filter, status filter, document selector
        assert selectboxes.count() >= 3, (
            f"Expected >=3 selectboxes (2 filters + doc selector), got {selectboxes.count()}"
        )

    def test_extracted_fields_visible(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        # Should show "Extracted Fields" section
        assert "Extracted Fields" in page_text, "Extracted Fields section not found"

    def test_extracted_line_items_visible(self, page, app_url):
        _navigate(page, app_url)
        page_text = page.inner_text("body")
        assert "Line Items" in page_text, "Line Items section not found"  # "Extracted Line Items"

    def test_metric_cards_in_detail(self, page, app_url):
        """The detail section should show Subtotal, Tax, Total metrics."""
        _navigate(page, app_url)
        metrics = page.locator('[data-testid="stMetric"]')
        # Aging cards + detail metrics
        assert metrics.count() >= 3, f"Expected >=3 metric cards, got {metrics.count()}"
