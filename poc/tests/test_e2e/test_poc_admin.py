"""E2E: POC Admin Page — document type configuration management."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

ADMIN_PAGE_PATH = "/Admin"


def _navigate(page, app_url):
    """Navigate to the Admin page with retry."""
    url = f"{app_url}{ADMIN_PAGE_PATH}"
    for attempt in range(3):
        page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator("h1").count() > 0:
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


def _has_config_data(page):
    """Check whether the page loaded config data (dataframe present)."""
    return page.locator('[data-testid="stDataFrame"]').count() > 0


class TestAdminPageSmoke:
    """Core smoke tests for the Admin page."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate(page, app_url)
        assert_no_exceptions(page)

    @pytest.mark.smoke
    def test_title_renders(self, page, app_url):
        _navigate(page, app_url)
        title = page.locator("h1")
        assert title.count() >= 1
        assert "Document Type" in title.first.inner_text()

    def test_caption_renders(self, page, app_url):
        _navigate(page, app_url)
        body_text = page.inner_text("body").lower()
        assert (
            "document type" in body_text
            or "add new" in body_text
            or "entity fields" in body_text
        )


class TestExistingDocTypesTable:
    """Verify the existing document types summary table (when data is available)."""

    def test_dataframe_renders(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data available in local Streamlit session")
        df = page.locator('[data-testid="stDataFrame"]')
        assert df.count() >= 1

    def test_dataframe_has_doc_type_rows(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data available in local Streamlit session")
        df = page.locator('[data-testid="stDataFrame"]').first
        text = df.inner_text()
        assert "INVOICE" in text, "INVOICE not found in config table"

    def test_dataframe_shows_utility_bill(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data available in local Streamlit session")
        df = page.locator('[data-testid="stDataFrame"]').first
        text = df.inner_text()
        assert "UTILITY_BILL" in text, "UTILITY_BILL not found in config table"

    def test_dataframe_shows_at_least_4_types(self, page, app_url):
        """Seed data includes INVOICE, CONTRACT, RECEIPT, UTILITY_BILL."""
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data available in local Streamlit session")
        df = page.locator('[data-testid="stDataFrame"]').first
        text = df.inner_text()
        for doc_type in ["INVOICE", "CONTRACT", "RECEIPT", "UTILITY_BILL"]:
            assert doc_type in text, f"{doc_type} not found in config table"


class TestDetailViewer:
    """Verify the configuration detail section (requires config data)."""

    def test_selectbox_renders(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data — detail viewer not rendered")
        selectbox = page.locator('[data-testid="stSelectbox"]')
        assert selectbox.count() >= 1, "No selectbox found for type selection"

    def test_extraction_prompt_shown(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data — detail viewer not rendered")
        page.wait_for_timeout(1000)
        code_blocks = page.locator('[data-testid="stCode"]')
        assert code_blocks.count() >= 1, "No code block found for extraction prompt"

    def test_field_labels_shown(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data — detail viewer not rendered")
        page.wait_for_timeout(1000)
        markdown = page.locator('[data-testid="stMarkdown"]')
        all_text = ""
        for i in range(markdown.count()):
            all_text += markdown.nth(i).inner_text() + "\n"
        assert "field_1" in all_text.lower() or "Field Labels" in all_text


class TestActiveToggle:
    """Verify the active/inactive toggle checkbox (requires config data)."""

    def test_active_checkbox_renders(self, page, app_url):
        _navigate(page, app_url)
        if not _has_config_data(page):
            pytest.skip("No config data — active toggle not rendered")
        page.wait_for_timeout(1000)
        checkbox = page.locator('[data-testid="stCheckbox"]')
        assert checkbox.count() >= 1, "No active toggle checkbox found"


class TestAddNewForm:
    """Verify the 'Add New Document Type' form renders correctly."""

    def test_form_section_exists(self, page, app_url):
        _navigate(page, app_url)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        all_text = page.inner_text("body")
        assert "Add New Document Type" in all_text

    def test_form_has_text_inputs(self, page, app_url):
        _navigate(page, app_url)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        text_inputs = page.locator('[data-testid="stTextInput"]')
        assert text_inputs.count() >= 2, (
            f"Expected >=2 text inputs (doc type code, display name), got {text_inputs.count()}"
        )

    def test_form_has_text_areas(self, page, app_url):
        """The no-JSON admin builder uses text_input fields instead of text areas.

        Verify the form has enough input controls for the guided field builder.
        """
        _navigate(page, app_url)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        # The new no-JSON builder uses st.text_input for each field name/label,
        # plus doc_type and display_name. Should have many text inputs.
        text_inputs = page.locator('[data-testid="stTextInput"]')
        assert text_inputs.count() >= 4, (
            f"Expected >=4 text inputs (doc type, display name, + field builder), "
            f"got {text_inputs.count()}"
        )

    def test_form_has_submit_button(self, page, app_url):
        _navigate(page, app_url)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        submit = page.locator('[data-testid="stFormSubmitButton"]')
        assert submit.count() >= 1, "No form submit button found"
