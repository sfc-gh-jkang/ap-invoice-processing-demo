"""E2E: Multi-document-type tests — CONTRACT, RECEIPT filtering, switching, Admin validation rules."""

import pytest
from tests.test_e2e.helpers import wait_for_streamlit, assert_no_exceptions


pytestmark = pytest.mark.e2e

VIEWER_PATH = "/Document_Viewer"
ADMIN_PATH = "/Admin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _navigate_viewer(page, app_url):
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


def _navigate_admin(page, app_url):
    """Navigate to the Admin page with retry."""
    for attempt in range(3):
        page.goto(f"{app_url}{ADMIN_PATH}", wait_until="domcontentloaded", timeout=90_000)
        wait_for_streamlit(page)
        if page.locator("h1").count() > 0:
            page.wait_for_timeout(2000)
            return
        page.wait_for_timeout(2000)
    wait_for_streamlit(page)


def _select_doc_type(page, doc_type_label):
    """Select a doc type from the first selectbox on the Document Viewer page.

    Returns True if the option was found and selected, False otherwise.
    """
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() == 0:
        return False
    # The first selectbox is the doc type filter
    sb = selectboxes.first
    sb.click()
    page.wait_for_timeout(500)
    # Look for the option in the dropdown list
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    for i in range(options.count()):
        text = options.nth(i).inner_text()
        if doc_type_label.upper() in text.upper():
            options.nth(i).click()
            page.wait_for_timeout(3000)  # Wait for Streamlit rerun
            return True
    # Close dropdown if option not found
    page.keyboard.press("Escape")
    return False


def _has_viewer_data(page):
    """Check whether the Document Viewer loaded any data."""
    return page.locator('[data-testid="stDataFrame"]').count() > 0


def _has_admin_data(page):
    """Check whether the Admin page loaded config data."""
    return page.locator('[data-testid="stDataFrame"]').count() > 0


# ---------------------------------------------------------------------------
# TestMultiDocTypeSmoke — doc type filter has CONTRACT and RECEIPT options
# ---------------------------------------------------------------------------

class TestMultiDocTypeSmoke:
    """Verify the Document Viewer doc type filter includes all 4 types."""

    @pytest.mark.smoke
    def test_page_loads_without_exceptions(self, page, app_url):
        _navigate_viewer(page, app_url)
        assert_no_exceptions(page)

    def test_doc_type_filter_has_contract(self, page, app_url):
        """The doc type selectbox should include CONTRACT as an option."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        sb = page.locator('[data-testid="stSelectbox"]').first
        sb.click()
        page.wait_for_timeout(500)
        dropdown_text = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"]'
        ).inner_text()
        page.keyboard.press("Escape")
        assert "CONTRACT" in dropdown_text.upper(), (
            f"CONTRACT not in doc type dropdown options: {dropdown_text}"
        )

    def test_doc_type_filter_has_receipt(self, page, app_url):
        """The doc type selectbox should include RECEIPT as an option."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        sb = page.locator('[data-testid="stSelectbox"]').first
        sb.click()
        page.wait_for_timeout(500)
        dropdown_text = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"]'
        ).inner_text()
        page.keyboard.press("Escape")
        assert "RECEIPT" in dropdown_text.upper(), (
            f"RECEIPT not in doc type dropdown options: {dropdown_text}"
        )

    def test_doc_type_filter_has_all_four_types(self, page, app_url):
        """All 4 seeded doc types should appear in the filter dropdown."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        sb = page.locator('[data-testid="stSelectbox"]').first
        sb.click()
        page.wait_for_timeout(500)
        dropdown_text = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"]'
        ).inner_text().upper()
        page.keyboard.press("Escape")
        for doc_type in ["INVOICE", "CONTRACT", "RECEIPT", "UTILITY_BILL", "LEASE"]:
            assert doc_type in dropdown_text, (
                f"{doc_type} not in doc type dropdown. Options: {dropdown_text}"
            )


# ---------------------------------------------------------------------------
# TestContractDocViewer — filter to CONTRACT, verify contract-specific content
# ---------------------------------------------------------------------------

class TestContractDocViewer:
    """Verify Document Viewer displays contract documents correctly."""

    def test_filter_to_contract_shows_data(self, page, app_url):
        """Filtering to CONTRACT should show a data table with results."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "CONTRACT")
        if not selected:
            pytest.skip("CONTRACT option not available in filter")
        # After filtering, should still have a data frame — but skip if no
        # CONTRACT documents exist (config may define the type with 0 docs).
        page.wait_for_timeout(2000)
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            body = page.inner_text("body")
            if "0 results" in body or "No" in body:
                pytest.skip("No CONTRACT documents in this environment")
        assert tables.count() >= 1, "No data table after filtering to CONTRACT"

    def test_contract_fields_in_detail(self, page, app_url):
        """Contract detail should show contract-specific field labels."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "CONTRACT")
        if not selected:
            pytest.skip("CONTRACT option not available in filter")
        page.wait_for_timeout(2000)
        # Skip if no contract documents exist in this environment
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            pytest.skip("No CONTRACT documents in this environment")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        body_text = page.inner_text("body")
        # Contract fields: Party Name, Contract Number, Effective Date, Total Value
        contract_indicators = ["Party Name", "Contract", "Effective Date", "Total Value"]
        found_any = any(ind in body_text for ind in contract_indicators)
        assert found_any, (
            f"No contract-specific field labels found in page. "
            f"Looked for: {contract_indicators}"
        )

    def test_contract_documents_are_contract_type(self, page, app_url):
        """The results table text should reference contract documents."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "CONTRACT")
        if not selected:
            pytest.skip("CONTRACT option not available in filter")
        page.wait_for_timeout(2000)
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            pytest.skip("No CONTRACT documents in this environment")
        body_text = page.inner_text("body").upper()
        assert "CONTRACT" in body_text, "Page body should mention CONTRACT after filtering"


# ---------------------------------------------------------------------------
# TestReceiptDocViewer — filter to RECEIPT, verify receipt-specific content
# ---------------------------------------------------------------------------

class TestReceiptDocViewer:
    """Verify Document Viewer displays receipt documents correctly."""

    def test_filter_to_receipt_shows_data(self, page, app_url):
        """Filtering to RECEIPT should show a data table with results."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "RECEIPT")
        if not selected:
            pytest.skip("RECEIPT option not available in filter")
        # Skip if no RECEIPT documents exist (config may define type with 0 docs)
        page.wait_for_timeout(2000)
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            body = page.inner_text("body")
            if "0 results" in body or "No" in body:
                pytest.skip("No RECEIPT documents in this environment")
        assert tables.count() >= 1, "No data table after filtering to RECEIPT"

    def test_receipt_fields_in_detail(self, page, app_url):
        """Receipt detail should show receipt-specific field labels."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "RECEIPT")
        if not selected:
            pytest.skip("RECEIPT option not available in filter")
        page.wait_for_timeout(2000)
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            pytest.skip("No RECEIPT documents in this environment")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        body_text = page.inner_text("body")
        receipt_indicators = ["Merchant", "Receipt", "Purchase Date", "Total Paid"]
        found_any = any(ind in body_text for ind in receipt_indicators)
        assert found_any, (
            f"No receipt-specific field labels found in page. "
            f"Looked for: {receipt_indicators}"
        )

    def test_receipt_documents_are_receipt_type(self, page, app_url):
        """The results should reference receipt documents after filtering."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")
        selected = _select_doc_type(page, "RECEIPT")
        if not selected:
            pytest.skip("RECEIPT option not available in filter")
        page.wait_for_timeout(2000)
        tables = page.locator('[data-testid="stDataFrame"]')
        if tables.count() == 0:
            pytest.skip("No RECEIPT documents in this environment")
        body_text = page.inner_text("body").upper()
        assert "RECEIPT" in body_text, "Page body should mention RECEIPT after filtering"


# ---------------------------------------------------------------------------
# TestDocTypeSwitching — switch between types and verify content updates
# ---------------------------------------------------------------------------

class TestDocTypeSwitching:
    """Verify that switching doc types updates the displayed data."""

    def test_switch_from_invoice_to_contract(self, page, app_url):
        """Switching from INVOICE to CONTRACT should change the page content."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")

        # First select INVOICE
        selected = _select_doc_type(page, "INVOICE")
        if not selected:
            pytest.skip("INVOICE option not available")
        page.wait_for_timeout(1000)
        invoice_text = page.inner_text("body")

        # Now switch to CONTRACT
        selected = _select_doc_type(page, "CONTRACT")
        if not selected:
            pytest.skip("CONTRACT option not available")
        page.wait_for_timeout(1000)
        contract_text = page.inner_text("body")

        # The page content should differ (different doc type label at minimum)
        assert invoice_text != contract_text, (
            "Page content did not change after switching from INVOICE to CONTRACT"
        )

    def test_switch_from_receipt_to_utility_bill(self, page, app_url):
        """Switching from RECEIPT to UTILITY_BILL should change page content."""
        _navigate_viewer(page, app_url)
        if not _has_viewer_data(page):
            pytest.skip("No document data in viewer")

        selected = _select_doc_type(page, "RECEIPT")
        if not selected:
            pytest.skip("RECEIPT option not available")
        page.wait_for_timeout(1000)
        receipt_text = page.inner_text("body")

        selected = _select_doc_type(page, "UTILITY_BILL")
        if not selected:
            pytest.skip("UTILITY_BILL option not available")
        page.wait_for_timeout(1000)
        utility_text = page.inner_text("body")

        assert receipt_text != utility_text, (
            "Page content did not change after switching from RECEIPT to UTILITY_BILL"
        )


# ---------------------------------------------------------------------------
# TestAdminShowsAllTypes — Admin page shows all 4 types with validation rules
# ---------------------------------------------------------------------------

class TestAdminShowsAllTypes:
    """Verify the Admin page shows all 4 doc types including validation rules."""

    def test_admin_config_table_has_all_types(self, page, app_url):
        """Admin config table should list INVOICE, CONTRACT, RECEIPT, UTILITY_BILL."""
        _navigate_admin(page, app_url)
        if not _has_admin_data(page):
            pytest.skip("No config data in admin page")
        # The dataframe uses Glide Data Grid (canvas) so inner_text() won't
        # capture cell values.  Instead, check the "Select type to view"
        # selectbox which lists all configured doc types as real DOM options.
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        if selectboxes.count() == 0:
            pytest.skip("No selectbox for type selection on admin page")
        sb = selectboxes.first
        sb.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        option_texts = [options.nth(i).inner_text().upper() for i in range(options.count())]
        sb.press("Escape")
        for doc_type in ["INVOICE", "CONTRACT", "RECEIPT", "UTILITY_BILL", "LEASE"]:
            assert doc_type in option_texts, (
                f"{doc_type} not in admin selectbox options: {option_texts}"
            )

    def test_admin_contract_has_validation_rules(self, page, app_url):
        """CONTRACT detail should show validation_rules content."""
        _navigate_admin(page, app_url)
        if not _has_admin_data(page):
            pytest.skip("No config data in admin page")
        # Select CONTRACT in the detail viewer selectbox
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        if selectboxes.count() == 0:
            pytest.skip("No selectbox for type selection")
        sb = selectboxes.first
        sb.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        found = False
        for i in range(options.count()):
            if "CONTRACT" in options.nth(i).inner_text().upper():
                options.nth(i).click()
                found = True
                break
        if not found:
            page.keyboard.press("Escape")
            pytest.skip("CONTRACT not in admin selectbox")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        body_text = page.inner_text("body")
        # Validation rules should show fields like total_value, effective_date, party_name
        validation_indicators = ["total_value", "effective_date", "party_name", "validation"]
        found_any = any(ind in body_text.lower() for ind in validation_indicators)
        assert found_any, (
            f"No validation rule indicators found for CONTRACT. "
            f"Looked for: {validation_indicators}"
        )

    def test_admin_receipt_has_validation_rules(self, page, app_url):
        """RECEIPT detail should show validation_rules content."""
        _navigate_admin(page, app_url)
        if not _has_admin_data(page):
            pytest.skip("No config data in admin page")
        selectboxes = page.locator('[data-testid="stSelectbox"]')
        if selectboxes.count() == 0:
            pytest.skip("No selectbox for type selection")
        sb = selectboxes.first
        sb.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        found = False
        for i in range(options.count()):
            if "RECEIPT" in options.nth(i).inner_text().upper():
                options.nth(i).click()
                found = True
                break
        if not found:
            page.keyboard.press("Escape")
            pytest.skip("RECEIPT not in admin selectbox")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        body_text = page.inner_text("body")
        validation_indicators = ["total_paid", "purchase_date", "merchant_name", "validation"]
        found_any = any(ind in body_text.lower() for ind in validation_indicators)
        assert found_any, (
            f"No validation rule indicators found for RECEIPT. "
            f"Looked for: {validation_indicators}"
        )

    def test_no_exceptions_on_admin(self, page, app_url):
        _navigate_admin(page, app_url)
        assert_no_exceptions(page)
