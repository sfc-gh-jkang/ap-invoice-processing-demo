"""Functional tests for the AP Ledger page (pages/1_AP_Ledger.py)."""

import re

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


LEDGER_PATH = "/AP_Ledger"


def _navigate(page, app_url):
    """Navigate to AP Ledger and wait for render."""
    page.goto(f"{app_url}{LEDGER_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


def _get_result_count(page):
    """Parse the invoice result count from the subheader."""
    header = page.locator("text=/Invoices \\(\\d+ results\\)/")
    if header.count() > 0:
        text = header.first.inner_text()
        match = re.search(r"\((\d+) results\)", text)
        if match:
            return int(match.group(1))
    return None


@pytest.mark.smoke
def test_ledger_filter_selectboxes(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stSelectbox"]').count() >= 3


def test_ledger_invoice_table_renders(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=/Invoices \\(\\d+ results\\)/").count() > 0


def test_ledger_invoice_count_positive(app_url, page):
    _navigate(page, app_url)
    count = _get_result_count(page)
    assert count is not None and count > 0


def test_ledger_dataframe_present(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stDataFrame"]').count() > 0


def test_ledger_aging_summary_metrics(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMetric"]').count() >= 1


def test_ledger_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Accounts Payable Ledger").count() > 0


def test_ledger_invoice_detail_section(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Invoice Detail").count() > 0
    assert page.locator('[data-testid="stSelectbox"]').count() >= 4


def test_ledger_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=All invoices extracted from PDF").count() > 0


def test_ledger_aging_summary_header(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Aging Summary").count() > 0


@pytest.mark.smoke
def test_ledger_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)


# ---------------------------------------------------------------------------
# Interactive / scroll tests (each gets its own page load)
# ---------------------------------------------------------------------------


def test_ledger_vendor_filter_changes_results(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() >= 2:
        options.nth(1).click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page)
        count_after = _get_result_count(page)
        if count_after is not None:
            assert count_after <= count_before


def test_ledger_status_filter(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        paid = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid.count() > 0:
            paid.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)
    assert_no_exceptions(page)


def test_ledger_aging_bucket_filter(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        selectboxes.nth(2).click()
        page.wait_for_timeout(500)
        current_option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="Current")
        if current_option.count() > 0:
            current_option.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)
    assert_no_exceptions(page)


def test_ledger_combined_vendor_and_status_filter(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    selectboxes.first.click()
    page.wait_for_timeout(500)
    vendor_opts = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if vendor_opts.count() >= 2:
        vendor_opts.nth(1).click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        paid = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid.count() > 0:
            paid.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)

    count_after = _get_result_count(page)
    if count_after is not None:
        assert count_after <= count_before
    assert_no_exceptions(page)


def test_ledger_invoice_detail_renders_fields(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator("text=Extracted Header Fields").count() > 0 or \
        page.locator("text=Extracted Line Items").count() > 0


def test_ledger_detail_line_items_dataframe(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    if page.locator("text=Extracted Line Items").count() > 0:
        assert page.locator('[data-testid="stDataFrame"]').count() >= 2


def test_ledger_aging_bucket_filter_options(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        selectboxes.nth(2).click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        assert options.count() >= 7, f"Expected >=7 aging options, got {options.count()}"


def test_ledger_filter_reset_restores_count(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Not enough vendor options to test reset")
    options.nth(1).click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page)

    vendor_select = page.locator('[data-testid="stSelectbox"]').first
    vendor_select.click()
    page.wait_for_timeout(500)
    all_option = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="All Vendors")
    if all_option.count() > 0:
        all_option.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page)

    count_after = _get_result_count(page)
    assert count_after == count_before


def test_ledger_detail_subtotal_tax_total_metrics(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    metrics = page.locator('[data-testid="stMetric"]')
    labels_found = []
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        for label in ["Subtotal", "Tax", "Total"]:
            if label in text and label not in labels_found:
                labels_found.append(label)
    assert len(labels_found) >= 3


def test_ledger_detail_extracted_field_labels(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    body_text = page.inner_text("body")
    for label in ["Vendor:", "Invoice #:", "Due Date:"]:
        assert label in body_text, f"Expected '{label}' in invoice detail section"


def test_ledger_detail_source_pdf_section(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator("text=Source PDF").count() > 0


def test_ledger_switch_invoice_detail(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() < 4:
        pytest.skip("No invoice detail selectbox found")

    selectboxes.nth(3).click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Only one invoice available")
    options.nth(1).click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page)

    assert page.locator("text=Extracted Header Fields").count() > 0 or \
        page.locator("text=Source PDF").count() > 0
    assert_no_exceptions(page)


def test_ledger_status_filter_pending(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        pending = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PENDING")
        if pending.count() > 0:
            pending.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)
            count_after = _get_result_count(page)
            if count_before is not None and count_after is not None:
                assert count_after <= count_before
    assert_no_exceptions(page)


def test_ledger_aging_filter_31_60_days(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        selectboxes.nth(2).click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="31-60 Days")
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)
            count_after = _get_result_count(page)
            if count_after is not None:
                assert count_after <= count_before
    assert_no_exceptions(page)


def test_ledger_three_filter_combination(app_url, page):
    _navigate(page, app_url)
    count_before = _get_result_count(page)
    if count_before is None:
        pytest.skip("No invoice results header found")

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 1:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        vendor_opts = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        if vendor_opts.count() >= 2:
            vendor_opts.nth(1).click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 2:
        selectboxes.nth(1).click()
        page.wait_for_timeout(500)
        paid = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="PAID")
        if paid.count() > 0:
            paid.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() >= 3:
        selectboxes.nth(2).click()
        page.wait_for_timeout(500)
        current = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]', has_text="Current")
        if current.count() > 0:
            current.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page)

    count_after = _get_result_count(page)
    if count_after is not None:
        assert count_after <= count_before
    assert_no_exceptions(page)


def test_ledger_scroll_and_select_invoice_detail(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)

    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() < 4:
        pytest.skip("No invoice detail selectbox found")

    selectboxes.nth(3).click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() > 0:
        options.first.click()
        page.wait_for_timeout(2000)
        wait_for_streamlit(page)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)

        body = page.inner_text("body")
        has_pdf = page.locator("text=Source PDF").count() > 0
        has_fields = "Extracted Header Fields" in body
        has_lines = "Extracted Line Items" in body
        assert has_pdf and has_fields and has_lines
    assert_no_exceptions(page)
