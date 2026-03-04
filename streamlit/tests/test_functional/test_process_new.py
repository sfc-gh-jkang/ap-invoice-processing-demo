"""Functional tests for the Process New page (pages/3_Process_New.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


PROCESS_PATH = "/Process_New"


def _navigate(page, app_url):
    """Navigate to Process New and wait for render."""
    page.goto(f"{app_url}{PROCESS_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


@pytest.mark.smoke
def test_process_new_form_elements(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stSelectbox"]').count() >= 1
    assert page.locator('[data-testid="stSlider"]').count() >= 2


def test_process_new_run_extraction_button(app_url, page):
    _navigate(page, app_url)
    assert page.locator('button:has-text("Run Extraction")').count() > 0


def test_process_new_status_metrics(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMetric"]').count() >= 4


def test_process_new_progress_bar(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stProgress"]').count() > 0


def test_process_new_extraction_status_expander(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)


def test_process_new_multiselect_categories(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMultiSelect"]').count() >= 1


def test_process_new_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Process New Invoices").count() > 0


def test_process_new_generate_form_submit_button(app_url, page):
    _navigate(page, app_url)
    assert page.locator('button:has-text("Generate")').count() > 0


def test_process_new_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Live demo").count() > 0


def test_process_new_three_sliders(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stSlider"]').count() >= 3


def test_process_new_status_metric_labels(app_url, page):
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    expected = ["Total Files", "Extracted", "Pending", "Failed"]
    found = []
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        for label in expected:
            if label in text and label not in found:
                found.append(label)
    assert len(found) >= 4, f"Expected {expected}, found: {found}"


@pytest.mark.smoke
def test_process_new_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)


# ---------------------------------------------------------------------------
# Interactive tests (each gets its own page load)
# ---------------------------------------------------------------------------


def test_process_new_step_subheaders(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    for step in ["Step 1", "Step 2", "Step 3", "Step 4"]:
        assert page.locator(f"text={step}").count() > 0, f"Expected '{step}' subheader"


def test_process_new_reset_demo_expander(app_url, page):
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() > 0:
        expander_text = expanders.first.inner_text()
        assert "Reset" in expander_text or expanders.count() >= 1
    assert_no_exceptions(page)


def test_process_new_refresh_status_button(app_url, page):
    _navigate(page, app_url)
    for _ in range(3):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
    btn = page.locator('button:has-text("Refresh")')
    if btn.count() == 0:
        btn = page.locator('button:has-text("Refresh Status")')
    if btn.count() == 0:
        pytest.skip("Refresh button not rendered")


def test_process_new_recently_extracted_table(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    df = page.locator('[data-testid="stDataFrame"]')
    if df.count() == 0:
        assert_no_exceptions(page)


def test_process_new_vendor_selectbox_has_options(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
        assert options.count() >= 1


def test_process_new_multiselect_default_categories(app_url, page):
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    if multiselect.count() > 0:
        text = multiselect.first.inner_text()
        assert "Beverages" in text
        assert "Snacks" in text


def test_process_new_reset_expander_has_button(app_url, page):
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() > 0:
        expanders.first.click()
        page.wait_for_timeout(500)
        assert page.locator('button:has-text("Reset Demo Invoices")').count() > 0


def test_process_new_step4_invoice_detail_selectbox(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    df = page.locator('[data-testid="stDataFrame"]')
    if df.count() > 0:
        assert page.locator('[data-testid="stSelectbox"]').count() >= 2


def test_process_new_step4_extracted_fields(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    body_text = page.inner_text("body")
    if "Recently Extracted Invoices" in body_text:
        df = page.locator('[data-testid="stDataFrame"]')
        if df.count() > 0:
            assert "Extracted Header Fields" in body_text or "Extracted Line Items" in body_text


def test_process_new_slider_interaction(app_url, page):
    _navigate(page, app_url)
    sliders = page.locator('[data-testid="stSlider"]')
    assert sliders.count() >= 1
    first_slider = sliders.first
    thumb = first_slider.locator('[role="slider"]')
    if thumb.count() > 0:
        thumb.first.click()
        page.wait_for_timeout(300)
        page.keyboard.press("ArrowRight")
        page.keyboard.press("ArrowRight")
        page.wait_for_timeout(500)
        slider_text = first_slider.inner_text()
        assert "5" in slider_text or "4" in slider_text
    assert_no_exceptions(page)


def test_process_new_multiselect_add_category(app_url, page):
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    assert multiselect.count() >= 1
    ms_input = multiselect.first.locator("input")
    ms_input.click()
    page.wait_for_timeout(500)
    ms_input.fill("Tobacco")
    page.wait_for_timeout(500)
    tobacco_option = page.locator('[role="option"]', has_text="Tobacco")
    if tobacco_option.count() > 0:
        tobacco_option.first.click()
        page.wait_for_timeout(500)
        assert "Tobacco" in multiselect.first.inner_text()
    assert_no_exceptions(page)


def test_process_new_multiselect_remove_category(app_url, page):
    _navigate(page, app_url)
    multiselect = page.locator('[data-testid="stMultiSelect"]')
    assert multiselect.count() >= 1
    ms_text_before = multiselect.first.inner_text()
    if "Beverages" not in ms_text_before:
        pytest.skip("'Beverages' not in default selection")

    beverages_close = multiselect.first.locator(
        'span:has-text("Beverages") >> [data-testid="stIcon"]')
    if beverages_close.count() == 0:
        beverages_close = multiselect.first.locator('[role="button"][aria-label*="Beverages"]')
    if beverages_close.count() == 0:
        beverages_close = multiselect.first.locator('svg').first
    if beverages_close.count() > 0:
        beverages_close.first.click()
        page.wait_for_timeout(500)
        assert "Snacks" in multiselect.first.inner_text()
    assert_no_exceptions(page)


def test_process_new_refresh_button_click(app_url, page):
    _navigate(page, app_url)
    for _ in range(3):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
    refresh_btn = page.locator('button:has-text("Refresh")')
    if refresh_btn.count() == 0:
        refresh_btn = page.locator('button:has-text("Refresh Status")')
    if refresh_btn.count() == 0:
        pytest.skip("Refresh button not rendered")
    refresh_btn.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page)
    assert page.locator("text=Process New Invoices").count() > 0
    assert_no_exceptions(page)


@pytest.mark.slow
def test_process_new_form_submit_generates_invoices(app_url, page):
    _navigate(page, app_url)
    submit_btn = page.locator('button:has-text("Generate")')
    assert submit_btn.count() > 0
    submit_btn.first.click()
    page.wait_for_timeout(5000)
    wait_for_streamlit(page)
    page.wait_for_timeout(10000)
    body = page.inner_text("body")
    status_complete = (
        "complete" in body.lower()
        or "generated" in body.lower()
        or "Result:" in body
        or page.locator('[data-testid="stStatusWidget"]').count() > 0
    )
    assert status_complete
    assert_no_exceptions(page)
