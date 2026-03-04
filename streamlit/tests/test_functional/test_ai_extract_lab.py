"""Functional tests for the AI Extract Lab page (pages/4_AI_Extract_Lab.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


EXTRACT_PATH = "/AI_Extract_Lab"
WAIT_SELECTORS = '[data-testid="stRadio"], [data-testid="stExpander"]'


def _navigate(page, app_url):
    """Navigate to AI Extract Lab and wait for render."""
    page.goto(f"{app_url}{EXTRACT_PATH}", wait_until="networkidle")
    wait_for_streamlit(page, selectors=WAIT_SELECTORS)


@pytest.mark.smoke
def test_extract_lab_reference_expander(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stExpander"]').count() >= 1
    assert page.locator("text=AI_EXTRACT Quick Reference").count() > 0


def test_extract_lab_prompt_mode_radio(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stRadio"]').count() >= 1
    for mode in ["Starter Template", "Visual Builder", "Raw JSON Editor"]:
        assert page.locator(f"text={mode}").count() > 0, f"Missing: {mode}"


def test_extract_lab_default_template_invoice_header(app_url, page):
    _navigate(page, app_url)
    textarea = page.locator("textarea").first
    if textarea.count() > 0:
        assert "vendor_name" in textarea.input_value()


def test_extract_lab_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=AI_EXTRACT Lab").count() > 0


def test_extract_lab_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Experiment with Snowflake Cortex AI_EXTRACT").count() > 0


def test_extract_lab_prompt_builder_subheader(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Prompt Builder").count() > 0


@pytest.mark.smoke
def test_extract_lab_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)


# ---------------------------------------------------------------------------
# Interactive tests (each gets its own page load)
# ---------------------------------------------------------------------------


def test_extract_lab_switch_to_line_items_template(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="Line Items Table")
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page, selectors="textarea")
            textarea = page.locator("textarea").first
            if textarea.count() > 0:
                assert "column_ordering" in textarea.input_value()


def test_extract_lab_visual_builder_mode(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        assert page.locator('[data-testid="stTextInput"]').count() >= 2


def test_extract_lab_visual_builder_table_schema_mode(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        radios = page.locator('[data-testid="stRadio"]')
        if radios.count() >= 2:
            assert page.locator("text=Entity").count() > 0 or \
                page.locator("text=Table").count() > 0


def test_extract_lab_raw_json_editor_mode(app_url, page):
    _navigate(page, app_url)
    raw = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw.count() > 0:
        raw.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors="textarea")
        assert page.locator("textarea").count() >= 1


def test_extract_lab_staged_file_selectbox(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator('[data-testid="stSelectbox"]').count() >= 1


def test_extract_lab_run_button(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator('button:has-text("Run AI_EXTRACT")').count() > 0


def test_extract_lab_file_uploader(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator('[data-testid="stFileUploader"]').count() > 0


def test_extract_lab_browse_staged_header(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator("text=Browse Staged Invoices").count() > 0


def test_extract_lab_upload_section_header(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator("text=Upload & Extract").count() > 0


def test_extract_lab_general_document_template(app_url, page):
    _navigate(page, app_url)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    if selectboxes.count() > 0:
        selectboxes.first.click()
        page.wait_for_timeout(500)
        option = page.locator(
            '[data-testid="stSelectboxVirtualDropdown"] [role="option"]',
            has_text="General Document")
        if option.count() > 0:
            option.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page, selectors="textarea")
            textarea = page.locator("textarea").first
            if textarea.count() > 0:
                assert len(textarea.input_value()) > 0


def test_extract_lab_raw_json_invalid_shows_no_crash(app_url, page):
    _navigate(page, app_url)
    raw = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw.count() > 0:
        raw.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors="textarea")
        textarea = page.locator("textarea").first
        if textarea.count() > 0:
            textarea.fill("{invalid json!!!}")
            page.wait_for_timeout(500)
    assert_no_exceptions(page)


def test_extract_lab_preview_response_format_expander(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        preview = page.locator("text=Preview")
        code_block = page.locator('[data-testid="stCode"]')
        assert preview.count() > 0 or code_block.count() > 0


def test_extract_lab_visual_builder_add_field_button(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
        assert page.locator('button:has-text("Add field")').count() > 0


def test_extract_lab_visual_builder_table_mode_inputs(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
        if table_label.count() > 0:
            table_label.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
            body_text = page.inner_text("body")
            assert "Table description" in body_text
            assert "Table name" in body_text


def test_extract_lab_visual_builder_add_column_button(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() > 0:
        vb.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
        table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
        if table_label.count() > 0:
            table_label.first.click()
            page.wait_for_timeout(1500)
            wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
            assert page.locator('button:has-text("Add column")').count() > 0


def test_extract_lab_invalid_json_error_message(app_url, page):
    _navigate(page, app_url)
    raw = page.locator('[data-testid="stRadio"] label', has_text="Raw JSON Editor")
    if raw.count() > 0:
        raw.first.click()
        page.wait_for_timeout(1500)
        wait_for_streamlit(page, selectors="textarea")
        textarea = page.locator("textarea").first
        if textarea.count() > 0:
            textarea.fill("{not valid json!!!")
            textarea.press("Tab")
            page.wait_for_timeout(1500)
            wait_for_streamlit(page, selectors='[data-testid="stAlert"]')
            assert page.locator("text=Invalid JSON").count() > 0


def test_extract_lab_upload_run_button(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator("text=Upload & Extract").count() > 0
    assert page.locator('[data-testid="stFileUploader"]').count() > 0


def test_extract_lab_add_field_click_adds_row(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() == 0:
        pytest.skip("Visual Builder radio not found")
    vb.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    inputs_before = page.locator('[data-testid="stTextInput"]').count()
    add_btn = page.locator('button:has-text("Add field")')
    assert add_btn.count() > 0
    add_btn.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    assert page.locator('[data-testid="stTextInput"]').count() > inputs_before
    assert_no_exceptions(page)


def test_extract_lab_delete_field_click(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() == 0:
        pytest.skip("Visual Builder radio not found")
    vb.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    inputs_before = page.locator('[data-testid="stTextInput"]').count()
    if inputs_before < 4:
        pytest.skip("Not enough fields to test deletion")
    delete_btn = page.locator('button:has-text("✕")')
    if delete_btn.count() == 0:
        pytest.skip("No delete buttons found")
    delete_btn.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    assert page.locator('[data-testid="stTextInput"]').count() < inputs_before
    assert_no_exceptions(page)


def test_extract_lab_add_column_click_adds_row(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() == 0:
        pytest.skip("Visual Builder radio not found")
    vb.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stRadio"]')
    table_label = page.locator('[data-testid="stRadio"] label', has_text="Table")
    if table_label.count() == 0:
        pytest.skip("Table sub-mode not found")
    table_label.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    inputs_before = page.locator('[data-testid="stTextInput"]').count()
    add_col_btn = page.locator('button:has-text("Add column")')
    assert add_col_btn.count() > 0
    add_col_btn.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    assert page.locator('[data-testid="stTextInput"]').count() > inputs_before
    assert_no_exceptions(page)


def test_extract_lab_edit_entity_field_updates_preview(app_url, page):
    _navigate(page, app_url)
    vb = page.locator('[data-testid="stRadio"] label', has_text="Visual Builder")
    if vb.count() == 0:
        pytest.skip("Visual Builder radio not found")
    vb.first.click()
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stTextInput"]')
    text_inputs = page.locator('[data-testid="stTextInput"] input')
    if text_inputs.count() == 0:
        pytest.skip("No text inputs found")
    text_inputs.first.click()
    text_inputs.first.fill("custom_test_field")
    text_inputs.first.press("Tab")
    page.wait_for_timeout(1500)
    wait_for_streamlit(page, selectors='[data-testid="stCode"]')
    code_blocks = page.locator('[data-testid="stCode"]')
    if code_blocks.count() > 0:
        assert "custom_test_field" in code_blocks.first.inner_text()
    assert_no_exceptions(page)


def test_extract_lab_switch_staged_file(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    selectboxes = page.locator('[data-testid="stSelectbox"]')
    staged_select = None
    for i in range(selectboxes.count()):
        sb_text = selectboxes.nth(i).inner_text()
        if "staged" in sb_text.lower() or ".pdf" in sb_text.lower():
            staged_select = selectboxes.nth(i)
            break
    if staged_select is None and selectboxes.count() >= 2:
        staged_select = selectboxes.last
    if staged_select is None:
        pytest.skip("No staged file selectbox found")
    staged_select.click()
    page.wait_for_timeout(500)
    options = page.locator('[data-testid="stSelectboxVirtualDropdown"] [role="option"]')
    if options.count() < 2:
        pytest.skip("Only one staged file")
    options.nth(1).click()
    page.wait_for_timeout(2000)
    wait_for_streamlit(page)
    assert_no_exceptions(page)


def test_extract_lab_quick_reference_expand_collapse(app_url, page):
    _navigate(page, app_url)
    expanders = page.locator('[data-testid="stExpander"]')
    if expanders.count() == 0:
        pytest.skip("No expanders found")
    expanders.first.click()
    page.wait_for_timeout(500)
    body = page.inner_text("body")
    assert "Supported file formats" in body or "Extraction modes" in body
    expanders.first.click()
    page.wait_for_timeout(500)
    assert_no_exceptions(page)


def test_extract_lab_template_textarea_edit_validates(app_url, page):
    _navigate(page, app_url)
    textarea = page.locator("textarea").first
    if textarea.count() == 0:
        pytest.skip("No textarea found")
    textarea.click()
    textarea.fill('{"my_field": "What is the custom value?"}')
    textarea.press("Tab")
    page.wait_for_timeout(1500)
    wait_for_streamlit(page)
    assert page.locator("text=Invalid JSON").count() == 0
    assert_no_exceptions(page)
