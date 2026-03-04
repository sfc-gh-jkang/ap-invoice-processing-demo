"""Functional tests for the Landing page (streamlit_app.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions, get_metric_value


def _navigate(page, app_url):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page, selectors='[data-testid="stMetric"]')
    # Under concurrent load, lower page content renders after metrics.
    # Scroll down to trigger lazy rendering, then give Streamlit a moment.
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(500)


@pytest.mark.smoke
def test_landing_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=AI-Powered Invoice Processing").count() > 0


def test_landing_pipeline_stats_metrics(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMetric"]').count() >= 4


def test_landing_graphviz_chart(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stGraphVizChart"]').count() > 0


def test_landing_business_value_sections(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Why This Matters").count() > 0
    assert page.locator("text=The Problem").count() > 0
    assert page.locator("text=The Solution").count() > 0


def test_landing_key_technologies_section(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Key Technologies").count() > 0
    for tech in ["Cortex AI_EXTRACT", "Streams + Tasks", "Streamlit Container Runtime",
                 "Inline PDF Rendering", "Analytical Views", "PDF Generation (UDTF)"]:
        assert page.locator(f"text={tech}").count() > 0, f"Missing technology: {tech}"


def test_landing_sidebar_navigation_guide(app_url, page):
    _navigate(page, app_url)
    sidebar = page.locator('[data-testid="stSidebar"]')
    sidebar_text = sidebar.inner_text()
    assert "Navigation Guide" in sidebar_text
    for item in ["Dashboard", "AP Ledger", "Analytics", "Process New"]:
        assert item in sidebar_text, f"Sidebar missing '{item}'"


def test_landing_invoices_extracted_metric_positive(app_url, page):
    _navigate(page, app_url)
    val = get_metric_value(page, "Invoices Extracted")
    assert val is not None and val > 0, f"Invoices Extracted should be > 0, got {val}"


def test_landing_vendors_identified_metric_positive(app_url, page):
    _navigate(page, app_url)
    val = get_metric_value(page, "Vendors Identified")
    assert val is not None and val > 0, f"Vendors Identified should be > 0, got {val}"


def test_landing_line_items_parsed_metric_positive(app_url, page):
    _navigate(page, app_url)
    val = get_metric_value(page, "Line Items Parsed")
    assert val is not None and val > 0, f"Line Items Parsed should be > 0, got {val}"


def test_landing_source_pdfs_metric_positive(app_url, page):
    _navigate(page, app_url)
    val = get_metric_value(page, "Source PDFs on Stage")
    assert val is not None and val > 0, f"Source PDFs on Stage should be > 0, got {val}"


def test_landing_architecture_header(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Architecture").count() > 0


def test_landing_live_pipeline_stats_header(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Live Pipeline Stats").count() > 0


def test_landing_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=End-to-end accounts payable automation").count() > 0


@pytest.mark.smoke
def test_landing_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)
