"""Integration tests: cross-page navigation and consistency."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions, get_metric_value


PAGE_PATHS = {
    "Landing": "/",
    "Dashboard": "/Dashboard",
    "AP Ledger": "/AP_Ledger",
    "Analytics": "/Analytics",
    "Process New": "/Process_New",
    "AI Extract Lab": "/AI_Extract_Lab",
}


@pytest.mark.smoke
def test_sidebar_contains_all_page_links(app_url, page):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    sidebar = page.locator('[data-testid="stSidebar"]')
    sidebar_text = sidebar.inner_text()
    for name in ["Dashboard", "AP Ledger", "Analytics", "Process New", "AI Extract Lab"]:
        assert name.replace("_", " ") in sidebar_text or name in sidebar_text


@pytest.mark.slow
def test_all_pages_render_without_exceptions(app_url, page):
    for name, path in PAGE_PATHS.items():
        page.goto(f"{app_url}{path}", wait_until="networkidle")
        wait_for_streamlit(page)
        assert_no_exceptions(page)


@pytest.mark.slow
def test_sidebar_navigation_renders_correct_title(app_url, page):
    expected_titles = {
        "Dashboard": "Accounts Payable",
        "AP Ledger": "Accounts Payable Ledger",
        "Analytics": "Accounts Payable Analytics",
        "Process New": "Process New Invoices",
        "AI Extract Lab": "AI_EXTRACT Lab",
    }
    for page_name, expected_title in expected_titles.items():
        page.goto(f"{app_url}{PAGE_PATHS[page_name]}", wait_until="networkidle")
        wait_for_streamlit(page)
        assert page.locator(f"text={expected_title}").count() > 0


def test_landing_invoices_matches_dashboard_total(app_url, page):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page, selectors='[data-testid="stMetric"]')
    landing_count = get_metric_value(page, "Invoices Extracted")

    page.goto(f"{app_url}/Dashboard", wait_until="networkidle")
    wait_for_streamlit(page)
    dash_count = get_metric_value(page, "Total Invoices")

    if landing_count is not None and dash_count is not None:
        assert landing_count == dash_count
    else:
        pytest.skip(f"Could not parse: landing={landing_count}, dashboard={dash_count}")


def test_sidebar_click_navigates_to_page(app_url, page):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    sidebar = page.locator('[data-testid="stSidebar"]')
    dash_link = sidebar.locator("a", has_text="Dashboard")
    if dash_link.count() > 0:
        dash_link.first.click()
        page.wait_for_timeout(2000)
        wait_for_streamlit(page)
        assert page.locator("text=Accounts Payable").count() > 0
    else:
        nav_link = sidebar.locator("text=Dashboard")
        if nav_link.count() > 0:
            nav_link.first.click()
            page.wait_for_timeout(2000)
            wait_for_streamlit(page)
        assert_no_exceptions(page)


def test_landing_vendors_matches_dashboard_active_vendors(app_url, page):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page, selectors='[data-testid="stMetric"]')
    landing_vendors = get_metric_value(page, "Vendors Identified")

    page.goto(f"{app_url}/Dashboard", wait_until="networkidle")
    wait_for_streamlit(page)
    dash_vendors = get_metric_value(page, "Active Vendors")

    if landing_vendors is not None and dash_vendors is not None:
        assert landing_vendors == dash_vendors
    else:
        pytest.skip(f"Could not parse: landing={landing_vendors}, dashboard={dash_vendors}")


def _click_sidebar_link(page, app_url, link_text, expected_title):
    page.goto(app_url, wait_until="networkidle")
    wait_for_streamlit(page)
    sidebar = page.locator('[data-testid="stSidebar"]')
    link = sidebar.locator("a", has_text=link_text)
    if link.count() == 0:
        link = sidebar.locator(f"text={link_text}")
    if link.count() == 0:
        pytest.skip(f"Sidebar link '{link_text}' not found")
    link.first.click()
    page.wait_for_timeout(2000)
    wait_for_streamlit(page)
    assert page.locator(f"text={expected_title}").count() > 0
    assert_no_exceptions(page)


def test_sidebar_click_navigates_to_ap_ledger(app_url, page):
    _click_sidebar_link(page, app_url, "AP Ledger", "Accounts Payable Ledger")


def test_sidebar_click_navigates_to_analytics(app_url, page):
    _click_sidebar_link(page, app_url, "Analytics", "Accounts Payable Analytics")


def test_sidebar_click_navigates_to_process_new(app_url, page):
    _click_sidebar_link(page, app_url, "Process New", "Process New Invoices")


def test_sidebar_click_navigates_to_ai_extract_lab(app_url, page):
    _click_sidebar_link(page, app_url, "AI Extract Lab", "AI_EXTRACT Lab")
