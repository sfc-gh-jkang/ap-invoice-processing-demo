"""Functional tests for the Analytics page (pages/2_Analytics.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions


ANALYTICS_PATH = "/Analytics"


def _navigate(page, app_url):
    page.goto(f"{app_url}{ANALYTICS_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)


@pytest.mark.smoke
def test_analytics_spend_by_vendor(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Spend by Vendor").count() > 0


def test_analytics_monthly_spend_trend(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Monthly Spend Trend").count() > 0


def test_analytics_spend_by_category(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator("text=Spend by Category").count() > 0


def test_analytics_aging_and_top_products(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator("text=Aging Distribution").count() > 0
    assert page.locator("text=Top 20 Products by Spend").count() > 0


def test_analytics_plotly_charts_render(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stPlotlyChart"]').count() >= 2


def test_analytics_four_plotly_charts(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator('[data-testid="stPlotlyChart"]').count() >= 4


def test_analytics_vendor_payment_terms(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    assert page.locator("text=Vendor Payment Terms Summary").count() > 0
    assert page.locator('[data-testid="stDataFrame"]').count() >= 1


def test_analytics_top_products_dataframe_has_rows(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1000)
    assert page.locator("text=Top 20 Products by Spend").count() > 0
    assert page.locator('[data-testid="stDataFrame"]').count() >= 2


def test_analytics_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Accounts Payable Analytics").count() > 0


def test_analytics_dividers_present(app_url, page):
    _navigate(page, app_url)
    assert page.locator("hr").count() >= 1


def test_analytics_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Spend analysis across vendors").count() > 0


def test_analytics_monthly_summary_caption(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    caption = page.locator("text=/Total:.*Monthly Avg:/")
    if caption.count() == 0:
        total_el = page.locator("text=Total: $")
        avg_el = page.locator("text=Monthly Avg: $")
        assert total_el.count() > 0 or avg_el.count() > 0


def test_analytics_two_dataframes_with_data(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    dataframes = page.locator('[data-testid="stDataFrame"]')
    assert dataframes.count() >= 2
    for i in range(min(2, dataframes.count())):
        box = dataframes.nth(i).bounding_box()
        assert box is not None and box["height"] > 0


def test_analytics_all_six_section_headers(app_url, page):
    _navigate(page, app_url)
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1500)
    for header in ["Spend by Vendor", "Monthly Spend Trend", "Spend by Category",
                   "Aging Distribution", "Top 20 Products by Spend",
                   "Vendor Payment Terms Summary"]:
        assert page.locator(f"text={header}").count() > 0, f"Missing: '{header}'"


@pytest.mark.smoke
def test_analytics_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)
