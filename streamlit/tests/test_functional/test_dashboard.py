"""Functional tests for the Dashboard page (pages/0_Dashboard.py)."""

import pytest

from tests.conftest import wait_for_streamlit, assert_no_exceptions, get_metric_value


DASHBOARD_PATH = "/Dashboard"


def _navigate(page, app_url):
    page.goto(f"{app_url}{DASHBOARD_PATH}", wait_until="networkidle")
    wait_for_streamlit(page)
    # Dashboard should always render metrics; retry once if Streamlit
    # returned an empty shell under concurrent load.
    if page.locator('[data-testid="stMetric"]').count() == 0:
        page.reload(wait_until="networkidle")
        wait_for_streamlit(page)


@pytest.mark.smoke
def test_dashboard_primary_kpi_metrics(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMetric"]').count() >= 4


def test_dashboard_secondary_metrics(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stMetric"]').count() >= 7


def test_dashboard_recent_invoices_table(app_url, page):
    _navigate(page, app_url)
    assert page.locator('[data-testid="stDataFrame"]').count() > 0


def test_dashboard_total_invoices_positive(app_url, page):
    _navigate(page, app_url)
    total = get_metric_value(page, "Total Invoices")
    assert total is not None and total > 0, f"Total Invoices should be > 0, got {total}"


def test_dashboard_active_vendors_positive(app_url, page):
    _navigate(page, app_url)
    vendors = get_metric_value(page, "Active Vendors")
    assert vendors is not None and vendors > 0


def test_dashboard_title(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Accounts Payable").count() > 0


def test_dashboard_total_spend_metric_present(app_url, page):
    _navigate(page, app_url)
    assert get_metric_value(page, "Total Spend") is not None


def test_dashboard_outstanding_metric_present(app_url, page):
    _navigate(page, app_url)
    assert get_metric_value(page, "Outstanding") is not None


def test_dashboard_overdue_metric_present(app_url, page):
    _navigate(page, app_url)
    assert get_metric_value(page, "Overdue") is not None


def test_dashboard_avg_days_to_pay_metric(app_url, page):
    _navigate(page, app_url)
    assert get_metric_value(page, "Avg Days to Pay") is not None


def test_dashboard_recent_invoices_subheader(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Recently Processed Invoices").count() > 0


def test_dashboard_caption_text(app_url, page):
    _navigate(page, app_url)
    assert page.locator("text=Powered by Snowflake AI_EXTRACT").count() > 0


def test_dashboard_extraction_pipeline_metric_label(app_url, page):
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    found = any("Extraction Pipeline" in metrics.nth(i).inner_text()
                 for i in range(metrics.count()))
    assert found, "Expected 'Extraction Pipeline' metric label"


def test_dashboard_overdue_delta_text(app_url, page):
    _navigate(page, app_url)
    metrics = page.locator('[data-testid="stMetric"]')
    found = any("Overdue" in metrics.nth(i).inner_text()
                 and "invoices" in metrics.nth(i).inner_text().lower()
                 for i in range(metrics.count()))
    assert found


def test_dashboard_total_spend_positive(app_url, page):
    _navigate(page, app_url)
    val = get_metric_value(page, "Total Spend")
    assert val is not None and val > 0


def test_dashboard_dividers_present(app_url, page):
    _navigate(page, app_url)
    assert page.locator("hr").count() >= 2


@pytest.mark.smoke
def test_dashboard_no_exceptions(app_url, page):
    _navigate(page, app_url)
    assert_no_exceptions(page)
