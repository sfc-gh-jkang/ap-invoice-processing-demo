"""Shared fixtures and helpers for Playwright E2E tests."""

import os
import time
import urllib.request
import urllib.error

import pytest


APP_PORT = 8503
APP_URL = f"http://localhost:{APP_PORT}"
SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "test-results")


def _server_is_up():
    """Return True if the Streamlit server is responding on APP_PORT."""
    try:
        resp = urllib.request.urlopen(APP_URL, timeout=5)
        return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def app_url():
    """Return the base URL of the already-running Streamlit server."""
    if not _server_is_up():
        deadline = time.time() + 30
        while time.time() < deadline:
            if _server_is_up():
                break
            time.sleep(1)
        else:
            raise RuntimeError("Streamlit server not reachable from worker")
    return APP_URL


# ---------------------------------------------------------------------------
# Helpers (importable by test files)
# ---------------------------------------------------------------------------

def wait_for_streamlit(page, selectors=None, timeout=90_000):
    """Wait for Streamlit page to finish rendering.

    Args:
        page: Playwright page object.
        selectors: Optional CSS selector string (comma-separated) to wait for.
            Defaults to common Streamlit data-testid selectors.
        timeout: Max wait in milliseconds.
    """
    if selectors is None:
        selectors = (
            '[data-testid="stMetric"], '
            '[data-testid="stMarkdown"], '
            '[data-testid="stDataFrame"]'
        )

    page.wait_for_load_state("networkidle", timeout=timeout)
    try:
        page.wait_for_selector(selectors, timeout=timeout)
    except Exception:
        pass
    # Brief buffer for Streamlit re-renders (reduced from 5000ms)
    page.wait_for_timeout(1000)


def get_metric_value(page, label_text):
    """Extract the numeric value from a stMetric whose text contains label_text.

    Streamlit renders metrics as ``label\\n\\nvalue`` (double newline separator).
    Returns the parsed int, or None if not found.
    """
    metrics = page.locator('[data-testid="stMetric"]')
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        if label_text in text:
            # Split on any newline, filter empty strings
            parts = [p.strip() for p in text.split("\n") if p.strip()]
            if len(parts) >= 2:
                raw = parts[1].replace(",", "").replace("$", "").strip()
                # Strip trailing non-digit suffixes like " days", "/110 processed"
                digits = ""
                for ch in raw:
                    if ch.isdigit():
                        digits += ch
                    else:
                        break
                if digits:
                    return int(digits)
            return None
    return None


def assert_no_exceptions(page):
    """Assert that the page has zero stException elements."""
    exceptions = page.locator('[data-testid="stException"]')
    count = exceptions.count()
    if count > 0:
        texts = [exceptions.nth(i).inner_text() for i in range(count)]
        raise AssertionError(
            f"Found {count} Streamlit exception(s) on page:\n" + "\n---\n".join(texts)
        )


# ---------------------------------------------------------------------------
# Pytest hooks — reporting
# ---------------------------------------------------------------------------

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Capture a screenshot on test failure for Playwright tests."""
    outcome = yield
    report = outcome.get_result()

    if report.when == "call" and report.failed:
        page = item.funcargs.get("page")
        if page is not None:
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            name = item.nodeid.replace("/", "_").replace("::", "__").replace(" ", "_")
            path = os.path.join(SCREENSHOT_DIR, f"{name}.png")
            try:
                page.screenshot(path=path, full_page=True)
            except Exception:
                pass  # Don't fail the teardown if screenshot fails
