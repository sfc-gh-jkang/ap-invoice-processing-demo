"""Shared fixtures for POC E2E Streamlit tests."""

import os
import time
import threading
import urllib.request
import urllib.error

import pytest


APP_PORT = 8504
APP_URL = f"http://localhost:{APP_PORT}"
SCREENSHOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "test-results")


def _server_is_up():
    try:
        resp = urllib.request.urlopen(APP_URL, timeout=5)
        return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


@pytest.fixture(scope="session")
def app_url():
    """Return the base URL of the already-running POC Streamlit server."""
    if not _server_is_up():
        deadline = time.time() + 30
        while time.time() < deadline:
            if _server_is_up():
                break
            time.sleep(1)
        else:
            raise RuntimeError("POC Streamlit server not reachable from worker")
    return APP_URL


# ---------------------------------------------------------------------------
# Pytest hooks — screenshot on failure (with hard timeout to prevent hangs)
# ---------------------------------------------------------------------------
@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed:
        page = item.funcargs.get("page")
        if page is not None:
            os.makedirs(SCREENSHOT_DIR, exist_ok=True)
            name = item.nodeid.replace("/", "_").replace("::", "__").replace(" ", "_")
            path = os.path.join(SCREENSHOT_DIR, f"{name}.png")

            def _take_screenshot():
                try:
                    page.screenshot(path=path, full_page=True, timeout=5_000)
                except Exception:
                    pass

            t = threading.Thread(target=_take_screenshot, daemon=True)
            t.start()
            t.join(timeout=8)
            # If thread is still alive after 8s, the browser is crashed — abandon it
