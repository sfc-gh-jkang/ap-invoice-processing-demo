"""Shared E2E helper functions for POC Streamlit tests."""


def wait_for_streamlit(page, selectors=None, timeout=90_000):
    """Wait for Streamlit page to finish rendering."""
    if selectors is None:
        selectors = (
            '[data-testid="stMetric"], '
            '[data-testid="stMarkdown"], '
            '[data-testid="stDataFrame"]'
        )
    page.wait_for_load_state("domcontentloaded", timeout=timeout)
    try:
        page.wait_for_selector(selectors, timeout=timeout)
    except Exception:
        pass
    page.wait_for_timeout(500)


def get_metric_value(page, label_text):
    """Extract numeric value from a stMetric whose text contains label_text.

    Comparison is case-insensitive because CSS ``text-transform: uppercase``
    causes ``inner_text()`` to return uppercased labels in some browsers.
    """
    metrics = page.locator('[data-testid="stMetric"]')
    label_lower = label_text.lower()
    for i in range(metrics.count()):
        text = metrics.nth(i).inner_text()
        if label_lower in text.lower():
            parts = [p.strip() for p in text.split("\n") if p.strip()]
            if len(parts) >= 2:
                raw = parts[1].replace(",", "").replace("$", "").strip()
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
            f"Found {count} Streamlit exception(s):\n" + "\n---\n".join(texts)
        )
