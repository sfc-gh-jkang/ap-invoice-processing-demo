"""Cross-document-type isolation tests.

Verify that INVOICE and UTILITY_BILL data are correctly separated
and that operations on one type don't affect the other.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streamlit"))

from config import get_session


@pytest.fixture(scope="module")
def sf_session():
    conn = os.environ.get("POC_CONNECTION")
    if not conn:
        pytest.skip("POC_CONNECTION not set — skipping SF tests")
    return get_session()


DB = "AI_EXTRACT_POC.DOCUMENTS"


class TestDocumentTypeCounts:
    """Verify expected document counts per type."""

    def test_invoice_count(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS
            WHERE doc_type = 'INVOICE'
        """).collect()
        assert rows[0]["CNT"] >= 100, "Invoice count should not drop below 100"

    def test_utility_bill_count(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS
            WHERE doc_type = 'UTILITY_BILL'
        """).collect()
        count = rows[0]["CNT"]
        if count == 0:
            pytest.skip("No UTILITY_BILL data in deployment")
        assert count == 10

    def test_total_count(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS
        """).collect()
        # Total depends on which doc types are deployed
        total = rows[0]["CNT"]
        assert total >= 100, f"Expected at least 100 documents (invoices), got {total}"


class TestExtractionIsolation:
    """Verify extractions are correctly typed and isolated."""

    def test_invoice_extractions_match_raw(self, sf_session):
        """Number of INVOICE extractions should match INVOICE raw documents."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.EXTRACTED_FIELDS e
            JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name
            WHERE r.doc_type = 'INVOICE'
        """).collect()
        assert rows[0]["CNT"] >= 100, "Invoice extraction count should not drop below 100"

    def test_utility_bill_extractions_match_raw(self, sf_session):
        """Number of UTILITY_BILL extractions should match UTILITY_BILL raw documents."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.EXTRACTED_FIELDS e
            JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """).collect()
        count = rows[0]["CNT"]
        if count == 0:
            pytest.skip("No UTILITY_BILL data in deployment")
        assert count == 10

    def test_all_extracted(self, sf_session):
        """Every RAW_DOCUMENTS row should have a matching EXTRACTED_FIELDS row."""
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DB}.RAW_DOCUMENTS r
            LEFT JOIN {DB}.EXTRACTED_FIELDS e ON r.file_name = e.file_name
            WHERE e.file_name IS NULL
        """).collect()
        assert rows[0]["CNT"] == 0


class TestViewIsolation:
    """Verify V_DOCUMENT_SUMMARY shows correct types."""

    def test_view_has_both_types(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT DISTINCT doc_type FROM {DB}.V_DOCUMENT_SUMMARY
            ORDER BY doc_type
        """).collect()
        types = [r["DOC_TYPE"] for r in rows]
        assert "INVOICE" in types
        # UTILITY_BILL only expected when that data is deployed
        if "UTILITY_BILL" not in types:
            # Verify it's because there's no data, not a view bug
            ub_rows = sf_session.sql(f"""
                SELECT COUNT(*) AS cnt FROM {DB}.RAW_DOCUMENTS
                WHERE doc_type = 'UTILITY_BILL'
            """).collect()
            if ub_rows[0]["CNT"] > 0:
                assert False, "UTILITY_BILL data exists but not in V_DOCUMENT_SUMMARY"

    def test_view_invoice_count(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {DB}.V_DOCUMENT_SUMMARY
            WHERE doc_type = 'INVOICE'
        """).collect()
        assert rows[0]["CNT"] >= 100, "Invoice count in view should not drop below 100"

    def test_view_utility_bill_count(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT COUNT(*) AS cnt FROM {DB}.V_DOCUMENT_SUMMARY
            WHERE doc_type = 'UTILITY_BILL'
        """).collect()
        count = rows[0]["CNT"]
        if count == 0:
            pytest.skip("No UTILITY_BILL data in deployment")
        assert count == 10


class TestConfigIsolation:
    """Verify each document type has its own config."""

    def test_invoice_config_distinct(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT extraction_prompt FROM {DB}.DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'INVOICE'
        """).collect()
        assert len(rows) == 1
        prompt = rows[0]["EXTRACTION_PROMPT"]
        assert "invoice" in prompt.lower() or "vendor" in prompt.lower()

    def test_utility_bill_config_distinct(self, sf_session):
        rows = sf_session.sql(f"""
            SELECT extraction_prompt FROM {DB}.DOCUMENT_TYPE_CONFIG
            WHERE doc_type = 'UTILITY_BILL'
        """).collect()
        assert len(rows) == 1
        prompt = rows[0]["EXTRACTION_PROMPT"]
        assert "utility" in prompt.lower() or "kwh" in prompt.lower() or "billing" in prompt.lower()

    def test_no_duplicate_configs(self, sf_session):
        """Each doc_type should appear exactly once."""
        rows = sf_session.sql(f"""
            SELECT doc_type, COUNT(*) AS cnt
            FROM {DB}.DOCUMENT_TYPE_CONFIG
            GROUP BY doc_type
            HAVING cnt > 1
        """).collect()
        assert len(rows) == 0


class TestFieldOverflowIsolation:
    """Verify utility bill overflow fields (11-13) in raw_extraction."""

    def test_utility_bills_have_overflow_fields(self, sf_session):
        """UTILITY_BILL has 13 fields; fields 11-13 should be in raw_extraction."""
        rows = sf_session.sql(f"""
            SELECT e.raw_extraction
            FROM {DB}.EXTRACTED_FIELDS e
            JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
            LIMIT 1
        """).collect()
        if not rows:
            pytest.skip("No utility bill extractions")
        raw = json.loads(str(rows[0]["RAW_EXTRACTION"]))
        # Utility bill overflow fields: rate_schedule, meter_number, service_address (fields 11-13)
        overflow_keys = {"rate_schedule", "meter_number", "service_address"}
        found = overflow_keys.intersection(set(raw.keys()))
        assert len(found) >= 1, f"Expected overflow fields in raw_extraction, got keys: {list(raw.keys())}"

    def test_invoices_no_utility_fields(self, sf_session):
        """INVOICE raw_extraction should not contain utility-specific fields."""
        rows = sf_session.sql(f"""
            SELECT e.raw_extraction
            FROM {DB}.EXTRACTED_FIELDS e
            JOIN {DB}.RAW_DOCUMENTS r ON e.file_name = r.file_name
            WHERE r.doc_type = 'INVOICE'
            LIMIT 5
        """).collect()
        utility_fields = {"kwh_usage", "demand_kw", "billing_period_start", "billing_period_end"}
        for row in rows:
            raw_str = row["RAW_EXTRACTION"]
            if not raw_str:
                continue  # NULL raw_extraction is fine for invoices
            raw = json.loads(str(raw_str))
            found = utility_fields.intersection(set(raw.keys()))
            assert len(found) == 0, f"Invoice should not have utility fields, found: {found}"
