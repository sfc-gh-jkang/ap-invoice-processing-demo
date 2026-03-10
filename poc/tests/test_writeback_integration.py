"""SQL Integration Tests — verify writeback table and view objects exist and are configured correctly."""

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# INVOICE_REVIEW table
# ---------------------------------------------------------------------------
class TestInvoiceReviewTable:
    """Verify INVOICE_REVIEW table created by 08_writeback.sql."""

    def test_table_exists(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS'"
        )
        assert sf_cursor.fetchone()[0] == 1, "INVOICE_REVIEW table does not exist"

    def test_columns(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "ORDER BY ORDINAL_POSITION"
        )
        cols = [row[0] for row in sf_cursor.fetchall()]
        expected = [
            "REVIEW_ID", "RECORD_ID", "FILE_NAME", "REVIEW_STATUS",
            "CORRECTED_VENDOR_NAME", "CORRECTED_INVOICE_NUMBER",
            "CORRECTED_PO_NUMBER", "CORRECTED_INVOICE_DATE",
            "CORRECTED_DUE_DATE", "CORRECTED_PAYMENT_TERMS",
            "CORRECTED_RECIPIENT", "CORRECTED_SUBTOTAL",
            "CORRECTED_TAX_AMOUNT", "CORRECTED_TOTAL",
            "REVIEWER_NOTES", "REVIEWED_BY", "REVIEWED_AT",
            "CORRECTIONS",
        ]
        assert cols == expected, f"Column mismatch: expected {expected}, got {cols}"

    def test_review_id_is_autoincrement(self, sf_cursor):
        sf_cursor.execute(
            "SELECT IS_IDENTITY FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND COLUMN_NAME = 'REVIEW_ID'"
        )
        result = sf_cursor.fetchone()[0]
        assert result == "YES", "REVIEW_ID should be AUTOINCREMENT"

    def test_column_types(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "ORDER BY ORDINAL_POSITION"
        )
        type_map = {row[0]: row[1] for row in sf_cursor.fetchall()}
        assert type_map["REVIEW_ID"] == "NUMBER"
        assert type_map["RECORD_ID"] == "NUMBER"
        assert type_map["FILE_NAME"] == "TEXT"
        assert type_map["REVIEW_STATUS"] == "TEXT"
        # Corrected string fields
        assert type_map["CORRECTED_VENDOR_NAME"] == "TEXT"
        assert type_map["CORRECTED_INVOICE_NUMBER"] == "TEXT"
        assert type_map["CORRECTED_PO_NUMBER"] == "TEXT"
        assert type_map["CORRECTED_PAYMENT_TERMS"] == "TEXT"
        assert type_map["CORRECTED_RECIPIENT"] == "TEXT"
        # Corrected date fields
        assert type_map["CORRECTED_INVOICE_DATE"] == "DATE"
        assert type_map["CORRECTED_DUE_DATE"] == "DATE"
        # Corrected numeric fields
        assert type_map["CORRECTED_SUBTOTAL"] == "NUMBER"
        assert type_map["CORRECTED_TAX_AMOUNT"] == "NUMBER"
        assert type_map["CORRECTED_TOTAL"] == "NUMBER"
        # Meta fields
        assert type_map["REVIEWER_NOTES"] == "TEXT"
        assert type_map["REVIEWED_BY"] == "TEXT"
        assert type_map["REVIEWED_AT"] in ("TIMESTAMP_NTZ", "TIMESTAMP_LTZ")

    def test_not_null_constraints(self, sf_cursor):
        """RECORD_ID, FILE_NAME, and REVIEW_STATUS should be NOT NULL."""
        sf_cursor.execute(
            "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "AND COLUMN_NAME IN ('RECORD_ID', 'FILE_NAME', 'REVIEW_STATUS')"
        )
        for row in sf_cursor.fetchall():
            assert row[1] == "NO", f"{row[0]} should be NOT NULL"

    def test_corrected_columns_are_nullable(self, sf_cursor):
        """All corrected_* columns should be nullable (corrections are optional)."""
        sf_cursor.execute(
            "SELECT COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "AND COLUMN_NAME LIKE 'CORRECTED_%'"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) == 10, f"Expected 10 corrected_* columns, got {len(rows)}"
        for row in rows:
            assert row[1] == "YES", f"{row[0]} should be nullable"

    def test_table_is_writable(self, sf_cursor):
        """Verify we can INSERT and DELETE from INVOICE_REVIEW."""
        # Clean up any stale rows from previous runs (idempotency)
        sf_cursor.execute("DELETE FROM INVOICE_REVIEW WHERE record_id = -999")

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW (record_id, file_name, review_status) "
            "VALUES (-999, '__test_file__.pdf', 'APPROVED')"
        )
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW WHERE record_id = -999"
        )
        assert sf_cursor.fetchone()[0] == 1, "Test INSERT failed"

        # Clean up
        sf_cursor.execute("DELETE FROM INVOICE_REVIEW WHERE record_id = -999")
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW WHERE record_id = -999"
        )
        assert sf_cursor.fetchone()[0] == 0, "Test DELETE failed"

    def test_corrected_total_at_position_14(self, sf_cursor):
        """CORRECTED_TOTAL should be at ordinal position 14.

        The CREATE TABLE DDL in 08_writeback.sql defines all corrected_*
        columns inline (positions 5-14), with CORRECTED_TOTAL at position 14
        after CORRECTED_TAX_AMOUNT.
        """
        sf_cursor.execute(
            "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "AND COLUMN_NAME = 'CORRECTED_TOTAL'"
        )
        result = sf_cursor.fetchone()
        assert result is not None, "CORRECTED_TOTAL column not found"
        assert result[1] == 14, (
            f"CORRECTED_TOTAL should be at ordinal position 14, got {result[1]}"
        )

    def test_corrected_total_coexists_with_new_columns(self, sf_cursor):
        """CORRECTED_TOTAL (position 5) and the 9 ALTER-TABLE-added corrected_*
        columns should all be queryable in a single SELECT."""
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, "
            " corrected_vendor_name, corrected_subtotal, corrected_tax_amount, "
            " reviewer_notes) "
            "VALUES (-996, '__coexist_test__.pdf', 'CORRECTED', 500.00, "
            "        'CoexistVendor', 400.00, 100.00, '__pytest_coexist__')"
        )
        sf_cursor.execute(
            "SELECT corrected_total, corrected_vendor_name, "
            "corrected_subtotal, corrected_tax_amount "
            "FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_coexist__'"
        )
        result = sf_cursor.fetchone()
        assert result is not None, "Test row not found"
        assert float(result[0]) == pytest.approx(500.00), "CORRECTED_TOTAL mismatch"
        assert result[1] == "CoexistVendor", "CORRECTED_VENDOR_NAME mismatch"
        assert float(result[2]) == pytest.approx(400.00), "CORRECTED_SUBTOTAL mismatch"
        assert float(result[3]) == pytest.approx(100.00), "CORRECTED_TAX_AMOUNT mismatch"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_coexist__'"
        )

    def test_insert_with_all_corrected_fields(self, sf_cursor):
        """Verify INSERT with all 9 corrected_* columns populated."""
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW ("
            "  record_id, file_name, review_status,"
            "  corrected_vendor_name, corrected_invoice_number,"
            "  corrected_po_number, corrected_invoice_date,"
            "  corrected_due_date, corrected_payment_terms,"
            "  corrected_recipient, corrected_subtotal,"
            "  corrected_tax_amount, corrected_total,"
            "  reviewer_notes"
            ") VALUES ("
            "  -998, '__test_all_cols__.pdf', 'CORRECTED',"
            "  'Test Vendor', 'INV-TEST-001',"
            "  'PO-TEST-001', '2025-01-15',"
            "  '2025-02-15', 'Net 30',"
            "  'Test Recipient', 100.00,"
            "  8.50, 108.50,"
            "  '__pytest_all_corrected__'"
            ")"
        )
        sf_cursor.execute(
            "SELECT corrected_vendor_name, corrected_invoice_number, "
            "corrected_po_number, corrected_invoice_date, "
            "corrected_due_date, corrected_payment_terms, "
            "corrected_recipient, corrected_subtotal, "
            "corrected_tax_amount, corrected_total "
            "FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_all_corrected__'"
        )
        result = sf_cursor.fetchone()
        assert result is not None, "Test row not found after INSERT"
        assert result[0] == "Test Vendor"
        assert result[1] == "INV-TEST-001"
        assert result[2] == "PO-TEST-001"
        assert str(result[3]) == "2025-01-15"
        assert str(result[4]) == "2025-02-15"
        assert result[5] == "Net 30"
        assert result[6] == "Test Recipient"
        assert float(result[7]) == pytest.approx(100.00)
        assert float(result[8]) == pytest.approx(8.50)
        assert float(result[9]) == pytest.approx(108.50)

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_all_corrected__'"
        )


# ---------------------------------------------------------------------------
# V_INVOICE_SUMMARY view
# ---------------------------------------------------------------------------
class TestVInvoiceSummary:
    """Verify V_INVOICE_SUMMARY view created by 08_writeback.sql."""

    def test_view_exists(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS "
            "WHERE TABLE_NAME = 'V_INVOICE_SUMMARY' AND TABLE_SCHEMA = 'DOCUMENTS'"
        )
        assert sf_cursor.fetchone()[0] == 1, "V_INVOICE_SUMMARY view does not exist"

    def test_is_queryable(self, sf_cursor):
        """View should be queryable without error."""
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 1")
        sf_cursor.fetchall()  # Should not raise

    def test_columns(self, sf_cursor):
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        expected_cols = [
            "RECORD_ID", "FILE_NAME", "DOC_TYPE", "VENDOR_NAME", "INVOICE_NUMBER",
            "PO_NUMBER", "INVOICE_DATE", "DUE_DATE", "PAYMENT_TERMS",
            "RECIPIENT", "SUBTOTAL", "TAX_AMOUNT", "TOTAL_AMOUNT",
            "EXTRACTION_STATUS", "EXTRACTED_AT",
            "LINE_ITEM_COUNT", "COMPUTED_LINE_TOTAL",
            "REVIEW_STATUS", "REVIEWER_NOTES",
            "REVIEWED_BY", "REVIEWED_AT",
            "RAW_EXTRACTION", "CORRECTIONS",
        ]
        assert cols == expected_cols, f"Column mismatch:\nExpected: {expected_cols}\nGot:      {cols}"

    def test_row_count_matches_extracted_fields(self, sf_cursor):
        """View should have one row per EXTRACTED_FIELDS record."""
        sf_cursor.execute("SELECT COUNT(*) FROM V_INVOICE_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count, (
            f"View has {view_count} rows but EXTRACTED_FIELDS has {ef_count}"
        )

    def test_vendor_names_populated(self, sf_cursor):
        """VENDOR_NAME should be populated (mapped from field_1)."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM V_INVOICE_SUMMARY WHERE vendor_name IS NULL"
        )
        nulls = sf_cursor.fetchone()[0]
        assert nulls == 0, f"Found {nulls} rows with NULL vendor_name"

    def test_line_item_counts_populated(self, sf_cursor):
        """Most rows should have LINE_ITEM_COUNT > 0.

        Re-extraction can orphan line items, and some doc types may not
        produce line items, so we allow up to 50% to be missing.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM V_INVOICE_SUMMARY "
            "WHERE line_item_count IS NULL OR line_item_count = 0"
        )
        missing = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM V_INVOICE_SUMMARY")
        total = sf_cursor.fetchone()[0]
        threshold = max(1, int(total * 0.5))
        assert missing <= threshold, (
            f"{missing}/{total} rows have NULL or zero line_item_count — "
            f"LEFT JOIN to EXTRACTED_TABLE_DATA may not be matching"
        )

    def test_review_status_null_before_review(self, sf_cursor):
        """Unreviewed records should have NULL review_status in the view."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM V_INVOICE_SUMMARY "
            "WHERE review_status IS NOT NULL "
            "AND record_id NOT IN (SELECT record_id FROM INVOICE_REVIEW)"
        )
        phantom = sf_cursor.fetchone()[0]
        assert phantom == 0, (
            f"{phantom} rows have non-NULL review_status without any INVOICE_REVIEW rows"
        )

    def test_is_not_a_dynamic_table(self, sf_cursor):
        """V_INVOICE_SUMMARY should be a regular view, not a dynamic table."""
        sf_cursor.execute(
            "SHOW DYNAMIC TABLES LIKE 'V_INVOICE_SUMMARY' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) == 0, (
            "V_INVOICE_SUMMARY should be a regular view, not a dynamic table"
        )

    def test_dt_invoice_summary_does_not_exist(self, sf_cursor):
        """The old DT_INVOICE_SUMMARY should have been dropped."""
        sf_cursor.execute(
            "SHOW DYNAMIC TABLES LIKE 'DT_INVOICE_SUMMARY' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) == 0, "DT_INVOICE_SUMMARY should have been dropped"
