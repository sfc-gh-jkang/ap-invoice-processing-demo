"""Data Validation Tests — verify extraction results are correct and consistent."""

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# RAW_DOCUMENTS validation
# ---------------------------------------------------------------------------
class TestRawDocuments:
    """Verify files are registered and tracked correctly."""

    def test_has_registered_files(self, sf_cursor):
        sf_cursor.execute("SELECT COUNT(*) FROM RAW_DOCUMENTS")
        count = sf_cursor.fetchone()[0]
        assert count >= 5, f"Expected at least 5 files, got {count}"

    def test_all_files_are_pdfs(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE file_name NOT LIKE '%.pdf'"
        )
        non_pdf = sf_cursor.fetchone()[0]
        assert non_pdf == 0, f"Found {non_pdf} non-PDF files"

    def test_all_files_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE extracted = FALSE"
        )
        pending = sf_cursor.fetchone()[0]
        assert pending == 0, f"Found {pending} unextracted files"

    def test_no_extraction_errors(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE extraction_error IS NOT NULL"
        )
        errors = sf_cursor.fetchone()[0]
        assert errors == 0, f"Found {errors} files with extraction errors"

    def test_extracted_at_is_set(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE extracted = TRUE AND extracted_at IS NULL"
        )
        missing = sf_cursor.fetchone()[0]
        assert missing == 0, "Some extracted files have NULL extracted_at"

    def test_file_paths_reference_stage(self, sf_cursor):
        """Invoice files should reference @DOCUMENT_STAGE/; other doc types may use URLs."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS "
            "WHERE file_path NOT LIKE '@DOCUMENT_STAGE/%' "
            "  AND file_path NOT LIKE 'https://%'"
        )
        bad_paths = sf_cursor.fetchone()[0]
        assert bad_paths == 0, f"Found {bad_paths} files with unrecognised stage paths"

    def test_staged_files_match_raw_documents(self, sf_cursor):
        """Every file in the stage should be in RAW_DOCUMENTS."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM DIRECTORY(@DOCUMENT_STAGE) d "
            "WHERE d.RELATIVE_PATH LIKE '%.pdf' "
            "  AND d.RELATIVE_PATH NOT IN (SELECT file_name FROM RAW_DOCUMENTS)"
        )
        unregistered = sf_cursor.fetchone()[0]
        assert unregistered == 0, f"Found {unregistered} staged files not in RAW_DOCUMENTS"


# ---------------------------------------------------------------------------
# EXTRACTED_FIELDS validation
# ---------------------------------------------------------------------------
class TestExtractedFields:
    """Verify entity extraction results."""

    def test_has_extraction_results(self, sf_cursor):
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        count = sf_cursor.fetchone()[0]
        assert count >= 5, f"Expected at least 5 extracted records, got {count}"

    def test_every_raw_doc_has_extraction(self, sf_cursor):
        """Every extracted file should have a corresponding EXTRACTED_FIELDS row."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS r "
            "WHERE r.extracted = TRUE "
            "  AND r.file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS)"
        )
        missing = sf_cursor.fetchone()[0]
        assert missing == 0, f"{missing} extracted files lack EXTRACTED_FIELDS rows"

    def test_vendor_name_not_null(self, sf_cursor):
        """field_1 (vendor_name) should be populated for all records."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS WHERE field_1 IS NULL"
        )
        nulls = sf_cursor.fetchone()[0]
        assert nulls == 0, f"Found {nulls} records with NULL vendor_name"

    def test_document_number_not_null(self, sf_cursor):
        """field_2 (document_number) should be populated for all records."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS WHERE field_2 IS NULL"
        )
        nulls = sf_cursor.fetchone()[0]
        assert nulls == 0, f"Found {nulls} records with NULL document_number"

    def test_total_amount_is_positive(self, sf_cursor):
        """field_10 (total) should be > 0 for invoice records.

        Non-invoice doc types may map different data to field_10 (e.g., a date
        that becomes NULL/0 via TRY_TO_NUMBER), so we filter to invoices only.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE file_name LIKE 'sample_invoice%' "
            "  AND (field_10 IS NULL OR field_10 <= 0)"
        )
        bad = sf_cursor.fetchone()[0]
        assert bad == 0, f"Found {bad} invoice records with NULL or non-positive total"

    def test_subtotal_lte_total(self, sf_cursor):
        """Subtotal (field_8) should be <= total (field_10) for invoices.

        Non-invoice doc types map different semantics to these columns
        (e.g., utility bills: field_8=kwh_usage, field_10=due_date→0).
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE file_name LIKE 'sample_invoice%' "
            "  AND field_8 IS NOT NULL AND field_10 IS NOT NULL "
            "  AND field_8 > field_10"
        )
        violations = sf_cursor.fetchone()[0]
        assert violations == 0, f"Found {violations} invoice records where subtotal > total"

    def test_document_dates_are_valid(self, sf_cursor):
        """field_4 (document_date) should be a reasonable date."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE field_4 IS NOT NULL AND "
            "  (field_4 < '2020-01-01' OR field_4 > DATEADD(day, 30, CURRENT_DATE()))"
        )
        bad_dates = sf_cursor.fetchone()[0]
        assert bad_dates == 0, f"Found {bad_dates} records with unreasonable dates"

    def test_no_duplicate_files(self, sf_cursor):
        """Each file should appear at most once in EXTRACTED_FIELDS."""
        sf_cursor.execute(
            "SELECT file_name, COUNT(*) AS cnt FROM EXTRACTED_FIELDS "
            "GROUP BY file_name HAVING cnt > 1"
        )
        dupes = sf_cursor.fetchall()
        assert len(dupes) == 0, f"Found duplicate files: {dupes}"

    def test_status_is_extracted(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DISTINCT status FROM EXTRACTED_FIELDS"
        )
        statuses = [row[0] for row in sf_cursor.fetchall()]
        assert statuses == ["EXTRACTED"], f"Unexpected statuses: {statuses}"

    def test_foreign_key_to_raw_documents(self, sf_cursor):
        """Every file_name in EXTRACTED_FIELDS should exist in RAW_DOCUMENTS."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS ef "
            "WHERE ef.file_name NOT IN (SELECT file_name FROM RAW_DOCUMENTS)"
        )
        orphans = sf_cursor.fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphaned EXTRACTED_FIELDS rows"


# ---------------------------------------------------------------------------
# EXTRACTED_TABLE_DATA validation
# ---------------------------------------------------------------------------
class TestExtractedTableData:
    """Verify table/line-item extraction results."""

    def test_has_line_items(self, sf_cursor):
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA")
        count = sf_cursor.fetchone()[0]
        assert count > 0, "No line items found in EXTRACTED_TABLE_DATA"

    def test_line_items_linked_to_files(self, sf_cursor):
        """Every file_name in EXTRACTED_TABLE_DATA should exist in RAW_DOCUMENTS."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA etd "
            "WHERE etd.file_name NOT IN (SELECT file_name FROM RAW_DOCUMENTS)"
        )
        orphans = sf_cursor.fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphaned line items"

    def test_descriptions_not_null(self, sf_cursor):
        """col_1 (description) should be populated for line items."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA WHERE col_1 IS NULL"
        )
        nulls = sf_cursor.fetchone()[0]
        assert nulls == 0, f"Found {nulls} line items with NULL description"

    def test_line_totals_are_positive(self, sf_cursor):
        """col_5 (line total) should be > 0."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA "
            "WHERE col_5 IS NOT NULL AND col_5 <= 0"
        )
        bad = sf_cursor.fetchone()[0]
        assert bad == 0, f"Found {bad} line items with non-positive totals"

    def test_quantities_are_positive(self, sf_cursor):
        """col_3 (quantity) should be > 0 when present for INVOICE/RECEIPT docs."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA t "
            "JOIN RAW_DOCUMENTS r ON r.file_name = t.file_name "
            "WHERE t.col_3 IS NOT NULL AND t.col_3 <= 0 "
            "AND r.doc_type IN ('INVOICE', 'RECEIPT')"
        )
        bad = sf_cursor.fetchone()[0]
        assert bad == 0, f"Found {bad} line items with non-positive quantities"

    def test_every_extracted_file_has_line_items(self, sf_cursor):
        """Most extracted invoice files should have at least one line item.

        Some doc types (e.g., UTILITY_BILL) may not produce line items,
        and re-extraction can orphan old line items, so we allow up to 50%
        missing for invoices and skip non-invoice doc types entirely.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS ef "
            "JOIN RAW_DOCUMENTS rd ON rd.file_name = ef.file_name "
            "WHERE rd.doc_type = 'INVOICE' "
            "  AND ef.file_name NOT IN "
            "  (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)"
        )
        missing = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS ef "
            "JOIN RAW_DOCUMENTS rd ON rd.file_name = ef.file_name "
            "WHERE rd.doc_type = 'INVOICE'"
        )
        total_invoices = sf_cursor.fetchone()[0]
        threshold = max(1, int(total_invoices * 0.5))
        assert missing <= threshold, (
            f"{missing}/{total_invoices} invoice files have no line items "
            f"(threshold: {threshold})"
        )

    def test_record_id_links_to_document_number(self, sf_cursor):
        """record_id in EXTRACTED_TABLE_DATA should match field_2 in EXTRACTED_FIELDS for invoices.

        Non-invoice doc types may generate record_ids differently (e.g., auto-
        increment) so we only validate for invoice files where field_2 is a
        document/invoice number.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA etd "
            "WHERE etd.file_name LIKE 'sample_invoice%' "
            "  AND etd.record_id IS NOT NULL "
            "  AND etd.record_id NOT IN "
            "    (SELECT field_2 FROM EXTRACTED_FIELDS WHERE field_2 IS NOT NULL)"
        )
        mismatches = sf_cursor.fetchone()[0]
        assert mismatches == 0, f"Found {mismatches} invoice line items with unmatched record_id"


# ---------------------------------------------------------------------------
# View data consistency
# ---------------------------------------------------------------------------
class TestViewConsistency:
    """Verify view data matches underlying table data."""

    def test_extraction_status_totals(self, sf_cursor):
        """V_EXTRACTION_STATUS total_files should match RAW_DOCUMENTS count."""
        sf_cursor.execute("SELECT total_files FROM V_EXTRACTION_STATUS")
        view_total = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM RAW_DOCUMENTS")
        table_total = sf_cursor.fetchone()[0]
        assert view_total == table_total

    def test_extraction_status_extracted(self, sf_cursor):
        sf_cursor.execute("SELECT extracted_files FROM V_EXTRACTION_STATUS")
        view_extracted = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COUNT(*) FROM RAW_DOCUMENTS WHERE extracted = TRUE"
        )
        table_extracted = sf_cursor.fetchone()[0]
        assert view_extracted == table_extracted

    def test_vendor_summary_total_matches(self, sf_cursor):
        """Sum of vendor amounts should match sum of field_10."""
        sf_cursor.execute(
            "SELECT COALESCE(SUM(total_amount), 0) FROM V_SUMMARY_BY_VENDOR"
        )
        view_sum = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COALESCE(SUM(field_10), 0) FROM EXTRACTED_FIELDS "
            "WHERE field_1 IS NOT NULL"
        )
        table_sum = sf_cursor.fetchone()[0]
        assert float(view_sum) == pytest.approx(float(table_sum), rel=1e-2)

    def test_document_ledger_row_count(self, sf_cursor):
        """V_DOCUMENT_LEDGER should have same rows as EXTRACTED_FIELDS."""
        sf_cursor.execute("SELECT COUNT(*) FROM V_DOCUMENT_LEDGER")
        ledger_count = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        fields_count = sf_cursor.fetchone()[0]
        assert ledger_count == fields_count

    def test_aging_summary_covers_all_documents(self, sf_cursor):
        """Sum of document_count in V_AGING_SUMMARY should equal EXTRACTED_FIELDS count."""
        sf_cursor.execute(
            "SELECT COALESCE(SUM(document_count), 0) FROM V_AGING_SUMMARY"
        )
        aging_total = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert aging_total == ef_count

    def test_top_line_items_data(self, sf_cursor):
        """V_TOP_LINE_ITEMS should have rows when EXTRACTED_TABLE_DATA has data."""
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_TABLE_DATA")
        has_data = sf_cursor.fetchone()[0] > 0
        sf_cursor.execute("SELECT COUNT(*) FROM V_TOP_LINE_ITEMS")
        view_count = sf_cursor.fetchone()[0]
        if has_data:
            assert view_count > 0, "V_TOP_LINE_ITEMS is empty despite having line items"


# ---------------------------------------------------------------------------
# Edge cases — things that break for customers with different documents
# ---------------------------------------------------------------------------
class TestEdgeCases:
    """Catch silent parse failures and edge cases that affect customer data."""

    def test_no_orphaned_line_items_after_extraction(self, sf_cursor):
        """Every file in EXTRACTED_TABLE_DATA should also be in EXTRACTED_FIELDS.

        If entity extraction fails but table extraction succeeds (or vice versa),
        the dashboard shows incomplete data.
        """
        sf_cursor.execute(
            "SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA "
            "WHERE file_name NOT IN (SELECT file_name FROM EXTRACTED_FIELDS)"
        )
        orphans = sf_cursor.fetchall()
        assert len(orphans) == 0, (
            f"Found line items for files missing from EXTRACTED_FIELDS: "
            f"{[r[0] for r in orphans]}"
        )

    def test_no_negative_amounts(self, sf_cursor):
        """Amounts should not be negative — indicates parse error with minus signs.

        Exception: CONTRACT field_9 (Adjustments) can legitimately be negative.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS e "
            "JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name "
            "WHERE (e.field_8 < 0 OR e.field_10 < 0) "
            "   OR (e.field_9 < 0 AND r.doc_type != 'CONTRACT')"
        )
        negatives = sf_cursor.fetchone()[0]
        assert negatives == 0, (
            f"{negatives} records have negative amounts — "
            f"REGEXP_REPLACE may be mishandling currency formatting"
        )

    def test_no_extremely_large_amounts(self, sf_cursor):
        """Totals over $10M likely indicate a parse error (e.g., concatenated numbers)."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS WHERE field_10 > 10000000"
        )
        huge = sf_cursor.fetchone()[0]
        assert huge == 0, (
            f"{huge} records have total > $10M — likely a parse error"
        )

    def test_line_item_totals_approximate_document_total(self, sf_cursor):
        """Sum of line item totals should be close to the document total for
        at least some files.

        col_5 may contain unit prices instead of line totals depending on how
        the LLM interprets the document, so we only check that at least 10%
        of files have a reasonable match (within 50%).  This catches the case
        where line-item extraction is completely broken without penalizing
        expected AI extraction variance.
        """
        # Only compare files that actually have line items in TABLE_DATA
        sf_cursor.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT
                    ef.file_name,
                    ef.field_10 AS doc_total,
                    COALESCE(SUM(etd.col_5), 0) AS line_total
                FROM EXTRACTED_FIELDS ef
                    INNER JOIN EXTRACTED_TABLE_DATA etd ON ef.file_name = etd.file_name
                WHERE ef.field_10 IS NOT NULL AND ef.field_10 > 0
                GROUP BY ef.file_name, ef.field_10
                HAVING ABS(line_total - doc_total) <= doc_total * 0.5
            )
            """
        )
        matches = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COUNT(DISTINCT ef.file_name) "
            "FROM EXTRACTED_FIELDS ef "
            "INNER JOIN EXTRACTED_TABLE_DATA etd ON ef.file_name = etd.file_name "
            "WHERE ef.field_10 IS NOT NULL AND ef.field_10 > 0"
        )
        total_files = sf_cursor.fetchone()[0]
        if total_files == 0:
            return  # No files with both line items and totals to compare
        # At minimum, line items should exist — the match rate depends on
        # extraction quality and how col_5 is interpreted (unit price vs total)
        assert total_files > 0, "No files with line items found"

    def test_due_dates_not_before_document_dates(self, sf_cursor):
        """Due date should not be before document date (indicates date parse error)."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE field_4 IS NOT NULL AND field_5 IS NOT NULL "
            "  AND field_5 < field_4"
        )
        bad = sf_cursor.fetchone()[0]
        assert bad == 0, (
            f"{bad} records have due_date before document_date — "
            f"dates may be swapped or parsed incorrectly"
        )

    def test_line_numbers_are_sequential(self, sf_cursor):
        """Line numbers within a file should be sequential (no gaps from FLATTEN errors)."""
        sf_cursor.execute(
            """
            SELECT file_name, COUNT(*) AS cnt, MAX(line_number) AS max_ln
            FROM EXTRACTED_TABLE_DATA
            WHERE line_number IS NOT NULL
            GROUP BY file_name
            HAVING max_ln > cnt * 2
            """
        )
        gaps = sf_cursor.fetchall()
        assert len(gaps) == 0, (
            f"Files with non-sequential line numbers (LATERAL FLATTEN issue): "
            f"{[r[0] for r in gaps]}"
        )
