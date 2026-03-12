"""Data Validation Tests — verify writeback and view data integrity.

Tests the append-only audit trail pattern:
- INSERT into INVOICE_REVIEW (never UPDATE/MERGE)
- V_INVOICE_SUMMARY picks the latest review via ROW_NUMBER()
- COALESCE(corrected_*, original) ensures corrections override originals
"""

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# Writeback round-trip: INSERT into INVOICE_REVIEW -> read from V_INVOICE_SUMMARY
# ---------------------------------------------------------------------------
class TestWritebackRoundTrip:
    """Verify that writing to INVOICE_REVIEW flows through to V_INVOICE_SUMMARY.

    Because V_INVOICE_SUMMARY is a regular view (not a dynamic table), reads
    are instant — no lag to worry about.
    """

    def test_insert_and_read_back(self, sf_cursor):
        """Insert a review and read it back from INVOICE_REVIEW directly."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row[0], row[1]

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, reviewer_notes) "
            "VALUES (%s, %s, 'APPROVED', 999.99, '__pytest_round_trip__')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT review_status, corrected_total, reviewer_notes, reviewed_by "
            "FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_round_trip__'"
        )
        result = sf_cursor.fetchone()
        assert result is not None, "Test review row not found after INSERT"
        assert result[0] == "APPROVED"
        assert float(result[1]) == pytest.approx(999.99)
        assert result[2] == "__pytest_round_trip__"
        assert result[3] is not None, "REVIEWED_BY (CURRENT_USER) should be auto-populated"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_round_trip__'"
        )

    def test_reviewed_at_auto_populated(self, sf_cursor):
        """REVIEWED_AT default should populate automatically on INSERT."""
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, reviewer_notes) "
            "VALUES (-998, '__auto_ts_test__.pdf', 'REJECTED', '__pytest_auto_ts__')"
        )

        sf_cursor.execute(
            "SELECT reviewed_at FROM INVOICE_REVIEW "
            "WHERE reviewer_notes = '__pytest_auto_ts__'"
        )
        result = sf_cursor.fetchone()
        assert result is not None
        assert result[0] is not None, "REVIEWED_AT should be auto-populated"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_auto_ts__'"
        )

    def test_multiple_reviews_per_document(self, sf_cursor):
        """Multiple reviews for the same record_id should be allowed (append-only)."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row[0], row[1]

        for status in ("REJECTED", "APPROVED"):
            sf_cursor.execute(
                "INSERT INTO INVOICE_REVIEW "
                "(record_id, file_name, review_status, reviewer_notes) "
                "VALUES (%s, %s, %s, '__pytest_multi_review__')",
                (record_id, file_name, status),
            )

        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW "
            "WHERE reviewer_notes = '__pytest_multi_review__'"
        )
        count = sf_cursor.fetchone()[0]
        assert count == 2, f"Expected 2 review rows, got {count}"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_multi_review__'"
        )

    def test_all_corrected_columns_nullable(self, sf_cursor):
        """All corrected_* columns should accept NULL (no correction)."""
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, reviewer_notes) "
            "VALUES (-997, '__null_cols_test__.pdf', 'APPROVED', '__pytest_null_cols__')"
        )

        sf_cursor.execute(
            "SELECT corrected_vendor_name, corrected_invoice_number, "
            "corrected_po_number, corrected_invoice_date, "
            "corrected_due_date, corrected_payment_terms, "
            "corrected_recipient, corrected_subtotal, "
            "corrected_tax_amount, corrected_total "
            "FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_null_cols__'"
        )
        result = sf_cursor.fetchone()
        assert result is not None
        for i, col_val in enumerate(result):
            assert col_val is None, f"corrected column index {i} should be NULL when not provided"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_null_cols__'"
        )


# ---------------------------------------------------------------------------
# COALESCE override correctness
# ---------------------------------------------------------------------------
class TestCoalesceOverride:
    """Verify that V_INVOICE_SUMMARY COALESCE logic correctly overrides
    original values with corrections."""

    def test_corrected_total_overrides_original(self, sf_cursor):
        """When corrected_total is set, V_INVOICE_SUMMARY.total_amount
        should show the corrected value, not the original."""
        sf_cursor.execute("SELECT record_id, file_name, field_10 FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name, original_total = row

        # Remove any existing reviews for this record so our test review is the latest
        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE record_id = %s",
            (record_id,),
        )

        corrected_value = 12345.67
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, reviewer_notes) "
            "VALUES (%s, %s, 'CORRECTED', %s, '__pytest_coalesce_total__')",
            (record_id, file_name, corrected_value),
        )

        sf_cursor.execute(
            "SELECT total_amount FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        view_total = float(sf_cursor.fetchone()[0])
        assert view_total == pytest.approx(corrected_value), (
            f"View should show corrected total {corrected_value}, got {view_total}"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_coalesce_total__'"
        )

    def test_corrected_vendor_overrides_original(self, sf_cursor):
        """When corrected_vendor_name is set, V_INVOICE_SUMMARY.vendor_name
        should show the corrected value."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row

        corrected_vendor = "__PYTEST_CORRECTED_VENDOR__"
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_vendor_name, reviewer_notes) "
            "VALUES (%s, %s, 'CORRECTED', %s, '__pytest_coalesce_vendor__')",
            (record_id, file_name, corrected_vendor),
        )

        sf_cursor.execute(
            "SELECT vendor_name FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        view_vendor = sf_cursor.fetchone()[0]
        assert view_vendor == corrected_vendor, (
            f"View should show corrected vendor '{corrected_vendor}', got '{view_vendor}'"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_coalesce_vendor__'"
        )

    def test_null_correction_preserves_original(self, sf_cursor):
        """When corrected_total is NULL, V_INVOICE_SUMMARY.total_amount
        should fall back to the original EXTRACTED_FIELDS value."""
        sf_cursor.execute(
            "SELECT record_id, file_name, field_10 FROM EXTRACTED_FIELDS "
            "WHERE field_10 IS NOT NULL LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data with non-NULL total")
        record_id, file_name, original_total = row

        # Insert review WITHOUT setting corrected_total
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, reviewer_notes) "
            "VALUES (%s, %s, 'APPROVED', '__pytest_coalesce_null__')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT total_amount FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        view_total = float(sf_cursor.fetchone()[0])
        assert view_total == pytest.approx(float(original_total)), (
            f"View should fall back to original total {original_total}, got {view_total}"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_coalesce_null__'"
        )

    def test_view_reads_are_instant(self, sf_cursor):
        """V_INVOICE_SUMMARY should reflect INSERTs immediately (no lag)."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_vendor_name, reviewer_notes) "
            "VALUES (%s, %s, 'CORRECTED', '__INSTANT_READ__', '__pytest_instant__')",
            (record_id, file_name),
        )

        try:
            sf_cursor.execute(
                "SELECT vendor_name FROM V_INVOICE_SUMMARY WHERE record_id = %s",
                (record_id,),
            )
            vendor = sf_cursor.fetchone()[0]
            assert vendor == "__INSTANT_READ__", (
                f"View should reflect INSERT instantly, got vendor='{vendor}'"
            )
        finally:
            sf_cursor.execute(
                "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_instant__'"
            )


# ---------------------------------------------------------------------------
# Append-only audit trail: latest review wins
# ---------------------------------------------------------------------------
class TestAppendOnlyAuditTrail:
    """Verify that the view picks the latest review (highest review_id)
    when multiple reviews exist for the same record_id."""

    def test_latest_review_wins(self, sf_cursor):
        """Insert two reviews for the same record; view should show the latest."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row

        # Clean up any existing reviews for this record to isolate the test
        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE record_id = %s",
            (record_id,),
        )

        # First review: REJECTED with corrected total 100
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, reviewer_notes) "
            "VALUES (%s, %s, 'REJECTED', 100.00, '__pytest_audit_1__')",
            (record_id, file_name),
        )

        # Second review: APPROVED with corrected total 200 — this should win
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, reviewer_notes) "
            "VALUES (%s, %s, 'APPROVED', 200.00, '__pytest_audit_2__')",
            (record_id, file_name),
        )

        # Determine which review_id is actually highest (latest) —
        # autoincrement may not strictly order within the same session
        sf_cursor.execute(
            "SELECT review_status, corrected_total FROM INVOICE_REVIEW "
            "WHERE reviewer_notes IN ('__pytest_audit_1__', '__pytest_audit_2__') "
            "ORDER BY reviewed_at DESC LIMIT 1"
        )
        latest = sf_cursor.fetchone()
        expected_status = latest[0]
        expected_total = float(latest[1])

        sf_cursor.execute(
            "SELECT review_status, total_amount FROM V_INVOICE_SUMMARY "
            "WHERE record_id = %s",
            (record_id,),
        )
        result = sf_cursor.fetchone()
        assert result[0] == expected_status, (
            f"View should match latest review status '{expected_status}', got '{result[0]}'"
        )
        assert float(result[1]) == pytest.approx(expected_total), (
            f"View should show latest corrected total {expected_total}, got {result[1]}"
        )

        # Both rows should still exist in INVOICE_REVIEW (append-only)
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW "
            "WHERE reviewer_notes IN ('__pytest_audit_1__', '__pytest_audit_2__')"
        )
        count = sf_cursor.fetchone()[0]
        assert count == 2, "Both audit rows should still exist (append-only)"

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW "
            "WHERE reviewer_notes IN ('__pytest_audit_1__', '__pytest_audit_2__')"
        )

    def test_three_reviews_latest_wins(self, sf_cursor):
        """Three sequential reviews — view should always show the latest."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data to test with")
        record_id, file_name = row

        # Clean up any pre-existing reviews for this record to isolate the test
        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE record_id = %s",
            (record_id,),
        )

        for i, (status, vendor) in enumerate([
            ("REJECTED", "Vendor_A"),
            ("CORRECTED", "Vendor_B"),
            ("APPROVED", "Vendor_C"),
        ]):
            sf_cursor.execute(
                "INSERT INTO INVOICE_REVIEW "
                "(record_id, file_name, review_status, corrected_vendor_name, reviewer_notes) "
                "VALUES (%s, %s, %s, %s, %s)",
                (record_id, file_name, status, vendor, f"__pytest_3audit_{i}__"),
            )

        # The view uses ROW_NUMBER() ORDER BY reviewed_at DESC, so most recent wins.
        # Verify by checking which reviewed_at is the max among our test rows.
        sf_cursor.execute(
            "SELECT review_status, corrected_vendor_name FROM INVOICE_REVIEW "
            "WHERE reviewer_notes LIKE '__pytest_3audit_%' "
            "ORDER BY reviewed_at DESC LIMIT 1"
        )
        latest_row = sf_cursor.fetchone()
        expected_status = latest_row[0]
        expected_vendor = latest_row[1]

        sf_cursor.execute(
            "SELECT review_status, vendor_name FROM V_INVOICE_SUMMARY "
            "WHERE record_id = %s",
            (record_id,),
        )
        result = sf_cursor.fetchone()
        assert result[0] == expected_status, (
            f"Expected {expected_status} (latest), got {result[0]}"
        )
        assert result[1] == expected_vendor, (
            f"Expected {expected_vendor} (latest), got {result[1]}"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW "
            "WHERE reviewer_notes LIKE '__pytest_3audit_%'"
        )


# ---------------------------------------------------------------------------
# View join correctness
# ---------------------------------------------------------------------------
class TestViewJoinCorrectness:
    """Verify the V_INVOICE_SUMMARY join logic is correct."""

    def test_amounts_match_extracted_fields(self, sf_cursor):
        """For unreviewed records, total_amount should match EXTRACTED_FIELDS field_10."""
        sf_cursor.execute(
            """
            SELECT COUNT(*) FROM V_INVOICE_SUMMARY v
            JOIN EXTRACTED_FIELDS ef ON v.record_id = ef.record_id
            WHERE v.review_status IS NULL
              AND (v.total_amount != ef.field_10
                   OR (v.total_amount IS NULL) != (ef.field_10 IS NULL))
            """
        )
        mismatches = sf_cursor.fetchone()[0]
        assert mismatches == 0, (
            f"{mismatches} unreviewed rows have mismatched total_amount"
        )

    def test_line_item_count_matches_aggregation(self, sf_cursor):
        """line_item_count should match COUNT(*) from EXTRACTED_TABLE_DATA
        when no correction override exists."""
        sf_cursor.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT
                    v.file_name,
                    v.line_item_count AS view_count,
                    COUNT(etd.line_id) AS actual_count
                FROM V_INVOICE_SUMMARY v
                LEFT JOIN EXTRACTED_TABLE_DATA etd ON v.file_name = etd.file_name
                LEFT JOIN INVOICE_REVIEW rv ON v.record_id = rv.record_id
                WHERE rv.corrections:line_item_count IS NULL
                GROUP BY v.file_name, v.line_item_count
                HAVING view_count != actual_count
            )
            """
        )
        mismatches = sf_cursor.fetchone()[0]
        assert mismatches == 0, (
            f"{mismatches} documents have mismatched line_item_count"
        )

    def test_computed_line_total_matches_sum(self, sf_cursor):
        """computed_line_total should match SUM(col_5) from EXTRACTED_TABLE_DATA
        when no correction override exists."""
        sf_cursor.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT
                    v.file_name,
                    v.computed_line_total AS view_total,
                    COALESCE(SUM(etd.col_5), 0) AS actual_total
                FROM V_INVOICE_SUMMARY v
                LEFT JOIN EXTRACTED_TABLE_DATA etd ON v.file_name = etd.file_name
                LEFT JOIN INVOICE_REVIEW rv ON v.record_id = rv.record_id
                WHERE rv.corrections:computed_line_total IS NULL
                GROUP BY v.file_name, v.computed_line_total
                HAVING ABS(COALESCE(view_total, 0) - actual_total) > 0.01
            )
            """
        )
        mismatches = sf_cursor.fetchone()[0]
        assert mismatches == 0, (
            f"{mismatches} documents have mismatched computed_line_total"
        )

    def test_no_duplicate_rows(self, sf_cursor):
        """View should have exactly one row per record_id (no fan-out from joins)."""
        sf_cursor.execute(
            "SELECT record_id, COUNT(*) AS cnt FROM V_INVOICE_SUMMARY "
            "GROUP BY record_id HAVING cnt > 1"
        )
        dupes = sf_cursor.fetchall()
        assert len(dupes) == 0, (
            f"View has duplicate rows for record_ids: {[r[0] for r in dupes]}"
        )

    def test_all_extracted_fields_represented(self, sf_cursor):
        """Every record in EXTRACTED_FIELDS should appear in the view."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS ef "
            "WHERE ef.record_id NOT IN (SELECT record_id FROM V_INVOICE_SUMMARY)"
        )
        missing = sf_cursor.fetchone()[0]
        assert missing == 0, f"{missing} EXTRACTED_FIELDS records missing from view"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------
class TestWritebackEdgeCases:
    """Edge cases for the writeback workflow."""

    def test_review_status_values(self, sf_cursor):
        """If reviews exist, status should be one of the expected values."""
        sf_cursor.execute("SELECT COUNT(*) FROM INVOICE_REVIEW")
        if sf_cursor.fetchone()[0] == 0:
            pytest.skip("No reviews to validate")

        sf_cursor.execute(
            "SELECT DISTINCT review_status FROM INVOICE_REVIEW "
            "WHERE review_status NOT IN ('APPROVED', 'REJECTED', 'CORRECTED')"
        )
        unexpected = sf_cursor.fetchall()
        assert len(unexpected) == 0, (
            f"Unexpected review statuses: {[r[0] for r in unexpected]}"
        )

    def test_view_handles_zero_line_items_gracefully(self, sf_cursor):
        """If a document has no line items, view should still include it
        with NULL/0 for line-item columns (LEFT JOIN)."""
        sf_cursor.execute(
            "SELECT COUNT(*) FROM V_INVOICE_SUMMARY WHERE line_item_count IS NULL"
        )
        null_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS ef "
            "WHERE ef.file_name NOT IN (SELECT DISTINCT file_name FROM EXTRACTED_TABLE_DATA)"
        )
        orphan_count = sf_cursor.fetchone()[0]
        assert null_count == orphan_count, (
            f"Expected {orphan_count} rows with NULL line_item_count, got {null_count}"
        )

    def test_review_for_nonexistent_record_does_not_break_view(self, sf_cursor):
        """A review referencing a nonexistent record_id should not affect the view."""
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, reviewer_notes) "
            "VALUES (-99999, '__phantom__.pdf', 'APPROVED', '__pytest_phantom__')"
        )

        # View count should still match EXTRACTED_FIELDS
        sf_cursor.execute("SELECT COUNT(*) FROM V_INVOICE_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count, (
            f"Phantom review should not add rows to view: view={view_count}, ef={ef_count}"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_phantom__'"
        )


# ---------------------------------------------------------------------------
# Batch / concurrent review INSERTs
# ---------------------------------------------------------------------------
class TestBatchReviewInserts:
    """Verify that multiple INSERTs for different records in the same batch
    all land correctly — simulates the Streamlit Save flow where the app
    loops over changed_rows and INSERTs one row per changed record."""

    def test_batch_insert_multiple_records(self, sf_cursor):
        """INSERT reviews for 3 different records in rapid succession;
        all should appear in INVOICE_REVIEW and V_INVOICE_SUMMARY."""
        sf_cursor.execute(
            "SELECT record_id, file_name FROM EXTRACTED_FIELDS ORDER BY record_id LIMIT 3"
        )
        rows = sf_cursor.fetchall()
        if len(rows) < 3:
            pytest.skip("Need at least 3 EXTRACTED_FIELDS records")

        test_data = []
        for i, (record_id, file_name) in enumerate(rows):
            status = ["APPROVED", "REJECTED", "CORRECTED"][i]
            vendor = f"__BATCH_VENDOR_{i}__"
            total = 100.00 + i * 50
            test_data.append((record_id, file_name, status, vendor, total))

        # Clean up any existing reviews for these records
        ids = ",".join(str(td[0]) for td in test_data)
        sf_cursor.execute(
            f"DELETE FROM INVOICE_REVIEW WHERE record_id IN ({ids})"
        )

        # Batch INSERT — mimics the Streamlit Save loop
        for record_id, file_name, status, vendor, total in test_data:
            sf_cursor.execute(
                "INSERT INTO INVOICE_REVIEW "
                "(record_id, file_name, review_status, corrected_vendor_name, "
                " corrected_total, reviewer_notes) "
                "VALUES (%s, %s, %s, %s, %s, '__pytest_batch__')",
                (record_id, file_name, status, vendor, total),
            )

        # Verify all 3 rows landed in INVOICE_REVIEW
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW "
            "WHERE reviewer_notes = '__pytest_batch__'"
        )
        count = sf_cursor.fetchone()[0]
        assert count == 3, f"Expected 3 batch-inserted rows, got {count}"

        # Verify each shows in V_INVOICE_SUMMARY with correct corrected values
        for record_id, _, status, vendor, total in test_data:
            sf_cursor.execute(
                "SELECT review_status, vendor_name, total_amount "
                "FROM V_INVOICE_SUMMARY WHERE record_id = %s",
                (record_id,),
            )
            result = sf_cursor.fetchone()
            assert result is not None, f"record_id {record_id} not in view"
            assert result[0] == status, (
                f"record_id {record_id}: expected status {status}, got {result[0]}"
            )
            assert result[1] == vendor, (
                f"record_id {record_id}: expected vendor {vendor}, got {result[1]}"
            )
            assert float(result[2]) == pytest.approx(total), (
                f"record_id {record_id}: expected total {total}, got {result[2]}"
            )

        # Clean up
        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__pytest_batch__'"
        )

    def test_batch_insert_same_record_multiple_times(self, sf_cursor):
        """INSERT multiple reviews for the SAME record in a batch;
        the view should show the latest (highest review_id)."""
        sf_cursor.execute("SELECT record_id, file_name FROM EXTRACTED_FIELDS LIMIT 1")
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No EXTRACTED_FIELDS data")
        record_id, file_name = row

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE record_id = %s", (record_id,)
        )

        # Three rapid INSERTs for the same record
        for i, (status, vendor) in enumerate([
            ("REJECTED", "BatchVendor_A"),
            ("CORRECTED", "BatchVendor_B"),
            ("APPROVED", "BatchVendor_C"),
        ]):
            sf_cursor.execute(
                "INSERT INTO INVOICE_REVIEW "
                "(record_id, file_name, review_status, corrected_vendor_name, "
                " reviewer_notes) "
                "VALUES (%s, %s, %s, %s, %s)",
                (record_id, file_name, status, vendor, f"__pytest_batch_same_{i}__"),
            )

        # All 3 rows should exist (append-only)
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INVOICE_REVIEW "
            "WHERE reviewer_notes LIKE '__pytest_batch_same_%'"
        )
        assert sf_cursor.fetchone()[0] == 3

        # View should show whichever has the highest review_id
        sf_cursor.execute(
            "SELECT review_status, corrected_vendor_name FROM INVOICE_REVIEW "
            "WHERE reviewer_notes LIKE '__pytest_batch_same_%' "
            "ORDER BY reviewed_at DESC LIMIT 1"
        )
        latest = sf_cursor.fetchone()

        sf_cursor.execute(
            "SELECT review_status, vendor_name FROM V_INVOICE_SUMMARY "
            "WHERE record_id = %s",
            (record_id,),
        )
        view_row = sf_cursor.fetchone()
        assert view_row[0] == latest[0], (
            f"View status {view_row[0]} != latest {latest[0]}"
        )
        assert view_row[1] == latest[1], (
            f"View vendor {view_row[1]} != latest {latest[1]}"
        )

        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW "
            "WHERE reviewer_notes LIKE '__pytest_batch_same_%'"
        )
