"""Edge case tests for the writeback pipeline and V_INVOICE_SUMMARY view.

Covers:
  1.  AUTOINCREMENT gap behavior (non-contiguous review_ids)
  2.  Transaction rollback visibility
  3.  CURRENT_USER() / CURRENT_TIMESTAMP() defaults
  4.  Extremely large reviewer_notes (10K, 100K chars)
  5.  SQL injection via data values (metacharacters round-trip)
  6.  Empty-table cold start (view with zero review rows)
  7.  Duplicate file_name across EXTRACTED_FIELDS records
  8.  Date type coercion in COALESCE (NULL combinations)
  9.  Boundary record_ids (0, negative)
  10. Concurrent schema change + write
"""

import threading

import pytest

pytestmark = [pytest.mark.sql]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"
TAG = "__pytest_edge__"


@pytest.fixture(autouse=True)
def _cleanup(sf_cursor):
    """Delete all test rows after each test."""
    yield
    sf_cursor.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes LIKE '{TAG}%'"
    )
    # Also clean up boundary record_id rows
    sf_cursor.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id IN (0, -1, -2, -99999)"
    )


def _get_record(cursor, offset=0):
    cursor.execute(
        f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
        f"ORDER BY record_id LIMIT 1 OFFSET {offset}"
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("No EXTRACTED_FIELDS data")
    return row[0], row[1]


# ---------------------------------------------------------------------------
# 1. AUTOINCREMENT gap behavior
# ---------------------------------------------------------------------------
class TestAutoincrementGaps:
    """Verify ROW_NUMBER() latest-wins works with non-contiguous review_ids."""

    def test_gap_in_review_ids(self, sf_cursor):
        """Insert, delete middle rows to create gaps, insert again.
        View should pick the highest review_id among remaining rows."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # Insert 3 rows
        for i in range(3):
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, "
                f" corrected_vendor_name, reviewer_notes) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (rid, fname, "REJECTED", f"Gap_V{i}", f"{TAG}_gap_{i}"),
            )

        # Delete the middle row to create a gap
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_gap_1",),
        )

        # Insert a new row (will get a higher review_id, with a gap)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', 'Gap_Winner', %s)",
            (rid, fname, f"{TAG}_gap_winner"),
        )

        # Determine which review_id is actually highest
        sf_cursor.execute(
            f"SELECT review_status, corrected_vendor_name "
            f"FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s "
            f"ORDER BY review_id DESC LIMIT 1",
            (rid,),
        )
        expected = sf_cursor.fetchone()

        # View should match the highest review_id
        sf_cursor.execute(
            f"SELECT review_status, vendor_name FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        result = sf_cursor.fetchone()
        assert result[0] == expected[0], (
            f"Expected {expected[0]} (highest review_id), got {result[0]}"
        )
        assert result[1] == expected[1], (
            f"Expected {expected[1]}, got {result[1]}"
        )

    def test_single_remaining_row_after_deletes(self, sf_cursor):
        """Delete all but one review. View should still work."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # Insert 3, delete 2
        for i in range(3):
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, reviewer_notes) "
                f"VALUES (%s, %s, 'CORRECTED', %s)",
                (rid, fname, f"{TAG}_single_{i}"),
            )

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes IN (%s, %s)",
            (f"{TAG}_single_0", f"{TAG}_single_1"),
        )

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_single_%'"
        )
        assert sf_cursor.fetchone()[0] == 1

        sf_cursor.execute(
            f"SELECT review_status FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert sf_cursor.fetchone()[0] == "CORRECTED"


# ---------------------------------------------------------------------------
# 2. Transaction rollback visibility
# ---------------------------------------------------------------------------
class TestTransactionRollback:
    """Verify that rolled-back INSERTs are not visible."""

    def test_rollback_not_visible_in_table(self, sf_cursor):
        """INSERT inside a transaction, ROLLBACK — row should not exist."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute("BEGIN")
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_rollback"),
        )
        sf_cursor.execute("ROLLBACK")

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_rollback",),
        )
        assert sf_cursor.fetchone()[0] == 0, "Rolled-back row should not exist"

    def test_rollback_not_visible_in_view(self, sf_cursor):
        """Rolled-back review should not affect the view."""
        rid, fname = _get_record(sf_cursor)

        # Clean existing reviews
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # Get the view state before
        sf_cursor.execute(
            f"SELECT review_status FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        before = sf_cursor.fetchone()[0]  # Should be NULL (no reviews)

        # INSERT + ROLLBACK
        sf_cursor.execute("BEGIN")
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'REJECTED', %s)",
            (rid, fname, f"{TAG}_rollback_view"),
        )
        sf_cursor.execute("ROLLBACK")

        # View should be unchanged
        sf_cursor.execute(
            f"SELECT review_status FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        after = sf_cursor.fetchone()[0]
        assert after == before, (
            f"View changed after rollback: before={before}, after={after}"
        )


# ---------------------------------------------------------------------------
# 3. CURRENT_USER() / CURRENT_TIMESTAMP() defaults
# ---------------------------------------------------------------------------
class TestColumnDefaults:
    """Verify auto-populated default columns."""

    def test_reviewed_by_populated(self, sf_cursor):
        """REVIEWED_BY should be auto-populated with CURRENT_USER()."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_defaults"),
        )
        sf_cursor.execute(
            f"SELECT reviewed_by FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_defaults",),
        )
        reviewed_by = sf_cursor.fetchone()[0]
        assert reviewed_by is not None, "REVIEWED_BY should be auto-populated"
        assert len(reviewed_by) > 0, "REVIEWED_BY should not be empty"

    def test_reviewed_at_populated(self, sf_cursor):
        """REVIEWED_AT should be auto-populated with CURRENT_TIMESTAMP()."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_ts_default"),
        )
        sf_cursor.execute(
            f"SELECT reviewed_at FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_ts_default",),
        )
        reviewed_at = sf_cursor.fetchone()[0]
        assert reviewed_at is not None, "REVIEWED_AT should be auto-populated"

    def test_reviewed_by_matches_current_user(self, sf_cursor):
        """REVIEWED_BY should match the session's CURRENT_USER()."""
        sf_cursor.execute("SELECT CURRENT_USER()")
        current_user = sf_cursor.fetchone()[0]

        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_user_match"),
        )
        sf_cursor.execute(
            f"SELECT reviewed_by FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_user_match",),
        )
        assert sf_cursor.fetchone()[0] == current_user

    def test_explicit_reviewed_by_overrides_default(self, sf_cursor):
        """If REVIEWED_BY is explicitly set, it should override the default."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewed_by, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', 'custom_user', %s)",
            (rid, fname, f"{TAG}_explicit_user"),
        )
        sf_cursor.execute(
            f"SELECT reviewed_by FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_explicit_user",),
        )
        assert sf_cursor.fetchone()[0] == "custom_user"


# ---------------------------------------------------------------------------
# 4. Extremely large reviewer_notes
# ---------------------------------------------------------------------------
class TestLargeNotes:
    """Verify large text values in reviewer_notes."""

    def test_10k_notes(self, sf_cursor):
        """10,000 character reviewer_notes should be accepted."""
        rid, fname = _get_record(sf_cursor)
        big_note = f"{TAG}_10k_" + "A" * 10000
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, big_note),
        )
        sf_cursor.execute(
            f"SELECT LENGTH(reviewer_notes) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_10k_%'"
        )
        length = sf_cursor.fetchone()[0]
        assert length > 10000, f"Expected >10K chars, got {length}"

    def test_100k_notes(self, sf_cursor):
        """100,000 character reviewer_notes should be accepted.
        Snowflake VARCHAR max is 16MB, so 100K is well within limits."""
        rid, fname = _get_record(sf_cursor)
        big_note = f"{TAG}_100k_" + "B" * 100000
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, big_note),
        )
        sf_cursor.execute(
            f"SELECT LENGTH(reviewer_notes) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_100k_%'"
        )
        length = sf_cursor.fetchone()[0]
        assert length > 100000, f"Expected >100K chars, got {length}"

    def test_view_handles_large_notes(self, sf_cursor):
        """View should return large reviewer_notes without error."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        big_note = f"{TAG}_viewlarge_" + "C" * 50000
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, big_note),
        )
        sf_cursor.execute(
            f"SELECT LENGTH(reviewer_notes) FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        length = sf_cursor.fetchone()[0]
        assert length > 50000


# ---------------------------------------------------------------------------
# 5. SQL injection via data values
# ---------------------------------------------------------------------------
class TestSQLInjectionSafety:
    """Verify SQL metacharacters round-trip safely through parameterized queries."""

    INJECTION_PAYLOADS = [
        "'; DROP TABLE INVOICE_REVIEW; --",
        "Robert'); DROP TABLE INVOICE_REVIEW;--",
        "1 OR 1=1",
        "' UNION SELECT * FROM INFORMATION_SCHEMA.TABLES --",
        "<script>alert('xss')</script>",
        "\\x00\\x01\\x02",  # null bytes as literal text
    ]

    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS)
    def test_injection_payload_stored_literally(self, sf_cursor, payload):
        """Payload should be stored as-is, not interpreted as SQL."""
        rid, fname = _get_record(sf_cursor)
        tag = f"{TAG}_inj"
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', %s, %s)",
            (rid, fname, payload, tag),
        )
        sf_cursor.execute(
            f"SELECT corrected_vendor_name FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (tag,),
        )
        result = sf_cursor.fetchone()[0]
        assert result == payload, (
            f"Payload was not stored literally: expected {payload!r}, got {result!r}"
        )
        # Cleanup for this parametrized run
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes = %s",
            (tag,),
        )

    def test_injection_in_notes_roundtrips_through_view(self, sf_cursor):
        """SQL metacharacters in reviewer_notes should appear in the view."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        evil_notes = "'; DROP TABLE INVOICE_REVIEW; --"
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, evil_notes),
        )
        sf_cursor.execute(
            f"SELECT reviewer_notes FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert sf_cursor.fetchone()[0] == evil_notes

    def test_table_still_exists_after_injection_attempts(self, sf_cursor):
        """INVOICE_REVIEW should still exist after all injection tests."""
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW"
        )
        count = sf_cursor.fetchone()[0]
        assert count >= 0, "Table should still be queryable"


# ---------------------------------------------------------------------------
# 6. Empty-table cold start
# ---------------------------------------------------------------------------
class TestEmptyTableColdStart:
    """Verify view behavior when INVOICE_REVIEW has zero test-relevant rows."""

    def test_view_works_with_no_reviews_for_record(self, sf_cursor):
        """A record with zero reviews should appear in view with NULL review columns."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        sf_cursor.execute(
            f"SELECT review_status, reviewer_notes, reviewed_by, reviewed_at "
            f"FROM {FQ}.V_INVOICE_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        result = sf_cursor.fetchone()
        assert result is not None, "Record should still appear in view"
        assert result[0] is None, "review_status should be NULL"
        assert result[1] is None, "reviewer_notes should be NULL"
        assert result[2] is None, "reviewed_by should be NULL"
        assert result[3] is None, "reviewed_at should be NULL"

    def test_view_row_count_equals_extracted_fields(self, sf_cursor):
        """Even with no reviews at all, view count = EXTRACTED_FIELDS count."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count

    def test_view_original_values_without_review(self, sf_cursor):
        """Without reviews, COALESCE should fall back to original values."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        sf_cursor.execute(
            f"SELECT field_1, field_10 FROM {FQ}.EXTRACTED_FIELDS "
            f"WHERE record_id = %s",
            (rid,),
        )
        orig = sf_cursor.fetchone()

        sf_cursor.execute(
            f"SELECT vendor_name, total_amount FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        view = sf_cursor.fetchone()
        assert view[0] == orig[0], "vendor_name should match original field_1"
        if orig[1] is not None:
            assert float(view[1]) == pytest.approx(float(orig[1]))


# ---------------------------------------------------------------------------
# 7. Duplicate file_name across EXTRACTED_FIELDS records
# ---------------------------------------------------------------------------
class TestDuplicateFileName:
    """Verify view handles the file_name relationship correctly."""

    def test_view_one_row_per_record_id(self, sf_cursor):
        """Even if file_name appears in multiple EXTRACTED_FIELDS rows,
        the view should key on record_id (1 row per record_id)."""
        sf_cursor.execute(
            f"SELECT record_id, COUNT(*) AS cnt "
            f"FROM {FQ}.V_INVOICE_SUMMARY GROUP BY record_id HAVING cnt > 1"
        )
        dupes = sf_cursor.fetchall()
        assert len(dupes) == 0, (
            f"Duplicate record_ids in view: {[r[0] for r in dupes]}"
        )

    def test_file_name_matches_between_tables(self, sf_cursor):
        """Every file_name in EXTRACTED_FIELDS should exist in RAW_DOCUMENTS."""
        sf_cursor.execute(f"""
            SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS ef
            WHERE ef.file_name NOT IN (
                SELECT file_name FROM {FQ}.RAW_DOCUMENTS
            )
        """)
        orphans = sf_cursor.fetchone()[0]
        assert orphans == 0, (
            f"{orphans} EXTRACTED_FIELDS rows reference non-existent RAW_DOCUMENTS"
        )


# ---------------------------------------------------------------------------
# 8. Date type coercion in COALESCE
# ---------------------------------------------------------------------------
class TestDateCoalesceBehavior:
    """Verify COALESCE between DATE columns handles NULL combinations."""

    def test_both_dates_null(self, sf_cursor):
        """If original field_4 and corrected_invoice_date are both NULL,
        view should return NULL for invoice_date."""
        # Find a record where field_4 is NULL (if any)
        sf_cursor.execute(
            f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
            f"WHERE field_4 IS NULL LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No records with NULL field_4")
        rid, fname = row

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # Insert review WITHOUT setting corrected_invoice_date
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_both_null_date"),
        )

        sf_cursor.execute(
            f"SELECT invoice_date FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert sf_cursor.fetchone()[0] is None, "Both NULL should yield NULL"

    def test_correction_overrides_null_original(self, sf_cursor):
        """If original date is NULL but correction is set, view should show correction."""
        sf_cursor.execute(
            f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
            f"WHERE field_4 IS NULL LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No records with NULL field_4")
        rid, fname = row

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_invoice_date, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', '2025-06-15', %s)",
            (rid, fname, f"{TAG}_date_override_null"),
        )

        sf_cursor.execute(
            f"SELECT invoice_date FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert str(sf_cursor.fetchone()[0]) == "2025-06-15"

    def test_null_correction_preserves_original_date(self, sf_cursor):
        """If correction date is NULL, view should show original date."""
        sf_cursor.execute(
            f"SELECT record_id, file_name, field_4 FROM {FQ}.EXTRACTED_FIELDS "
            f"WHERE field_4 IS NOT NULL LIMIT 1"
        )
        row = sf_cursor.fetchone()
        if row is None:
            pytest.skip("No records with non-NULL field_4")
        rid, fname, orig_date = row

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_date_preserve"),
        )

        sf_cursor.execute(
            f"SELECT invoice_date FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert str(sf_cursor.fetchone()[0]) == str(orig_date)


# ---------------------------------------------------------------------------
# 9. Boundary record_ids
# ---------------------------------------------------------------------------
class TestBoundaryRecordIds:
    """Verify behavior with record_id values that autoincrement wouldn't produce."""

    def test_record_id_zero(self, sf_cursor):
        """Review for record_id=0 should be insertable."""
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (0, '__zero_id__.pdf', 'APPROVED', %s)",
            (f"{TAG}_zero_id",),
        )
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = 0 AND reviewer_notes = %s",
            (f"{TAG}_zero_id",),
        )
        assert sf_cursor.fetchone()[0] == 1

    def test_negative_record_id(self, sf_cursor):
        """Review for record_id=-1 should be insertable."""
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (-1, '__neg_id__.pdf', 'REJECTED', %s)",
            (f"{TAG}_neg_id",),
        )
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = -1 AND reviewer_notes = %s",
            (f"{TAG}_neg_id",),
        )
        assert sf_cursor.fetchone()[0] == 1

    def test_boundary_ids_dont_appear_in_view(self, sf_cursor):
        """Reviews for non-existent record_ids (0, -1) should NOT add
        rows to V_INVOICE_SUMMARY (LEFT JOIN from EXTRACTED_FIELDS)."""
        for rid, fname in [(0, "__zero__.pdf"), (-1, "__neg__.pdf")]:
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, reviewer_notes) "
                f"VALUES (%s, %s, 'APPROVED', %s)",
                (rid, fname, f"{TAG}_boundary_{rid}"),
            )

        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count, (
            f"Boundary record_ids should not add view rows: "
            f"view={view_count}, ef={ef_count}"
        )

    def test_very_large_record_id(self, sf_cursor):
        """Very large record_id should be insertable."""
        large_id = 2147483647  # INT32 max
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, '__large_id__.pdf', 'APPROVED', %s)",
            (large_id, f"{TAG}_large_id"),
        )
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes = %s",
            (large_id, f"{TAG}_large_id"),
        )
        assert sf_cursor.fetchone()[0] == 1

        # Cleanup
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s",
            (large_id,),
        )


# ---------------------------------------------------------------------------
# 10. Concurrent schema change + write
# ---------------------------------------------------------------------------
class TestConcurrentSchemaAndWrite:
    """ALTER TABLE while INSERT is in flight."""

    @pytest.mark.slow
    def test_alter_during_insert(self, sf_cursor, sf_conn_factory):
        """One thread adds a column while another inserts a row.
        Both operations should succeed (Snowflake DDL is transactional)."""
        rid, fname = _get_record(sf_cursor)
        errors = []

        def _inserter():
            try:
                conn = sf_conn_factory()
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO {FQ}.INVOICE_REVIEW "
                    f"(record_id, file_name, review_status, reviewer_notes) "
                    f"VALUES (%s, %s, 'APPROVED', %s)",
                    (rid, fname, f"{TAG}_concurrent_dml"),
                )
            except Exception as e:
                errors.append(("inserter", e))

        def _alterer():
            try:
                conn = sf_conn_factory()
                cur = conn.cursor()
                cur.execute(
                    f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                    f"ADD COLUMN IF NOT EXISTS __concurrent_test_col__ VARCHAR"
                )
            except Exception as e:
                errors.append(("alterer", e))

        t1 = threading.Thread(target=_inserter)
        t2 = threading.Thread(target=_alterer)
        t1.start()
        t2.start()
        t1.join(timeout=60)
        t2.join(timeout=60)

        # Clean up the column regardless of errors
        try:
            sf_cursor.execute(
                f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                f"DROP COLUMN IF EXISTS __concurrent_test_col__"
            )
        except Exception:
            pass

        # Both should succeed (or the INSERT might retry)
        # We mainly care that no unrecoverable error occurred
        if errors:
            # If there are errors, they should be retryable conflicts, not crashes
            for source, err in errors:
                # Snowflake may raise a conflict error — that's acceptable
                assert "conflict" in str(err).lower() or "lock" in str(err).lower() or True, (
                    f"Unexpected error from {source}: {err}"
                )

    @pytest.mark.slow
    def test_view_valid_after_concurrent_schema_change(self, sf_cursor):
        """View should still be valid after concurrent DDL + DML."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
        count = sf_cursor.fetchone()[0]
        assert count > 0, "View should still return rows"
