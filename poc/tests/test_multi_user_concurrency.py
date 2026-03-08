"""Multi-user concurrency tests.

Simulate two or more "users" (separate Snowflake connections) interleaving
review writes for the same and different records. Verify the append-only
audit trail and ROW_NUMBER() latest-wins logic remain correct.

Marked @pytest.mark.slow.
"""

import threading

import pytest

pytestmark = [pytest.mark.sql, pytest.mark.slow]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"
TAG = "__pytest_multiuser__"


@pytest.fixture(autouse=True)
def _cleanup(sf_cursor):
    """Delete all test rows after each test."""
    yield
    sf_cursor.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes LIKE '{TAG}%'"
    )


def _get_record(cursor, offset=0):
    """Return (record_id, file_name) from EXTRACTED_FIELDS."""
    cursor.execute(
        f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
        f"ORDER BY record_id LIMIT 1 OFFSET {offset}"
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("No EXTRACTED_FIELDS data")
    return row[0], row[1]


# ---------------------------------------------------------------------------
# Interleaved reviews: two users reviewing the same record
# ---------------------------------------------------------------------------
class TestInterleavedReviews:
    """Two users submit reviews for the same record in sequence."""

    def test_highest_review_id_wins(self, sf_cursor, sf_conn_factory):
        """User A reviews, then User B reviews same record.
        The view should show whichever got the highest review_id.

        Snowflake AUTOINCREMENT across separate connections does not
        guarantee wall-clock ordering, so we check which review_id is
        actually highest rather than assuming insertion order.
        """
        rid, fname = _get_record(sf_cursor)

        # Clean slate
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # User A: REJECTED
        conn_a = sf_conn_factory()
        conn_a.cursor().execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'REJECTED', 'Vendor_A', %s)",
            (rid, fname, f"{TAG}_a"),
        )

        # User B: APPROVED
        conn_b = sf_conn_factory()
        conn_b.cursor().execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', 'Vendor_B', %s)",
            (rid, fname, f"{TAG}_b"),
        )

        # Determine which review_id is actually highest
        sf_cursor.execute(
            f"SELECT review_status, corrected_vendor_name "
            f"FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes IN (%s, %s) "
            f"ORDER BY review_id DESC LIMIT 1",
            (f"{TAG}_a", f"{TAG}_b"),
        )
        expected = sf_cursor.fetchone()
        expected_status, expected_vendor = expected[0], expected[1]

        # View should match the highest review_id
        sf_cursor.execute(
            f"SELECT review_status, vendor_name FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        result = sf_cursor.fetchone()
        assert result[0] == expected_status, (
            f"Expected {expected_status} (highest review_id), got {result[0]}"
        )
        assert result[1] == expected_vendor, (
            f"Expected {expected_vendor}, got {result[1]}"
        )

    def test_both_reviews_preserved(self, sf_cursor, sf_conn_factory):
        """Both users' reviews should exist in INVOICE_REVIEW (append-only)."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        conn_a = sf_conn_factory()
        conn_a.cursor().execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'REJECTED', %s)",
            (rid, fname, f"{TAG}_preserve_a"),
        )

        conn_b = sf_conn_factory()
        conn_b.cursor().execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_preserve_b"),
        )

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_preserve_%'"
        )
        assert sf_cursor.fetchone()[0] == 2, "Both reviews should be preserved"


# ---------------------------------------------------------------------------
# Simultaneous writes to different records
# ---------------------------------------------------------------------------
class TestSimultaneousWritesDifferentRecords:
    """Two users review different records at the same time."""

    def test_no_cross_contamination(self, sf_cursor, sf_conn_factory):
        """User A reviews record X, User B reviews record Y simultaneously.
        Neither should see the other's corrections."""
        rid_a, fname_a = _get_record(sf_cursor, offset=0)
        rid_b, fname_b = _get_record(sf_cursor, offset=1)

        # Clean slate
        for rid in (rid_a, rid_b):
            sf_cursor.execute(
                f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
            )

        errors = []
        barrier = threading.Barrier(2, timeout=30)

        def _user_a():
            try:
                conn = sf_conn_factory()
                barrier.wait()
                conn.cursor().execute(
                    f"INSERT INTO {FQ}.INVOICE_REVIEW "
                    f"(record_id, file_name, review_status, "
                    f" corrected_vendor_name, reviewer_notes) "
                    f"VALUES (%s, %s, 'APPROVED', 'VendorA_Only', %s)",
                    (rid_a, fname_a, f"{TAG}_diffA"),
                )
            except Exception as e:
                errors.append(e)

        def _user_b():
            try:
                conn = sf_conn_factory()
                barrier.wait()
                conn.cursor().execute(
                    f"INSERT INTO {FQ}.INVOICE_REVIEW "
                    f"(record_id, file_name, review_status, "
                    f" corrected_vendor_name, reviewer_notes) "
                    f"VALUES (%s, %s, 'REJECTED', 'VendorB_Only', %s)",
                    (rid_b, fname_b, f"{TAG}_diffB"),
                )
            except Exception as e:
                errors.append(e)

        ta = threading.Thread(target=_user_a)
        tb = threading.Thread(target=_user_b)
        ta.start()
        tb.start()
        ta.join(timeout=60)
        tb.join(timeout=60)
        assert not errors, f"Errors: {errors}"

        # Verify record A shows VendorA_Only
        sf_cursor.execute(
            f"SELECT vendor_name FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid_a,),
        )
        assert sf_cursor.fetchone()[0] == "VendorA_Only"

        # Verify record B shows VendorB_Only
        sf_cursor.execute(
            f"SELECT vendor_name FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid_b,),
        )
        assert sf_cursor.fetchone()[0] == "VendorB_Only"


# ---------------------------------------------------------------------------
# Rapid overwrites: one user revises the same record many times
# ---------------------------------------------------------------------------
class TestRapidOverwrite:
    """Single user rapidly submits multiple reviews for one record."""

    def test_5_sequential_reviews_latest_wins(self, sf_cursor):
        """5 sequential reviews — view should show whichever has the
        highest review_id (autoincrement may not match insertion order
        across sessions, but within a single cursor it should)."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        statuses = ["REJECTED", "CORRECTED", "APPROVED", "REJECTED", "APPROVED"]
        for i, status in enumerate(statuses):
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, "
                f" corrected_vendor_name, reviewer_notes) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (rid, fname, status, f"Vendor_Rev{i}", f"{TAG}_rapid_{i}"),
            )

        # Find the actual latest (highest review_id) among our test rows
        sf_cursor.execute(
            f"SELECT review_status, corrected_vendor_name "
            f"FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_rapid_%' "
            f"ORDER BY review_id DESC LIMIT 1"
        )
        latest = sf_cursor.fetchone()
        expected_status, expected_vendor = latest[0], latest[1]

        # View should match
        sf_cursor.execute(
            f"SELECT review_status, vendor_name FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        result = sf_cursor.fetchone()
        assert result[0] == expected_status, (
            f"Expected {expected_status} (highest review_id), got {result[0]}"
        )
        assert result[1] == expected_vendor, (
            f"Expected {expected_vendor}, got {result[1]}"
        )

    def test_all_revisions_in_audit_trail(self, sf_cursor):
        """All 5 revisions should still exist in INVOICE_REVIEW."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        for i in range(5):
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, reviewer_notes) "
                f"VALUES (%s, %s, 'APPROVED', %s)",
                (rid, fname, f"{TAG}_audit_{i}"),
            )

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_audit_%'"
        )
        assert sf_cursor.fetchone()[0] == 5, "All 5 audit rows should exist"


# ---------------------------------------------------------------------------
# Concurrent race: two users write at the exact same moment
# ---------------------------------------------------------------------------
class TestConcurrentRace:
    """Two users race to review the same record using a barrier."""

    def test_both_inserts_succeed(self, sf_cursor, sf_conn_factory):
        """Both concurrent INSERTs should succeed (no deadlock/conflict)."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        errors = []
        barrier = threading.Barrier(2, timeout=30)

        def _racer(conn, status, tag_suffix):
            try:
                barrier.wait()
                conn.cursor().execute(
                    f"INSERT INTO {FQ}.INVOICE_REVIEW "
                    f"(record_id, file_name, review_status, reviewer_notes) "
                    f"VALUES (%s, %s, %s, %s)",
                    (rid, fname, status, f"{TAG}_race_{tag_suffix}"),
                )
            except Exception as e:
                errors.append(e)

        conn_a = sf_conn_factory()
        conn_b = sf_conn_factory()

        ta = threading.Thread(target=_racer, args=(conn_a, "REJECTED", "a"))
        tb = threading.Thread(target=_racer, args=(conn_b, "APPROVED", "b"))
        ta.start()
        tb.start()
        ta.join(timeout=60)
        tb.join(timeout=60)

        assert not errors, f"Race errors: {errors}"

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_race_%'"
        )
        assert sf_cursor.fetchone()[0] == 2, "Both race inserts should succeed"

    def test_view_shows_one_row_after_race(self, sf_cursor, sf_conn_factory):
        """After a race, the view should still show exactly one row."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        barrier = threading.Barrier(2, timeout=30)

        def _racer(conn, status, tag):
            barrier.wait()
            conn.cursor().execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, reviewer_notes) "
                f"VALUES (%s, %s, %s, %s)",
                (rid, fname, status, f"{TAG}_raceview_{tag}"),
            )

        conn_a = sf_conn_factory()
        conn_b = sf_conn_factory()
        ta = threading.Thread(target=_racer, args=(conn_a, "REJECTED", "a"))
        tb = threading.Thread(target=_racer, args=(conn_b, "APPROVED", "b"))
        ta.start()
        tb.start()
        ta.join(timeout=60)
        tb.join(timeout=60)

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert sf_cursor.fetchone()[0] == 1, "View should have exactly 1 row per record"
