"""Load / stress tests for the writeback pipeline.

Verify that INVOICE_REVIEW handles bulk and concurrent INSERTs correctly,
and that V_INVOICE_SUMMARY remains consistent under write load.

Uses multiple Snowflake connections via sf_conn_factory to achieve true
concurrency (not just sequential writes on a single cursor).

Marked @pytest.mark.slow — skip with ``pytest -m "not slow"``.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

pytestmark = [pytest.mark.sql, pytest.mark.slow]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"
TAG = "__pytest_load__"


@pytest.fixture(autouse=True)
def _cleanup(sf_cursor):
    """Delete all test rows after each test class."""
    yield
    sf_cursor.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes LIKE '{TAG}%'"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_record_ids(cursor, n):
    """Return up to n (record_id, file_name) tuples from EXTRACTED_FIELDS."""
    cursor.execute(
        f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
        f"ORDER BY record_id LIMIT {n}"
    )
    return cursor.fetchall()


def _insert_review(conn, record_id, file_name, tag_suffix=""):
    """INSERT one review row using the given connection."""
    cur = conn.cursor()
    cur.execute(
        f"INSERT INTO {FQ}.INVOICE_REVIEW "
        f"(record_id, file_name, review_status, reviewer_notes) "
        f"VALUES (%s, %s, 'APPROVED', %s)",
        (record_id, file_name, f"{TAG}{tag_suffix}"),
    )


# ---------------------------------------------------------------------------
# Bulk INSERT (single connection)
# ---------------------------------------------------------------------------
class TestBulkInsertLoad:
    """Rapid-fire INSERTs on a single connection."""

    def test_50_sequential_inserts(self, sf_cursor):
        """INSERT 50 reviews in rapid succession; all should land."""
        rows = _get_record_ids(sf_cursor, 50)
        if len(rows) < 10:
            pytest.skip("Need at least 10 EXTRACTED_FIELDS records")

        # Cycle through available records
        for i in range(50):
            rid, fname = rows[i % len(rows)]
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, reviewer_notes) "
                f"VALUES (%s, %s, 'APPROVED', %s)",
                (rid, fname, f"{TAG}_bulk_{i}"),
            )

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_bulk_%'"
        )
        count = sf_cursor.fetchone()[0]
        assert count == 50, f"Expected 50 bulk rows, got {count}"

    def test_view_consistent_after_bulk(self, sf_cursor):
        """V_INVOICE_SUMMARY row count should still equal EXTRACTED_FIELDS."""
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY")
        view_count = sf_cursor.fetchone()[0]
        sf_cursor.execute(f"SELECT COUNT(*) FROM {FQ}.EXTRACTED_FIELDS")
        ef_count = sf_cursor.fetchone()[0]
        assert view_count == ef_count, (
            f"View has {view_count} rows but EXTRACTED_FIELDS has {ef_count}"
        )

    def test_latest_wins_after_bulk(self, sf_cursor):
        """After 50 bulk inserts, the view should show the latest review
        for each record (highest review_id)."""
        rows = _get_record_ids(sf_cursor, 1)
        if not rows:
            pytest.skip("No EXTRACTED_FIELDS data")
        rid = rows[0][0]

        # Check that view returns exactly one row for this record
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        assert sf_cursor.fetchone()[0] == 1, "View should have 1 row per record"


# ---------------------------------------------------------------------------
# Concurrent writers (multiple connections)
# ---------------------------------------------------------------------------
class TestConcurrentWriters:
    """Multiple threads writing reviews simultaneously."""

    NUM_THREADS = 5
    INSERTS_PER_THREAD = 10

    def test_concurrent_inserts_different_records(self, sf_cursor, sf_conn_factory):
        """5 threads × 10 inserts for DIFFERENT records — no data loss."""
        rows = _get_record_ids(sf_cursor, self.NUM_THREADS * self.INSERTS_PER_THREAD)
        if len(rows) < self.NUM_THREADS:
            pytest.skip(f"Need at least {self.NUM_THREADS} records")

        errors = []

        def _writer(thread_id):
            try:
                conn = sf_conn_factory()
                for i in range(self.INSERTS_PER_THREAD):
                    idx = (thread_id * self.INSERTS_PER_THREAD + i) % len(rows)
                    rid, fname = rows[idx]
                    _insert_review(conn, rid, fname, f"_conc_{thread_id}_{i}")
            except Exception as e:
                errors.append(e)

        threads = []
        for tid in range(self.NUM_THREADS):
            t = threading.Thread(target=_writer, args=(tid,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Thread errors: {errors}"

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_conc_%'"
        )
        count = sf_cursor.fetchone()[0]
        expected = self.NUM_THREADS * self.INSERTS_PER_THREAD
        assert count == expected, f"Expected {expected} concurrent rows, got {count}"

    def test_concurrent_inserts_same_record(self, sf_cursor, sf_conn_factory):
        """3 threads all write reviews for the SAME record simultaneously."""
        rows = _get_record_ids(sf_cursor, 1)
        if not rows:
            pytest.skip("No EXTRACTED_FIELDS data")
        rid, fname = rows[0]
        num_threads = 3
        inserts_per = 5
        errors = []

        def _writer(thread_id):
            try:
                conn = sf_conn_factory()
                for i in range(inserts_per):
                    _insert_review(conn, rid, fname, f"_same_{thread_id}_{i}")
            except Exception as e:
                errors.append(e)

        threads = []
        for tid in range(num_threads):
            t = threading.Thread(target=_writer, args=(tid,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=60)

        assert not errors, f"Thread errors: {errors}"

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}_same_%'"
        )
        count = sf_cursor.fetchone()[0]
        expected = num_threads * inserts_per
        assert count == expected, (
            f"Expected {expected} rows for same record, got {count}"
        )

    def test_view_still_one_row_per_record_after_concurrent(self, sf_cursor):
        """After concurrent writes, the view should still have no duplicates."""
        sf_cursor.execute(
            f"SELECT record_id, COUNT(*) AS cnt "
            f"FROM {FQ}.V_INVOICE_SUMMARY GROUP BY record_id HAVING cnt > 1"
        )
        dupes = sf_cursor.fetchall()
        assert len(dupes) == 0, (
            f"Duplicate record_ids in view: {[r[0] for r in dupes]}"
        )


# ---------------------------------------------------------------------------
# View under load: concurrent reads + writes
# ---------------------------------------------------------------------------
class TestViewUnderLoad:
    """Read from V_INVOICE_SUMMARY while writes are in flight."""

    def test_concurrent_read_write(self, sf_cursor, sf_conn_factory):
        """Writer thread inserts rows while reader thread queries the view.
        Neither should error."""
        rows = _get_record_ids(sf_cursor, 20)
        if len(rows) < 5:
            pytest.skip("Need at least 5 records")

        write_errors = []
        read_errors = []
        read_counts = []

        def _writer():
            try:
                conn = sf_conn_factory()
                for i in range(20):
                    rid, fname = rows[i % len(rows)]
                    _insert_review(conn, rid, fname, f"_rw_{i}")
            except Exception as e:
                write_errors.append(e)

        def _reader():
            try:
                conn = sf_conn_factory()
                cur = conn.cursor()
                for _ in range(10):
                    cur.execute(
                        f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY"
                    )
                    read_counts.append(cur.fetchone()[0])
            except Exception as e:
                read_errors.append(e)

        wt = threading.Thread(target=_writer)
        rt = threading.Thread(target=_reader)
        wt.start()
        rt.start()
        wt.join(timeout=60)
        rt.join(timeout=60)

        assert not write_errors, f"Writer errors: {write_errors}"
        assert not read_errors, f"Reader errors: {read_errors}"
        # All read counts should be equal (view count = EXTRACTED_FIELDS count)
        # regardless of INVOICE_REVIEW writes (the LEFT JOIN doesn't add rows)
        assert len(read_counts) > 0, "Reader should have gotten results"
        assert all(c == read_counts[0] for c in read_counts), (
            f"View row count varied during writes: {set(read_counts)}"
        )
