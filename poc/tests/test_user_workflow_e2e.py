"""
End-to-end user workflow test — fake user modifies invoices, verifies
propagation through V_DOCUMENT_SUMMARY, then reverses all changes.

Simulates the exact Snowpark calls that 3_Review.py makes:
  1. Read documents from V_DOCUMENT_SUMMARY
  2. INSERT INTO INVOICE_REVIEW ... SELECT ... PARSE_JSON(?)
  3. Verify view reflects corrected values via COALESCE
  4. Verify append-only audit trail in INVOICE_REVIEW
  5. Clean up all test rows and verify originals are restored

Tests are numbered to enforce execution order.  All test review rows
are tagged with a unique reviewer_notes prefix so cleanup is safe.
"""

import json
import os
import time

import pytest


pytestmark = pytest.mark.sql

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")
FQ = f"{POC_DB}.{POC_SCHEMA}"

# Unique tag — every test review row uses this prefix in reviewer_notes
TAG = "__e2e_user_workflow__"
TAG_LIKE = f"{TAG}%"  # LIKE pattern for cleanup queries

# Edge-case tests use a separate tag so cleanup is independent
TAG_EDGE = "__e2e_edge_case__"
TAG_EDGE_LIKE = f"{TAG_EDGE}%"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def sf_cursor():
    import snowflake.connector

    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {POC_ROLE}")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    # Safety-net cleanup: remove any leftover test rows even if tests fail
    cur.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW "
        f"WHERE reviewer_notes LIKE '{TAG}%'"
    )
    cur.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW "
        f"WHERE reviewer_notes LIKE '{TAG_EDGE}%'"
    )
    cur.close()
    conn.close()


@pytest.fixture(scope="module")
def targets(sf_cursor):
    """Pick 3 unreviewed invoices as test targets.

    Returns a list of dicts with original values from V_DOCUMENT_SUMMARY.
    """
    sf_cursor.execute(f"""
        SELECT record_id, file_name, doc_type,
               vendor_name, invoice_number, po_number,
               invoice_date, due_date, payment_terms,
               recipient, subtotal, tax_amount, total_amount,
               review_status
        FROM {FQ}.V_DOCUMENT_SUMMARY
        WHERE review_status IS NULL AND doc_type = 'INVOICE'
        ORDER BY record_id
        LIMIT 3
    """)
    cols = [d[0] for d in sf_cursor.description]
    rows = sf_cursor.fetchall()
    assert len(rows) >= 3, (
        f"Need at least 3 unreviewed invoices, found {len(rows)}. "
        "Run the extraction pipeline first."
    )
    return [dict(zip(cols, r)) for r in rows]


@pytest.fixture(scope="module")
def edge_targets(sf_cursor):
    """Pick 5 unreviewed invoices for edge-case tests (offset past happy-path targets)."""
    sf_cursor.execute(f"""
        SELECT record_id, file_name, doc_type,
               vendor_name, invoice_number, po_number,
               invoice_date, due_date, payment_terms,
               recipient, subtotal, tax_amount, total_amount,
               review_status
        FROM {FQ}.V_DOCUMENT_SUMMARY
        WHERE review_status IS NULL AND doc_type = 'INVOICE'
        ORDER BY record_id
        LIMIT 5 OFFSET 3
    """)
    cols = [d[0] for d in sf_cursor.description]
    rows = sf_cursor.fetchall()
    assert len(rows) >= 5, (
        f"Need at least 5 unreviewed invoices at offset 3, found {len(rows)}. "
        "Run the extraction pipeline first."
    )
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Tests — numbered for execution order
# ---------------------------------------------------------------------------


class TestUserWorkflowE2E:
    """Simulate a real user editing invoices on the Review page."""

    def test_01_baseline_unreviewed(self, sf_cursor, targets):
        """All 3 target invoices start with review_status=NULL."""
        for t in targets:
            assert t["REVIEW_STATUS"] is None, (
                f"Record {t['RECORD_ID']} already reviewed — "
                "test requires unreviewed invoices"
            )
            assert t["VENDOR_NAME"] is not None, (
                f"Record {t['RECORD_ID']} has no vendor_name"
            )
            assert t["TOTAL_AMOUNT"] is not None, (
                f"Record {t['RECORD_ID']} has no total_amount"
            )

    def test_02_approve_no_corrections(self, sf_cursor, targets):
        """User approves invoice #1 without changing any fields.

        View should show REVIEW_STATUS='APPROVED' and all original
        field values unchanged (COALESCE falls through to ef.field_*).
        """
        t = targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'APPROVED', %s, PARSE_JSON(%s)""",
            (rid, fname, f"{TAG}approve_01", json.dumps({})),
        )

        # Verify view
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row is not None, f"Record {rid} not in V_DOCUMENT_SUMMARY"
        assert row[0] == "APPROVED", f"Expected APPROVED, got {row[0]}"
        # Fields should be unchanged — COALESCE falls through
        assert str(row[1]) == str(t["VENDOR_NAME"]), (
            f"Vendor changed unexpectedly: {row[1]} != {t['VENDOR_NAME']}"
        )
        assert float(row[2]) == float(t["TOTAL_AMOUNT"]), (
            f"Total changed unexpectedly: {row[2]} != {t['TOTAL_AMOUNT']}"
        )

    def test_03_correct_vendor_and_total(self, sf_cursor, targets):
        """User corrects invoice #2: changes vendor_name and total_amount.

        View should show corrected values via COALESCE, not originals.
        """
        t = targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        new_vendor = "CORRECTED Vendor Inc"
        new_total = 999.99
        notes = f"{TAG}correct_vendor_total_02"

        corrections = {
            "vendor_name": new_vendor,
            "total_amount": new_total,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_vendor_name, corrected_total,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'CORRECTED', %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, new_vendor, new_total, notes,
             json.dumps(corrections)),
        )

        # Verify view shows corrected values
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "CORRECTED"
        assert row[1] == new_vendor, (
            f"Vendor not corrected: got '{row[1]}', expected '{new_vendor}'"
        )
        assert float(row[2]) == new_total, (
            f"Total not corrected: got {row[2]}, expected {new_total}"
        )
        # Original values should differ
        assert row[1] != t["VENDOR_NAME"], "Vendor should differ from original"

    def test_04_correct_multiple_fields(self, sf_cursor, targets):
        """User corrects invoice #3: invoice_number, due_date, payment_terms, recipient.

        Tests that multiple COALESCE columns propagate independently.
        """
        t = targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        new_inv_num = "CORRECTED-INV-99999"
        new_due = "2099-12-31"
        new_terms = "NET 999"
        new_recipient = "Test Recipient Corp"
        notes = f"{TAG}correct_multi_03"

        corrections = {
            "invoice_number": new_inv_num,
            "due_date": new_due,
            "payment_terms": new_terms,
            "recipient": new_recipient,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_invoice_number, corrected_due_date,
                corrected_payment_terms, corrected_recipient,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'CORRECTED', %s, %s, %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, new_inv_num, new_due, new_terms, new_recipient,
             notes, json.dumps(corrections)),
        )

        # Verify all 4 corrected fields in view
        sf_cursor.execute(
            f"""SELECT review_status, invoice_number, due_date,
                       payment_terms, recipient, vendor_name, total_amount
                FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s""",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "CORRECTED"
        assert row[1] == new_inv_num, f"invoice_number: {row[1]} != {new_inv_num}"
        assert str(row[2]) == new_due, f"due_date: {row[2]} != {new_due}"
        assert row[3] == new_terms, f"payment_terms: {row[3]} != {new_terms}"
        assert row[4] == new_recipient, f"recipient: {row[4]} != {new_recipient}"
        # Uncorrected fields should remain at original values
        assert str(row[5]) == str(t["VENDOR_NAME"]), (
            f"vendor_name should be unchanged: {row[5]} != {t['VENDOR_NAME']}"
        )
        assert float(row[6]) == float(t["TOTAL_AMOUNT"]), (
            f"total_amount should be unchanged: {row[6]} != {t['TOTAL_AMOUNT']}"
        )

    def test_05_second_edit_overwrites_first(self, sf_cursor, targets):
        """User re-edits invoice #2 — changes status to APPROVED and new total.

        The view must show the SECOND edit (latest reviewed_at wins).
        This proves the NOORDER autoincrement fix works with real data.
        """
        t = targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        final_total = 1234.56
        final_vendor = "Final Vendor LLC"
        notes = f"{TAG}re_edit_04"

        # Brief pause so reviewed_at is strictly later than test_03's insert
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")

        corrections = {
            "vendor_name": final_vendor,
            "total_amount": final_total,
        }

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                corrected_vendor_name, corrected_total,
                reviewer_notes, corrections
            ) SELECT %s, %s, 'APPROVED', %s, %s, %s, PARSE_JSON(%s)""",
            (rid, fname, final_vendor, final_total, notes,
             json.dumps(corrections)),
        )

        # View must show the SECOND edit, not the first
        sf_cursor.execute(
            f"SELECT review_status, vendor_name, total_amount "
            f"FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        row = sf_cursor.fetchone()
        assert row[0] == "APPROVED", (
            f"Status should be APPROVED (2nd edit), got {row[0]}"
        )
        assert row[1] == final_vendor, (
            f"Vendor should be '{final_vendor}' (2nd edit), got '{row[1]}'"
        )
        assert float(row[2]) == final_total, (
            f"Total should be {final_total} (2nd edit), got {row[2]}"
        )

    def test_06_other_records_untouched(self, sf_cursor, targets):
        """Records outside the 3 targets must not be affected."""
        target_ids = [t["RECORD_ID"] for t in targets]
        placeholders = ",".join(["%s"] * len(target_ids))

        sf_cursor.execute(
            f"""SELECT COUNT(*) FROM {FQ}.V_DOCUMENT_SUMMARY
                WHERE record_id NOT IN ({placeholders})
                  AND review_status IS NOT NULL
                  AND reviewed_at > DATEADD('minute', -5, CURRENT_TIMESTAMP())
            """,
            target_ids,
        )
        count = sf_cursor.fetchone()[0]
        # There might be pre-existing reviewed records — we just check that
        # no NEW reviews appeared in the last 5 minutes for non-target records
        # by verifying reviewer_notes doesn't contain our tag
        sf_cursor.execute(
            f"""SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW
                WHERE record_id NOT IN ({placeholders})
                  AND reviewer_notes LIKE %s
            """,
            target_ids + [TAG_LIKE],
        )
        leaked = sf_cursor.fetchone()[0]
        assert leaked == 0, (
            f"{leaked} test review rows leaked to non-target records"
        )

    def test_07_audit_trail_complete(self, sf_cursor, targets):
        """INVOICE_REVIEW has the full append-only audit trail.

        Invoice #1: 1 row (approved)
        Invoice #2: 2 rows (corrected, then re-approved)
        Invoice #3: 1 row (corrected)
        Total: 4 test rows
        """
        # Total test rows
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        total = sf_cursor.fetchone()[0]
        assert total == 4, f"Expected 4 audit rows, got {total}"

        # Per-record breakdown
        expected = {
            targets[0]["RECORD_ID"]: 1,  # approved once
            targets[1]["RECORD_ID"]: 2,  # corrected then re-approved
            targets[2]["RECORD_ID"]: 1,  # corrected once
        }
        for rid, expected_count in expected.items():
            sf_cursor.execute(
                f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
                f"WHERE record_id = %s AND reviewer_notes LIKE %s",
                (rid, TAG_LIKE),
            )
            actual = sf_cursor.fetchone()[0]
            assert actual == expected_count, (
                f"Record {rid}: expected {expected_count} audit rows, got {actual}"
            )

        # Verify chronological ordering: for invoice #2, the re-edit is later
        rid2 = targets[1]["RECORD_ID"]
        sf_cursor.execute(
            f"""SELECT reviewer_notes, review_status, reviewed_at
                FROM {FQ}.INVOICE_REVIEW
                WHERE record_id = %s AND reviewer_notes LIKE %s
                ORDER BY reviewed_at ASC""",
            (rid2, TAG_LIKE),
        )
        rows = sf_cursor.fetchall()
        assert rows[0][1] == "CORRECTED", "First edit should be CORRECTED"
        assert rows[1][1] == "APPROVED", "Second edit should be APPROVED"
        assert rows[1][2] > rows[0][2], (
            "Second edit reviewed_at must be later than first"
        )

    def test_08_rollback_and_verify_originals_restored(self, sf_cursor, targets):
        """Delete all test rows and verify originals are fully restored.

        After cleanup:
        - All 3 records should return to review_status=NULL
        - All original field values should be back (no corrections in view)
        - 0 leftover test rows in INVOICE_REVIEW
        """
        # --- Rollback ---
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        deleted = sf_cursor.fetchone()[0]
        assert deleted == 4, f"Expected to delete 4 rows, deleted {deleted}"

        # --- Verify 0 leftover ---
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG}%'"
        )
        remaining = sf_cursor.fetchone()[0]
        assert remaining == 0, f"{remaining} test rows still in INVOICE_REVIEW"

        # --- Verify all 3 records restored to original state ---
        for t in targets:
            rid = t["RECORD_ID"]
            sf_cursor.execute(
                f"""SELECT review_status, vendor_name, invoice_number,
                           total_amount, due_date, payment_terms, recipient
                    FROM {FQ}.V_DOCUMENT_SUMMARY
                    WHERE record_id = %s""",
                (rid,),
            )
            row = sf_cursor.fetchone()
            assert row is not None, f"Record {rid} missing from view after cleanup"

            # review_status should be NULL (back to unreviewed)
            assert row[0] is None, (
                f"Record {rid}: review_status should be NULL after rollback, "
                f"got '{row[0]}'"
            )

            # All original field values restored
            assert str(row[1]) == str(t["VENDOR_NAME"]), (
                f"Record {rid}: vendor_name not restored: "
                f"'{row[1]}' != '{t['VENDOR_NAME']}'"
            )
            assert str(row[2]) == str(t["INVOICE_NUMBER"]), (
                f"Record {rid}: invoice_number not restored: "
                f"'{row[2]}' != '{t['INVOICE_NUMBER']}'"
            )
            assert float(row[3]) == float(t["TOTAL_AMOUNT"]), (
                f"Record {rid}: total_amount not restored: "
                f"{row[3]} != {t['TOTAL_AMOUNT']}"
            )


# ---------------------------------------------------------------------------
# Edge-case & boundary tests — try to break the app
# ---------------------------------------------------------------------------


class TestUserWorkflowEdgeCases:
    """Edge cases, boundary values, special characters, and adversarial inputs.

    Uses edge_targets (5 invoices at OFFSET 3) so they don't collide with
    the happy-path tests above.  All rows tagged with TAG_EDGE for cleanup.
    """

    # ── helpers ────────────────────────────────────────────────────────────

    def _insert_review(self, sf_cursor, rid, fname, status, notes,
                       corrections, **legacy_cols):
        """Insert a review row matching the exact SQL path the Streamlit app uses."""
        # Build legacy column names and placeholders dynamically
        leg_names = []
        leg_placeholders = []
        leg_vals = []
        for col, val in legacy_cols.items():
            leg_names.append(col)
            leg_placeholders.append("%s")
            leg_vals.append(val)

        extra_cols = (", " + ", ".join(leg_names)) if leg_names else ""
        extra_phs = (", " + ", ".join(leg_placeholders)) if leg_placeholders else ""

        sf_cursor.execute(
            f"""INSERT INTO {FQ}.INVOICE_REVIEW (
                record_id, file_name, review_status,
                reviewer_notes, corrections{extra_cols}
            ) SELECT %s, %s, %s, %s, PARSE_JSON(%s){extra_phs}""",
            [rid, fname, status, notes, json.dumps(corrections)] + leg_vals,
        )

    def _view_row(self, sf_cursor, rid):
        """Fetch a single record from V_DOCUMENT_SUMMARY."""
        sf_cursor.execute(
            f"""SELECT review_status, vendor_name, invoice_number,
                       total_amount, invoice_date, due_date,
                       payment_terms, recipient, subtotal,
                       tax_amount, reviewer_notes, corrections
                FROM {FQ}.V_DOCUMENT_SUMMARY WHERE record_id = %s""",
            (rid,),
        )
        return sf_cursor.fetchone()

    # ── 1. Empty / Null value edge cases ──────────────────────────────────

    def test_10_empty_string_correction(self, sf_cursor, edge_targets):
        """Empty string in corrections VARIANT overrides original (not NULL)."""
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        original_vendor = t["VENDOR_NAME"]

        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}empty_str_10",
            {"vendor_name": ""},
        )
        row = self._view_row(sf_cursor, rid)
        # COALESCE(''::VARCHAR, ...) = '' — empty string is NOT null
        assert row[0] == "CORRECTED"
        assert row[1] == "", (
            f"Empty string correction should override original '{original_vendor}', "
            f"got '{row[1]}'"
        )

    def test_11_null_correction_falls_through(self, sf_cursor, edge_targets):
        """null in corrections VARIANT falls through to original via COALESCE."""
        t = edge_targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            f"{TAG_EDGE}null_val_11",
            {"vendor_name": None, "total_amount": None},
        )
        row = self._view_row(sf_cursor, rid)
        assert row[0] == "APPROVED"
        # null in JSON → COALESCE skips to original
        assert str(row[1]) == str(t["VENDOR_NAME"]), (
            f"Null correction should fall through: got '{row[1]}', "
            f"expected '{t['VENDOR_NAME']}'"
        )
        assert float(row[3]) == float(t["TOTAL_AMOUNT"]), (
            f"Null correction should fall through: got {row[3]}, "
            f"expected {t['TOTAL_AMOUNT']}"
        )

    def test_12_all_fields_null_corrections(self, sf_cursor, edge_targets):
        """Corrections VARIANT with all nulls — every field falls through."""
        t = edge_targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        all_null = {
            "vendor_name": None, "invoice_number": None,
            "po_number": None, "invoice_date": None,
            "due_date": None, "payment_terms": None,
            "recipient": None, "subtotal": None,
            "tax_amount": None, "total_amount": None,
        }
        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            f"{TAG_EDGE}all_null_12", all_null,
        )
        row = self._view_row(sf_cursor, rid)
        assert row[0] == "APPROVED"
        assert str(row[1]) == str(t["VENDOR_NAME"])
        assert str(row[2]) == str(t["INVOICE_NUMBER"])
        assert float(row[3]) == float(t["TOTAL_AMOUNT"])

    def test_13_empty_corrections_json(self, sf_cursor, edge_targets):
        """Empty corrections object {} — no fields overridden."""
        t = edge_targets[3]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            f"{TAG_EDGE}empty_json_13", {},
        )
        row = self._view_row(sf_cursor, rid)
        assert row[0] == "APPROVED"
        assert str(row[1]) == str(t["VENDOR_NAME"])
        assert float(row[3]) == float(t["TOTAL_AMOUNT"])

    # ── 2. Special characters & injection ─────────────────────────────────

    def test_14_sql_injection_in_vendor_name(self, sf_cursor, edge_targets):
        """SQL injection attempt stored literally, doesn't break anything."""
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        evil_vendor = "'; DROP TABLE INVOICE_REVIEW; --"

        # Need SYSTEM$WAIT so reviewed_at is later than test_10's row
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}sqli_14",
            {"vendor_name": evil_vendor},
        )
        # Table still exists
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW WHERE 1=1"
        )
        assert sf_cursor.fetchone()[0] > 0, "Table destroyed by injection!"

        row = self._view_row(sf_cursor, rid)
        assert row[1] == evil_vendor, (
            f"Injection string not stored literally: got '{row[1]}'"
        )

    def test_15_html_xss_in_notes(self, sf_cursor, edge_targets):
        """XSS attempt in reviewer_notes stored literally."""
        t = edge_targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        xss_notes = f"{TAG_EDGE}xss_15 <script>alert('xss')</script>"

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            xss_notes, {},
        )
        row = self._view_row(sf_cursor, rid)
        assert "<script>" in row[10], (
            f"XSS string not stored literally in notes: '{row[10]}'"
        )

    def test_16_unicode_vendor_name(self, sf_cursor, edge_targets):
        """Full Unicode roundtrip: CJK, emoji, accented characters."""
        t = edge_targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        unicode_vendor = "日本語テスト 🎉 Ñoño Ü"

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}unicode_16",
            {"vendor_name": unicode_vendor},
        )
        row = self._view_row(sf_cursor, rid)
        assert row[1] == unicode_vendor, (
            f"Unicode roundtrip failed: got '{row[1]}'"
        )

    def test_17_apostrophes_and_ampersands(self, sf_cursor, edge_targets):
        """Apostrophes and special chars in corrections — parameterized queries handle them."""
        t = edge_targets[3]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        tricky_vendor = "O'Malley & Sons <Corp>"

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}apostrophe_17",
            {"vendor_name": tricky_vendor},
        )
        row = self._view_row(sf_cursor, rid)
        assert row[1] == tricky_vendor, (
            f"Apostrophe/ampersand roundtrip failed: got '{row[1]}'"
        )

    def test_18_very_long_string(self, sf_cursor, edge_targets):
        """10,000-char vendor name — VARCHAR(16777216) should accept it."""
        t = edge_targets[4]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        long_vendor = "A" * 10_000

        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}long_str_18",
            {"vendor_name": long_vendor},
        )
        row = self._view_row(sf_cursor, rid)
        assert len(row[1]) == 10_000, (
            f"Long string truncated: got {len(row[1])} chars, expected 10000"
        )

    def test_19_newlines_and_tabs_in_notes(self, sf_cursor, edge_targets):
        """Newlines, tabs, carriage returns in reviewer_notes stored literally."""
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]
        messy_notes = f"{TAG_EDGE}whitespace_19\nline2\ttab\rcarriage"

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            messy_notes, {},
        )
        # Fetch the actual notes from INVOICE_REVIEW (not the view, which
        # shows only the latest row's notes)
        sf_cursor.execute(
            f"SELECT reviewer_notes FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s "
            f"ORDER BY reviewed_at DESC LIMIT 1",
            (rid, f"{TAG_EDGE}whitespace_19%"),
        )
        stored = sf_cursor.fetchone()[0]
        assert "\n" in stored, "Newline not preserved"
        assert "\t" in stored, "Tab not preserved"

    # ── 3. Numeric boundary cases ─────────────────────────────────────────

    def test_20_zero_total_amount(self, sf_cursor, edge_targets):
        """Zero correction — COALESCE returns 0, not the original."""
        t = edge_targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}zero_20",
            {"total_amount": 0},
        )
        row = self._view_row(sf_cursor, rid)
        assert float(row[3]) == 0.0, (
            f"Zero correction should override, got {row[3]}"
        )

    def test_21_negative_total_amount(self, sf_cursor, edge_targets):
        """Negative total — valid NUMBER(12,2), stored and displayed."""
        t = edge_targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}negative_21",
            {"total_amount": -500.50},
        )
        row = self._view_row(sf_cursor, rid)
        assert float(row[3]) == -500.50, (
            f"Negative total not stored: got {row[3]}"
        )

    def test_22_max_number_12_2(self, sf_cursor, edge_targets):
        """Maximum NUMBER(12,2) value: 9999999999.99."""
        t = edge_targets[3]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}max_num_22",
            {"total_amount": 9999999999.99},
        )
        row = self._view_row(sf_cursor, rid)
        assert float(row[3]) == 9999999999.99, (
            f"Max NUMBER(12,2) not stored: got {row[3]}"
        )

    def test_23_overflow_number_12_2_breaks_view(self, sf_cursor, edge_targets):
        """Overflow value in VARIANT — INSERT succeeds but VIEW cast fails.

        The VARIANT column stores any JSON value.  The view casts to
        NUMBER(12,2) which overflows.  This proves the failure is graceful
        (query error, not data corruption).
        """
        t = edge_targets[4]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}overflow_23",
            {"total_amount": 99999999999.99},  # 11 digits before decimal
        )

        # The INSERT succeeds — VARIANT stores anything
        sf_cursor.execute(
            f"SELECT corrections FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s "
            f"ORDER BY reviewed_at DESC LIMIT 1",
            (rid, f"{TAG_EDGE}overflow_23%"),
        )
        stored = sf_cursor.fetchone()
        assert stored is not None, "INSERT should have succeeded"

        # The VIEW query should fail with overflow error
        with pytest.raises(Exception, match="(?i)out of.*range|overflow|numeric"):
            self._view_row(sf_cursor, rid)

        # Clean up the overflow row so it doesn't break subsequent view queries
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s",
            (rid, f"{TAG_EDGE}overflow_23%"),
        )

    def test_24_decimal_precision_rounding(self, sf_cursor, edge_targets):
        """NUMBER(12,2) rounds to 2 decimal places."""
        t = edge_targets[4]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        # After overflow cleanup, insert a valid row
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}precision_24",
            {"total_amount": 123.456789},
        )
        row = self._view_row(sf_cursor, rid)
        # Snowflake rounds NUMBER(12,2) — 123.456789 → 123.46
        assert float(row[3]) == 123.46, (
            f"Expected 123.46 (rounded), got {row[3]}"
        )

    # ── 4. Date edge cases ────────────────────────────────────────────────

    def test_25_far_future_date(self, sf_cursor, edge_targets):
        """due_date = 9999-12-31 — valid DATE, roundtrip OK."""
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}future_date_25",
            {"due_date": "9999-12-31"},
        )
        row = self._view_row(sf_cursor, rid)
        assert str(row[5]) == "9999-12-31", (
            f"Far future date roundtrip failed: got '{row[5]}'"
        )

    def test_26_epoch_date(self, sf_cursor, edge_targets):
        """invoice_date = 1970-01-01 — Unix epoch, valid DATE."""
        t = edge_targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}epoch_date_26",
            {"invoice_date": "1970-01-01"},
        )
        row = self._view_row(sf_cursor, rid)
        assert str(row[4]) == "1970-01-01", (
            f"Epoch date roundtrip failed: got '{row[4]}'"
        )

    def test_27_invalid_date_in_variant_breaks_view(self, sf_cursor, edge_targets):
        """Invalid date string in VARIANT — INSERT succeeds, VIEW cast fails."""
        t = edge_targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}bad_date_27",
            {"invoice_date": "not-a-date"},
        )

        # VIEW should fail on ::DATE cast
        with pytest.raises(Exception, match="(?i)failed to cast|date"):
            self._view_row(sf_cursor, rid)

        # Clean up so view works for subsequent tests
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s",
            (rid, f"{TAG_EDGE}bad_date_27%"),
        )

    # ── 5. Rapid-fire / duplicate submissions ─────────────────────────────

    def test_28_rapid_double_submit(self, sf_cursor, edge_targets):
        """Two INSERTs in rapid succession — both stored, latest wins in view."""
        t = edge_targets[3]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}rapid_a_28",
            {"vendor_name": "Rapid Edit A"},
        )
        # No wait — submit immediately again
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}rapid_b_28",
            {"vendor_name": "Rapid Edit B"},
        )

        # Both rows in audit trail
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s",
            (rid, f"{TAG_EDGE}rapid_%_28%"),
        )
        assert sf_cursor.fetchone()[0] == 2, "Both rapid submissions should be stored"

        # View shows the latest (by reviewed_at)
        row = self._view_row(sf_cursor, rid)
        # Could be A or B depending on timing — just verify it's one of them
        assert row[1] in ("Rapid Edit A", "Rapid Edit B"), (
            f"View should show one of the rapid edits, got '{row[1]}'"
        )

    def test_29_identical_correction_values(self, sf_cursor, edge_targets):
        """Same correction values submitted twice — both create audit rows."""
        t = edge_targets[4]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        for i in range(2):
            self._insert_review(
                sf_cursor, rid, fname, "CORRECTED",
                f"{TAG_EDGE}identical_{i}_29",
                {"vendor_name": "Same Value"},
            )

        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE record_id = %s AND reviewer_notes LIKE %s",
            (rid, f"{TAG_EDGE}identical_%_29%"),
        )
        assert sf_cursor.fetchone()[0] == 2, (
            "Identical submissions should both be stored in audit trail"
        )

    # ── 6. Status edge cases ──────────────────────────────────────────────

    def test_30_reject_then_correct(self, sf_cursor, edge_targets):
        """REJECT then CORRECT — latest status wins in view."""
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "REJECTED",
            f"{TAG_EDGE}reject_30", {},
        )
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}correct_30",
            {"vendor_name": "After Rejection"},
        )

        row = self._view_row(sf_cursor, rid)
        assert row[0] == "CORRECTED", (
            f"After REJECT→CORRECT, status should be CORRECTED, got '{row[0]}'"
        )
        assert row[1] == "After Rejection"

    def test_31_approve_then_reject(self, sf_cursor, edge_targets):
        """APPROVE then REJECT — latest status wins."""
        t = edge_targets[1]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "APPROVED",
            f"{TAG_EDGE}approve_31", {},
        )
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "REJECTED",
            f"{TAG_EDGE}reject_31", {},
        )

        row = self._view_row(sf_cursor, rid)
        assert row[0] == "REJECTED", (
            f"After APPROVE→REJECT, status should be REJECTED, got '{row[0]}'"
        )

    def test_32_correct_back_to_original_values(self, sf_cursor, edge_targets):
        """CORRECT with new values, then CORRECT again with originals.

        Status stays CORRECTED but field values match the original extraction.
        """
        t = edge_targets[2]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}change_32",
            {"vendor_name": "Temporary Change"},
        )
        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        # Correct back to original value
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}revert_32",
            {"vendor_name": t["VENDOR_NAME"]},
        )

        row = self._view_row(sf_cursor, rid)
        assert row[0] == "CORRECTED", "Status should still be CORRECTED"
        assert str(row[1]) == str(t["VENDOR_NAME"]), (
            f"Vendor should match original after revert: "
            f"got '{row[1]}', expected '{t['VENDOR_NAME']}'"
        )

    # ── 7. Cross-field consistency ────────────────────────────────────────

    def test_33_variant_only_no_legacy_columns(self, sf_cursor, edge_targets):
        """Corrections VARIANT populated, legacy corrected_* columns NULL.

        COALESCE picks up VARIANT values (first in chain).
        """
        t = edge_targets[3]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        # _insert_review doesn't set legacy columns by default
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}variant_only_33",
            {"vendor_name": "From Variant Only", "total_amount": 777.77},
        )

        row = self._view_row(sf_cursor, rid)
        assert row[1] == "From Variant Only"
        assert float(row[3]) == 777.77

    def test_34_legacy_columns_only_no_variant(self, sf_cursor, edge_targets):
        """Legacy corrected_* columns set, corrections VARIANT is {}.

        COALESCE falls through VARIANT (empty = no matching key = NULL)
        and picks up legacy column.
        """
        t = edge_targets[4]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}legacy_only_34",
            {},  # empty corrections VARIANT
            corrected_vendor_name="From Legacy Column",
            corrected_total=888.88,
        )

        row = self._view_row(sf_cursor, rid)
        assert row[1] == "From Legacy Column", (
            f"Legacy column should be picked by COALESCE: got '{row[1]}'"
        )
        assert float(row[3]) == 888.88

    def test_35_variant_and_legacy_disagree(self, sf_cursor, edge_targets):
        """VARIANT and legacy columns set to different values — VARIANT wins.

        COALESCE order: corrections:field > corrected_field > ef.field_N
        """
        t = edge_targets[0]
        rid, fname = t["RECORD_ID"], t["FILE_NAME"]

        sf_cursor.execute("SELECT SYSTEM$WAIT(1)")
        self._insert_review(
            sf_cursor, rid, fname, "CORRECTED",
            f"{TAG_EDGE}disagree_35",
            {"vendor_name": "VARIANT Wins"},
            corrected_vendor_name="Legacy Loses",
        )

        row = self._view_row(sf_cursor, rid)
        assert row[1] == "VARIANT Wins", (
            f"VARIANT should take priority over legacy: got '{row[1]}'"
        )

    # ── 8. Cleanup ────────────────────────────────────────────────────────

    def test_36_cleanup_edge_cases(self, sf_cursor, edge_targets):
        """Delete all edge-case test rows and verify originals restored."""
        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG_EDGE}%'"
        )
        deleted = sf_cursor.fetchone()[0]
        assert deleted > 0, "Should have deleted at least one edge-case row"

        # Verify 0 leftover
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes LIKE '{TAG_EDGE}%'"
        )
        assert sf_cursor.fetchone()[0] == 0, "Leftover edge-case rows"

        # All 5 edge targets should be back to unreviewed
        for t in edge_targets:
            rid = t["RECORD_ID"]
            row = self._view_row(sf_cursor, rid)
            assert row is not None, f"Record {rid} missing after cleanup"
            assert row[0] is None, (
                f"Record {rid}: review_status should be NULL, got '{row[0]}'"
            )
            assert str(row[1]) == str(t["VENDOR_NAME"]), (
                f"Record {rid}: vendor_name not restored"
            )
