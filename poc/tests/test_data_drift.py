"""Data drift / schema evolution tests.

Verify that the writeback pipeline and V_INVOICE_SUMMARY view handle:
  1. Boundary values (extreme strings, numbers, dates)
  2. Complete NULL / complete override COALESCE patterns
  3. Schema changes (extra columns added to INVOICE_REVIEW)
  4. View column stability (no accidental redefinition)
  5. Data type precision boundaries

Marked @pytest.mark.slow for the schema-change test (ALTER + DROP).
"""

import pytest

pytestmark = [pytest.mark.sql]

DB = "AI_EXTRACT_POC"
SCHEMA = "DOCUMENTS"
FQ = f"{DB}.{SCHEMA}"
TAG = "__pytest_drift__"


@pytest.fixture(autouse=True)
def _cleanup(sf_cursor):
    """Delete all test rows after each test."""
    yield
    sf_cursor.execute(
        f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes LIKE '{TAG}%'"
    )


def _get_record(cursor):
    cursor.execute(
        f"SELECT record_id, file_name FROM {FQ}.EXTRACTED_FIELDS "
        f"ORDER BY record_id LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        pytest.skip("No EXTRACTED_FIELDS data")
    return row[0], row[1]


# ---------------------------------------------------------------------------
# Boundary values
# ---------------------------------------------------------------------------
class TestBoundaryValues:
    """INSERT reviews with extreme/edge-case values."""

    def test_very_long_string(self, sf_cursor):
        """1000-char vendor name should be accepted."""
        rid, fname = _get_record(sf_cursor)
        long_vendor = "X" * 1000
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', %s, %s)",
            (rid, fname, long_vendor, f"{TAG}_long"),
        )
        sf_cursor.execute(
            f"SELECT corrected_vendor_name FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_long",),
        )
        result = sf_cursor.fetchone()[0]
        assert len(result) == 1000, f"Expected 1000 chars, got {len(result)}"

    def test_special_characters(self, sf_cursor):
        """Unicode, quotes, and special chars in vendor name."""
        rid, fname = _get_record(sf_cursor)
        special = "O'Reilly & Associés — «Ñoño» ™ 日本語"
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_vendor_name, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', %s, %s)",
            (rid, fname, special, f"{TAG}_special"),
        )
        sf_cursor.execute(
            f"SELECT corrected_vendor_name FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_special",),
        )
        assert sf_cursor.fetchone()[0] == special

    def test_max_precision_number(self, sf_cursor):
        """NUMBER(12,2) max: 9999999999.99 should be accepted."""
        rid, fname = _get_record(sf_cursor)
        max_val = 9999999999.99
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_total, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', %s, %s)",
            (rid, fname, max_val, f"{TAG}_maxnum"),
        )
        sf_cursor.execute(
            f"SELECT corrected_total FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_maxnum",),
        )
        assert float(sf_cursor.fetchone()[0]) == pytest.approx(max_val)

    def test_zero_amounts(self, sf_cursor):
        """Zero for all numeric corrected columns."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_subtotal, corrected_tax_amount, corrected_total, "
            f" reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', 0, 0, 0, %s)",
            (rid, fname, f"{TAG}_zero"),
        )
        sf_cursor.execute(
            f"SELECT corrected_subtotal, corrected_tax_amount, corrected_total "
            f"FROM {FQ}.INVOICE_REVIEW WHERE reviewer_notes = %s",
            (f"{TAG}_zero",),
        )
        row = sf_cursor.fetchone()
        assert all(float(v) == 0.0 for v in row), f"Expected all zeros, got {row}"

    def test_negative_amounts(self, sf_cursor):
        """Negative numbers (credit notes) should be accepted."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_total, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', -500.00, %s)",
            (rid, fname, f"{TAG}_neg"),
        )
        sf_cursor.execute(
            f"SELECT corrected_total FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_neg",),
        )
        assert float(sf_cursor.fetchone()[0]) == pytest.approx(-500.00)

    def test_epoch_date(self, sf_cursor):
        """1970-01-01 as invoice_date should be accepted."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_invoice_date, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', '1970-01-01', %s)",
            (rid, fname, f"{TAG}_epoch"),
        )
        sf_cursor.execute(
            f"SELECT corrected_invoice_date FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_epoch",),
        )
        assert str(sf_cursor.fetchone()[0]) == "1970-01-01"

    def test_far_future_date(self, sf_cursor):
        """9999-12-31 as due_date should be accepted."""
        rid, fname = _get_record(sf_cursor)
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_due_date, reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', '9999-12-31', %s)",
            (rid, fname, f"{TAG}_future"),
        )
        sf_cursor.execute(
            f"SELECT corrected_due_date FROM {FQ}.INVOICE_REVIEW "
            f"WHERE reviewer_notes = %s",
            (f"{TAG}_future",),
        )
        assert str(sf_cursor.fetchone()[0]) == "9999-12-31"


# ---------------------------------------------------------------------------
# NULL / COALESCE patterns
# ---------------------------------------------------------------------------
class TestNullCoalescePatterns:
    """Verify COALESCE behavior for full-NULL and full-override scenarios."""

    def test_all_corrections_null_falls_back(self, sf_cursor):
        """Review with all corrected_* NULL — view should show original values."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        # Get original values from EXTRACTED_FIELDS
        sf_cursor.execute(
            f"SELECT field_1, field_10 FROM {FQ}.EXTRACTED_FIELDS "
            f"WHERE record_id = %s",
            (rid,),
        )
        orig = sf_cursor.fetchone()
        orig_vendor, orig_total = orig[0], orig[1]

        # Insert review with NO corrections
        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, reviewer_notes) "
            f"VALUES (%s, %s, 'APPROVED', %s)",
            (rid, fname, f"{TAG}_allnull"),
        )

        sf_cursor.execute(
            f"SELECT vendor_name, total_amount FROM {FQ}.V_INVOICE_SUMMARY "
            f"WHERE record_id = %s",
            (rid,),
        )
        view = sf_cursor.fetchone()
        assert view[0] == orig_vendor, (
            f"Expected original vendor '{orig_vendor}', got '{view[0]}'"
        )
        if orig_total is not None:
            assert float(view[1]) == pytest.approx(float(orig_total))

    def test_all_corrections_populated_overrides(self, sf_cursor):
        """Review with ALL corrected_* populated — view should show corrections."""
        rid, fname = _get_record(sf_cursor)

        sf_cursor.execute(
            f"DELETE FROM {FQ}.INVOICE_REVIEW WHERE record_id = %s", (rid,)
        )

        sf_cursor.execute(
            f"INSERT INTO {FQ}.INVOICE_REVIEW "
            f"(record_id, file_name, review_status, "
            f" corrected_vendor_name, corrected_invoice_number, "
            f" corrected_po_number, corrected_invoice_date, "
            f" corrected_due_date, corrected_payment_terms, "
            f" corrected_recipient, corrected_subtotal, "
            f" corrected_tax_amount, corrected_total, "
            f" reviewer_notes) "
            f"VALUES (%s, %s, 'CORRECTED', "
            f" 'OverrideVendor', 'OVR-001', "
            f" 'PO-OVR', '2099-01-01', "
            f" '2099-06-01', 'Net 99', "
            f" 'Override Recipient', 8888.00, "
            f" 888.00, 9776.00, "
            f" %s)",
            (rid, fname, f"{TAG}_alloverride"),
        )

        sf_cursor.execute(
            f"SELECT vendor_name, invoice_number, po_number, "
            f"       invoice_date, due_date, payment_terms, "
            f"       recipient, subtotal, tax_amount, total_amount "
            f"FROM {FQ}.V_INVOICE_SUMMARY WHERE record_id = %s",
            (rid,),
        )
        v = sf_cursor.fetchone()
        assert v[0] == "OverrideVendor"
        assert v[1] == "OVR-001"
        assert v[2] == "PO-OVR"
        assert str(v[3]) == "2099-01-01"
        assert str(v[4]) == "2099-06-01"
        assert v[5] == "Net 99"
        assert v[6] == "Override Recipient"
        assert float(v[7]) == pytest.approx(8888.00)
        assert float(v[8]) == pytest.approx(888.00)
        assert float(v[9]) == pytest.approx(9776.00)


# ---------------------------------------------------------------------------
# Schema evolution: extra column on INVOICE_REVIEW
# ---------------------------------------------------------------------------
class TestSchemaEvolution:
    """Verify the view survives schema changes to the underlying table."""

    @pytest.mark.slow
    def test_extra_column_does_not_break_view(self, sf_cursor):
        """Adding a column to INVOICE_REVIEW should not break V_INVOICE_SUMMARY.

        The view uses explicit column references (not SELECT *), so extra
        columns in the base table should be invisible to it.
        """
        try:
            # Add a column
            sf_cursor.execute(
                f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                f"ADD COLUMN IF NOT EXISTS __test_extra_col__ VARCHAR"
            )

            # View should still work
            sf_cursor.execute(
                f"SELECT COUNT(*) FROM {FQ}.V_INVOICE_SUMMARY"
            )
            count = sf_cursor.fetchone()[0]
            assert count > 0, "View should still return rows"

            # Extra column should NOT appear in view output
            sf_cursor.execute(f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY LIMIT 0")
            cols = [desc[0] for desc in sf_cursor.description]
            assert "__TEST_EXTRA_COL__" not in cols, (
                "Extra column should not leak into view"
            )
        finally:
            # Clean up: drop the test column
            sf_cursor.execute(
                f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                f"DROP COLUMN IF EXISTS __test_extra_col__"
            )

    @pytest.mark.slow
    def test_view_survives_column_add_and_drop(self, sf_cursor):
        """ALTER ADD then ALTER DROP — view should remain valid."""
        try:
            sf_cursor.execute(
                f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                f"ADD COLUMN IF NOT EXISTS __drift_test__ NUMBER"
            )
            # Insert a row using the new column (just to exercise it)
            rid, fname = _get_record(sf_cursor)
            sf_cursor.execute(
                f"INSERT INTO {FQ}.INVOICE_REVIEW "
                f"(record_id, file_name, review_status, "
                f" __drift_test__, reviewer_notes) "
                f"VALUES (%s, %s, 'APPROVED', 42, %s)",
                (rid, fname, f"{TAG}_drift"),
            )

            # View should still work
            sf_cursor.execute(
                f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY WHERE record_id = %s",
                (rid,),
            )
            assert sf_cursor.fetchone() is not None
        finally:
            sf_cursor.execute(
                f"ALTER TABLE {FQ}.INVOICE_REVIEW "
                f"DROP COLUMN IF EXISTS __drift_test__"
            )


# ---------------------------------------------------------------------------
# View column stability
# ---------------------------------------------------------------------------
class TestViewColumnStability:
    """Guard against accidental view redefinition."""

    EXPECTED_COLUMNS = [
        "RECORD_ID", "FILE_NAME", "DOC_TYPE", "VENDOR_NAME", "INVOICE_NUMBER",
        "PO_NUMBER", "INVOICE_DATE", "DUE_DATE", "PAYMENT_TERMS",
        "RECIPIENT", "SUBTOTAL", "TAX_AMOUNT", "TOTAL_AMOUNT",
        "EXTRACTION_STATUS", "EXTRACTED_AT",
        "LINE_ITEM_COUNT", "COMPUTED_LINE_TOTAL",
        "REVIEW_STATUS", "REVIEWER_NOTES",
        "REVIEWED_BY", "REVIEWED_AT",
        "RAW_EXTRACTION", "CORRECTIONS",
    ]

    def test_column_names(self, sf_cursor):
        """V_INVOICE_SUMMARY should have exactly the expected columns."""
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        assert cols == self.EXPECTED_COLUMNS, (
            f"Column mismatch:\nExpected: {self.EXPECTED_COLUMNS}\nGot:      {cols}"
        )

    def test_column_count(self, sf_cursor):
        """View should have exactly 23 columns."""
        sf_cursor.execute(f"SELECT * FROM {FQ}.V_INVOICE_SUMMARY LIMIT 0")
        assert len(sf_cursor.description) == 23, (
            f"Expected 23 columns, got {len(sf_cursor.description)}"
        )
