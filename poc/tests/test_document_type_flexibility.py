"""Document Type Flexibility Tests — verify config-driven extraction architecture.

Tests cover:
  1. DOCUMENT_TYPE_CONFIG schema & seed data integrity
  2. VARIANT column existence and types on all tables
  3. Dynamic field support (>10 fields via raw_extraction)
  4. Config helper functions (get_doc_type_config, get_field_names_from_labels, etc.)
  5. SP_EXTRACT_BY_DOC_TYPE stored procedure existence & signature
  6. V_INVOICE_SUMMARY three-level COALESCE with VARIANT corrections
  7. Cross-doc-type isolation (configs don't bleed across types)
  8. Active/inactive filtering
"""

import json

import pytest


pytestmark = pytest.mark.sql


# ---------------------------------------------------------------------------
# 1. DOCUMENT_TYPE_CONFIG — schema & constraints
# ---------------------------------------------------------------------------
class TestDocTypeConfigSchema:
    """Validate the DOCUMENT_TYPE_CONFIG table structure."""

    def test_table_exists(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' AND TABLE_SCHEMA = 'DOCUMENTS'"
        )
        assert sf_cursor.fetchone()[0] == 1

    def test_columns_and_order(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "ORDER BY ORDINAL_POSITION"
        )
        cols = [r[0] for r in sf_cursor.fetchall()]
        expected = [
            "DOC_TYPE", "DISPLAY_NAME", "EXTRACTION_PROMPT",
            "FIELD_LABELS", "CREATED_AT", "UPDATED_AT",
            "TABLE_EXTRACTION_SCHEMA", "REVIEW_FIELDS", "ACTIVE",
            "VALIDATION_RULES",
        ]
        assert cols == expected, f"Column mismatch: {cols}"

    def test_primary_key_is_doc_type(self, sf_cursor):
        sf_cursor.execute("SHOW PRIMARY KEYS IN TABLE DOCUMENT_TYPE_CONFIG")
        rows = sf_cursor.fetchall()
        pk_cols = [r[4] for r in rows]
        assert "DOC_TYPE" in pk_cols

    def test_field_labels_is_variant(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' "
            "AND COLUMN_NAME = 'FIELD_LABELS'"
        )
        assert sf_cursor.fetchone()[0] == "VARIANT"

    def test_table_extraction_schema_is_variant(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' "
            "AND COLUMN_NAME = 'TABLE_EXTRACTION_SCHEMA'"
        )
        assert sf_cursor.fetchone()[0] == "VARIANT"

    def test_review_fields_is_variant(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' "
            "AND COLUMN_NAME = 'REVIEW_FIELDS'"
        )
        assert sf_cursor.fetchone()[0] == "VARIANT"

    def test_active_is_boolean(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' "
            "AND COLUMN_NAME = 'ACTIVE'"
        )
        assert sf_cursor.fetchone()[0] == "BOOLEAN"

    def test_active_defaults_to_true(self, sf_cursor):
        sf_cursor.execute(
            "SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'DOCUMENT_TYPE_CONFIG' "
            "AND COLUMN_NAME = 'ACTIVE'"
        )
        row = sf_cursor.fetchone()
        assert row is not None
        assert row[0] is not None and "TRUE" in row[0].upper()


# ---------------------------------------------------------------------------
# 2. Seed data integrity
# ---------------------------------------------------------------------------
class TestDocTypeSeeds:
    """Validate seed rows for all four document types."""

    EXPECTED_TYPES = ["CONTRACT", "INVOICE", "RECEIPT", "UTILITY_BILL"]

    def test_four_seeds_exist(self, sf_cursor):
        sf_cursor.execute("SELECT doc_type FROM DOCUMENT_TYPE_CONFIG ORDER BY doc_type")
        types = [r[0] for r in sf_cursor.fetchall()]
        assert types == self.EXPECTED_TYPES

    def test_all_seeds_active(self, sf_cursor):
        sf_cursor.execute(
            "SELECT doc_type FROM DOCUMENT_TYPE_CONFIG WHERE active = FALSE"
        )
        inactive = [r[0] for r in sf_cursor.fetchall()]
        assert inactive == [], f"Unexpected inactive seeds: {inactive}"

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_display_name_not_null(self, sf_cursor, doc_type):
        sf_cursor.execute(
            f"SELECT display_name FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None and len(row[0]) > 0

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_extraction_prompt_not_null(self, sf_cursor, doc_type):
        sf_cursor.execute(
            f"SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None and len(row[0]) > 10

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_field_labels_is_valid_json(self, sf_cursor, doc_type):
        sf_cursor.execute(
            f"SELECT field_labels FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(labels, dict)
        assert "field_1" in labels, f"{doc_type} missing field_1 in labels"

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_field_labels_has_ui_keys(self, sf_cursor, doc_type):
        """Each seed should have sender_label, amount_label, date_label, reference_label."""
        sf_cursor.execute(
            f"SELECT field_labels FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        for key in ["sender_label", "amount_label", "date_label", "reference_label"]:
            assert key in labels, f"{doc_type} missing '{key}' in field_labels"

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_table_extraction_schema_valid(self, sf_cursor, doc_type):
        sf_cursor.execute(
            f"SELECT table_extraction_schema FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        raw = sf_cursor.fetchone()[0]
        schema = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(schema, dict), f"{doc_type} table_extraction_schema should be a dict"
        assert "columns" in schema, f"{doc_type} missing 'columns' in table_extraction_schema"

    @pytest.mark.parametrize("doc_type", EXPECTED_TYPES)
    def test_review_fields_valid(self, sf_cursor, doc_type):
        sf_cursor.execute(
            f"SELECT review_fields FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{doc_type}'"
        )
        raw = sf_cursor.fetchone()[0]
        rf = json.loads(raw) if isinstance(raw, str) else raw
        assert isinstance(rf, dict), f"{doc_type} review_fields should be a dict"
        assert "correctable" in rf, f"{doc_type} missing 'correctable' in review_fields"
        assert len(rf["correctable"]) > 0


# ---------------------------------------------------------------------------
# 3. UTILITY_BILL — >10 fields (proves VARIANT flexibility)
# ---------------------------------------------------------------------------
class TestUtilityBillExceedsFixedColumns:
    """UTILITY_BILL has 13 fields, exceeding the 10 fixed field columns."""

    def test_utility_bill_has_13_field_keys(self, sf_cursor):
        sf_cursor.execute(
            "SELECT field_labels FROM DOCUMENT_TYPE_CONFIG "
            "WHERE doc_type = 'UTILITY_BILL'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        field_keys = [k for k in labels if k.startswith("field_")]
        assert len(field_keys) == 13, (
            f"UTILITY_BILL should have 13 field_* keys, got {len(field_keys)}: {field_keys}"
        )

    def test_utility_bill_fields_11_to_13_exist(self, sf_cursor):
        sf_cursor.execute(
            "SELECT field_labels FROM DOCUMENT_TYPE_CONFIG "
            "WHERE doc_type = 'UTILITY_BILL'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        for key in ["field_11", "field_12", "field_13"]:
            assert key in labels, f"UTILITY_BILL missing {key}"

    def test_utility_bill_prompt_has_13_fields(self, sf_cursor):
        """Extraction prompt should list all 13 fields."""
        sf_cursor.execute(
            "SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG "
            "WHERE doc_type = 'UTILITY_BILL'"
        )
        prompt = sf_cursor.fetchone()[0]
        for field in ["utility_company", "account_number", "meter_number",
                       "service_address", "billing_period_start", "billing_period_end",
                       "rate_schedule", "kwh_usage", "demand_kw", "previous_balance",
                       "current_charges", "total_due", "due_date"]:
            assert field in prompt, f"UTILITY_BILL prompt missing '{field}'"

    def test_invoice_has_10_field_keys(self, sf_cursor):
        """INVOICE should have exactly 10 field keys (fits in fixed columns)."""
        sf_cursor.execute(
            "SELECT field_labels FROM DOCUMENT_TYPE_CONFIG "
            "WHERE doc_type = 'INVOICE'"
        )
        raw = sf_cursor.fetchone()[0]
        labels = json.loads(raw) if isinstance(raw, str) else raw
        field_keys = [k for k in labels if k.startswith("field_")]
        assert len(field_keys) == 10, f"INVOICE should have 10 field keys, got {len(field_keys)}"


# ---------------------------------------------------------------------------
# 4. VARIANT columns on data tables
# ---------------------------------------------------------------------------
class TestVariantColumns:
    """Verify VARIANT columns exist on EXTRACTED_FIELDS, EXTRACTED_TABLE_DATA, INVOICE_REVIEW."""

    def test_extracted_fields_has_raw_extraction(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_FIELDS' AND COLUMN_NAME = 'RAW_EXTRACTION'"
        )
        row = sf_cursor.fetchone()
        assert row is not None, "RAW_EXTRACTION column missing from EXTRACTED_FIELDS"
        assert row[0] == "VARIANT"

    def test_extracted_table_data_has_raw_line_data(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_TABLE_DATA' AND COLUMN_NAME = 'RAW_LINE_DATA'"
        )
        row = sf_cursor.fetchone()
        assert row is not None, "RAW_LINE_DATA column missing from EXTRACTED_TABLE_DATA"
        assert row[0] == "VARIANT"

    def test_invoice_review_has_corrections(self, sf_cursor):
        sf_cursor.execute(
            "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND COLUMN_NAME = 'CORRECTIONS'"
        )
        row = sf_cursor.fetchone()
        assert row is not None, "CORRECTIONS column missing from INVOICE_REVIEW"
        assert row[0] == "VARIANT"

    def test_raw_extraction_is_nullable(self, sf_cursor):
        sf_cursor.execute(
            "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_FIELDS' AND COLUMN_NAME = 'RAW_EXTRACTION'"
        )
        assert sf_cursor.fetchone()[0] == "YES"

    def test_raw_line_data_is_nullable(self, sf_cursor):
        sf_cursor.execute(
            "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_TABLE_DATA' AND COLUMN_NAME = 'RAW_LINE_DATA'"
        )
        assert sf_cursor.fetchone()[0] == "YES"

    def test_corrections_is_nullable(self, sf_cursor):
        sf_cursor.execute(
            "SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'INVOICE_REVIEW' AND COLUMN_NAME = 'CORRECTIONS'"
        )
        assert sf_cursor.fetchone()[0] == "YES"


# ---------------------------------------------------------------------------
# 5. SP_EXTRACT_BY_DOC_TYPE stored procedure
# ---------------------------------------------------------------------------
class TestExtractByDocTypeSP:
    """Verify the Python SP for config-driven extraction."""

    def test_sp_exists(self, sf_cursor):
        sf_cursor.execute("SHOW PROCEDURES LIKE 'SP_EXTRACT_BY_DOC_TYPE'")
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "SP_EXTRACT_BY_DOC_TYPE not found"

    def test_sp_is_python(self, sf_cursor):
        sf_cursor.execute(
            "DESCRIBE PROCEDURE SP_EXTRACT_BY_DOC_TYPE(VARCHAR)"
        )
        rows = sf_cursor.fetchall()
        props = {r[0]: r[1] for r in rows}
        assert props.get("language", "").upper() == "PYTHON"

    def test_sp_takes_varchar_param(self, sf_cursor):
        sf_cursor.execute("SHOW PROCEDURES LIKE 'SP_EXTRACT_BY_DOC_TYPE'")
        rows = sf_cursor.fetchall()
        cols = [desc[0] for desc in sf_cursor.description]
        args_idx = cols.index("arguments")
        args_str = rows[0][args_idx].upper()
        assert "VARCHAR" in args_str, f"SP should accept VARCHAR param, got: {args_str}"

    def test_sp_returns_varchar(self, sf_cursor):
        sf_cursor.execute("SHOW PROCEDURES LIKE 'SP_EXTRACT_BY_DOC_TYPE'")
        rows = sf_cursor.fetchall()
        cols = [desc[0] for desc in sf_cursor.description]
        args_idx = cols.index("arguments")
        assert "RETURN VARCHAR" in rows[0][args_idx].upper()


# ---------------------------------------------------------------------------
# 6. V_INVOICE_SUMMARY — VARIANT columns included
# ---------------------------------------------------------------------------
class TestViewVariantColumns:
    """V_INVOICE_SUMMARY should expose RAW_EXTRACTION and CORRECTIONS."""

    def test_view_has_raw_extraction(self, sf_cursor):
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        assert "RAW_EXTRACTION" in cols

    def test_view_has_corrections(self, sf_cursor):
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        assert "CORRECTIONS" in cols

    def test_raw_extraction_at_position_22(self, sf_cursor):
        """RAW_EXTRACTION should be column 22 (0-indexed 21)."""
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        assert cols[21] == "RAW_EXTRACTION"

    def test_corrections_at_position_23(self, sf_cursor):
        """CORRECTIONS should be column 23 (0-indexed 22), the last column."""
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        cols = [desc[0] for desc in sf_cursor.description]
        assert cols[22] == "CORRECTIONS"

    def test_view_has_23_columns(self, sf_cursor):
        sf_cursor.execute("SELECT * FROM V_INVOICE_SUMMARY LIMIT 0")
        assert len(sf_cursor.description) == 23


# ---------------------------------------------------------------------------
# 7. Three-level COALESCE — VARIANT correction overrides legacy
# ---------------------------------------------------------------------------
class TestThreeLevelCoalesce:
    """Insert a review with VARIANT corrections and verify view picks them up."""

    @pytest.fixture(autouse=True)
    def _cleanup(self, sf_cursor):
        """Remove test review rows after each test."""
        yield
        sf_cursor.execute(
            "DELETE FROM INVOICE_REVIEW WHERE reviewer_notes = '__FLEX_TEST__'"
        )

    def _get_first_record(self, sf_cursor):
        sf_cursor.execute(
            "SELECT record_id, file_name, field_1, field_10 "
            "FROM EXTRACTED_FIELDS LIMIT 1"
        )
        row = sf_cursor.fetchone()
        assert row is not None, "Need at least 1 row in EXTRACTED_FIELDS"
        return row

    def test_variant_correction_overrides_fixed(self, sf_cursor):
        """corrections:vendor_name should override corrected_vendor_name."""
        rec = self._get_first_record(sf_cursor)
        record_id, file_name = rec[0], rec[1]

        # Insert review with BOTH fixed corrected_vendor_name AND variant correction
        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_vendor_name, "
            " reviewer_notes, corrections) "
            "SELECT %s, %s, 'Corrected', 'FIXED_VENDOR', '__FLEX_TEST__', "
            "PARSE_JSON('{\"vendor_name\": \"VARIANT_VENDOR\"}')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT vendor_name FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        result = sf_cursor.fetchone()[0]
        assert result == "VARIANT_VENDOR", (
            f"VARIANT correction should win, got '{result}'"
        )

    def test_fixed_correction_used_when_no_variant(self, sf_cursor):
        """When corrections VARIANT is NULL, corrected_vendor_name should be used."""
        rec = self._get_first_record(sf_cursor)
        record_id, file_name = rec[0], rec[1]

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_vendor_name, "
            " reviewer_notes) "
            "VALUES (%s, %s, 'Corrected', 'FIXED_ONLY_VENDOR', '__FLEX_TEST__')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT vendor_name FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        result = sf_cursor.fetchone()[0]
        assert result == "FIXED_ONLY_VENDOR"

    def test_original_used_when_no_corrections(self, sf_cursor):
        """When no review row exists, original extraction value should show."""
        rec = self._get_first_record(sf_cursor)
        record_id, original_vendor = rec[0], rec[2]

        sf_cursor.execute(
            "SELECT vendor_name FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        result = sf_cursor.fetchone()[0]
        assert result == original_vendor

    def test_variant_total_amount_override(self, sf_cursor):
        """corrections:total_amount should override corrected_total."""
        rec = self._get_first_record(sf_cursor)
        record_id, file_name = rec[0], rec[1]

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, corrected_total, "
            " reviewer_notes, corrections) "
            "SELECT %s, %s, 'Corrected', 999.99, '__FLEX_TEST__', "
            "PARSE_JSON('{\"total_amount\": 7777.77}')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT total_amount FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        result = float(sf_cursor.fetchone()[0])
        assert result == pytest.approx(7777.77)

    def test_variant_line_item_count_override(self, sf_cursor):
        """corrections:line_item_count should override computed line count."""
        rec = self._get_first_record(sf_cursor)
        record_id, file_name = rec[0], rec[1]

        sf_cursor.execute(
            "INSERT INTO INVOICE_REVIEW "
            "(record_id, file_name, review_status, reviewer_notes, corrections) "
            "SELECT %s, %s, 'Corrected', '__FLEX_TEST__', "
            "PARSE_JSON('{\"line_item_count\": 42}')",
            (record_id, file_name),
        )

        sf_cursor.execute(
            "SELECT line_item_count FROM V_INVOICE_SUMMARY WHERE record_id = %s",
            (record_id,),
        )
        result = int(sf_cursor.fetchone()[0])
        assert result == 42


# ---------------------------------------------------------------------------
# 8. Active / Inactive filtering
# ---------------------------------------------------------------------------
class TestActiveFiltering:
    """Verify that active flag controls which doc types are returned."""

    @pytest.fixture(autouse=True)
    def _cleanup(self, sf_cursor):
        yield
        # Restore any deactivated seeds
        sf_cursor.execute(
            "UPDATE DOCUMENT_TYPE_CONFIG SET active = TRUE "
            "WHERE doc_type IN ('INVOICE', 'CONTRACT', 'RECEIPT', 'UTILITY_BILL')"
        )
        # Remove test row
        sf_cursor.execute(
            "DELETE FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '__TEST_INACTIVE__'"
        )

    def test_inactive_type_excluded_from_active_query(self, sf_cursor):
        """Deactivated doc type should not appear in active-only queries."""
        sf_cursor.execute(
            "UPDATE DOCUMENT_TYPE_CONFIG SET active = FALSE WHERE doc_type = 'CONTRACT'"
        )
        sf_cursor.execute(
            "SELECT doc_type FROM DOCUMENT_TYPE_CONFIG WHERE active = TRUE ORDER BY doc_type"
        )
        active = [r[0] for r in sf_cursor.fetchall()]
        assert "CONTRACT" not in active
        assert "INVOICE" in active

    def test_inactive_type_still_in_full_query(self, sf_cursor):
        """Deactivated doc type should still exist in unfiltered query."""
        sf_cursor.execute(
            "UPDATE DOCUMENT_TYPE_CONFIG SET active = FALSE WHERE doc_type = 'CONTRACT'"
        )
        sf_cursor.execute(
            "SELECT doc_type FROM DOCUMENT_TYPE_CONFIG ORDER BY doc_type"
        )
        all_types = [r[0] for r in sf_cursor.fetchall()]
        assert "CONTRACT" in all_types

    def test_new_type_defaults_to_active(self, sf_cursor):
        """A new row without explicit active flag should default to TRUE."""
        sf_cursor.execute(
            "INSERT INTO DOCUMENT_TYPE_CONFIG (doc_type, display_name, field_labels) "
            "SELECT '__TEST_INACTIVE__', 'Test', PARSE_JSON('{\"field_1\": \"Test\"}')"
        )
        sf_cursor.execute(
            "SELECT active FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '__TEST_INACTIVE__'"
        )
        assert sf_cursor.fetchone()[0] is True


# ---------------------------------------------------------------------------
# 9. Cross-type isolation
# ---------------------------------------------------------------------------
class TestCrossTypeIsolation:
    """Configs for different doc types should be independent."""

    def test_invoice_and_utility_bill_have_different_prompts(self, sf_cursor):
        sf_cursor.execute(
            "SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'INVOICE'"
        )
        inv_prompt = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'UTILITY_BILL'"
        )
        util_prompt = sf_cursor.fetchone()[0]
        assert inv_prompt != util_prompt

    def test_invoice_and_utility_bill_have_different_labels(self, sf_cursor):
        sf_cursor.execute(
            "SELECT field_labels:field_1::VARCHAR FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'INVOICE'"
        )
        inv_f1 = sf_cursor.fetchone()[0]
        sf_cursor.execute(
            "SELECT field_labels:field_1::VARCHAR FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'UTILITY_BILL'"
        )
        util_f1 = sf_cursor.fetchone()[0]
        assert inv_f1 != util_f1

    def test_receipt_has_fewer_correctable_fields(self, sf_cursor):
        """RECEIPT should have fewer correctable fields than INVOICE."""
        sf_cursor.execute(
            "SELECT review_fields FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'INVOICE'"
        )
        raw = sf_cursor.fetchone()[0]
        inv_rf = json.loads(raw) if isinstance(raw, str) else raw
        inv_count = len(inv_rf["correctable"])

        sf_cursor.execute(
            "SELECT review_fields FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = 'RECEIPT'"
        )
        raw = sf_cursor.fetchone()[0]
        rcpt_rf = json.loads(raw) if isinstance(raw, str) else raw
        rcpt_count = len(rcpt_rf["correctable"])

        assert rcpt_count < inv_count, (
            f"RECEIPT ({rcpt_count}) should have fewer correctable fields than INVOICE ({inv_count})"
        )

    def test_each_type_has_unique_display_name(self, sf_cursor):
        sf_cursor.execute("SELECT display_name FROM DOCUMENT_TYPE_CONFIG")
        names = [r[0] for r in sf_cursor.fetchall()]
        assert len(names) == len(set(names)), f"Duplicate display names: {names}"
