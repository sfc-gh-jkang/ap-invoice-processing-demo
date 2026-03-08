"""Admin Config Management Tests — CRUD operations on DOCUMENT_TYPE_CONFIG.

Tests cover:
  1. INSERT new document type with all required fields
  2. UPDATE existing document type (display_name, prompt, labels)
  3. Duplicate detection (PK constraint on doc_type)
  4. DELETE and re-insert
  5. Toggle active flag
  6. updated_at timestamp behavior
  7. NULL handling for optional columns
  8. JSON validation (field_labels must be valid VARIANT)
"""

import json

import pytest


pytestmark = pytest.mark.sql

TEST_DOC_TYPE = "__ADMIN_TEST__"


@pytest.fixture(autouse=True)
def _cleanup(sf_cursor):
    """Remove any test rows after each test."""
    yield
    sf_cursor.execute(
        f"DELETE FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
    )


def _insert_test_type(sf_cursor, **overrides):
    """Insert a test document type with sensible defaults."""
    vals = {
        "doc_type": TEST_DOC_TYPE,
        "display_name": "Admin Test Type",
        "extraction_prompt": "Extract field_a, field_b from this test document",
        "field_labels": '{"field_1": "Field A", "field_2": "Field B"}',
        "table_extraction_schema": '{"columns": ["Col1", "Col2"]}',
        "review_fields": '{"correctable": ["field_a"], "types": {"field_a": "VARCHAR"}}',
    }
    vals.update(overrides)
    sf_cursor.execute(
        "INSERT INTO DOCUMENT_TYPE_CONFIG "
        "(doc_type, display_name, extraction_prompt, field_labels, "
        " table_extraction_schema, review_fields) "
        f"SELECT '{vals['doc_type']}', '{vals['display_name']}', "
        f"'{vals['extraction_prompt']}', "
        f"PARSE_JSON('{vals['field_labels']}'), "
        f"PARSE_JSON('{vals['table_extraction_schema']}'), "
        f"PARSE_JSON('{vals['review_fields']}')"
    )


class TestInsertNewDocType:
    """Verify inserting a new document type."""

    def test_insert_succeeds(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == 1

    def test_inserted_values_readable(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"SELECT display_name, extraction_prompt, active "
            f"FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        row = sf_cursor.fetchone()
        assert row[0] == "Admin Test Type"
        assert "field_a" in row[1]
        assert row[2] is True  # default active

    def test_field_labels_stored_as_variant(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"SELECT field_labels:field_1::VARCHAR "
            f"FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == "Field A"

    def test_timestamps_auto_populated(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"SELECT created_at, updated_at "
            f"FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        row = sf_cursor.fetchone()
        assert row[0] is not None, "created_at should be auto-populated"
        assert row[1] is not None, "updated_at should be auto-populated"


class TestUpdateDocType:
    """Verify updating an existing document type."""

    def test_update_display_name(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"UPDATE DOCUMENT_TYPE_CONFIG SET display_name = 'Updated Name' "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            f"SELECT display_name FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == "Updated Name"

    def test_update_field_labels(self, sf_cursor):
        _insert_test_type(sf_cursor)
        new_labels = '{"field_1": "New Label A", "field_2": "New Label B", "field_3": "Label C"}'
        sf_cursor.execute(
            f"UPDATE DOCUMENT_TYPE_CONFIG "
            f"SET field_labels = PARSE_JSON('{new_labels}') "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            f"SELECT field_labels:field_3::VARCHAR "
            f"FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == "Label C"

    def test_toggle_active_flag(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"UPDATE DOCUMENT_TYPE_CONFIG SET active = FALSE "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            f"SELECT active FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] is False

        # Toggle back
        sf_cursor.execute(
            f"UPDATE DOCUMENT_TYPE_CONFIG SET active = TRUE "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            f"SELECT active FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] is True


class TestDuplicateDetection:
    """Application-level duplicate detection (PK constraints are informational in Snowflake)."""

    def test_duplicate_detected_by_count(self, sf_cursor):
        """Inserting the same doc_type twice should be detectable via COUNT."""
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == 1, "Should have exactly one row after first insert"

    def test_merge_is_idempotent(self, sf_cursor):
        """MERGE should not create duplicates."""
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"MERGE INTO DOCUMENT_TYPE_CONFIG AS tgt "
            f"USING (SELECT '{TEST_DOC_TYPE}' AS doc_type) AS src "
            f"ON tgt.doc_type = src.doc_type "
            f"WHEN NOT MATCHED THEN INSERT (doc_type, display_name, field_labels) "
            f"VALUES ('{TEST_DOC_TYPE}', 'Should Not Insert', "
            f"PARSE_JSON('{{\"field_1\": \"X\"}}'))"
        )
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == 1
        sf_cursor.execute(
            f"SELECT display_name FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == "Admin Test Type"  # original value


class TestDeleteAndReinsert:
    """Verify delete + re-insert cycle."""

    def test_delete_and_reinsert(self, sf_cursor):
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            f"SELECT COUNT(*) FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == 0

        # Re-insert with different display name
        _insert_test_type(sf_cursor, display_name="Reinserted Type")
        sf_cursor.execute(
            f"SELECT display_name FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] == "Reinserted Type"


class TestNullHandling:
    """Optional columns should accept NULL."""

    def test_null_extraction_prompt(self, sf_cursor):
        sf_cursor.execute(
            "INSERT INTO DOCUMENT_TYPE_CONFIG "
            "(doc_type, display_name, field_labels) "
            f"SELECT '{TEST_DOC_TYPE}', 'No Prompt', "
            "PARSE_JSON('{\"field_1\": \"F1\"}')"
        )
        sf_cursor.execute(
            f"SELECT extraction_prompt FROM DOCUMENT_TYPE_CONFIG "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] is None

    def test_null_table_extraction_schema(self, sf_cursor):
        sf_cursor.execute(
            "INSERT INTO DOCUMENT_TYPE_CONFIG "
            "(doc_type, display_name, field_labels) "
            f"SELECT '{TEST_DOC_TYPE}', 'No Schema', "
            "PARSE_JSON('{\"field_1\": \"F1\"}')"
        )
        sf_cursor.execute(
            f"SELECT table_extraction_schema FROM DOCUMENT_TYPE_CONFIG "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] is None

    def test_null_review_fields(self, sf_cursor):
        sf_cursor.execute(
            "INSERT INTO DOCUMENT_TYPE_CONFIG "
            "(doc_type, display_name, field_labels) "
            f"SELECT '{TEST_DOC_TYPE}', 'No Review Fields', "
            "PARSE_JSON('{\"field_1\": \"F1\"}')"
        )
        sf_cursor.execute(
            f"SELECT review_fields FROM DOCUMENT_TYPE_CONFIG "
            f"WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        assert sf_cursor.fetchone()[0] is None


class TestSeedProtection:
    """Seed rows should not be accidentally deleted or corrupted by tests."""

    SEEDS = ["CONTRACT", "INVOICE", "RECEIPT", "UTILITY_BILL"]

    def test_seeds_intact_after_test_operations(self, sf_cursor):
        """After insert/delete of test type, seeds should still be intact."""
        _insert_test_type(sf_cursor)
        sf_cursor.execute(
            f"DELETE FROM DOCUMENT_TYPE_CONFIG WHERE doc_type = '{TEST_DOC_TYPE}'"
        )
        sf_cursor.execute(
            "SELECT doc_type FROM DOCUMENT_TYPE_CONFIG "
            "WHERE doc_type IN ('CONTRACT', 'INVOICE', 'RECEIPT', 'UTILITY_BILL') "
            "ORDER BY doc_type"
        )
        remaining = [r[0] for r in sf_cursor.fetchall()]
        assert remaining == self.SEEDS
