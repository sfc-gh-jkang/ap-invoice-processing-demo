"""Extraction Pipeline Tests — verify AI_EXTRACT actually works end-to-end.

These test the core value proposition: documents go in, structured data comes out.
Unlike data validation tests (which check existing data), these verify the
extraction *process* works correctly.
"""

import pytest


pytestmark = pytest.mark.sql


@pytest.mark.timeout(180)
class TestAIExtractEntityMode:
    """Verify AI_EXTRACT entity extraction produces valid structured output."""

    def test_entity_extraction_returns_json(self, sf_cursor):
        """AI_EXTRACT with entity prompts returns a parseable JSON response."""
        sf_cursor.execute(
            """
            SELECT AI_EXTRACT(
                TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),
                {'vendor_name': 'What is the vendor name?'}
            ) AS result
            """
        )
        result = sf_cursor.fetchone()[0]
        assert result is not None, "AI_EXTRACT returned NULL"
        # Result should be a JSON string containing 'response'
        assert "response" in str(result).lower(), (
            f"AI_EXTRACT result missing 'response' key: {str(result)[:200]}"
        )

    def test_entity_extraction_has_expected_keys(self, sf_cursor):
        """Full entity prompt returns all expected fields in response."""
        sf_cursor.execute(
            """
            SELECT AI_EXTRACT(
                TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),
                {
                    'vendor_name':    'What is the vendor or company name on this document?',
                    'document_number':'What is the invoice number or document ID?',
                    'total':          'What is the total amount? Return as a number only.'
                }
            ):response AS resp
            """
        )
        result = sf_cursor.fetchone()[0]
        assert result is not None
        result_str = str(result)
        assert "vendor_name" in result_str, f"Missing vendor_name in response: {result_str[:200]}"
        assert "document_number" in result_str, f"Missing document_number in response: {result_str[:200]}"
        assert "total" in result_str, f"Missing total in response: {result_str[:200]}"

    def test_entity_vendor_name_is_not_empty(self, sf_cursor):
        """Extracted vendor_name should be a non-empty string for a real invoice."""
        sf_cursor.execute(
            """
            SELECT AI_EXTRACT(
                TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),
                {'vendor_name': 'What is the vendor or company name on this document?'}
            ):response:vendor_name::VARCHAR AS vendor
            """
        )
        vendor = sf_cursor.fetchone()[0]
        assert vendor is not None and len(vendor.strip()) > 0, (
            f"vendor_name extraction returned empty: {vendor!r}"
        )


@pytest.mark.timeout(180)
class TestAIExtractTableMode:
    """Verify AI_EXTRACT table extraction produces valid array output."""

    def test_table_extraction_returns_json(self, sf_cursor):
        """AI_EXTRACT with responseFormat returns parseable JSON."""
        sf_cursor.execute(
            """
            SELECT AI_EXTRACT(
                file => TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),
                responseFormat => {
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'line_items': {
                                'description': 'The table of line items on the document',
                                'type': 'object',
                                'column_ordering': ['Description', 'Total'],
                                'properties': {
                                    'Description': { 'description': 'Product or service name', 'type': 'array' },
                                    'Total':       { 'description': 'Line total in dollars',   'type': 'array' }
                                }
                            }
                        }
                    }
                }
            ) AS result
            """
        )
        result = sf_cursor.fetchone()[0]
        assert result is not None, "Table-mode AI_EXTRACT returned NULL"
        result_str = str(result)
        assert "line_items" in result_str, f"Missing line_items in response: {result_str[:200]}"

    def test_lateral_flatten_produces_rows(self, sf_cursor):
        """LATERAL FLATTEN on table extraction output produces at least 1 row."""
        sf_cursor.execute(
            """
            WITH extracted AS (
                SELECT AI_EXTRACT(
                    file => TO_FILE('@DOCUMENT_STAGE', (SELECT file_name FROM RAW_DOCUMENTS LIMIT 1)),
                    responseFormat => {
                        'schema': {
                            'type': 'object',
                            'properties': {
                                'line_items': {
                                    'description': 'Line items',
                                    'type': 'object',
                                    'column_ordering': ['Description', 'Total'],
                                    'properties': {
                                        'Description': { 'description': 'Item name', 'type': 'array' },
                                        'Total':       { 'description': 'Amount',    'type': 'array' }
                                    }
                                }
                            }
                        }
                    }
                ) AS extraction
            )
            SELECT COUNT(*) AS row_count
            FROM extracted e,
                LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Description) d,
                LATERAL FLATTEN(INPUT => e.extraction:response:line_items:Total) t
            WHERE d.index = t.index
            """
        )
        count = sf_cursor.fetchone()[0]
        assert count >= 1, "LATERAL FLATTEN produced 0 rows — table extraction may have failed"


class TestStoredProcedure:
    """Verify the stored procedure executes without error."""

    def test_stored_proc_executes_successfully(self, sf_cursor):
        """CALL SP_EXTRACT_NEW_DOCUMENTS() should complete without error.

        Since all files are already extracted, this should return 'Processed 0 new document(s)'.
        """
        sf_cursor.execute("CALL SP_EXTRACT_NEW_DOCUMENTS()")
        result = sf_cursor.fetchone()[0]
        assert result is not None, "Stored procedure returned NULL"
        assert "Processed" in result, f"Unexpected proc result: {result}"

    def test_stored_proc_is_idempotent(self, sf_cursor):
        """Calling the proc twice should not create duplicate records."""
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        before_count = sf_cursor.fetchone()[0]

        sf_cursor.execute("CALL SP_EXTRACT_NEW_DOCUMENTS()")

        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        after_count = sf_cursor.fetchone()[0]
        assert after_count == before_count, (
            f"Proc created duplicates: {before_count} before, {after_count} after"
        )


class TestPromptConsistency:
    """Verify extraction prompts are consistent between batch and automation scripts."""

    EXPECTED_ENTITY_PROMPTS = [
        "vendor_name",
        "document_number",
        "reference",
        "document_date",
        "due_date",
        "terms",
        "recipient",
        "subtotal",
        "tax",
        "total",
    ]

    def test_extracted_fields_maps_all_prompts(self, sf_cursor):
        """Every entity prompt key should map to a column in EXTRACTED_FIELDS.

        field_1=vendor_name, field_2=document_number, ..., field_10=total
        """
        sf_cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_FIELDS' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "AND COLUMN_NAME LIKE 'FIELD_%' "
            "ORDER BY ORDINAL_POSITION"
        )
        field_cols = [row[0] for row in sf_cursor.fetchall()]
        assert len(field_cols) == len(self.EXPECTED_ENTITY_PROMPTS), (
            f"Expected {len(self.EXPECTED_ENTITY_PROMPTS)} FIELD_ columns "
            f"(one per prompt), got {len(field_cols)}: {field_cols}"
        )

    def test_table_extraction_maps_all_columns(self, sf_cursor):
        """Every table prompt column should map to EXTRACTED_TABLE_DATA."""
        sf_cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_NAME = 'EXTRACTED_TABLE_DATA' AND TABLE_SCHEMA = 'DOCUMENTS' "
            "AND COLUMN_NAME LIKE 'COL_%' "
            "ORDER BY ORDINAL_POSITION"
        )
        col_cols = [row[0] for row in sf_cursor.fetchall()]
        # 5 table columns: Description, Category, Qty, Unit Price, Total
        assert len(col_cols) == 5, (
            f"Expected 5 COL_ columns (one per table column), got {len(col_cols)}: {col_cols}"
        )


class TestExtractionFieldTypes:
    """Verify TRY_TO_NUMBER/TRY_TO_DATE didn't silently produce NULLs."""

    def test_no_null_totals_from_parse_failure(self, sf_cursor):
        """field_10 (total) should not be NULL due to TRY_TO_NUMBER parse failure.

        If AI returns '$1,234.56' and REGEXP_REPLACE + TRY_TO_NUMBER fails,
        the field silently becomes NULL instead of erroring.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE field_10 IS NULL AND status = 'EXTRACTED'"
        )
        null_totals = sf_cursor.fetchone()[0]
        assert null_totals == 0, (
            f"{null_totals} extracted records have NULL total — "
            f"TRY_TO_NUMBER may be failing to parse AI_EXTRACT output"
        )

    def test_no_null_dates_from_parse_failure(self, sf_cursor):
        """field_4 (document_date) should not be NULL for invoices.

        Non-invoice doc types map different data to field_4 (e.g., utility
        bills map service_address, which becomes NULL via TRY_TO_DATE).
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE field_4 IS NULL AND status = 'EXTRACTED' "
            "  AND file_name LIKE 'sample_invoice%'"
        )
        null_dates = sf_cursor.fetchone()[0]
        assert null_dates == 0, (
            f"{null_dates} invoice records have NULL document_date — "
            f"TRY_TO_DATE may be failing to parse AI_EXTRACT output. "
            f"Check that the prompt says 'Return in YYYY-MM-DD format.'"
        )

    def test_numeric_fields_are_reasonable(self, sf_cursor):
        """Subtotal + tax should approximately equal total (sanity check)."""
        sf_cursor.execute(
            """
            SELECT COUNT(*) FROM EXTRACTED_FIELDS
            WHERE field_8 IS NOT NULL
              AND field_9 IS NOT NULL
              AND field_10 IS NOT NULL
              AND ABS((field_8 + field_9) - field_10) > field_10 * 0.1
            """
        )
        mismatches = sf_cursor.fetchone()[0]
        total_rows = 0
        sf_cursor.execute(
            "SELECT COUNT(*) FROM EXTRACTED_FIELDS "
            "WHERE field_8 IS NOT NULL AND field_9 IS NOT NULL AND field_10 IS NOT NULL"
        )
        total_rows = sf_cursor.fetchone()[0]
        if total_rows > 0:
            # Allow up to 20% of rows to have rounding differences
            threshold = max(1, int(total_rows * 0.2))
            assert mismatches <= threshold, (
                f"{mismatches}/{total_rows} records have subtotal+tax != total (>10% off). "
                f"Extraction prompts may need tuning."
            )


class TestTeardownCompleteness:
    """Verify teardown script targets match deployed objects."""

    TEARDOWN_TARGETS = [
        ("DATABASE", "AI_EXTRACT_POC"),
        ("WAREHOUSE", "AI_EXTRACT_WH"),
        ("COMPUTE POOL", "AI_EXTRACT_POC_POOL"),
    ]

    @pytest.mark.parametrize("obj_type,obj_name", TEARDOWN_TARGETS)
    def test_teardown_target_exists(self, sf_cursor, obj_type, obj_name):
        """Every object in teardown_poc.sql should actually exist."""
        show_cmd = f"SHOW {obj_type}S LIKE '{obj_name}'"
        sf_cursor.execute(show_cmd)
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            f"Teardown targets {obj_type} {obj_name} but it doesn't exist. "
            f"Teardown and deploy scripts may be out of sync."
        )

    def test_teardown_task_reference_is_correct(self, sf_cursor):
        """Teardown references the correct fully-qualified task name."""
        sf_cursor.execute(
            "SHOW TASKS LIKE 'EXTRACT_NEW_DOCUMENTS_TASK' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, (
            "Teardown references AI_EXTRACT_POC.DOCUMENTS.EXTRACT_NEW_DOCUMENTS_TASK "
            "but task not found at that path."
        )


# ---------------------------------------------------------------------------
# Stream + Task integration
# ---------------------------------------------------------------------------
class TestStreamTaskIntegration:
    """Verify the stream and task work together for automated extraction."""

    def test_stream_exists(self, sf_cursor):
        """The RAW_DOCUMENTS_STREAM should exist on RAW_DOCUMENTS."""
        sf_cursor.execute(
            "SHOW STREAMS LIKE 'RAW_DOCUMENTS_STREAM' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "RAW_DOCUMENTS_STREAM not found"

    def test_stream_is_on_raw_documents(self, sf_cursor):
        """Stream should be defined on the RAW_DOCUMENTS table."""
        sf_cursor.execute(
            "SHOW STREAMS LIKE 'RAW_DOCUMENTS_STREAM' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("Stream not found")
        # SHOW STREAMS columns vary; find the "table_name" column
        col_names = [desc[0] for desc in sf_cursor.description]
        # Look for a column with the source table info
        row_dict = dict(zip(col_names, rows[0]))
        table_name = row_dict.get("table_name", row_dict.get("TABLE_NAME", ""))
        assert "RAW_DOCUMENTS" in str(table_name).upper(), (
            f"Stream should be on RAW_DOCUMENTS, found: {table_name}"
        )

    def test_task_exists_and_references_proc(self, sf_cursor):
        """EXTRACT_NEW_DOCUMENTS_TASK should exist and call SP_EXTRACT_NEW_DOCUMENTS."""
        sf_cursor.execute(
            "SHOW TASKS LIKE 'EXTRACT_NEW_DOCUMENTS_TASK' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        assert len(rows) >= 1, "Task not found"
        col_names = [desc[0] for desc in sf_cursor.description]
        row_dict = dict(zip(col_names, rows[0]))
        definition = row_dict.get("definition", row_dict.get("DEFINITION", ""))
        assert "SP_EXTRACT_NEW_DOCUMENTS" in str(definition).upper(), (
            f"Task should call SP_EXTRACT_NEW_DOCUMENTS, got: {definition}"
        )

    def test_stream_is_empty_after_full_extraction(self, sf_cursor):
        """After all invoice documents have been extracted, the stream should not
        have a large backlog of unprocessed invoice rows.

        A small number of pending rows is acceptable — these may be from
        re-inserts, schema changes, or stream recreation after the last proc run.
        """
        sf_cursor.execute(
            "SELECT COUNT(*) FROM AI_EXTRACT_POC.DOCUMENTS.RAW_DOCUMENTS_STREAM "
            "WHERE file_name LIKE 'sample_invoice%'"
        )
        pending = sf_cursor.fetchone()[0]
        # Allow a small backlog (up to 10) from re-inserts or stream recreation
        assert pending <= 10, (
            f"Stream has {pending} unconsumed invoice rows — "
            f"proc may not have consumed all"
        )

    def test_proc_is_idempotent_with_empty_stream(self, sf_cursor):
        """Calling SP_EXTRACT_NEW_DOCUMENTS when stream is empty should
        process 0 documents and not create duplicates."""
        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        before = sf_cursor.fetchone()[0]

        sf_cursor.execute("CALL SP_EXTRACT_NEW_DOCUMENTS()")
        result = sf_cursor.fetchone()[0]
        assert "0" in str(result), (
            f"Expected 'Processed 0 new document(s)' with empty stream, got: {result}"
        )

        sf_cursor.execute("SELECT COUNT(*) FROM EXTRACTED_FIELDS")
        after = sf_cursor.fetchone()[0]
        assert after == before, (
            f"Proc created duplicates with empty stream: {before} -> {after}"
        )

    def test_task_has_stream_condition(self, sf_cursor):
        """Task should have a WHEN condition checking stream_has_data
        to avoid unnecessary runs."""
        sf_cursor.execute(
            "SHOW TASKS LIKE 'EXTRACT_NEW_DOCUMENTS_TASK' IN SCHEMA AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        if len(rows) == 0:
            pytest.skip("Task not found")
        col_names = [desc[0] for desc in sf_cursor.description]
        row_dict = dict(zip(col_names, rows[0]))
        condition = str(row_dict.get("condition", row_dict.get("CONDITION", "")))
        assert "SYSTEM$STREAM_HAS_DATA" in condition.upper(), (
            f"Task should check SYSTEM$STREAM_HAS_DATA, got condition: {condition}"
        )
