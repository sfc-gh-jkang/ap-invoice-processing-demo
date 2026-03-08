"""Unit tests for confidence scoring, company abbreviation resolution,
and their integration into the extraction pipeline.

Uses AST extraction to pull pure-Python functions from 06_automate.sql's
embedded Python body — no Snowflake connection required for unit tests.
SQL-based integration tests verify the functions work end-to-end.
"""

import ast
import json
import os
import re

import pytest


# ---------------------------------------------------------------------------
# AST extraction: pull Python functions from the SP body in 06_automate.sql
# ---------------------------------------------------------------------------
_SQL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "sql", "06_automate.sql"
)


def _extract_python_body():
    """Extract the Python body between $$ markers from SP_EXTRACT_BY_DOC_TYPE."""
    with open(_SQL_PATH) as f:
        content = f.read()

    # Find the Python SP body — between the first $$ and closing $$;
    # after CREATE OR REPLACE PROCEDURE SP_EXTRACT_BY_DOC_TYPE
    marker = "CREATE OR REPLACE PROCEDURE SP_EXTRACT_BY_DOC_TYPE(P_DOC_TYPE VARCHAR)"
    start = content.find(marker)
    assert start != -1, "SP_EXTRACT_BY_DOC_TYPE not found in 06_automate.sql"

    first_dd = content.find("$$", start)
    assert first_dd != -1, "Opening $$ not found"
    body_start = first_dd + 2  # skip past $$

    close_dd = content.find("$$;", body_start)
    assert close_dd != -1, "Closing $$; not found"

    return content[body_start:close_dd].strip()


def _load_functions():
    """Load target functions from the SP Python body via AST extraction."""
    source = _extract_python_body()
    tree = ast.parse(source)

    needed = {
        "_normalize",
        "_resolve_company_name",
        "_compute_heuristic_confidence",
        "_apply_validation_rules",
    }
    # Also need the dicts
    needed_vars = {
        "_COMPANY_ABBREVIATIONS",
        "_FIELD_DESCRIPTIONS",
    }

    segments = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef) and node.name in needed:
            segments.append(ast.get_source_segment(source, node))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in needed_vars:
                    segments.append(ast.get_source_segment(source, node))

    preamble = "import re\nimport json\nfrom datetime import datetime\n\n"
    code = preamble + "\n\n".join(s for s in segments if s)
    ns = {}
    exec(compile(code, "<06_automate_functions>", "exec"), ns)
    return ns


_funcs = _load_functions()
_normalize = _funcs["_normalize"]
_resolve_company_name = _funcs["_resolve_company_name"]
_compute_heuristic_confidence = _funcs["_compute_heuristic_confidence"]
_apply_validation_rules = _funcs["_apply_validation_rules"]
_COMPANY_ABBREVIATIONS = _funcs["_COMPANY_ABBREVIATIONS"]
_FIELD_DESCRIPTIONS = _funcs["_FIELD_DESCRIPTIONS"]


# ===========================================================================
# 1. Company abbreviation resolution
# ===========================================================================
class TestCompanyAbbreviations:
    """Verify _COMPANY_ABBREVIATIONS dict and _resolve_company_name()."""

    def test_pseg_resolved(self):
        assert _resolve_company_name("PSE&G") == "Public Service Electric and Gas"

    def test_pseg_lowercase(self):
        assert _resolve_company_name("pse&g") == "Public Service Electric and Gas"

    def test_pseg_mixed_case(self):
        assert _resolve_company_name("Pse&G") == "Public Service Electric and Gas"

    def test_coned_resolved(self):
        assert _resolve_company_name("ConEdison") == "Consolidated Edison"

    def test_con_ed_resolved(self):
        assert _resolve_company_name("Con Ed") == "Consolidated Edison"

    def test_con_edison_resolved(self):
        assert _resolve_company_name("Con Edison") == "Consolidated Edison"

    def test_or_resolved(self):
        assert _resolve_company_name("O&R") == "Orange and Rockland Utilities"

    def test_jcpl_resolved(self):
        assert _resolve_company_name("JCP&L") == "Jersey Central Power & Light"

    def test_national_grid_passthrough(self):
        """Full names not in abbreviation dict should pass through."""
        assert _resolve_company_name("National Grid") == "National Grid"

    def test_pge_resolved(self):
        assert _resolve_company_name("PG&E") == "Pacific Gas and Electric Company"

    def test_unknown_company_passthrough(self):
        assert _resolve_company_name("Acme Power Corp") == "Acme Power Corp"

    def test_none_passthrough(self):
        assert _resolve_company_name(None) is None

    def test_empty_string_passthrough(self):
        assert _resolve_company_name("") == ""

    def test_whitespace_handling(self):
        """Leading/trailing whitespace should be stripped before lookup."""
        assert _resolve_company_name("  PSE&G  ") == "Public Service Electric and Gas"

    def test_abbreviations_dict_has_entries(self):
        """Sanity check: dict should have a reasonable number of entries."""
        assert len(_COMPANY_ABBREVIATIONS) >= 30

    def test_all_values_are_strings(self):
        for k, v in _COMPANY_ABBREVIATIONS.items():
            assert isinstance(k, str), f"Key {k} is not a string"
            assert isinstance(v, str), f"Value for {k} is not a string"

    def test_all_keys_are_lowercase(self):
        for k in _COMPANY_ABBREVIATIONS:
            assert k == k.lower(), f"Key '{k}' is not lowercase"


# ===========================================================================
# 2. Heuristic confidence scoring
# ===========================================================================
class TestHeuristicConfidence:
    """Verify _compute_heuristic_confidence() scoring logic."""

    def test_all_present_varchar_fields(self):
        normalized = {"vendor_name": "Acme Corp", "invoice_number": "INV-001"}
        field_types = {"vendor_name": "VARCHAR", "invoice_number": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["vendor_name"] == 1.0
        assert scores["invoice_number"] == 1.0

    def test_null_field_low_confidence(self):
        normalized = {"vendor_name": None}
        field_types = {"vendor_name": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["vendor_name"] == 0.1

    def test_empty_string_low_confidence(self):
        normalized = {"vendor_name": ""}
        field_types = {"vendor_name": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["vendor_name"] == 0.1

    def test_zero_number_slight_reduction(self):
        normalized = {"total_due": "0"}
        field_types = {"total_due": "NUMBER"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["total_due"] == 0.9

    def test_positive_number_full_confidence(self):
        normalized = {"total_due": "150.00"}
        field_types = {"total_due": "NUMBER"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["total_due"] == 1.0

    def test_iso_date_full_confidence(self):
        normalized = {"due_date": "2024-03-15"}
        field_types = {"due_date": "DATE"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["due_date"] == 1.0

    def test_non_iso_date_reduced_confidence(self):
        normalized = {"due_date": "March 15, 2024"}
        field_types = {"due_date": "DATE"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["due_date"] == 0.8

    def test_validation_warning_reduces_score(self):
        normalized = {"total_due": "999999"}
        field_types = {"total_due": "NUMBER"}
        warnings = [{"field": "total_due", "rule": "max", "message": "exceeds max"}]
        scores = _compute_heuristic_confidence(normalized, field_types, warnings)
        assert scores["total_due"] == 0.7

    def test_multiple_warnings_reduce_more(self):
        normalized = {"total_due": "-5"}
        field_types = {"total_due": "NUMBER"}
        warnings = [
            {"field": "total_due", "rule": "min", "message": "below min"},
            {"field": "total_due", "rule": "pattern", "message": "bad format"},
        ]
        scores = _compute_heuristic_confidence(normalized, field_types, warnings)
        assert scores["total_due"] == 0.4

    def test_score_clamped_at_zero(self):
        normalized = {"total_due": "-5"}
        field_types = {"total_due": "NUMBER"}
        warnings = [
            {"field": "total_due", "rule": "min", "message": "x"},
            {"field": "total_due", "rule": "max", "message": "x"},
            {"field": "total_due", "rule": "pattern", "message": "x"},
            {"field": "total_due", "rule": "required", "message": "x"},
        ]
        scores = _compute_heuristic_confidence(normalized, field_types, warnings)
        assert scores["total_due"] == 0.0

    def test_mixed_fields(self):
        normalized = {
            "vendor_name": "Acme Corp",
            "total_due": "150.00",
            "due_date": "2024-03-15",
            "notes": None,
        }
        field_types = {
            "vendor_name": "VARCHAR",
            "total_due": "NUMBER",
            "due_date": "DATE",
            "notes": "VARCHAR",
        }
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["vendor_name"] == 1.0
        assert scores["total_due"] == 1.0
        assert scores["due_date"] == 1.0
        assert scores["notes"] == 0.1

    def test_empty_normalized_returns_empty(self):
        scores = _compute_heuristic_confidence({}, {}, [])
        assert scores == {}

    def test_no_warnings_param(self):
        """None for warnings should not crash."""
        normalized = {"vendor_name": "Test"}
        field_types = {"vendor_name": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, None)
        assert scores["vendor_name"] == 1.0


# ===========================================================================
# 3. Integration: abbreviation + confidence together
# ===========================================================================
class TestAbbreviationConfidenceIntegration:
    """Verify that resolved names still get correct confidence scores."""

    def test_resolved_name_gets_full_confidence(self):
        """After resolving PSE&G → full name, confidence should be 1.0."""
        name = _resolve_company_name("PSE&G")
        normalized = {"utility_company": name}
        field_types = {"utility_company": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["utility_company"] == 1.0

    def test_null_company_after_resolve(self):
        name = _resolve_company_name(None)
        normalized = {"utility_company": name}
        field_types = {"utility_company": "VARCHAR"}
        scores = _compute_heuristic_confidence(normalized, field_types, [])
        assert scores["utility_company"] == 0.1


# ===========================================================================
# 4. Field descriptions dict coverage
# ===========================================================================
class TestFieldDescriptions:
    """Verify _FIELD_DESCRIPTIONS dict quality."""

    def test_has_monetary_fields(self):
        monetary = ["total_due", "total_amount", "subtotal", "tax_amount",
                     "current_charges", "previous_balance"]
        for f in monetary:
            assert f in _FIELD_DESCRIPTIONS, f"Missing description for {f}"

    def test_has_date_fields(self):
        dates = ["due_date", "invoice_date", "billing_period_start", "billing_period_end"]
        for f in dates:
            assert f in _FIELD_DESCRIPTIONS, f"Missing description for {f}"

    def test_has_name_fields(self):
        names = ["vendor_name", "utility_company", "merchant_name"]
        for f in names:
            assert f in _FIELD_DESCRIPTIONS, f"Missing description for {f}"

    def test_has_reference_fields(self):
        refs = ["invoice_number", "account_number", "meter_number"]
        for f in refs:
            assert f in _FIELD_DESCRIPTIONS, f"Missing description for {f}"

    def test_descriptions_are_nonempty_strings(self):
        for k, v in _FIELD_DESCRIPTIONS.items():
            assert isinstance(v, str), f"Description for {k} is not a string"
            assert len(v) > 10, f"Description for {k} is too short: '{v}'"

    def test_minimum_description_count(self):
        assert len(_FIELD_DESCRIPTIONS) >= 30


# ===========================================================================
# 5. SQL integration: confidence scores in Snowflake data
# ===========================================================================
CONNECTION_NAME = os.environ.get("POC_CONNECTION", "default")
POC_DB = os.environ.get("POC_DB", "AI_EXTRACT_POC")
POC_SCHEMA = os.environ.get("POC_SCHEMA", "DOCUMENTS")
POC_WH = os.environ.get("POC_WH", "AI_EXTRACT_WH")
POC_ROLE = os.environ.get("POC_ROLE", "AI_EXTRACT_APP")


@pytest.fixture(scope="session")
def sf_cursor():
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
    cur = conn.cursor()
    cur.execute(f"USE ROLE {POC_ROLE}")
    cur.execute(f"USE DATABASE {POC_DB}")
    cur.execute(f"USE SCHEMA {POC_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {POC_WH}")
    yield cur
    cur.close()
    conn.close()


@pytest.mark.sql
class TestConfidenceInSnowflake:
    """Verify confidence scores are stored in EXTRACTED_FIELDS after re-extraction."""

    def test_confidence_present_utility_bill_01(self, sf_cursor):
        sf_cursor.execute("""
            SELECT raw_extraction:_confidence::VARCHAR
            FROM EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        row = sf_cursor.fetchone()
        assert row is not None and row[0] is not None, \
            "Confidence scores not found in utility_bill_01.pdf"
        confidence = json.loads(row[0])
        assert isinstance(confidence, dict)
        assert len(confidence) >= 10

    def test_confidence_values_in_range(self, sf_cursor):
        sf_cursor.execute("""
            SELECT raw_extraction:_confidence::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        for row in sf_cursor.fetchall():
            confidence = json.loads(row[0])
            for field, score in confidence.items():
                assert 0.0 <= score <= 1.0, \
                    f"Score for {field} out of range: {score}"

    def test_confidence_keys_match_extraction_fields(self, sf_cursor):
        """Confidence keys should match the extracted field names."""
        sf_cursor.execute("""
            SELECT raw_extraction:_confidence::VARCHAR,
                   raw_extraction::VARCHAR
            FROM EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_01.pdf'
        """)
        row = sf_cursor.fetchone()
        confidence = json.loads(row[0])
        raw = json.loads(row[1])
        # Confidence keys should be a subset of raw keys (excluding metadata keys)
        metadata_keys = {"_confidence", "_validation_warnings"}
        data_keys = {k for k in raw if not k.startswith("_")}
        conf_keys = set(confidence.keys())
        assert conf_keys == data_keys, \
            f"Confidence keys mismatch. Missing: {data_keys - conf_keys}, Extra: {conf_keys - data_keys}"

    def test_pseg_resolved_in_snowflake(self, sf_cursor):
        """Bill 06 (PSE&G) should now have full company name."""
        sf_cursor.execute("""
            SELECT raw_extraction:utility_company::VARCHAR
            FROM EXTRACTED_FIELDS
            WHERE file_name = 'utility_bill_06.pdf'
        """)
        val = sf_cursor.fetchone()[0]
        assert "public service" in val.lower(), \
            f"Expected 'Public Service Electric and Gas', got: {val}"

    def test_all_utility_bills_have_confidence(self, sf_cursor):
        sf_cursor.execute("""
            SELECT e.file_name, e.raw_extraction:_confidence::VARCHAR
            FROM EXTRACTED_FIELDS e
            JOIN RAW_DOCUMENTS r ON r.file_name = e.file_name
            WHERE r.doc_type = 'UTILITY_BILL'
        """)
        rows = sf_cursor.fetchall()
        assert len(rows) == 10
        for row in rows:
            assert row[1] is not None, f"{row[0]} missing confidence scores"
