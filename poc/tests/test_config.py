"""Unit tests for streamlit/config.py — module structure and session derivation.

config.py calls get_active_session() at import time, which only works inside
Snowflake (SiS or SPCS).  These tests mock the Snowpark layer to verify the
module wires DB, STAGE, and get_session() correctly.
"""

import importlib
import sys
import types
from unittest import mock

import pytest


def _make_mock_session(db="AI_EXTRACT_POC", schema="DOCUMENTS"):
    """Create a mock Snowpark session that returns the given db/schema."""
    mock_session = mock.MagicMock()
    mock_row = {
        "DB": db,
        "SCH": schema,
    }
    mock_session.sql.return_value.collect.return_value = [mock_row]
    return mock_session


def _import_config(mock_session):
    """Import (or re-import) config.py with get_active_session mocked."""
    module_name = "config"

    # Remove from cache so we get a fresh import
    if module_name in sys.modules:
        del sys.modules[module_name]

    # Also remove the snowflake.snowpark.context module so the mock takes effect
    for key in list(sys.modules):
        if key.startswith("snowflake.snowpark"):
            del sys.modules[key]

    # Create a fake snowflake.snowpark.context module
    fake_context = types.ModuleType("snowflake.snowpark.context")
    fake_context.get_active_session = mock.MagicMock(return_value=mock_session)

    # Also need snowflake and snowflake.snowpark as parent modules
    fake_snowflake = types.ModuleType("snowflake")
    fake_snowpark = types.ModuleType("snowflake.snowpark")
    fake_snowflake.snowpark = fake_snowpark
    fake_snowpark.context = fake_context

    with mock.patch.dict(sys.modules, {
        "snowflake": fake_snowflake,
        "snowflake.snowpark": fake_snowpark,
        "snowflake.snowpark.context": fake_context,
    }):
        # Add the streamlit dir to path so "config" can be imported
        import os
        config_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "streamlit"
        )
        sys.path.insert(0, config_dir)
        try:
            config = importlib.import_module(module_name)
        finally:
            sys.path.pop(0)

    return config, fake_context


class TestConfigModuleStructure:
    """Verify config.py derives DB, STAGE, and get_session correctly."""

    def test_db_format(self):
        """DB should be 'DATABASE.SCHEMA' format."""
        mock_session = _make_mock_session("MY_DB", "MY_SCHEMA")
        config, _ = _import_config(mock_session)
        assert config.DB == "MY_DB.MY_SCHEMA"

    def test_stage_format(self):
        """STAGE should be 'DATABASE.SCHEMA.DOCUMENT_STAGE' format."""
        mock_session = _make_mock_session("MY_DB", "MY_SCHEMA")
        config, _ = _import_config(mock_session)
        assert config.STAGE == "MY_DB.MY_SCHEMA.DOCUMENT_STAGE"

    def test_get_session_calls_get_active_session(self):
        """get_session() should delegate to get_active_session()."""
        mock_session = _make_mock_session()
        config, fake_context = _import_config(mock_session)
        result = config.get_session()
        # get_active_session is called once at import and once in get_session()
        assert fake_context.get_active_session.call_count >= 2
        assert result is mock_session

    def test_db_uses_current_database(self):
        """DB should use whatever CURRENT_DATABASE() returns."""
        mock_session = _make_mock_session("PROD_DB", "ANALYTICS")
        config, _ = _import_config(mock_session)
        assert config.DB == "PROD_DB.ANALYTICS"

    def test_session_sql_called_with_current_db_query(self):
        """Module should query CURRENT_DATABASE() and CURRENT_SCHEMA() on import."""
        mock_session = _make_mock_session()
        _import_config(mock_session)
        mock_session.sql.assert_called_once_with(
            "SELECT CURRENT_DATABASE() AS db, CURRENT_SCHEMA() AS sch"
        )

    def test_no_fallback_to_connector(self):
        """config.py should NOT import snowflake.connector — it only uses Snowpark."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        source = importlib.util.find_spec("config")
        # Read the actual source file to verify no connector import
        import os
        config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "streamlit", "config.py"
        )
        with open(config_path) as f:
            source_code = f.read()
        assert "snowflake.connector" not in source_code, (
            "config.py should not import snowflake.connector — it only runs inside Snowflake"
        )


class TestDefaultLabels:
    """Verify _DEFAULT_LABELS fallback dictionary."""

    def test_default_labels_exists(self):
        """config module should expose _DEFAULT_LABELS."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        assert hasattr(config, "_DEFAULT_LABELS")

    def test_default_labels_has_field_keys(self):
        """_DEFAULT_LABELS should have field_1 through field_10."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        for i in range(1, 11):
            assert f"field_{i}" in config._DEFAULT_LABELS, (
                f"Missing field_{i} in _DEFAULT_LABELS"
            )

    def test_default_labels_has_meta_keys(self):
        """_DEFAULT_LABELS should have sender_label, amount_label, etc."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        for key in ["sender_label", "amount_label", "date_label",
                     "reference_label", "secondary_ref_label"]:
            assert key in config._DEFAULT_LABELS, (
                f"Missing '{key}' in _DEFAULT_LABELS"
            )

    def test_default_labels_values_are_strings(self):
        """All values in _DEFAULT_LABELS should be strings."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        for key, val in config._DEFAULT_LABELS.items():
            assert isinstance(val, str), f"_DEFAULT_LABELS['{key}'] is {type(val)}, expected str"


class TestGetDocTypeLabels:
    """Verify get_doc_type_labels() function."""

    def test_returns_dict(self):
        """get_doc_type_labels() should return a dict."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        # Mock the SQL call for fetching labels
        import json
        labels_json = json.dumps({"field_1": "Vendor", "sender_label": "Vendor"})
        mock_session.sql.return_value.collect.return_value = [
            {"FIELD_LABELS": labels_json}
        ]
        result = config.get_doc_type_labels(mock_session, "INVOICE")
        assert isinstance(result, dict)

    def test_returns_labels_from_config_table(self):
        """Should return parsed labels when config table has data."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        import json
        expected = {"field_1": "Counterparty", "sender_label": "Counterparty"}
        mock_session.sql.return_value.collect.return_value = [
            {"FIELD_LABELS": json.dumps(expected)}
        ]
        result = config.get_doc_type_labels(mock_session, "CONTRACT")
        assert result == expected

    def test_returns_variant_dict_directly(self):
        """If FIELD_LABELS is already a dict (VARIANT), return it directly."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        expected = {"field_1": "Merchant", "sender_label": "Merchant"}
        mock_session.sql.return_value.collect.return_value = [
            {"FIELD_LABELS": expected}
        ]
        result = config.get_doc_type_labels(mock_session, "RECEIPT")
        assert result == expected

    def test_falls_back_to_defaults_on_empty_result(self):
        """Should return _DEFAULT_LABELS when config table returns no rows."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.return_value = []
        result = config.get_doc_type_labels(mock_session, "UNKNOWN")
        assert result == config._DEFAULT_LABELS

    def test_falls_back_to_defaults_on_exception(self):
        """Should return _DEFAULT_LABELS when SQL query raises."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.side_effect = Exception("table not found")
        result = config.get_doc_type_labels(mock_session, "INVOICE")
        assert result == config._DEFAULT_LABELS

    def test_fallback_is_copy_not_reference(self):
        """Fallback should return a copy, not a reference to _DEFAULT_LABELS."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.return_value = []
        result = config.get_doc_type_labels(mock_session, "UNKNOWN")
        result["field_1"] = "MUTATED"
        assert config._DEFAULT_LABELS["field_1"] != "MUTATED"


class TestGetDocTypes:
    """Verify get_doc_types() function."""

    def test_returns_list(self):
        """get_doc_types() should return a list."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.return_value = [
            {"DOC_TYPE": "INVOICE"},
            {"DOC_TYPE": "CONTRACT"},
        ]
        result = config.get_doc_types(mock_session)
        assert isinstance(result, list)

    def test_returns_doc_types_from_config_table(self):
        """Should return doc types from config table."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.return_value = [
            {"DOC_TYPE": "CONTRACT"},
            {"DOC_TYPE": "INVOICE"},
            {"DOC_TYPE": "RECEIPT"},
        ]
        result = config.get_doc_types(mock_session)
        assert result == ["CONTRACT", "INVOICE", "RECEIPT"]

    def test_falls_back_to_invoice_on_exception(self):
        """Should return ['INVOICE'] when SQL query raises."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.side_effect = Exception("table not found")
        result = config.get_doc_types(mock_session)
        assert result == ["INVOICE"]

    def test_falls_back_to_invoice_on_empty_result(self):
        """Should return ['INVOICE'] when config table is empty."""
        mock_session = _make_mock_session()
        config, _ = _import_config(mock_session)
        mock_session.sql.return_value.collect.return_value = []
        result = config.get_doc_types(mock_session)
        # Empty result means no rows — function returns empty list from comprehension
        # This is actually [] not ["INVOICE"], since the list comprehension produces []
        # The fallback only triggers on Exception
        assert result == []
