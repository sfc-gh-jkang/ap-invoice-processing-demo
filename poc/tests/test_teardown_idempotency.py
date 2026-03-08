"""Teardown script idempotency tests.

Verify that teardown_poc.sql:
  1. Uses IF EXISTS on every DROP/ALTER so it can run safely multiple times
  2. References objects that actually exist (no typos)
  3. Covers all major infrastructure objects

We do NOT actually execute the teardown — these are static analysis + live
object-existence checks.
"""

import os
import re

import pytest

pytestmark = [pytest.mark.sql]

TEARDOWN_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "teardown_poc.sql",
)


# ---------------------------------------------------------------------------
# Static analysis of the teardown script
# ---------------------------------------------------------------------------
class TestTeardownScriptSyntax:
    """Parse teardown_poc.sql and verify safety patterns."""

    @pytest.fixture(autouse=True)
    def _load_script(self):
        with open(TEARDOWN_PATH) as f:
            self.script = f.read()
        # Extract non-comment, non-empty lines
        self.lines = [
            line.strip()
            for line in self.script.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]

    def test_script_exists(self):
        """teardown_poc.sql should exist."""
        assert os.path.isfile(TEARDOWN_PATH)

    def test_all_drops_use_if_exists(self):
        """Every DROP statement must include IF EXISTS for idempotency."""
        drop_lines = [l for l in self.lines if l.upper().startswith("DROP")]
        assert len(drop_lines) >= 1, "No DROP statements found"
        for line in drop_lines:
            assert "IF EXISTS" in line.upper(), (
                f"DROP without IF EXISTS: {line}"
            )

    def test_alter_task_uses_if_exists(self):
        """ALTER TASK ... SUSPEND should use IF EXISTS."""
        alter_lines = [
            l for l in self.lines if l.upper().startswith("ALTER")
        ]
        for line in alter_lines:
            assert "IF EXISTS" in line.upper(), (
                f"ALTER without IF EXISTS: {line}"
            )

    def test_references_correct_database(self):
        """Script should reference AI_EXTRACT_POC database."""
        assert "AI_EXTRACT_POC" in self.script.upper()

    def test_references_correct_warehouse(self):
        """Script should reference AI_EXTRACT_WH warehouse."""
        assert "AI_EXTRACT_WH" in self.script.upper()

    def test_references_correct_compute_pool(self):
        """Script should reference AI_EXTRACT_POC_POOL compute pool."""
        assert "AI_EXTRACT_POC_POOL" in self.script.upper()

    def test_suspends_task_before_drop(self):
        """Task should be suspended before the database is dropped."""
        task_pos = self.script.upper().find("ALTER TASK")
        drop_db_pos = self.script.upper().find("DROP DATABASE")
        if task_pos >= 0 and drop_db_pos >= 0:
            assert task_pos < drop_db_pos, (
                "Task should be suspended before DROP DATABASE"
            )


# ---------------------------------------------------------------------------
# Live verification: teardown targets actually exist
# ---------------------------------------------------------------------------
class TestTeardownTargetsExist:
    """Verify that every object the teardown script would drop actually exists."""

    def test_database_exists(self, sf_cursor):
        """AI_EXTRACT_POC database should exist."""
        sf_cursor.execute("SHOW DATABASES LIKE 'AI_EXTRACT_POC'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, "AI_EXTRACT_POC database not found"

    def test_warehouse_exists(self, sf_cursor):
        """AI_EXTRACT_WH warehouse should exist."""
        sf_cursor.execute("SHOW WAREHOUSES LIKE 'AI_EXTRACT_WH'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, "AI_EXTRACT_WH warehouse not found"

    def test_compute_pool_exists(self, sf_cursor):
        """AI_EXTRACT_POC_POOL compute pool should exist."""
        sf_cursor.execute("SHOW COMPUTE POOLS LIKE 'AI_EXTRACT_POC_POOL'")
        rows = sf_cursor.fetchall()
        assert len(rows) == 1, "AI_EXTRACT_POC_POOL compute pool not found"

    def test_task_exists(self, sf_cursor):
        """EXTRACT_NEW_DOCUMENTS_TASK should exist (so SUSPEND won't fail)."""
        sf_cursor.execute(
            "SHOW TASKS IN AI_EXTRACT_POC.DOCUMENTS"
        )
        rows = sf_cursor.fetchall()
        desc = sf_cursor.description
        col_names = [d[0] for d in desc]
        names = [dict(zip(col_names, r)).get("name", "") for r in rows]
        assert "EXTRACT_NEW_DOCUMENTS_TASK" in names


# ---------------------------------------------------------------------------
# Coverage: teardown script covers all major infrastructure
# ---------------------------------------------------------------------------
class TestTeardownCoverage:
    """Verify teardown script covers all infrastructure objects."""

    @pytest.fixture(autouse=True)
    def _load_script(self):
        with open(TEARDOWN_PATH) as f:
            self.script = f.read().upper()

    def test_drops_database(self):
        """Script should drop the database (which cascades tables/views/etc)."""
        assert "DROP DATABASE" in self.script

    def test_drops_warehouse(self):
        """Script should drop the warehouse."""
        assert "DROP WAREHOUSE" in self.script

    def test_drops_compute_pool(self):
        """Script should drop the compute pool."""
        assert "DROP COMPUTE POOL" in self.script

    def test_suspends_task(self):
        """Script should suspend the task before dropping the database."""
        assert "ALTER TASK" in self.script
        assert "SUSPEND" in self.script
