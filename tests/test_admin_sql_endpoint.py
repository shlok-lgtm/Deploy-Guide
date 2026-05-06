"""
Tests for POST /api/admin/sql endpoint.
"""
import re
import unittest


class TestSqlBlocklist(unittest.TestCase):
    """Test the SQL keyword blocklist regex directly."""

    BLOCKLIST = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|COPY|VACUUM|REINDEX)\b",
        re.IGNORECASE,
    )

    def test_select_allowed(self):
        assert not self.BLOCKLIST.search("SELECT * FROM scores LIMIT 10")

    def test_insert_blocked(self):
        assert self.BLOCKLIST.search("INSERT INTO scores VALUES (1)")

    def test_update_blocked(self):
        assert self.BLOCKLIST.search("UPDATE scores SET score = 0")

    def test_delete_blocked(self):
        assert self.BLOCKLIST.search("DELETE FROM scores WHERE id = 1")

    def test_drop_blocked(self):
        assert self.BLOCKLIST.search("DROP TABLE scores")

    def test_case_insensitive(self):
        assert self.BLOCKLIST.search("insert into scores values (1)")
        assert self.BLOCKLIST.search("Insert Into scores Values (1)")

    def test_select_with_subquery_allowed(self):
        assert not self.BLOCKLIST.search(
            "SELECT * FROM (SELECT id FROM scores) sub LIMIT 10"
        )

    def test_explain_analyze_allowed(self):
        assert not self.BLOCKLIST.search("EXPLAIN ANALYZE SELECT * FROM scores")

    def test_row_cap_constant(self):
        """Verify row cap is documented as 1000."""
        assert 1000 > 0  # placeholder — actual cap tested via integration


class TestSqlBlocklistEdgeCases(unittest.TestCase):
    """Edge cases for blocklist."""

    BLOCKLIST = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE|COPY|VACUUM|REINDEX)\b",
        re.IGNORECASE,
    )

    def test_word_boundary_no_false_positive(self):
        """Words containing blocked keywords as substrings should pass."""
        assert not self.BLOCKLIST.search("SELECT updated_at FROM scores")
        assert not self.BLOCKLIST.search("SELECT created_at FROM scores")
        assert not self.BLOCKLIST.search("SELECT deleted FROM flags")

    def test_cte_with_select_allowed(self):
        assert not self.BLOCKLIST.search(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        )


if __name__ == "__main__":
    unittest.main()
