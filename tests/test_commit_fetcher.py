"""Unit tests for lucille.github.commit_fetcher (pure functions only)."""
import pytest
from context import lucille  # noqa: F401
from lucille.github.commit_fetcher import (
    deduplicate_ticket_keys,
    extract_project_key,
    parse_ticket_keys,
)


class TestParseTicketKeys:
    def test_single_oot_ticket(self):
        assert parse_ticket_keys("fix OOT-123: some bug") == ["OOT-123"]

    def test_multiple_different_projects(self):
        keys = parse_ticket_keys("OOT-123 and SSJ-456 related")
        assert set(keys) == {"OOT-123", "SSJ-456"}

    def test_no_match(self):
        assert parse_ticket_keys("chore: update dependencies") == []

    def test_uppercase_only(self):
        # lowercase project keys should not match the default uppercase pattern
        assert parse_ticket_keys("oot-123: something") == []

    def test_multiple_occurrences_same_project(self):
        keys = parse_ticket_keys("OOT-123 is related to OOT-456")
        assert "OOT-123" in keys
        assert "OOT-456" in keys

    def test_devops_project(self):
        keys = parse_ticket_keys("DEVOPS-89: deploy pipeline fix")
        assert keys == ["DEVOPS-89"]

    def test_custom_pattern(self):
        keys = parse_ticket_keys("PROJ-1 and PROJ-2", pattern=r"PROJ-\d+")
        assert set(keys) == {"PROJ-1", "PROJ-2"}

    def test_ticket_in_multiline_message(self):
        message = "OOT-500: implement feature\n\nCloses OOT-501"
        keys = parse_ticket_keys(message)
        assert "OOT-500" in keys
        assert "OOT-501" in keys


class TestDeduplicateTicketKeys:
    def test_removes_duplicates(self):
        assert deduplicate_ticket_keys(["OOT-123", "OOT-123"]) == ["OOT-123"]

    def test_returns_sorted(self):
        result = deduplicate_ticket_keys(["OOT-456", "OOT-123"])
        assert result == ["OOT-123", "OOT-456"]

    def test_mixed_projects_sorted(self):
        result = deduplicate_ticket_keys(["SSJ-1", "OOT-1"])
        assert result == ["OOT-1", "SSJ-1"]

    def test_empty_input(self):
        assert deduplicate_ticket_keys([]) == []

    def test_already_unique_unchanged(self):
        result = deduplicate_ticket_keys(["OOT-1", "SSJ-2"])
        assert set(result) == {"OOT-1", "SSJ-2"}


class TestExtractProjectKey:
    def test_oot(self):
        assert extract_project_key("OOT-123") == "OOT"

    def test_ssj(self):
        assert extract_project_key("SSJ-4567") == "SSJ"

    def test_devops(self):
        assert extract_project_key("DEVOPS-89") == "DEVOPS"

    def test_dip(self):
        assert extract_project_key("DIP-10") == "DIP"
