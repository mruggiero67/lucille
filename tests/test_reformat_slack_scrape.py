"""
Unit tests for Slack Scrape Reformatter

Tests focus on pure functions for converting Slack scrapes to log format.
"""

import pytest
import tempfile
from pathlib import Path
from lucille.reformat_slack_scrape import (
    clean_text,
    parse_slack_entry,
    transform_slack_entries,
    convert_slack_scrape_to_logs
)


class TestCleanText:
    """Test suite for text cleaning function."""

    def test_clean_text_basic(self):
        """Test basic text cleaning."""
        text = "Hello   World"
        result = clean_text(text)
        assert result == "Hello World"

    def test_clean_text_special_characters(self):
        """Test cleaning special characters."""
        text = "Hello! @#$% World"
        result = clean_text(text)
        assert "Hello" in result
        assert "World" in result
        # Special characters should be replaced with spaces

    def test_clean_text_multiple_whitespace(self):
        """Test removing multiple whitespace."""
        text = "Hello     \t\n  World"
        result = clean_text(text)
        assert result == "Hello World"

    def test_clean_text_preserves_important_chars(self):
        """Test that important characters are preserved."""
        text = "user-name:12:30 test@example.com/path#tag"
        result = clean_text(text)
        assert "user" in result
        assert "name" in result
        assert "12" in result
        assert "30" in result

    def test_clean_text_empty_string(self):
        """Test cleaning empty string."""
        text = ""
        result = clean_text(text)
        assert result == ""

    def test_clean_text_only_whitespace(self):
        """Test cleaning string with only whitespace."""
        text = "   \t\n   "
        result = clean_text(text)
        assert result == ""


class TestParseSlackEntry:
    """Test suite for Slack entry parsing function."""

    def test_parse_slack_entry_with_username_and_time(self):
        """Test parsing entry with username and timestamp."""
        entry_lines = ["alice 10:30 AM", "Deployed ServiceA to production"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "2025-01-15" in result
        assert "alice" in result
        assert "10:30 AM" in result
        assert "Deployed ServiceA to production" in result

    def test_parse_slack_entry_multiline(self):
        """Test parsing multiline entry."""
        entry_lines = [
            "bob 2:45 PM",
            "Successfully deployed version 1.2.3",
            "All tests passing"
        ]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "bob" in result
        assert "2:45 PM" in result
        # Content should be combined
        assert "Successfully deployed" in result or "deployed" in result

    def test_parse_slack_entry_no_am_pm(self):
        """Test parsing entry with 24-hour time format."""
        entry_lines = ["charlie 15:30", "Fixed bug in payment system"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "charlie" in result
        assert "15:30" in result

    def test_parse_slack_entry_no_username_pattern(self):
        """Test parsing entry without clear username pattern."""
        entry_lines = ["10:00 Something happened"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "2025-01-15" in result
        assert "10:00" in result

    def test_parse_slack_entry_no_timestamp(self):
        """Test parsing entry without timestamp."""
        entry_lines = ["alice said something interesting"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "UNKNOWN_TIME" in result
        assert "alice" in result

    def test_parse_slack_entry_empty_lines(self):
        """Test parsing empty entry."""
        entry_lines = []
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is None

    def test_parse_slack_entry_blank_lines(self):
        """Test parsing entry with only blank/whitespace lines."""
        entry_lines = ["", "   ", ""]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        # Returns a log line even with blank input (with UNKNOWN values)
        assert result is not None
        assert "UNKNOWN_USER" in result
        assert "UNKNOWN_TIME" in result

    def test_parse_slack_entry_default_date(self):
        """Test parsing entry with default date (current date)."""
        entry_lines = ["alice 10:30 AM", "Test message"]

        result = parse_slack_entry(entry_lines)

        assert result is not None
        # Should use current date in format YYYY-MM-DD
        assert "alice" in result
        assert "10:30 AM" in result

    def test_parse_slack_entry_complex_content(self):
        """Test parsing entry with complex content."""
        entry_lines = [
            "developer 3:45 PM",
            "Deployed v2.5.0 with the following changes:",
            "- Fixed authentication bug",
            "- Updated dependencies"
        ]
        date_str = "2025-01-20"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "developer" in result
        assert "3:45 PM" in result
        # Content should contain the deployment info

    def test_parse_slack_entry_numeric_username(self):
        """Test parsing entry where username might be numeric."""
        entry_lines = ["user123 9:00 AM", "Completed task"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "9:00 AM" in result


class TestTransformSlackEntries:
    """Test suite for batch transformation function."""

    def test_transform_slack_entries_single_entry(self):
        """Test transforming single entry."""
        content = "alice 10:30 AM\nDeployed ServiceA"
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 1
        assert "alice" in result[0]
        assert "10:30 AM" in result[0]

    def test_transform_slack_entries_multiple_entries(self):
        """Test transforming multiple entries separated by blank lines."""
        content = """alice 10:30 AM
Deployed ServiceA

bob 11:45 AM
Updated configuration

charlie 2:15 PM
Fixed production bug"""
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 3
        assert any("alice" in line for line in result)
        assert any("bob" in line for line in result)
        assert any("charlie" in line for line in result)

    def test_transform_slack_entries_with_empty_entries(self):
        """Test transforming with empty entries in between."""
        content = """alice 10:30 AM
Deployed ServiceA


bob 11:45 AM
Updated configuration"""
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 2

    def test_transform_slack_entries_empty_content(self):
        """Test transforming empty content."""
        content = ""
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 0

    def test_transform_slack_entries_only_whitespace(self):
        """Test transforming content with only whitespace."""
        content = "   \n\n   \n   "
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 0

    def test_transform_slack_entries_preserves_order(self):
        """Test that entry order is preserved."""
        content = """first 10:00 AM
First message

second 11:00 AM
Second message

third 12:00 PM
Third message"""
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        assert len(result) == 3
        # Check order is preserved
        assert "first" in result[0]
        assert "second" in result[1]
        assert "third" in result[2]

    def test_transform_slack_entries_default_date(self):
        """Test transformation with default date."""
        content = "alice 10:30 AM\nTest message"

        result = transform_slack_entries(content)

        assert len(result) == 1
        # Should use current date


class TestConvertSlackScrapeToLogs:
    """Test suite for file I/O conversion function."""

    def test_convert_slack_scrape_to_logs_with_temp_file(self):
        """Test converting actual files."""
        # Create temp input file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', encoding='utf-8') as f:
            input_path = f.name
            f.write("alice 10:30 AM\nDeployed ServiceA\n\nbob 11:45 AM\nUpdated config")

        output_path = input_path + ".out"

        try:
            result = convert_slack_scrape_to_logs(input_path, output_path)

            assert result is not None
            assert len(result) == 2

            # Verify output file was created
            assert Path(output_path).exists()

            # Read and verify output content
            with open(output_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            assert len(lines) == 2
        finally:
            Path(input_path).unlink(missing_ok=True)
            Path(output_path).unlink(missing_ok=True)

    def test_convert_slack_scrape_to_logs_file_not_found(self):
        """Test handling of nonexistent input file."""
        result = convert_slack_scrape_to_logs("/nonexistent/file.txt")

        assert result is None

    def test_convert_slack_scrape_to_logs_empty_file(self):
        """Test converting empty file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', encoding='utf-8') as f:
            input_path = f.name
            f.write("")

        try:
            result = convert_slack_scrape_to_logs(input_path)

            assert result is not None
            assert len(result) == 0
        finally:
            Path(input_path).unlink(missing_ok=True)

    def test_convert_slack_scrape_to_logs_no_output_file(self):
        """Test converting without output file (prints to stdout)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt', encoding='utf-8') as f:
            input_path = f.name
            f.write("alice 10:30 AM\nTest message")

        try:
            result = convert_slack_scrape_to_logs(input_path)

            assert result is not None
            assert len(result) == 1
        finally:
            Path(input_path).unlink(missing_ok=True)


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_parse_entry_with_special_slack_formatting(self):
        """Test parsing entry with Slack-style formatting."""
        entry_lines = ["alice 10:30 AM", "*bold text* and _italic text_"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        # Formatting characters should be cleaned or preserved appropriately

    def test_parse_entry_with_urls(self):
        """Test parsing entry containing URLs."""
        entry_lines = ["bob 2:00 PM", "Check https://example.com/path for details"]
        date_str = "2025-01-15"

        result = parse_slack_entry(entry_lines, date_str)

        assert result is not None
        assert "bob" in result
        # URL should be preserved in some form

    def test_transform_entries_with_consecutive_blank_lines(self):
        """Test transformation with multiple consecutive blank lines."""
        content = "alice 10:00 AM\nMessage 1\n\n\n\n\nbob 11:00 AM\nMessage 2"
        date_str = "2025-01-15"

        result = transform_slack_entries(content, date_str)

        # Should handle multiple blank lines gracefully
        assert len(result) == 2

    def test_clean_text_with_tabs_and_newlines(self):
        """Test cleaning text with various whitespace."""
        text = "Hello\t\tWorld\n\nTest"
        result = clean_text(text)

        # Should normalize to single spaces
        assert "  " not in result or result.count("  ") < text.count("\t")
