"""
Unit tests for Slack Deployment Parser

Tests focus on pure functions for parsing deployment messages from Slack.
"""

import pytest
import csv
import tempfile
from pathlib import Path
from lucille.slack_deploys import SlackDeploymentParser


class TestSlackDeploymentParser:
    """Test suite for SlackDeploymentParser class."""

    def test_initialization(self):
        """Test parser initialization."""
        parser = SlackDeploymentParser()
        assert parser is not None
        assert len(parser.deployment_patterns) > 0
        assert len(parser.time_patterns) > 0
        assert len(parser.date_patterns) > 0

    def test_parse_deployment_line_format1(self):
        """Test parsing format 1: 'YYYY-MM-DD deployed user H:MM AM/PM Service version released'."""
        parser = SlackDeploymentParser()
        line = "2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-05-20"
        assert result["user"] == "jakub"
        assert result["time"] == "6:40 AM"
        assert result["service"] == "BankingInsights"
        assert result["version"] == "1.55.0"
        assert result["timestamp"] == "2025-05-20 6:40 AM"

    def test_parse_deployment_line_format2_with_github_url(self):
        """Test parsing format 2 with GitHub URL."""
        parser = SlackDeploymentParser()
        line = "2025-05-20 bryan 3:45 PM New release at https://github.com/jarisdev/PartnerGateway Release - 4.26.3"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-05-20"
        assert result["user"] == "bryan"
        assert result["service"] == "PartnerGateway"
        assert result["version"] == "4.26.3"

    def test_parse_deployment_line_format2_with_service_version(self):
        """Test parsing format 2 with service and version pattern."""
        parser = SlackDeploymentParser()
        line = "2025-06-10 michael 10:30 AM Ledger 2.15.0 deployed successfully"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-06-10"
        assert result["user"] == "michael"
        assert result["service"] == "Ledger"
        assert result["version"] == "2.15.0"

    def test_parse_deployment_line_format3_with_parenthetical_time(self):
        """Test parsing format 3: 'YYYY-MM-DD User (H:MM AM/PM): message'."""
        parser = SlackDeploymentParser()
        line = "2025-10-24 GitHub (2:00 AM): New release published - Settlement 3.8.1"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-10-24"
        assert result["user"] == "GitHub"
        assert result["time"] == "2:00 AM"
        assert result["service"] == "Settlement"
        assert result["version"] == "3.8.1"

    def test_parse_deployment_line_format3_github_url(self):
        """Test parsing format 3 with GitHub URL."""
        parser = SlackDeploymentParser()
        line = "2025-01-23 GitHub (10:21 AM) https://github.com/jarisdev/PaymentScheduler Release - 1.2.0"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["service"] == "PaymentScheduler"
        assert result["version"] == "1.2.0"

    def test_parse_deployment_line_format4_no_meridiem(self):
        """Test parsing format 4: 'YYYY-MM-DD user H:MM Service version' (no AM/PM)."""
        parser = SlackDeploymentParser()
        line = "2025-06-12 bryan 2:15 PartnerGateway 4.26.3"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-06-12"
        assert result["user"] == "bryan"
        assert result["time"] == "2:15"
        assert result["service"] == "PartnerGateway"
        assert result["version"] == "4.26.3"

    def test_parse_deployment_line_fallback_with_keywords(self):
        """Test fallback parsing when deployment keywords are present."""
        parser = SlackDeploymentParser()
        line = "2025-07-15 alice 9:30 AM Successfully deployed ApplicationSession 2.0.1"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert result["date"] == "2025-07-15"
        assert result["service"] == "ApplicationSession"
        assert result["version"] == "2.0.1"

    def test_parse_deployment_line_invalid(self):
        """Test parsing truly invalid line without deployment keywords."""
        parser = SlackDeploymentParser()
        line = "Just a random message with no relevant info"

        result = parser._parse_deployment_line(line)

        assert result is None

    def test_parse_deployment_line_empty(self):
        """Test parsing empty line returns None."""
        parser = SlackDeploymentParser()
        line = ""

        result = parser._parse_deployment_line(line)

        assert result is None

    def test_parse_slack_export_multiple_lines(self):
        """Test parsing multiple deployment messages."""
        parser = SlackDeploymentParser()
        content = """2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released
2025-06-12 bryan 2:15 PartnerGateway 4.26.3
2025-07-01 alice 3:30 PM Ledger 2.1.0 deployed"""

        deployments = parser.parse_slack_export(content)

        assert len(deployments) == 3
        assert deployments[0]["service"] == "BankingInsights"
        assert deployments[1]["service"] == "PartnerGateway"
        assert deployments[2]["service"] == "Ledger"

    def test_parse_slack_export_with_empty_lines(self):
        """Test parsing content with empty lines."""
        parser = SlackDeploymentParser()
        content = """2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released

2025-06-12 bryan 2:15 PartnerGateway 4.26.3

"""

        deployments = parser.parse_slack_export(content)

        assert len(deployments) == 2

    def test_parse_slack_export_empty_content(self):
        """Test parsing empty content."""
        parser = SlackDeploymentParser()
        content = ""

        deployments = parser.parse_slack_export(content)

        assert len(deployments) == 0

    def test_parse_slack_export_no_valid_deployments(self):
        """Test parsing content with no valid deployment lines."""
        parser = SlackDeploymentParser()
        content = """Just some chat messages
Nothing relevant here
Random conversation"""

        deployments = parser.parse_slack_export(content)

        assert len(deployments) == 0


class TestDeploymentAnalysis:
    """Test suite for deployment analysis functions."""

    def test_analyze_deployments_basic(self):
        """Test basic deployment analysis."""
        parser = SlackDeploymentParser()
        deployments = [
            {
                "date": "2025-01-01",
                "user": "alice",
                "service": "ServiceA",
                "version": "1.0.0",
                "time": "10:00 AM",
                "timestamp": "2025-01-01 10:00 AM",
                "raw_message": "test"
            },
            {
                "date": "2025-01-01",
                "user": "bob",
                "service": "ServiceB",
                "version": "2.0.0",
                "time": "11:00 AM",
                "timestamp": "2025-01-01 11:00 AM",
                "raw_message": "test"
            },
            {
                "date": "2025-01-02",
                "user": "alice",
                "service": "ServiceA",
                "version": "1.0.1",
                "time": "09:00 AM",
                "timestamp": "2025-01-02 09:00 AM",
                "raw_message": "test"
            },
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["total_deployments"] == 3
        assert analysis["unique_services"] == 2
        assert analysis["days_with_deployments"] == 2
        assert analysis["avg_deployments_per_day"] == 1.5
        assert analysis["service_breakdown"]["ServiceA"] == 2
        assert analysis["service_breakdown"]["ServiceB"] == 1
        assert analysis["user_breakdown"]["alice"] == 2
        assert analysis["user_breakdown"]["bob"] == 1

    def test_analyze_deployments_most_deployed_service(self):
        """Test identification of most deployed service."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "user": "alice", "service": "ServiceA", "version": "1.0.0", "time": "10:00 AM", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test"},
            {"date": "2025-01-01", "user": "bob", "service": "ServiceA", "version": "1.0.1", "time": "11:00 AM", "timestamp": "2025-01-01 11:00 AM", "raw_message": "test"},
            {"date": "2025-01-02", "user": "charlie", "service": "ServiceA", "version": "1.0.2", "time": "09:00 AM", "timestamp": "2025-01-02 09:00 AM", "raw_message": "test"},
            {"date": "2025-01-02", "user": "alice", "service": "ServiceB", "version": "2.0.0", "time": "10:00 AM", "timestamp": "2025-01-02 10:00 AM", "raw_message": "test"},
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["most_deployed_service"][0] == "ServiceA"
        assert analysis["most_deployed_service"][1] == 3

    def test_analyze_deployments_busiest_day(self):
        """Test identification of busiest deployment day."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "user": "alice", "service": "ServiceA", "version": "1.0.0", "time": "10:00 AM", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test"},
            {"date": "2025-01-01", "user": "bob", "service": "ServiceB", "version": "1.0.1", "time": "11:00 AM", "timestamp": "2025-01-01 11:00 AM", "raw_message": "test"},
            {"date": "2025-01-01", "user": "charlie", "service": "ServiceC", "version": "1.0.2", "time": "12:00 PM", "timestamp": "2025-01-01 12:00 PM", "raw_message": "test"},
            {"date": "2025-01-02", "user": "alice", "service": "ServiceA", "version": "2.0.0", "time": "10:00 AM", "timestamp": "2025-01-02 10:00 AM", "raw_message": "test"},
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["busiest_day"][0] == "2025-01-01"
        assert analysis["busiest_day"][1] == 3

    def test_analyze_deployments_empty(self):
        """Test analysis of empty deployment list."""
        parser = SlackDeploymentParser()
        deployments = []

        analysis = parser.analyze_deployments(deployments)

        assert "error" in analysis
        assert analysis["error"] == "No deployments found"

    def test_analyze_deployments_single_deployment(self):
        """Test analysis with single deployment."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "user": "alice", "service": "ServiceA", "version": "1.0.0", "time": "10:00 AM", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test"},
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["total_deployments"] == 1
        assert analysis["unique_services"] == 1
        assert analysis["avg_deployments_per_day"] == 1.0

    def test_analyze_deployments_daily_breakdown(self):
        """Test daily breakdown calculation."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "user": "alice", "service": "ServiceA", "version": "1.0.0", "time": "10:00 AM", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test"},
            {"date": "2025-01-01", "user": "bob", "service": "ServiceB", "version": "1.0.1", "time": "11:00 AM", "timestamp": "2025-01-01 11:00 AM", "raw_message": "test"},
            {"date": "2025-01-03", "user": "charlie", "service": "ServiceC", "version": "1.0.2", "time": "09:00 AM", "timestamp": "2025-01-03 09:00 AM", "raw_message": "test"},
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["daily_breakdown"]["2025-01-01"] == 2
        assert analysis["daily_breakdown"]["2025-01-03"] == 1
        assert "2025-01-02" not in analysis["daily_breakdown"]


class TestCSVExport:
    """Test suite for CSV export functionality."""

    def test_save_to_csv_basic(self):
        """Test saving deployments to CSV file."""
        parser = SlackDeploymentParser()
        deployments = [
            {
                "date": "2025-01-01",
                "time": "10:00 AM",
                "user": "alice",
                "service": "ServiceA",
                "version": "1.0.0",
                "timestamp": "2025-01-01 10:00 AM",
                "raw_message": "test deployment"
            },
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name

        try:
            parser.save_to_csv(deployments, temp_path)

            # Verify file was created and contains correct data
            with open(temp_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 1
            assert rows[0]["service"] == "ServiceA"
            assert rows[0]["user"] == "alice"
            assert rows[0]["version"] == "1.0.0"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_save_to_csv_multiple_rows(self):
        """Test saving multiple deployments to CSV."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "time": "10:00 AM", "user": "alice", "service": "ServiceA", "version": "1.0.0", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test1"},
            {"date": "2025-01-02", "time": "11:00 AM", "user": "bob", "service": "ServiceB", "version": "2.0.0", "timestamp": "2025-01-02 11:00 AM", "raw_message": "test2"},
            {"date": "2025-01-03", "time": "12:00 PM", "user": "charlie", "service": "ServiceC", "version": "3.0.0", "timestamp": "2025-01-03 12:00 PM", "raw_message": "test3"},
        ]

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name

        try:
            parser.save_to_csv(deployments, temp_path)

            with open(temp_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            assert len(rows) == 3
            assert rows[0]["service"] == "ServiceA"
            assert rows[1]["service"] == "ServiceB"
            assert rows[2]["service"] == "ServiceC"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_save_to_csv_empty_list(self):
        """Test saving empty deployment list."""
        parser = SlackDeploymentParser()
        deployments = []

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name

        try:
            # Should not raise an error, just log a message
            parser.save_to_csv(deployments, temp_path)
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_parse_line_with_multi_word_username(self):
        """Test parsing line with multi-word username."""
        parser = SlackDeploymentParser()
        line = "2025-05-20 John Doe 6:40 AM ServiceName 1.0.0 deployed"

        result = parser._parse_deployment_line(line)

        assert result is not None
        assert "John" in result["user"] or "Doe" in result["user"]

    def test_parse_line_with_special_characters_in_service(self):
        """Test parsing service names with hyphens and underscores."""
        parser = SlackDeploymentParser()
        line = "2025-06-12 bryan 2:15 partner-backend 4.26.3"

        result = parser._parse_deployment_line(line)

        assert result is not None
        # Should handle hyphenated service names

    def test_parse_line_with_version_prefix_v(self):
        """Test parsing versions with 'v' prefix."""
        parser = SlackDeploymentParser()
        line = "2025-07-01 alice 10:30 AM Ledger v2.15.0 released"

        result = parser._parse_deployment_line(line)

        assert result is not None
        # Version should be captured with or without 'v'

    def test_analyze_deployments_with_missing_dates(self):
        """Test analysis when some deployments have missing dates."""
        parser = SlackDeploymentParser()
        deployments = [
            {"date": "2025-01-01", "user": "alice", "service": "ServiceA", "version": "1.0.0", "time": "10:00 AM", "timestamp": "2025-01-01 10:00 AM", "raw_message": "test"},
            {"date": None, "user": "bob", "service": "ServiceB", "version": "2.0.0", "time": "11:00 AM", "timestamp": "unknown", "raw_message": "test"},
        ]

        analysis = parser.analyze_deployments(deployments)

        assert analysis["total_deployments"] == 2
        assert analysis["days_with_deployments"] == 1

    def test_parse_slack_export_with_whitespace_lines(self):
        """Test parsing content with whitespace-only lines."""
        parser = SlackDeploymentParser()
        content = """2025-05-20 deployed jakub 6:40 AM BankingInsights 1.55.0 released

\t
2025-06-12 bryan 2:15 PartnerGateway 4.26.3"""

        deployments = parser.parse_slack_export(content)

        assert len(deployments) == 2
