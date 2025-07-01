"""
Unit tests for Jira Lead Time Baseline Calculator
Run with: pytest test_jira_lead_time_analyzer.py -v
"""

import pytest
from unittest.mock import Mock, patch, mock_open
from datetime import datetime, timedelta
import json
import csv
import io
from pathlib import Path

# Import the module under test
from context import lucille
from lucille.jira_lead_time_baseline_calculator import (
    JiraLeadTimeAnalyzer,
    load_config,
    create_sample_config,
)


class TestJiraLeadTimeAnalyzer:
    """Test cases for JiraLeadTimeAnalyzer class."""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration for testing."""
        return {
            "jira": {
                "base_url": "https://test.atlassian.net",
                "username": "test@example.com",
                "api_token": "test_token",
            },
            "epic_keys": ["PROJ-123", "PROJ-456"],
            "days_back": 30,
            "done_statuses": ["Done", "Closed"],
            "development_statuses": ["In Development", "In Progress"],
            "output_directory": "./test_output",
        }

    @pytest.fixture
    def analyzer(self, sample_config):
        """Create analyzer instance for testing."""
        return JiraLeadTimeAnalyzer(sample_config)

    @pytest.fixture
    def sample_jira_story(self):
        """Sample Jira story data for testing."""
        return {
            "key": "PROJ-101",
            "epic_key": "PROJ-123",
            "fields": {
                "summary": "Test Story",
                "issuetype": {"name": "Story"},
                "status": {"name": "Done"},
                "assignee": {"displayName": "John Doe"},
                "priority": {"name": "High"},
                "customfield_10016": 5,  # story points
                "created": "2025-06-01T10:00:00.000Z",
                "resolutiondate": "2025-06-15T16:00:00.000Z",
            },
            "changelog": {
                "histories": [
                    {
                        "created": "2025-06-02T09:00:00.000Z",
                        "author": {"displayName": "Jane Smith"},
                        "items": [
                            {
                                "field": "status",
                                "fromString": "To Do",
                                "toString": "In Development",
                            }
                        ],
                    },
                    {
                        "created": "2025-06-15T16:00:00.000Z",
                        "author": {"displayName": "John Doe"},
                        "items": [
                            {
                                "field": "status",
                                "fromString": "In Development",
                                "toString": "Done",
                            }
                        ],
                    },
                ]
            },
        }

    def test_init(self, sample_config):
        """Test analyzer initialization."""
        analyzer = JiraLeadTimeAnalyzer(sample_config)

        assert analyzer.base_url == "https://test.atlassian.net"
        assert analyzer.username == "test@example.com"
        assert analyzer.api_token == "test_token"
        assert analyzer.epic_keys == ["PROJ-123", "PROJ-456"]
        assert analyzer.days_back == 30
        assert analyzer.done_statuses == ["DONE", "CLOSED"]
        assert analyzer.dev_statuses == ["IN DEVELOPMENT", "IN PROGRESS"]
        assert "Authorization" in analyzer.headers
        assert analyzer.headers["Accept"] == "application/json"

    def test_parse_datetime_formats(self, analyzer):
        """Test parsing various datetime formats."""
        # Test Z format
        dt1 = analyzer._parse_datetime("2025-06-01T10:00:00.000Z")
        assert dt1 is not None
        assert dt1.year == 2025
        assert dt1.month == 6
        assert dt1.day == 1

        # Test timezone format
        dt2 = analyzer._parse_datetime("2025-06-01T10:00:00.000-0700")
        assert dt2 is not None

        # Test invalid format
        dt3 = analyzer._parse_datetime("invalid-date")
        assert dt3 is None

        # Test None input
        dt4 = analyzer._parse_datetime(None)
        assert dt4 is None

    def test_extract_status_timeline(self, analyzer, sample_jira_story):
        """Test extracting status timeline from changelog."""
        changelog = sample_jira_story["changelog"]["histories"]
        timeline = analyzer._extract_status_timeline(changelog)

        assert len(timeline) == 2
        assert timeline[0]["from_status"] == "To Do"
        assert timeline[0]["to_status"] == "In Development"
        assert timeline[1]["from_status"] == "In Development"
        assert timeline[1]["to_status"] == "Done"
        assert timeline[0]["author"] == "Jane Smith"

    def test_find_first_dev_start(self, analyzer):
        """Test finding first development start date."""
        timeline = [
            {"date": datetime(2025, 6, 1), "to_status": "To Do", "from_status": None},
            {
                "date": datetime(2025, 6, 2),
                "to_status": "In Development",
                "from_status": "To Do",
            },
            {
                "date": datetime(2025, 6, 10),
                "to_status": "In Progress",
                "from_status": "In Development",
            },
        ]

        first_dev = analyzer._find_first_dev_start(timeline)
        assert first_dev == datetime(2025, 6, 2)

    def test_find_last_dev_start(self, analyzer):
        """Test finding last development start date."""
        timeline = [
            {
                "date": datetime(2025, 6, 2),
                "to_status": "In Development",
                "from_status": "To Do",
            },
            {
                "date": datetime(2025, 6, 5),
                "to_status": "Code Review",
                "from_status": "In Development",
            },
            {
                "date": datetime(2025, 6, 10),
                "to_status": "In Progress",  # Back to dev
                "from_status": "Code Review",
            },
        ]

        last_dev = analyzer._find_last_dev_start(timeline)
        assert last_dev == datetime(2025, 6, 10)

    def test_calculate_lead_times(self, analyzer):
        """Test lead time calculations."""
        timestamps = {
            "created_date": datetime(2025, 6, 1),
            "first_dev_start": datetime(2025, 6, 2),
            "last_dev_start": datetime(2025, 6, 10),
            "resolved_date": datetime(2025, 6, 15),
        }

        lead_times = analyzer._calculate_lead_times(timestamps)

        assert lead_times["total_lead_time"] is not None
        assert lead_times["dev_lead_time"] is not None
        assert lead_times["time_to_dev"] is not None
        assert lead_times["pure_dev_time"] is not None

        # Check that total > dev time
        assert lead_times["total_lead_time"] > lead_times["dev_lead_time"]

    def test_calculate_lead_times_with_none_values(self, analyzer):
        """Test lead time calculations with None values."""
        timestamps = {
            "created_date": datetime(2025, 6, 1),
            "first_dev_start": None,
            "last_dev_start": None,
            "resolved_date": datetime(2025, 6, 15),
        }

        lead_times = analyzer._calculate_lead_times(timestamps)

        assert lead_times["total_lead_time"] is not None
        assert lead_times["dev_lead_time"] is None
        assert lead_times["time_to_dev"] is None
        assert lead_times["pure_dev_time"] is None

    def test_parse_story_timeline(self, analyzer, sample_jira_story):
        """Test parsing complete story timeline."""
        result = analyzer.parse_story_timeline(sample_jira_story)

        assert result["key"] == "PROJ-101"
        assert result["epic_key"] == "PROJ-123"
        assert result["summary"] == "Test Story"
        assert result["issue_type"] == "Story"
        assert result["assignee"] == "John Doe"
        assert result["story_points"] == 5
        assert result["created_date"] is not None
        assert result["resolved_date"] is not None
        assert result["first_dev_start"] is not None
        assert result["total_lead_time"] is not None
        assert len(result["timeline"]) == 2

    def test_percentile_calculation(self, analyzer):
        """Test percentile calculation."""
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        p50 = analyzer._percentile(values, 50)
        p95 = analyzer._percentile(values, 95)

        assert p50 == 5.5  # Median
        assert p95 > p50  # 95th percentile should be higher

    @patch("requests.get")
    def test_get_completed_stories_empty_epics(self, mock_get, sample_config):
        """Test with empty epic keys."""
        sample_config["epic_keys"] = []
        analyzer = JiraLeadTimeAnalyzer(sample_config)

        stories = analyzer.get_completed_stories()
        assert stories == []
        mock_get.assert_not_called()

    def test_analyze_lead_times(self, analyzer):
        """Test lead time analysis with sample data."""
        stories = [
            {
                "key": "PROJ-101",
                "epic_key": "PROJ-123",
                "fields": {
                    "summary": "Test Story 1",
                    "issuetype": {"name": "Story"},
                    "status": {"name": "Done"},
                    "assignee": {"displayName": "John Doe"},
                    "priority": {"name": "High"},
                    "customfield_10016": 3,
                    "created": "2025-06-01T10:00:00.000Z",
                    "resolutiondate": "2025-06-10T16:00:00.000Z",
                },
                "changelog": {"histories": []},
            },
            {
                "key": "PROJ-102",
                "epic_key": "PROJ-123",
                "fields": {
                    "summary": "Test Story 2",
                    "issuetype": {"name": "Story"},
                    "status": {"name": "Done"},
                    "assignee": {"displayName": "Ted Foo"},
                    "priority": {"name": "High"},
                    "customfield_10016": 3,
                    "created": "2025-06-01T10:00:00.000Z",
                    "resolutiondate": "2025-06-10T16:00:00.000Z",
                },
                "changelog": {"histories": []},
            }
        ]

        analysis = analyzer.analyze_lead_times(stories)

        assert "stories" in analysis
        assert "metrics" in analysis
        assert "epic_breakdown" in analysis
        assert "epic_metrics" in analysis
        assert analysis["total_stories"] == 2
        assert analysis["epics_analyzed"] == 1
        assert "PROJ-123" in analysis["epic_breakdown"]

    @patch("builtins.open", new_callable=mock_open)
    @patch("pathlib.Path.mkdir")
    def test_save_detailed_csv(self, mock_mkdir, mock_file, analyzer):
        """Test saving detailed CSV output."""
        analysis = {
            "stories": [
                {
                    "epic_key": "PROJ-123",
                    "key": "PROJ-101",
                    "summary": "Test Story",
                    "issue_type": "Story",
                    "assignee": "John Doe",
                    "priority": "High",
                    "story_points": 3,
                    "created_date": datetime(2025, 6, 1),
                    "resolved_date": datetime(2025, 6, 10),
                    "first_dev_start": datetime(2025, 6, 2),
                    "last_dev_start": datetime(2025, 6, 2),
                    "total_lead_time": 9.0,
                    "dev_lead_time": 8.0,
                    "time_to_dev": 1.0,
                    "pure_dev_time": 8.0,
                    "final_status": "Done",
                    "created": datetime(2025, 6, 1),
                    "resolved": datetime(2025, 6, 10),
                    "timeline": [],
                }
            ]
        }

        filepath = analyzer.save_detailed_csv(analysis, "test.csv")

        mock_mkdir.assert_called_once()
        mock_file.assert_called_once()
        assert filepath.endswith("test.csv")

    def test_print_analysis(self, analyzer, capsys):
        """Test printing analysis summary."""
        analysis = {
            "analysis_period": "Last 30 days",
            "total_stories": 5,
            "epics_analyzed": 2,
            "epic_metrics": {
                "PROJ-123": {"story_count": 3, "median_dev_time": 5.0},
                "PROJ-456": {"story_count": 2, "median_dev_time": 7.0},
            },
            "metrics": {
                "total_lead_time": {
                    "count": 5,
                    "median": 8.0,
                    "mean": 8.5,
                    "percentile_95": 12.0,
                    "percentile_85": 10.0,
                },
                "dev_lead_time": {
                    "count": 5,
                    "median": 6.0,
                    "mean": 6.5,
                    "percentile_95": 10.0,
                    "percentile_85": 8.0,
                },
            },
        }

        analyzer.print_analysis(analysis)
        captured = capsys.readouterr()

        assert "LEAD TIME BASELINE ANALYSIS" in captured.out
        assert "Total Stories Analyzed: 5" in captured.out
        assert "Epics Analyzed: 2" in captured.out
        assert "PROJ-123: 3 stories" in captured.out


class TestConfigurationFunctions:
    """Test configuration-related functions."""

    def test_load_config_success(self):
        """Test successful config loading."""
        config_data = {
            "jira": {
                "base_url": "https://test.atlassian.net",
                "username": "test@example.com",
                "api_token": "test_token",
            }
        }

        with patch(
            "builtins.open",
            mock_open(read_data="jira:\n  base_url: https://test.atlassian.net"),
        ):
            with patch("yaml.safe_load", return_value=config_data):
                config = load_config("test_config.yaml")

                assert config["jira"]["base_url"] == "https://test.atlassian.net"

    def test_load_config_file_not_found(self):
        """Test config loading with missing file."""
        with patch("builtins.open", side_effect=FileNotFoundError):
            with pytest.raises(SystemExit):
                load_config("missing_config.yaml")

    @patch("builtins.open", new_callable=mock_open)
    @patch("yaml.dump")
    def test_create_sample_config(self, mock_yaml_dump, mock_file):
        """Test creating sample configuration."""
        create_sample_config("sample_config.yaml")

        mock_file.assert_called_once_with("sample_config.yaml", "w")
        mock_yaml_dump.assert_called_once()

        # Check that the dumped config has expected structure
        dumped_config = mock_yaml_dump.call_args[0][0]
        assert "jira" in dumped_config
        assert "epic_keys" in dumped_config
        assert "days_back" in dumped_config


class TestEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def analyzer(self):
        config = {
            "jira": {
                "base_url": "https://test.atlassian.net",
                "username": "test",
                "api_token": "token",
            },
            "epic_keys": ["PROJ-123"],
            "days_back": 30,
            "done_statuses": ["Done"],
            "development_statuses": ["In Development"],
            "output_directory": "./test",
        }
        return JiraLeadTimeAnalyzer(config)

    def test_empty_stories_analysis(self, analyzer):
        """Test analysis with empty stories list."""
        analysis = analyzer.analyze_lead_times([])

        assert analysis["total_stories"] == 0
        assert analysis["epics_analyzed"] == 0
        assert len(analysis["stories"]) == 0

    def test_story_without_changelog(self, analyzer):
        """Test parsing story without changelog."""
        story = {
            "key": "PROJ-101",
            "epic_key": "PROJ-123",
            "fields": {
                "summary": "Test Story",
                "issuetype": {"name": "Story"},
                "status": {"name": "Done"},
                "created": "2025-06-01T10:00:00.000Z",
                "resolutiondate": "2025-06-10T16:00:00.000Z",
            },
            # No changelog
        }

        result = analyzer.parse_story_timeline(story)

        assert result["key"] == "PROJ-101"
        assert result["first_dev_start"] is None
        assert result["dev_lead_time"] is None
        assert len(result["timeline"]) == 0

    def test_story_with_missing_fields(self, analyzer):
        """Test parsing story with missing optional fields."""
        story = {
            "key": "PROJ-101",
            "epic_key": "PROJ-123",
            "fields": {
                "summary": "Test Story",
                "issuetype": {"name": "Story"},
                "status": {"name": "Done"},
                "created": "2025-06-01T10:00:00.000Z",
                # Missing assignee, priority, story_points, resolutiondate
            },
            "changelog": {"histories": []},
        }

        result = analyzer.parse_story_timeline(story)

        assert result["key"] == "PROJ-101"
        assert result["assignee"] == "Unassigned"
        assert result["priority"] == "Unknown"
        assert result["story_points"] is None
        assert result["resolved_date"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
