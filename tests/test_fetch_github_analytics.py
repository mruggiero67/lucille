#!/usr/bin/env python3
"""
Unit tests for fetch_github_analytics.py
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import json
import csv
import tempfile
import os
from datetime import datetime, timedelta
from io import StringIO
import requests

# Import the classes we want to test
from context import lucille
from lucille.fetch_github_analytics import (
    GitHubMetricsExtractor,
    MultiRepoMetricsCollector,
    load_config,
)

class TestGitHubMetricsExtractor(unittest.TestCase):
    """Test GitHubMetricsExtractor class"""

    def setUp(self):
        """Set up test fixtures"""
        self.extractor = GitHubMetricsExtractor("test_token", "test-org", "test-repo")
        self.sample_commit = {
            "sha": "abc123",
            "commit": {
                "author": {
                    "name": "Test Author",
                    "email": "test@example.com",
                    "date": "2025-01-01T12:00:00Z",
                },
                "committer": {
                    "name": "Test Committer",
                    "email": "committer@example.com",
                    "date": "2025-01-01T12:00:00Z",
                },
                "message": "Test commit message",
            },
            "stats": {"additions": 10, "deletions": 5, "total": 15},
        }

    def test_initialization(self):
        """Test extractor initialization"""
        self.assertEqual(self.extractor.token, "test_token")
        self.assertEqual(self.extractor.org, "test-org")
        self.assertEqual(self.extractor.repo, "test-repo")
        self.assertEqual(self.extractor.base_url, "https://api.github.com")
        self.assertIn("Authorization", self.extractor.headers)

    @patch("requests.Session.get")
    def test_make_request_success(self, mock_get):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"test": "data"}
        mock_get.return_value = mock_response

        response = self.extractor._make_request("https://api.github.com/test")

        self.assertEqual(response.status_code, 200)
        mock_get.assert_called_once()

    @patch("requests.Session.get")
    @patch("time.sleep")
    def test_make_request_rate_limit(self, mock_sleep, mock_get):
        """Test API request with rate limiting"""
        # First response: rate limited
        rate_limited_response = Mock()
        rate_limited_response.status_code = 403
        rate_limited_response.headers = {
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(
                int((datetime.now() + timedelta(seconds=10)).timestamp())
            ),
        }

        # Second response: successful
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"test": "data"}

        mock_get.side_effect = [rate_limited_response, success_response]

        response = self.extractor._make_request("https://api.github.com/test")

        self.assertEqual(response.status_code, 200)
        mock_sleep.assert_called_once()
        self.assertEqual(mock_get.call_count, 2)

    @patch("requests.Session.get")
    def test_paginated_request(self, mock_get):
        """Test paginated API requests"""
        # First page
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = [{"id": 1}, {"id": 2}]
        page1_response.headers = {
            "Link": '<https://api.github.com/test?page=2>; rel="next"'
        }

        # Second page
        page2_response = Mock()
        page2_response.status_code = 200
        page2_response.json.return_value = [{"id": 3}]
        page2_response.headers = {
            "Link": '<https://api.github.com/test?page=1>; rel="prev"'
        }

        mock_get.side_effect = [page1_response, page2_response]

        result = self.extractor._paginated_request("https://api.github.com/test")

        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[2]["id"], 3)

    def test_parse_github_date_z_format(self):
        """Test parsing GitHub date with Z format"""
        date_str = "2025-01-01T12:00:00Z"
        parsed_date = self.extractor._parse_github_date(date_str)

        self.assertIsInstance(parsed_date, datetime)
        self.assertEqual(parsed_date.year, 2025)
        self.assertEqual(parsed_date.month, 1)
        self.assertEqual(parsed_date.day, 1)

    def test_parse_github_date_iso_format(self):
        """Test parsing GitHub date with ISO format"""
        date_str = "2025-01-01T12:00:00+00:00"
        parsed_date = self.extractor._parse_github_date(date_str)

        self.assertIsInstance(parsed_date, datetime)
        self.assertEqual(parsed_date.year, 2025)

    def test_parse_github_date_invalid(self):
        """Test parsing invalid date falls back gracefully"""
        date_str = "invalid-date"
        parsed_date = self.extractor._parse_github_date(date_str)

        # Should return current time as fallback
        self.assertIsInstance(parsed_date, datetime)

    @patch.object(GitHubMetricsExtractor, "_paginated_request")
    def test_get_commits(self, mock_paginated):
        """Test getting commits"""
        mock_paginated.return_value = [self.sample_commit]
        since_date = datetime(2025, 1, 1)

        commits = self.extractor.get_commits(since_date)

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0]["sha"], "abc123")
        mock_paginated.assert_called_once()

    @patch.object(GitHubMetricsExtractor, "_paginated_request")
    def test_get_pull_requests(self, mock_paginated):
        """Test getting pull requests"""
        sample_pr = {
            "number": 123,
            "title": "Test PR",
            "updated_at": "2025-01-01T12:00:00Z",
            "user": {"login": "testuser"},
        }
        mock_paginated.return_value = [sample_pr]
        since_date = datetime(2025, 1, 1)

        prs = self.extractor.get_pull_requests(since_date)

        self.assertEqual(len(prs), 1)
        self.assertEqual(prs[0]["number"], 123)

    def test_export_to_csv_commits(self):
        """Test CSV export for commits"""
        metrics = {"commits": [self.sample_commit]}

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_files = self.extractor.export_to_csv(metrics, temp_dir)

            self.assertIn("commits", csv_files)

            # Check if CSV file was created and has content
            commits_file = csv_files["commits"]
            self.assertTrue(os.path.exists(commits_file))

            with open(commits_file, "r") as f:
                reader = csv.reader(f)
                rows = list(reader)

                # Should have header + 1 data row
                self.assertEqual(len(rows), 2)
                self.assertIn("sha", rows[0])  # Header check
                self.assertEqual(rows[1][1], "abc123")  # SHA check

    def test_export_to_csv_pull_requests(self):
        """Test CSV export for pull requests"""
        sample_pr = {
            "number": 123,
            "title": "Test PR",
            "state": "merged",
            "user": {"login": "testuser"},
            "created_at": "2025-01-01T12:00:00Z",
            "updated_at": "2025-01-01T12:00:00Z",
            "closed_at": "2025-01-01T13:00:00Z",
            "merged_at": "2025-01-01T13:00:00Z",
            "merge_commit_sha": "def456",
            "additions": 20,
            "deletions": 10,
            "changed_files": 3,
            "commits": 2,
        }

        metrics = {"pull_requests": [sample_pr]}

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_files = self.extractor.export_to_csv(metrics, temp_dir)

            self.assertIn("pull_requests", csv_files)

            prs_file = csv_files["pull_requests"]
            self.assertTrue(os.path.exists(prs_file))

            with open(prs_file, "r") as f:
                reader = csv.reader(f)
                rows = list(reader)

                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[1][1], "123")  # PR number

    def test_export_to_csv_empty_metrics(self):
        """Test CSV export with empty metrics"""
        metrics = {}

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_files = self.extractor.export_to_csv(metrics, temp_dir)

            # Should return empty dict for empty metrics
            self.assertEqual(csv_files, {})


class TestMultiRepoMetricsCollector(unittest.TestCase):
    """Test MultiRepoMetricsCollector class"""

    def setUp(self):
        """Set up test fixtures"""
        self.collector = MultiRepoMetricsCollector("test_token")
        self.sample_result = {
            "repo": "test-org/test-repo",
            "repo_config": {"org": "test-org", "repo": "test-repo"},
            "commits": [
                {
                    "sha": "abc123",
                    "commit": {
                        "author": {"name": "Test Author"},
                        "committer": {"name": "Test Committer"},
                    },
                }
            ],
            "deployments": [],
            "releases": [],
            "pull_requests": [],
            "workflow_runs": [],
            "date_range": {
                "since": "2025-01-01T00:00:00",
                "until": "2025-07-01T00:00:00",
            },
        }

    def test_initialization(self):
        """Test collector initialization"""
        self.assertEqual(self.collector.token, "test_token")
        self.assertEqual(self.collector.results, [])

    @patch.object(GitHubMetricsExtractor, "collect_all_metrics")
    @patch.object(GitHubMetricsExtractor, "export_to_csv")
    @patch("time.sleep")
    def test_collect_from_repos_success(self, mock_sleep, mock_export, mock_collect):
        """Test successful collection from multiple repos"""
        mock_collect.return_value = {"commits": [], "deployments": [], "releases": []}
        mock_export.return_value = {"commits": "test.csv"}

        repo_configs = [
            {"org": "org1", "repo": "repo1"},
            {"org": "org2", "repo": "repo2"},
        ]

        results = self.collector.collect_from_repos(repo_configs)

        self.assertEqual(len(results), 2)
        self.assertEqual(mock_collect.call_count, 2)
        self.assertEqual(mock_export.call_count, 2)

    @patch.object(GitHubMetricsExtractor, "collect_all_metrics")
    def test_collect_from_repos_with_error(self, mock_collect):
        """Test collection with one repo failing"""
        # First call succeeds, second fails
        mock_collect.side_effect = [
            {"commits": [], "deployments": []},
            Exception("API Error"),
        ]

        repo_configs = [
            {"org": "org1", "repo": "repo1"},
            {"org": "org2", "repo": "repo2"},
        ]

        results = self.collector.collect_from_repos(repo_configs)

        # Should only have 1 successful result
        self.assertEqual(len(results), 1)

    def test_create_summary_csvs(self):
        """Test creating summary CSV files"""
        self.collector.results = [self.sample_result]

        with tempfile.TemporaryDirectory() as temp_dir:
            summary_files = self.collector.create_summary_csvs(temp_dir)

            # Should create multiple summary files
            expected_files = [
                "commits",
                "deployments",
                "releases",
                "repository_summary",
            ]
            for file_type in expected_files:
                self.assertIn(file_type, summary_files)
                self.assertTrue(os.path.exists(summary_files[file_type]))

    def test_create_summary_csvs_no_results(self):
        """Test creating summary CSVs with no results"""
        summary_files = self.collector.create_summary_csvs()
        self.assertEqual(summary_files, {})

    def test_analyze_repository_metrics(self):
        """Test repository metrics analysis"""
        analysis = self.collector.analyze_repository_metrics(self.sample_result)

        self.assertIn("repo", analysis)
        self.assertIn("basic_stats", analysis)
        self.assertIn("deployment_analysis", analysis)
        self.assertIn("release_analysis", analysis)
        self.assertIn("contributor_analysis", analysis)

        # Check basic stats
        self.assertEqual(analysis["basic_stats"]["total_commits"], 1)

    def test_analyze_repository_metrics_with_deployments(self):
        """Test analysis with deployment data"""
        result_with_deployments = self.sample_result.copy()
        result_with_deployments["deployments"] = [
            {"created_at": "2025-01-01T12:00:00Z"},
            {"created_at": "2025-01-15T12:00:00Z"},
            {"created_at": "2025-02-01T12:00:00Z"},
        ]

        analysis = self.collector.analyze_repository_metrics(result_with_deployments)

        self.assertIn("deployment_analysis", analysis)
        deployment_stats = analysis["deployment_analysis"]
        self.assertIn("avg_days_between_deployments", deployment_stats)
        self.assertIn("deployments_per_month", deployment_stats)

    def test_analyze_repository_metrics_with_contributors(self):
        """Test analysis with contributor data"""
        result_with_contributors = self.sample_result.copy()
        result_with_contributors["commits"] = [
            {"commit": {"author": {"name": "Alice"}}},
            {"commit": {"author": {"name": "Bob"}}},
            {"commit": {"author": {"name": "Alice"}}},
        ]

        analysis = self.collector.analyze_repository_metrics(result_with_contributors)

        contributor_stats = analysis["contributor_analysis"]
        self.assertEqual(contributor_stats["total_contributors"], 2)
        self.assertIn("Alice", contributor_stats["top_contributors"])
        self.assertEqual(contributor_stats["top_contributors"]["Alice"], 2)

    def test_parse_github_date(self):
        """Test GitHub date parsing in collector"""
        # Test Z format
        date_str = "2025-01-01T12:00:00Z"
        parsed = self.collector._parse_github_date(date_str)
        self.assertIsInstance(parsed, datetime)

        # Test ISO format
        date_str = "2025-01-01T12:00:00+00:00"
        parsed = self.collector._parse_github_date(date_str)
        self.assertIsInstance(parsed, datetime)

        # Test invalid format
        date_str = "invalid"
        parsed = self.collector._parse_github_date(date_str)
        self.assertIsInstance(parsed, datetime)

    @patch("builtins.print")
    def test_print_overall_summary_no_results(self, mock_print):
        """Test printing summary with no results"""
        self.collector.print_overall_summary()
        # Should print warning about no results
        # We can't easily assert the exact logging output, but we can verify it doesn't crash

    def test_print_overall_summary_with_results(self):
        """Test printing summary with results"""
        self.collector.results = [self.sample_result]

        # Should not raise an exception
        try:
            self.collector.print_overall_summary()
        except Exception as e:
            self.fail(f"print_overall_summary raised an exception: {e}")


class TestIntegration(unittest.TestCase):
    """Integration tests that test multiple components together"""

    @patch("requests.Session.get")
    def test_end_to_end_single_repo(self, mock_get):
        """Test end-to-end workflow for a single repository"""
        # Mock API responses
        commits_response = Mock()
        commits_response.status_code = 200
        commits_response.json.return_value = [
            {
                "sha": "abc123",
                "commit": {
                    "author": {
                        "name": "Test Author",
                        "email": "test@example.com",
                        "date": "2025-01-01T12:00:00Z",
                    },
                    "committer": {
                        "name": "Test Author",
                        "email": "test@example.com",
                        "date": "2025-01-01T12:00:00Z",
                    },
                    "message": "Test commit",
                },
                "stats": {"additions": 10, "deletions": 5, "total": 15},
            }
        ]
        commits_response.headers = {}

        prs_response = Mock()
        prs_response.status_code = 200
        prs_response.json.return_value = []
        prs_response.headers = {}

        # Mock all the API calls
        mock_get.return_value = commits_response

        extractor = GitHubMetricsExtractor("test_token", "test-org", "test-repo")

        # Test getting commits
        since_date = datetime.now() - timedelta(days=30)
        commits = extractor.get_commits(since_date)

        self.assertEqual(len(commits), 1)
        self.assertEqual(commits[0]["sha"], "abc123")

    def test_csv_export_integration(self):
        """Test CSV export integration"""
        extractor = GitHubMetricsExtractor("test_token", "test-org", "test-repo")

        metrics = {
            "commits": [
                {
                    "sha": "abc123",
                    "commit": {
                        "author": {
                            "name": "Test Author",
                            "email": "test@example.com",
                            "date": "2025-01-01T12:00:00Z",
                        },
                        "committer": {
                            "name": "Test Author",
                            "email": "test@example.com",
                            "date": "2025-01-01T12:00:00Z",
                        },
                        "message": "Test commit",
                    },
                    "stats": {"additions": 10, "deletions": 5, "total": 15},
                }
            ],
            "pull_requests": [],
            "workflow_runs": [],
            "deployments": [],
            "releases": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_files = extractor.export_to_csv(metrics, temp_dir)

            # Verify commits CSV was created and has correct data
            self.assertIn("commits", csv_files)
            commits_file = csv_files["commits"]

            with open(commits_file, "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["sha"], "abc123")
                self.assertEqual(rows[0]["author_name"], "Test Author")


class TestErrorHandling(unittest.TestCase):
    """Test error handling scenarios"""

    def test_extractor_with_invalid_token(self):
        """Test extractor behavior with invalid token"""
        extractor = GitHubMetricsExtractor("invalid_token", "test-org", "test-repo")

        # Should initialize without error
        self.assertEqual(extractor.token, "invalid_token")

    @patch("requests.Session.get")
    def test_request_with_http_error(self, mock_get):
        """Test handling of HTTP errors"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError("Not Found")
        mock_get.return_value = mock_response

        extractor = GitHubMetricsExtractor("test_token", "test-org", "test-repo")

        with self.assertRaises(requests.HTTPError):
            extractor._make_request("https://api.github.com/test")

    def test_csv_export_with_malformed_data(self):
        """Test CSV export with malformed data"""
        extractor = GitHubMetricsExtractor("test_token", "test-org", "test-repo")

        # Malformed commit data
        metrics = {
            "commits": [
                {
                    "sha": "abc123",
                    # Missing required commit data
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            # Should handle malformed data gracefully
            csv_files = extractor.export_to_csv(metrics, temp_dir)

            # Should still create the file even with malformed data
            self.assertIn("commits", csv_files)


if __name__ == "__main__":
    # Create a test suite with all test cases
    test_classes = [
        TestGitHubMetricsExtractor,
        TestMultiRepoMetricsCollector,
        TestIntegration,
        TestErrorHandling,
    ]

    suite = unittest.TestSuite()

    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Print summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print(f"\nFAILURES:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")

    if result.errors:
        print(f"\nERRORS:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")

    # Exit with appropriate code
    exit_code = 0 if result.wasSuccessful() else 1
    exit(exit_code)
