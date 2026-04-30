"""
Unit tests for stale_tickets_to_csv.py and comment_stale_tickets.py.
Focuses on pure functions; side-effecting (network/file) calls are mocked.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

# Allow imports when running tests from the repo root
sys.path.insert(0, str(Path(__file__).parent.parent))
from lucille.jira.stale_tickets_to_csv import (
    find_status_since,
    flatten_issue,
    load_config,
    mk_output_path,
    parse_jira_timestamp,
)
from lucille.jira.comment_stale_tickets import (
    build_comment_adf,
    format_status_since,
    read_csv,
)


# ---------------------------------------------------------------------------
# stale_tickets_to_csv — parse_jira_timestamp
# ---------------------------------------------------------------------------


class TestParseJiraTimestamp:
    def test_utc_z_suffix(self):
        ts = "2024-03-15T10:30:00.000Z"
        dt = parse_jira_timestamp(ts)
        assert dt.tzinfo is not None
        assert dt.year == 2024
        assert dt.month == 3
        assert dt.day == 15

    def test_offset_format(self):
        ts = "2024-03-15T10:30:00.000-0700"
        dt = parse_jira_timestamp(ts)
        assert dt.tzinfo is not None

    def test_iso_with_colon_offset(self):
        ts = "2024-03-15T10:30:00+00:00"
        dt = parse_jira_timestamp(ts)
        assert dt.tzinfo is not None

    def test_empty_string_returns_now(self):
        before = datetime.now(timezone.utc)
        dt = parse_jira_timestamp("")
        after = datetime.now(timezone.utc)
        assert before <= dt <= after


# ---------------------------------------------------------------------------
# stale_tickets_to_csv — find_status_since
# ---------------------------------------------------------------------------


class TestFindStatusSince:
    def _make_changelog(self, transitions):
        """transitions: list of (toString, created) tuples."""
        histories = [
            {
                "created": created,
                "items": [{"field": "status", "toString": to_status}],
            }
            for to_status, created in transitions
        ]
        return {"changelog": {"histories": histories}}

    def test_returns_date_of_last_transition_to_status(self):
        changelog = self._make_changelog(
            [
                ("In Progress", "2024-01-10T08:00:00.000Z"),
                ("Review", "2024-02-01T08:00:00.000Z"),
                ("In Progress", "2024-03-05T08:00:00.000Z"),
            ]
        )
        result = find_status_since(changelog, "In Progress")
        assert result == "2024-03-05"

    def test_returns_none_when_status_never_entered(self):
        changelog = self._make_changelog(
            [("In Progress", "2024-01-10T08:00:00.000Z")]
        )
        assert find_status_since(changelog, "Review") is None

    def test_returns_none_for_empty_changelog(self):
        assert find_status_since({"changelog": {"histories": []}}, "In Progress") is None

    def test_returns_none_when_changelog_key_missing(self):
        assert find_status_since({}, "In Progress") is None

    def test_non_status_fields_are_ignored(self):
        changelog = {
            "changelog": {
                "histories": [
                    {
                        "created": "2024-01-10T08:00:00.000Z",
                        "items": [{"field": "assignee", "toString": "In Progress"}],
                    }
                ]
            }
        }
        assert find_status_since(changelog, "In Progress") is None


# ---------------------------------------------------------------------------
# stale_tickets_to_csv — flatten_issue
# ---------------------------------------------------------------------------


class TestFlattenIssue:
    BASE_URL = "https://example.atlassian.net"

    def _make_issue(self, key="FOO-1", assignee=None, status_name="In Progress", summary="A ticket"):
        return {
            "key": key,
            "fields": {
                "summary": summary,
                "status": {"name": status_name},
                "assignee": assignee,
            },
        }

    def test_basic_issue(self):
        issue = self._make_issue(
            key="FOO-42",
            assignee={"displayName": "Alice", "accountId": "abc123"},
        )
        row = flatten_issue(issue, "2024-03-01", self.BASE_URL)
        assert row["issue_key"] == "FOO-42"
        assert row["assignee"] == "Alice"
        assert row["assignee_account_id"] == "abc123"
        assert row["status"] == "In Progress"
        assert row["status_since"] == "2024-03-01"
        assert row["url"] == "https://example.atlassian.net/browse/FOO-42"

    def test_unassigned_issue(self):
        issue = self._make_issue(assignee=None)
        row = flatten_issue(issue, None, self.BASE_URL)
        assert row["assignee"] == "Unassigned"
        assert row["assignee_account_id"] == ""
        assert row["status_since"] == ""

    def test_url_uses_base_url(self):
        issue = self._make_issue(key="BAR-7")
        row = flatten_issue(issue, "2024-01-01", "https://other.atlassian.net")
        assert row["url"] == "https://other.atlassian.net/browse/BAR-7"


# ---------------------------------------------------------------------------
# stale_tickets_to_csv — mk_output_path
# ---------------------------------------------------------------------------


class TestMkOutputPath:
    def test_contains_date_prefix(self):
        today = datetime.now().strftime("%Y_%m_%d")
        path = mk_output_path("/tmp", "stale tickets")
        assert path.name.startswith(today)

    def test_label_is_snake_cased(self):
        path = mk_output_path("/tmp", "stale tickets")
        assert "stale_tickets" in path.name

    def test_csv_extension(self):
        path = mk_output_path("/tmp", "stale_tickets")
        assert path.suffix == ".csv"

    def test_expands_tilde(self):
        path = mk_output_path("~/Desktop/debris", "stale_tickets")
        assert "~" not in str(path)


# ---------------------------------------------------------------------------
# stale_tickets_to_csv — load_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def _write_config(self, tmp_path, data):
        p = tmp_path / "config.yaml"
        p.write_text(yaml.dump(data))
        return str(p)

    def test_valid_config_loads(self, tmp_path):
        data = {
            "jira": {"base_url": "https://x.atlassian.net", "username": "u", "api_token": "t"},
            "query": {"jql": "project = X", "name": "test"},
            "output": {"directory": "/tmp"},
        }
        config = load_config(self._write_config(tmp_path, data))
        assert config["jira"]["base_url"] == "https://x.atlassian.net"

    def test_missing_jira_section_raises(self, tmp_path):
        data = {"query": {"jql": "..."}, "output": {"directory": "/tmp"}}
        with pytest.raises(ValueError, match="jira"):
            load_config(self._write_config(tmp_path, data))

    def test_missing_jql_raises(self, tmp_path):
        data = {
            "jira": {"base_url": "x", "username": "u", "api_token": "t"},
            "query": {"name": "test"},
            "output": {"directory": "/tmp"},
        }
        with pytest.raises(ValueError, match="jql"):
            load_config(self._write_config(tmp_path, data))

    def test_file_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")


# ---------------------------------------------------------------------------
# comment_stale_tickets — build_comment_adf
# ---------------------------------------------------------------------------


class TestBuildCommentAdf:
    def test_structure(self):
        result = build_comment_adf("Alice", "acc123", "In Progress", "2024-03-01")
        assert result["body"]["type"] == "doc"
        assert result["body"]["version"] == 1
        para = result["body"]["content"][0]
        assert para["type"] == "paragraph"

    def test_includes_mention_when_account_id_present(self):
        result = build_comment_adf("Alice", "acc123", "In Progress", "2024-03-01")
        content = result["body"]["content"][0]["content"]
        node_types = [n["type"] for n in content]
        assert "mention" in node_types
        mention = next(n for n in content if n["type"] == "mention")
        assert mention["attrs"]["id"] == "acc123"
        assert "@Alice" in mention["attrs"]["text"]

    def test_outro_text_includes_status_and_date(self):
        result = build_comment_adf("Alice", "acc123", "Review", "2024-06-15")
        content = result["body"]["content"][0]["content"]
        all_text = " ".join(n.get("text", "") for n in content)
        assert "Review" in all_text
        assert "2024-06-15" in all_text
        assert "Any update?" in all_text

    def test_no_mention_when_no_account_id(self):
        result = build_comment_adf("Alice", "", "In Progress", "2024-03-01")
        content = result["body"]["content"][0]["content"]
        node_types = [n["type"] for n in content]
        assert "mention" not in node_types
        assert "text" in node_types

    def test_unassigned_falls_back_to_team(self):
        result = build_comment_adf("Unassigned", "", "Blocked", "2024-01-01")
        content = result["body"]["content"][0]["content"]
        text = content[0]["text"]
        assert "team" in text


# ---------------------------------------------------------------------------
# comment_stale_tickets — format_status_since
# ---------------------------------------------------------------------------


class TestFormatStatusSince:
    def test_passthrough_yyyy_mm_dd(self):
        assert format_status_since("2024-03-15") == "2024-03-15"

    def test_empty_string_returns_fallback(self):
        result = format_status_since("")
        assert result == "an unknown date"

    def test_arbitrary_string_passes_through(self):
        assert format_status_since("some date") == "some date"


# ---------------------------------------------------------------------------
# comment_stale_tickets — read_csv
# ---------------------------------------------------------------------------


class TestReadCsv:
    def test_reads_rows(self, tmp_path):
        csv_path = tmp_path / "tickets.csv"
        csv_path.write_text(
            "issue_key,summary,assignee,assignee_account_id,status,status_since,url\n"
            "FOO-1,A ticket,Alice,acc1,In Progress,2024-03-01,https://x/browse/FOO-1\n"
            "FOO-2,B ticket,Bob,acc2,Review,2024-02-15,https://x/browse/FOO-2\n"
        )
        rows = read_csv(str(csv_path))
        assert len(rows) == 2
        assert rows[0]["issue_key"] == "FOO-1"
        assert rows[1]["assignee"] == "Bob"

    def test_empty_csv_returns_empty_list(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text(
            "issue_key,summary,assignee,assignee_account_id,status,status_since,url\n"
        )
        rows = read_csv(str(csv_path))
        assert rows == []

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            read_csv("/nonexistent/file.csv")
