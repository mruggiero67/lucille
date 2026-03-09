"""
Tests for DeploymentDetector.

Uses fixture data — no live API calls.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from lucille.cfr.sources.github_client import GitHubRelease, GitHubPR
from lucille.cfr.logic.deployment_detector import DeploymentDetector, DeploymentEvent

FIXTURES = Path(__file__).parent / "fixtures"

SAMPLE_CONFIG = {
    "cfr": {
        "deployment_detection": "release",
        "production_branch": "main",
        "lookback_days": 90,
        "intervention_window_hours": 72,
        "agent_github_label": "agent-generated",
        "agent_author_patterns": ["claude-agent"],
        "failure_issue_types": ["Bug", "Incident"],
        "failure_labels": ["hotfix", "rollback"],
        "failure_title_patterns": ["hotfix", "rollback", "revert"],
    }
}


def _make_release(tag: str, iso: str, repo: str = "analytics") -> GitHubRelease:
    return GitHubRelease(
        tag_name=tag,
        published_at=datetime.fromisoformat(iso.replace("Z", "+00:00")),
        repo=repo,
        html_url=f"https://github.com/jarisdev/{repo}/releases/tag/{tag}",
    )


def _make_pr(number: int, merged_iso: str, repo: str = "analytics") -> GitHubPR:
    return GitHubPR(
        number=number,
        title=f"PR {number}",
        body="",
        author="alice",
        labels=[],
        merged_at=datetime.fromisoformat(merged_iso.replace("Z", "+00:00")),
        base_branch="main",
        html_url=f"https://github.com/jarisdev/{repo}/pull/{number}",
        repo=repo,
    )


class TestDeploymentDetector:
    def setup_method(self):
        self.gh = MagicMock()
        self.detector = DeploymentDetector(self.gh, SAMPLE_CONFIG)
        self.since = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_no_releases_returns_empty(self):
        self.gh.get_releases.return_value = []
        events = self.detector._from_releases("analytics", self.since)
        assert events == []

    def test_single_release_creates_one_event(self):
        releases = [_make_release("v1.4.0", "2026-02-01T10:00:00Z")]
        self.gh.get_releases.return_value = releases
        self.gh.get_prs_merged_between.return_value = []

        events = self.detector._from_releases("analytics", self.since)

        assert len(events) == 1
        assert events[0].deployment_id == "v1.4.0"
        assert events[0].repo == "analytics"

    def test_three_releases_creates_three_events_in_order(self):
        releases = [
            _make_release("v1.4.0", "2026-02-01T10:00:00Z"),
            _make_release("v1.4.1", "2026-02-15T14:00:00Z"),
            _make_release("v1.4.2", "2026-03-01T09:00:00Z"),
        ]
        self.gh.get_releases.return_value = releases
        self.gh.get_prs_merged_between.return_value = []

        events = self.detector._from_releases("analytics", self.since)

        assert len(events) == 3
        assert [e.deployment_id for e in events] == ["v1.4.0", "v1.4.1", "v1.4.2"]

    def test_prs_are_assigned_to_correct_deployment_window(self):
        releases = [
            _make_release("v1.4.0", "2026-02-01T10:00:00Z"),
            _make_release("v1.4.1", "2026-02-15T14:00:00Z"),
        ]
        prs_v140 = [_make_pr(100, "2026-01-28T12:00:00Z")]
        prs_v141 = [_make_pr(101, "2026-02-10T09:00:00Z")]

        self.gh.get_releases.return_value = releases
        self.gh.get_prs_merged_between.side_effect = [prs_v140, prs_v141]

        events = self.detector._from_releases("analytics", self.since)

        assert len(events[0].prs) == 1
        assert events[0].prs[0].number == 100
        assert len(events[1].prs) == 1
        assert events[1].prs[0].number == 101

    def test_merge_to_main_mode_creates_one_event_per_pr(self):
        config = {**SAMPLE_CONFIG, "cfr": {**SAMPLE_CONFIG["cfr"], "deployment_detection": "merge_to_main"}}
        detector = DeploymentDetector(self.gh, config)

        prs = [
            _make_pr(201, "2026-02-05T10:00:00Z"),
            _make_pr(202, "2026-02-12T14:00:00Z"),
        ]
        self.gh.get_prs_merged_between.return_value = prs

        events = detector._from_merges("analytics", self.since)

        assert len(events) == 2
        assert events[0].deployment_id == "PR#201"
        assert events[1].deployment_id == "PR#202"
