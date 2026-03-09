#!/usr/bin/env python3
"""
Maps GitHub releases (or PR merges) to DeploymentEvent objects.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from ..sources.github_client import GitHubClient, GitHubPR, GitHubRelease
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.sources.github_client import GitHubClient, GitHubPR, GitHubRelease

logger = logging.getLogger(__name__)


@dataclass
class DeploymentEvent:
    deployment_id: str          # release tag name (e.g. "v1.4.2") or "PR#123"
    timestamp: datetime         # when the deployment went out
    repo: str
    prs: List[GitHubPR] = field(default_factory=list)
    prior_timestamp: Optional[datetime] = None


class DeploymentDetector:
    def __init__(self, github_client: GitHubClient, config: Dict[str, Any]):
        self.gh = github_client
        self.config = config
        self.cfr = config["cfr"]

    def detect(self, repo: str, since: datetime) -> List[DeploymentEvent]:
        mode = self.cfr.get("deployment_detection", "release")
        if mode == "release":
            return self._from_releases(repo, since)
        elif mode == "merge_to_main":
            return self._from_merges(repo, since)
        else:
            logger.warning(f"Unknown deployment_detection mode '{mode}', falling back to 'release'")
            return self._from_releases(repo, since)

    def _from_releases(self, repo: str, since: datetime) -> List[DeploymentEvent]:
        releases = self.gh.get_releases(repo, since=since)
        if not releases:
            logger.info(f"{repo}: no releases found since {since.date()}")
            return []
        events = []
        for i, release in enumerate(releases):
            prior_ts = releases[i - 1].published_at if i > 0 else since
            prs = self.gh.get_prs_merged_between(repo, prior_ts, release.published_at)
            events.append(
                DeploymentEvent(
                    deployment_id=release.tag_name,
                    timestamp=release.published_at,
                    repo=repo,
                    prs=prs,
                    prior_timestamp=prior_ts,
                )
            )
        logger.info(f"{repo}: {len(events)} deployments detected via releases")
        return events

    def _from_merges(self, repo: str, since: datetime) -> List[DeploymentEvent]:
        """Each merged PR to production_branch is treated as its own deployment."""
        now = datetime.now(tz=timezone.utc)
        production_branch = self.cfr.get("production_branch", "main")
        prs = self.gh.get_prs_merged_between(repo, since, now)
        prs = [p for p in prs if p.base_branch == production_branch]
        events = []
        for pr in sorted(prs, key=lambda p: p.merged_at):
            events.append(
                DeploymentEvent(
                    deployment_id=f"PR#{pr.number}",
                    timestamp=pr.merged_at,
                    repo=repo,
                    prs=[pr],
                    prior_timestamp=None,
                )
            )
        logger.info(f"{repo}: {len(events)} deployments detected via merge-to-main")
        return events
