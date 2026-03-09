#!/usr/bin/env python3
"""
Classifies PRs and deployments as agent / human / hybrid.

Priority order for agent detection:
  1. GitHub label matches cfr.agent_github_label
  2. PR author login matches any pattern in cfr.agent_author_patterns
  3. PR body contains "[AGENT]" marker (fragile fallback)
"""

import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal

try:
    from ..sources.github_client import GitHubPR
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.sources.github_client import GitHubPR

logger = logging.getLogger(__name__)

Category = Literal["agent", "human", "hybrid"]


def is_agent_pr(pr: GitHubPR, config: Dict[str, Any]) -> bool:
    cfr = config["cfr"]

    # 1. GitHub label
    agent_label = cfr.get("agent_github_label", "agent-generated")
    if agent_label in pr.labels:
        return True

    # 2. Author pattern match (e.g. "claude-agent", "jaris-bot")
    for pattern in cfr.get("agent_author_patterns", []):
        if re.search(pattern, pr.author, re.IGNORECASE):
            return True

    # 3. [AGENT] marker in body
    if "[AGENT]" in (pr.body or "").upper():
        return True

    return False


def classify_deployment(prs: List[GitHubPR], config: Dict[str, Any]) -> Category:
    """
    Returns "agent" if all PRs are agent-authored,
            "human" if none are,
            "hybrid" if it's a mix.
    """
    if not prs:
        return "human"
    agent_flags = [is_agent_pr(pr, config) for pr in prs]
    if all(agent_flags):
        return "agent"
    if not any(agent_flags):
        return "human"
    return "hybrid"
