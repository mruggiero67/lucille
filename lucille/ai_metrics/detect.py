"""Pure-function detectors for AI-authorship, bots, and reverts.

Every function here is side-effect-free and depends only on standard-library
types — trivial to unit test without any GitHub credentials.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# AI signature detection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AISignature:
    """A named regex that identifies an AI assistant in a commit message."""
    name: str
    pattern: str

    def compiled(self) -> re.Pattern:
        return re.compile(self.pattern, re.IGNORECASE | re.MULTILINE)


# Defaults chosen for precision over recall: we would rather miss a genuine
# AI-authored PR than falsely accuse a human-only PR.
DEFAULT_AI_SIGNATURES: Tuple[AISignature, ...] = (
    AISignature("claude",  r"Co-Authored-By:\s*Claude\b[^\n]*anthropic\.com"),
    AISignature("cursor",  r"Co-Authored-By:[^\n]*\bcursor(agent)?\b[^\n]*"),
    AISignature("gemini",  r"Co-Authored-By:[^\n]*(?:gemini|google-labs)[^\n]*"),
    AISignature("generic", r"^\s*(?:🤖\s*)?Generated (?:with|by) (?:Claude Code|Codex|Cursor|Gemini)"),
)


def detect_ai_signatures(
    commit_messages: Iterable[str],
    signatures: Sequence[AISignature] = DEFAULT_AI_SIGNATURES,
) -> List[str]:
    """Return the set of AI-signature names matched across the given commit messages.

    Args:
        commit_messages: All commit messages belonging to a single PR.
        signatures: The signature set to check against.

    Returns:
        Sorted list of unique signature names that matched at least one commit.
        Empty list means "no AI signature detected".
    """
    joined = "\n".join(commit_messages)
    hits = {sig.name for sig in signatures if sig.compiled().search(joined)}
    return sorted(hits)


def is_ai_touched(
    commit_messages: Iterable[str],
    signatures: Sequence[AISignature] = DEFAULT_AI_SIGNATURES,
) -> bool:
    """True iff any commit in the PR matches any AI signature."""
    return bool(detect_ai_signatures(commit_messages, signatures))


# ---------------------------------------------------------------------------
# Bot detection (dependabot, renovate, etc.)
# ---------------------------------------------------------------------------


# Known automated-PR authors we want to exclude from AI-vs-human comparisons
# entirely. GitHub also exposes `user.type == "Bot"` on many of these, which we
# check first in ``is_bot_pr``; the login list is a defensive fallback for
# accounts registered as Users but behaving as bots.
BOT_LOGIN_PATTERNS: Tuple[str, ...] = (
    r"\[bot\]$",           # dependabot[bot], renovate[bot], github-actions[bot]
    r"^dependabot",
    r"^renovate",
    r"^snyk-bot",
    r"^greenkeeper",
    r"^whitesource",
    r"^mend-",
    r"^imgbot",
)


def is_bot_pr(user_login: Optional[str], user_type: Optional[str] = None) -> bool:
    """True if the PR was opened by an automation bot (not a human, not an AI CLI).

    Args:
        user_login: GitHub login string (``pr["user"]["login"]``).
        user_type:  GitHub type string   (``pr["user"]["type"]``) — 'User' or 'Bot'.
    """
    if user_type == "Bot":
        return True
    if not user_login:
        return False
    login_l = user_login.lower()
    return any(re.search(p, login_l) for p in BOT_LOGIN_PATTERNS)


# ---------------------------------------------------------------------------
# Revert detection
# ---------------------------------------------------------------------------


_REVERT_TITLE_RE = re.compile(r'^\s*Revert\s+"(?P<inner>.+?)"\s*$')
_REVERT_COMMIT_TRAILER_RE = re.compile(
    r"^\s*This reverts commit\s+(?P<sha>[0-9a-f]{7,40})\b",
    re.MULTILINE | re.IGNORECASE,
)


def is_revert_by_title(pr_title: str) -> bool:
    """True if the PR title matches GitHub's default revert-PR shape."""
    return bool(_REVERT_TITLE_RE.match(pr_title or ""))


def extract_reverted_title(pr_title: str) -> Optional[str]:
    """Return the inner (reverted) title, or None if this isn't a revert PR."""
    m = _REVERT_TITLE_RE.match(pr_title or "")
    return m.group("inner") if m else None


def extract_reverted_shas(commit_messages: Iterable[str]) -> List[str]:
    """Return every commit SHA cited in a ``This reverts commit …`` trailer.

    A single revert PR may cite multiple SHAs (rare but valid). SHAs are
    returned lowercased and deduplicated, preserving first-seen order.
    """
    seen: List[str] = []
    for msg in commit_messages:
        for m in _REVERT_COMMIT_TRAILER_RE.finditer(msg or ""):
            sha = m.group("sha").lower()
            if sha not in seen:
                seen.append(sha)
    return seen


def is_revert_pr(
    pr_title: str,
    commit_messages: Iterable[str],
) -> bool:
    """True if either the title or any commit trailer marks this as a revert."""
    if is_revert_by_title(pr_title):
        return True
    return bool(extract_reverted_shas(commit_messages))
