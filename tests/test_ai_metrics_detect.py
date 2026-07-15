"""Unit tests for lucille.ai_metrics.detect (pure functions)."""

import pytest

from context import lucille  # noqa: F401
from lucille.ai_metrics.detect import (
    AISignature,
    DEFAULT_AI_SIGNATURES,
    detect_ai_signatures,
    extract_reverted_shas,
    extract_reverted_title,
    is_ai_touched,
    is_bot_pr,
    is_revert_by_title,
    is_revert_pr,
)


# ---------------------------------------------------------------------------
# AI signature detection
# ---------------------------------------------------------------------------


CLAUDE_TRAILER = "Co-Authored-By: Claude <noreply@anthropic.com>"
CURSOR_TRAILER = "Co-Authored-By: Cursor Agent <cursoragent@cursor.com>"
GEMINI_TRAILER = "Co-Authored-By: gemini-cli <gemini@google.com>"
GENERIC_MARKER = "🤖 Generated with Claude Code (Sonnet 4.5) via pi"


class TestDetectAiSignatures:
    def test_claude_trailer_detected(self):
        assert detect_ai_signatures([f"fix bug\n\n{CLAUDE_TRAILER}"]) == ["claude"]

    def test_cursor_trailer_detected(self):
        assert detect_ai_signatures([f"add feature\n\n{CURSOR_TRAILER}"]) == ["cursor"]

    def test_gemini_trailer_detected(self):
        assert detect_ai_signatures([f"refactor\n\n{GEMINI_TRAILER}"]) == ["gemini"]

    def test_generic_marker_detected(self):
        assert detect_ai_signatures([f"do a thing\n\n{GENERIC_MARKER}"]) == ["generic"]

    def test_multiple_signatures_on_single_pr(self):
        assert detect_ai_signatures([
            f"first commit\n\n{CLAUDE_TRAILER}",
            f"second commit\n\n{CURSOR_TRAILER}",
        ]) == ["claude", "cursor"]

    def test_no_signature_returns_empty(self):
        assert detect_ai_signatures(["ordinary human commit message"]) == []

    def test_empty_input(self):
        assert detect_ai_signatures([]) == []

    def test_case_insensitive_match(self):
        assert detect_ai_signatures(["co-authored-by: claude <noreply@anthropic.com>"]) == ["claude"]

    def test_does_not_match_bare_human_coauthor(self):
        # A regular human co-authoring shouldn't trip the detector.
        assert detect_ai_signatures(["Co-Authored-By: Alice <alice@example.com>"]) == []

    def test_custom_signature_list(self):
        custom = [AISignature("myai", r"MyAI-Bot")]
        assert detect_ai_signatures(["commit body\n\nMyAI-Bot signed"], custom) == ["myai"]
        assert detect_ai_signatures([CLAUDE_TRAILER], custom) == []  # defaults not used

    def test_default_signatures_are_defined(self):
        names = {s.name for s in DEFAULT_AI_SIGNATURES}
        assert {"claude", "cursor", "gemini", "generic"} <= names


class TestIsAiTouched:
    def test_true_when_any_signature_matches(self):
        assert is_ai_touched([CLAUDE_TRAILER])

    def test_false_for_human_only(self):
        assert not is_ai_touched(["human wrote this"])

    def test_true_when_only_one_of_many_commits_is_ai(self):
        assert is_ai_touched(["human commit 1", "human commit 2", CLAUDE_TRAILER])


# ---------------------------------------------------------------------------
# Bot detection
# ---------------------------------------------------------------------------


class TestIsBotPr:
    @pytest.mark.parametrize("login", [
        "dependabot[bot]",
        "renovate[bot]",
        "github-actions[bot]",
        "snyk-bot",
        "renovate-bot",
        "dependabot-preview[bot]",
        "mend-for-github-com[bot]",
    ])
    def test_detects_known_bot_logins(self, login):
        assert is_bot_pr(login)

    def test_detects_bot_type(self):
        assert is_bot_pr("something-that-doesnt-match-a-pattern", user_type="Bot")

    def test_human_login_is_not_bot(self):
        assert not is_bot_pr("mruggiero67", user_type="User")

    def test_none_login(self):
        assert not is_bot_pr(None)

    def test_empty_login(self):
        assert not is_bot_pr("")

    def test_bot_substring_in_normal_name_is_not_bot(self):
        # Avoid false positives on humans who happen to have "bot" in their name.
        assert not is_bot_pr("robert-smith")


# ---------------------------------------------------------------------------
# Revert detection
# ---------------------------------------------------------------------------


class TestIsRevertByTitle:
    def test_standard_revert_title(self):
        assert is_revert_by_title('Revert "add feature X"')

    def test_revert_title_extraction(self):
        assert extract_reverted_title('Revert "add feature X"') == "add feature X"

    def test_non_revert_title(self):
        assert not is_revert_by_title("add feature X")
        assert extract_reverted_title("add feature X") is None

    def test_empty_title(self):
        assert not is_revert_by_title("")
        assert extract_reverted_title(None) is None  # type: ignore[arg-type]

    def test_word_revert_in_title_but_not_a_revert_pr(self):
        assert not is_revert_by_title("Fix bug in Revert button")


class TestExtractRevertedShas:
    def test_single_reverted_sha(self):
        msg = "Revert change\n\nThis reverts commit abc1234def5678."
        assert extract_reverted_shas([msg]) == ["abc1234def5678"]

    def test_multiple_reverted_shas(self):
        msg = (
            "Revert two things\n\n"
            "This reverts commit aaaaaaa1234567.\n"
            "This reverts commit bbbbbbb7654321.\n"
        )
        assert extract_reverted_shas([msg]) == ["aaaaaaa1234567", "bbbbbbb7654321"]

    def test_deduplicates(self):
        msg = "This reverts commit abc1234.\nThis reverts commit ABC1234.\n"
        assert extract_reverted_shas([msg]) == ["abc1234"]

    def test_no_trailer(self):
        assert extract_reverted_shas(["ordinary commit"]) == []

    def test_short_sha_accepted(self):
        assert extract_reverted_shas(["This reverts commit abc1234."]) == ["abc1234"]

    def test_non_hex_ignored(self):
        assert extract_reverted_shas(["This reverts commit not-a-sha."]) == []


class TestIsRevertPr:
    def test_title_shape_only(self):
        assert is_revert_pr('Revert "foo"', ["human wrote this"])

    def test_trailer_only(self):
        assert is_revert_pr("hotfix", ["revert change\n\nThis reverts commit abc1234."])

    def test_neither(self):
        assert not is_revert_pr("normal PR", ["normal commit"])

    def test_both(self):
        assert is_revert_pr('Revert "x"', ["This reverts commit abc1234."])
