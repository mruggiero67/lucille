"""Unit tests for lucille.jira.ticket_changelog (pure functions only)."""
from datetime import datetime, timezone
from context import lucille  # noqa: F401
from lucille.jira.ticket_changelog import find_ticket_start_date, select_start_date


def _make_history(timestamp_iso: str, to_status: str, from_status: str = "To Do") -> dict:
    return {
        "created": timestamp_iso,
        "items": [
            {"field": "status", "fromString": from_status, "toString": to_status}
        ],
    }


class TestFindTicketStartDate:
    def test_returns_first_in_progress_transition(self):
        histories = [
            _make_history("2025-06-02T09:00:00.000Z", "In Progress"),
            _make_history("2025-06-10T09:00:00.000Z", "Done"),
        ]
        result = find_ticket_start_date(histories, ["In Progress"])
        assert result is not None
        assert result.year == 2025
        assert result.month == 6
        assert result.day == 2

    def test_returns_first_not_last_when_re_entered(self):
        histories = [
            _make_history("2025-06-02T09:00:00.000Z", "In Progress"),
            _make_history("2025-06-08T09:00:00.000Z", "In Progress"),
        ]
        result = find_ticket_start_date(histories, ["In Progress"])
        assert result is not None
        assert result.day == 2

    def test_returns_none_when_no_dev_transition(self):
        histories = [_make_history("2025-06-10T09:00:00.000Z", "Done")]
        assert find_ticket_start_date(histories, ["In Progress"]) is None

    def test_returns_none_for_empty_histories(self):
        assert find_ticket_start_date([], ["In Progress"]) is None

    def test_case_insensitive_status_match(self):
        histories = [_make_history("2025-06-02T09:00:00.000Z", "in progress")]
        result = find_ticket_start_date(histories, ["In Progress"])
        assert result is not None

    def test_matches_any_dev_status_in_list(self):
        histories = [_make_history("2025-06-02T09:00:00.000Z", "In Development")]
        result = find_ticket_start_date(histories, ["In Progress", "In Development"])
        assert result is not None

    def test_ignores_non_status_changelog_items(self):
        history = {
            "created": "2025-06-02T09:00:00.000Z",
            "items": [
                {"field": "assignee", "fromString": None, "toString": "Alice"},
                {"field": "status", "fromString": "To Do", "toString": "In Progress"},
            ],
        }
        result = find_ticket_start_date([history], ["In Progress"])
        assert result is not None


class TestSelectStartDate:
    def test_uses_transition_when_present(self):
        transition = datetime(2025, 6, 2, tzinfo=timezone.utc)
        created = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert select_start_date(transition, created) == transition

    def test_falls_back_to_created_when_none(self):
        created = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert select_start_date(None, created) == created

    def test_returns_transition_even_when_later_than_created(self):
        transition = datetime(2025, 6, 5, tzinfo=timezone.utc)
        created = datetime(2025, 6, 1, tzinfo=timezone.utc)
        assert select_start_date(transition, created) == transition
