"""Tests for lucille.github.session."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from context import lucille  # noqa: F401
from lucille.github.session import (
    create_github_session,
    paginate,
)


# ---------------------------------------------------------------------------
# create_github_session
# ---------------------------------------------------------------------------


class TestCreateGithubSession:
    def test_returns_session_with_auth_headers(self):
        s = create_github_session("my-token")
        assert s.headers["Authorization"] == "token my-token"
        assert s.headers["Accept"] == "application/vnd.github+json"
        assert s.headers["X-GitHub-Api-Version"] == "2022-11-28"

    def test_returns_a_new_session_each_call(self):
        a = create_github_session("t1")
        b = create_github_session("t2")
        assert a is not b
        assert a.headers["Authorization"] != b.headers["Authorization"]


# ---------------------------------------------------------------------------
# Helpers for building fake Responses
# ---------------------------------------------------------------------------


def _resp(
    body,
    *,
    next_url=None,
    status=200,
    remaining="5000",
    reset="0",
    text="",
):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.json.return_value = body
    r.links = {"next": {"url": next_url}} if next_url else {}
    r.headers = {"X-RateLimit-Remaining": remaining, "X-RateLimit-Reset": reset}
    r.text = text
    r.raise_for_status = MagicMock()
    return r


# ---------------------------------------------------------------------------
# paginate
# ---------------------------------------------------------------------------


class TestPaginate:
    def test_single_page(self):
        session = MagicMock()
        session.get.return_value = _resp([{"id": 1}, {"id": 2}])
        items = list(paginate(session, "https://api.github.com/x"))
        assert items == [{"id": 1}, {"id": 2}]
        assert session.get.call_count == 1

    def test_follows_link_header(self):
        session = MagicMock()
        session.get.side_effect = [
            _resp([{"id": 1}], next_url="https://api.github.com/x?page=2"),
            _resp([{"id": 2}, {"id": 3}]),
        ]
        items = list(paginate(session, "https://api.github.com/x"))
        assert items == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert session.get.call_count == 2

    def test_first_request_uses_params_subsequent_do_not(self):
        session = MagicMock()
        session.get.side_effect = [
            _resp([{"id": 1}], next_url="https://api.github.com/x?page=2"),
            _resp([]),
        ]
        list(paginate(session, "https://api.github.com/x", {"state": "all"}))
        # First call gets params; second call gets params=None (Link URL has them).
        first_call, second_call = session.get.call_args_list
        assert first_call.kwargs["params"]["state"] == "all"
        assert first_call.kwargs["params"]["per_page"] == 100
        assert second_call.kwargs["params"] is None

    def test_default_per_page(self):
        session = MagicMock()
        session.get.return_value = _resp([])
        list(paginate(session, "https://api.github.com/x"))
        params = session.get.call_args.kwargs["params"]
        assert params["per_page"] == 100

    def test_custom_per_page(self):
        session = MagicMock()
        session.get.return_value = _resp([])
        list(paginate(session, "https://api.github.com/x", per_page=25))
        assert session.get.call_args.kwargs["params"]["per_page"] == 25

    def test_max_pages_caps_iteration(self):
        session = MagicMock()
        session.get.side_effect = [
            _resp([{"id": i}], next_url="https://api.github.com/x?page=next")
            for i in range(1, 10)
        ]
        items = list(paginate(session, "https://api.github.com/x", max_pages=2))
        assert items == [{"id": 1}, {"id": 2}]
        assert session.get.call_count == 2

    def test_empty_page_terminates(self):
        session = MagicMock()
        # Even with a Link header, an empty body terminates iteration.
        session.get.return_value = _resp([], next_url="https://api.github.com/x?page=2")
        items = list(paginate(session, "https://api.github.com/x"))
        assert items == []
        assert session.get.call_count == 1

    def test_generator_is_lazy(self):
        session = MagicMock()
        session.get.side_effect = [
            _resp([{"id": 1}], next_url="https://api.github.com/x?page=2"),
            _resp([{"id": 2}]),
        ]
        it = paginate(session, "https://api.github.com/x")
        assert next(it) == {"id": 1}
        # We should have made only one call so far.
        assert session.get.call_count == 1
        # Consuming the rest triggers the second call.
        rest = list(it)
        assert rest == [{"id": 2}]
        assert session.get.call_count == 2


# ---------------------------------------------------------------------------
# Rate-limit and retry logic
# ---------------------------------------------------------------------------


class TestRateLimitAndRetries:
    def test_preemptive_sleep_when_remaining_low(self):
        session = MagicMock()
        session.get.return_value = _resp(
            [], remaining="2", reset=str(int(1_000_000_000)),
        )
        with patch("lucille.github.session.time.sleep") as sleep_mock, \
             patch("lucille.github.session.time.time", return_value=999_999_990):
            list(paginate(session, "https://api.github.com/x"))
        # Should have slept once (reset - now = 10s, plus 5s cushion).
        sleep_mock.assert_called_once()
        assert sleep_mock.call_args.args[0] >= 10

    def test_no_preemptive_sleep_when_remaining_high(self):
        session = MagicMock()
        session.get.return_value = _resp([], remaining="1000")
        with patch("lucille.github.session.time.sleep") as sleep_mock:
            list(paginate(session, "https://api.github.com/x"))
        sleep_mock.assert_not_called()

    def test_reactive_backoff_on_403_rate_limit(self):
        session = MagicMock()
        # Two rate-limit 403s, then success.
        session.get.side_effect = [
            _resp(None, status=403, text="API rate limit exceeded"),
            _resp(None, status=403, text="API rate limit exceeded"),
            _resp([{"id": 1}]),
        ]
        with patch("lucille.github.session.time.sleep") as sleep_mock:
            items = list(paginate(session, "https://api.github.com/x"))
        assert items == [{"id": 1}]
        assert session.get.call_count == 3
        assert sleep_mock.call_count == 2  # one sleep per 403

    def test_403_without_rate_limit_body_is_not_retried(self):
        session = MagicMock()
        bad = _resp(None, status=403, text="permission denied")
        bad.raise_for_status = MagicMock(side_effect=requests.HTTPError("403"))
        session.get.return_value = bad
        with pytest.raises(requests.HTTPError):
            list(paginate(session, "https://api.github.com/x"))
        # Should not have retried.
        assert session.get.call_count == 1

    def test_transient_error_retried_then_succeeds(self):
        session = MagicMock()
        session.get.side_effect = [
            requests.ConnectionError("boom"),
            _resp([{"id": 1}]),
        ]
        with patch("lucille.github.session.time.sleep"):
            items = list(paginate(session, "https://api.github.com/x"))
        assert items == [{"id": 1}]
        assert session.get.call_count == 2

    def test_transient_error_gives_up_after_max_retries(self):
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("boom")
        with patch("lucille.github.session.time.sleep"), \
             pytest.raises(requests.ConnectionError):
            list(paginate(session, "https://api.github.com/x"))
        # 5 attempts total per _MAX_TRANSIENT_RETRIES.
        assert session.get.call_count == 5

    def test_non_403_http_error_raises_immediately(self):
        session = MagicMock()
        bad = _resp(None, status=404)
        bad.raise_for_status = MagicMock(side_effect=requests.HTTPError("404"))
        session.get.return_value = bad
        with pytest.raises(requests.HTTPError):
            list(paginate(session, "https://api.github.com/x"))
        assert session.get.call_count == 1

    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_5xx_retried_then_succeeds(self, status):
        session = MagicMock()
        bad = _resp(None, status=status)
        session.get.side_effect = [bad, bad, _resp([{"id": 1}])]
        with patch("lucille.github.session.time.sleep") as sleep_mock:
            items = list(paginate(session, "https://api.github.com/x"))
        assert items == [{"id": 1}]
        assert session.get.call_count == 3
        assert sleep_mock.call_count == 2

    def test_5xx_gives_up_after_max_retries(self):
        session = MagicMock()
        bad = _resp(None, status=503)
        bad.raise_for_status = MagicMock(side_effect=requests.HTTPError("503"))
        session.get.return_value = bad
        with patch("lucille.github.session.time.sleep"), \
             pytest.raises(requests.HTTPError):
            list(paginate(session, "https://api.github.com/x"))
        # 4 attempts total per _MAX_SERVER_ERROR_RETRIES.
        assert session.get.call_count == 4

    def test_4xx_non_rate_limit_not_retried(self):
        # A 500-adjacent but non-retriable 4xx like 422 should raise immediately.
        session = MagicMock()
        bad = _resp(None, status=422)
        bad.raise_for_status = MagicMock(side_effect=requests.HTTPError("422"))
        session.get.return_value = bad
        with pytest.raises(requests.HTTPError):
            list(paginate(session, "https://api.github.com/x"))
        assert session.get.call_count == 1
