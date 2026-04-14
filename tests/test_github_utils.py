from unittest.mock import MagicMock, patch

from context import lucille  # noqa: F401
from lucille.github.github_utils import fetch_org_repos


def _make_response(repos: list, status_code: int = 200) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = repos
    response.raise_for_status.return_value = None
    return response


@patch("lucille.github.github_utils.requests.get")
def test_fetch_org_repos_returns_non_archived(mock_get):
    page1 = [
        {"name": "active-repo", "archived": False},
        {"name": "archived-repo", "archived": True},
        {"name": "another-active", "archived": False},
    ]
    mock_get.side_effect = [
        _make_response(page1),
        _make_response([]),  # empty second page signals end of pagination
    ]

    result = fetch_org_repos("myorg", "token123")

    assert result == ["active-repo", "another-active"]


@patch("lucille.github.github_utils.requests.get")
def test_fetch_org_repos_paginates(mock_get):
    page1 = [{"name": f"repo-{i}", "archived": False} for i in range(100)]
    page2 = [{"name": "last-repo", "archived": False}]
    mock_get.side_effect = [
        _make_response(page1),
        _make_response(page2),
        _make_response([]),
    ]

    result = fetch_org_repos("myorg", "token123")

    assert len(result) == 101
    assert "repo-0" in result
    assert "last-repo" in result


@patch("lucille.github.github_utils.requests.get")
def test_fetch_org_repos_empty_org(mock_get):
    mock_get.return_value = _make_response([])

    result = fetch_org_repos("emptyorg", "token123")

    assert result == []


@patch("lucille.github.github_utils.requests.get")
def test_fetch_org_repos_passes_correct_headers(mock_get):
    mock_get.return_value = _make_response([])

    fetch_org_repos("myorg", "mytoken")

    call_kwargs = mock_get.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["Authorization"] == "token mytoken"
    assert headers["Accept"] == "application/vnd.github+json"
