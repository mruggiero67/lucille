"""Tests for lucille.github.github_utils.

Pagination and auth are covered by tests/test_github_session.py. These tests
focus on this module's own concerns: filtering out archived repos and
returning name-only strings.
"""

from unittest.mock import patch

from context import lucille  # noqa: F401
from lucille.github.github_utils import fetch_org_repos


@patch("lucille.github.github_utils.paginate")
@patch("lucille.github.github_utils.create_github_session")
def test_fetch_org_repos_returns_non_archived(mock_session, mock_paginate):
    mock_paginate.return_value = iter([
        {"name": "active-repo", "archived": False},
        {"name": "archived-repo", "archived": True},
        {"name": "another-active", "archived": False},
    ])
    assert fetch_org_repos("myorg", "t") == ["active-repo", "another-active"]


@patch("lucille.github.github_utils.paginate")
@patch("lucille.github.github_utils.create_github_session")
def test_fetch_org_repos_missing_archived_key_treated_as_active(mock_session, mock_paginate):
    # If the API omits the 'archived' field for some reason, err on the side
    # of including the repo.
    mock_paginate.return_value = iter([{"name": "no-archived-key"}])
    assert fetch_org_repos("myorg", "t") == ["no-archived-key"]


@patch("lucille.github.github_utils.paginate")
@patch("lucille.github.github_utils.create_github_session")
def test_fetch_org_repos_empty_org(mock_session, mock_paginate):
    mock_paginate.return_value = iter([])
    assert fetch_org_repos("emptyorg", "t") == []


@patch("lucille.github.github_utils.paginate")
@patch("lucille.github.github_utils.create_github_session")
def test_fetch_org_repos_uses_correct_url(mock_session, mock_paginate):
    mock_paginate.return_value = iter([])
    fetch_org_repos("myorg", "t")
    call_args = mock_paginate.call_args
    assert call_args.args[1].endswith("/orgs/myorg/repos")


@patch("lucille.github.github_utils.paginate")
@patch("lucille.github.github_utils.create_github_session")
def test_fetch_org_repos_requests_all_repo_types(mock_session, mock_paginate):
    # We want private + public + forks so the caller sees the complete picture.
    mock_paginate.return_value = iter([])
    fetch_org_repos("myorg", "t")
    call_args = mock_paginate.call_args
    assert call_args.args[2] == {"type": "all"}


@patch("lucille.github.github_utils.create_github_session")
@patch("lucille.github.github_utils.paginate")
def test_fetch_org_repos_passes_token_to_session(mock_paginate, mock_session):
    mock_paginate.return_value = iter([])
    fetch_org_repos("myorg", "mytoken")
    mock_session.assert_called_once_with("mytoken")
