"""Unit tests for vendor_spend.databricks_auth."""

from unittest.mock import MagicMock

import pytest

from lucille.vendor_spend.config import DatabricksConfig
from lucille.vendor_spend.databricks_auth import (
    _oidc_token_url,
    clear_token_cache,
    mint_oauth_token,
    resolve_bearer_token,
)


@pytest.fixture(autouse=True)
def _reset_cache():
    clear_token_cache()
    yield
    clear_token_cache()


def _ok_response(payload):
    r = MagicMock()
    r.json.return_value = payload
    r.raise_for_status.return_value = None
    return r


class TestOidcTokenUrl:
    def test_default_host(self):
        assert (
            _oidc_token_url("https://accounts.cloud.databricks.com", "acct-uuid")
            == "https://accounts.cloud.databricks.com/oidc/accounts/acct-uuid/v1/token"
        )

    def test_strips_trailing_slash(self):
        assert (
            _oidc_token_url("https://accounts.cloud.databricks.com/", "acct-uuid")
            == "https://accounts.cloud.databricks.com/oidc/accounts/acct-uuid/v1/token"
        )


class TestMintOauthToken:
    def test_posts_with_basic_auth_and_returns_access_token(self):
        session = MagicMock()
        session.post.return_value = _ok_response(
            {"access_token": "minted-abc", "token_type": "Bearer", "expires_in": 3600}
        )

        token = mint_oauth_token(
            "https://accounts.cloud.databricks.com",
            "acct-uuid",
            "client-id-1",
            "client-secret-1",
            session=session,
        )

        assert token == "minted-abc"
        session.post.assert_called_once()
        url = session.post.call_args.args[0]
        kwargs = session.post.call_args.kwargs
        assert url == "https://accounts.cloud.databricks.com/oidc/accounts/acct-uuid/v1/token"
        assert kwargs["auth"] == ("client-id-1", "client-secret-1")
        assert kwargs["data"] == {"grant_type": "client_credentials", "scope": "all-apis"}

    def test_missing_access_token_raises(self):
        session = MagicMock()
        session.post.return_value = _ok_response({"token_type": "Bearer"})
        with pytest.raises(RuntimeError, match="missing access_token"):
            mint_oauth_token(
                "https://accounts.cloud.databricks.com",
                "acct",
                "id",
                "secret",
                session=session,
            )


class TestResolveBearerToken:
    def _cfg(self):
        return DatabricksConfig(account_id="acct-uuid")

    def test_static_token_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "static-tok")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        session = MagicMock()
        token = resolve_bearer_token(self._cfg(), session=session)
        assert token == "static-tok"
        session.post.assert_not_called()  # no minting needed

    def test_falls_back_to_oauth_when_no_static_token(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        session = MagicMock()
        session.post.return_value = _ok_response({"access_token": "minted"})

        token = resolve_bearer_token(self._cfg(), session=session)
        assert token == "minted"
        session.post.assert_called_once()

    def test_caches_minted_token(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        session = MagicMock()
        session.post.return_value = _ok_response({"access_token": "minted-1"})

        cfg = self._cfg()
        t1 = resolve_bearer_token(cfg, session=session)
        t2 = resolve_bearer_token(cfg, session=session)

        assert t1 == t2 == "minted-1"
        assert session.post.call_count == 1  # second call hit the cache

    def test_use_cache_false_remints(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        session = MagicMock()
        session.post.side_effect = [
            _ok_response({"access_token": "first"}),
            _ok_response({"access_token": "second"}),
        ]

        cfg = self._cfg()
        assert resolve_bearer_token(cfg, session=session, use_cache=False) == "first"
        assert resolve_bearer_token(cfg, session=session, use_cache=False) == "second"
        assert session.post.call_count == 2

    def test_raises_when_nothing_configured(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        with pytest.raises(RuntimeError, match="No Databricks credentials"):
            resolve_bearer_token(self._cfg())

    def test_raises_when_only_client_id_set(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        with pytest.raises(RuntimeError, match="No Databricks credentials"):
            resolve_bearer_token(self._cfg())

    def test_honours_custom_env_var_names(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("MY_DBX_ID", "cid")
        monkeypatch.setenv("MY_DBX_SECRET", "csec")
        session = MagicMock()
        session.post.return_value = _ok_response({"access_token": "minted"})

        cfg = DatabricksConfig(
            account_id="acct",
            client_id_env="MY_DBX_ID",
            client_secret_env="MY_DBX_SECRET",
        )
        assert resolve_bearer_token(cfg, session=session) == "minted"
