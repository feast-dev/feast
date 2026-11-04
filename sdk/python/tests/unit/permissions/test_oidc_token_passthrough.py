"""
Tests for OIDC client-side token passthrough feature.

Covers:
  - Config validation (OidcClientAuthConfig)
  - Token manager (OidcAuthClientManager.get_token)
  - Routing (RepoConfig.auth_config property)
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from feast.permissions.auth_model import OidcClientAuthConfig
from feast.permissions.client.oidc_authentication_client_manager import (
    OidcAuthClientManager,
)
from feast.repo_config import RepoConfig

# ---------------------------------------------------------------------------
#  Config validation
# ---------------------------------------------------------------------------


class TestOidcClientAuthConfigValidation:
    def test_bare_oidc_valid(self):
        cfg = OidcClientAuthConfig(type="oidc")
        assert cfg.token_env_var is None
        assert cfg.auth_discovery_url is None
        assert cfg.client_id is None

    def test_token_alone_valid(self):
        cfg = OidcClientAuthConfig(type="oidc", token="eyJhbGciOiJSUzI1NiJ9.test")
        assert cfg.token == "eyJhbGciOiJSUzI1NiJ9.test"

    def test_token_env_var_alone_valid(self):
        cfg = OidcClientAuthConfig(type="oidc", token_env_var="MY_VAR")
        assert cfg.token_env_var == "MY_VAR"

    def test_token_plus_custom_env_var_invalid(self):
        with pytest.raises(ValueError, match="Only one of"):
            OidcClientAuthConfig(
                type="oidc",
                token="eyJtoken",
                token_env_var="MY_VAR",
            )

    def test_client_secret_without_discovery_url_invalid(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            OidcClientAuthConfig(
                type="oidc",
                client_secret="my-secret",  # pragma: allowlist secret
            )

    def test_full_client_credentials_valid(self):
        cfg = OidcClientAuthConfig(
            type="oidc",
            client_secret="my-secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp.example.com/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert cfg.client_secret == "my-secret"  # pragma: allowlist secret

    def test_full_ropg_valid(self):
        cfg = OidcClientAuthConfig(
            type="oidc",
            username="user1",
            password="pass1",  # pragma: allowlist secret
            client_secret="my-secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp.example.com/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert cfg.username == "user1"

    def test_ropg_without_discovery_url_invalid(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            OidcClientAuthConfig(
                type="oidc",
                username="user1",
                password="pass1",  # pragma: allowlist secret
                client_secret="my-secret",  # pragma: allowlist secret
            )

    def test_username_without_client_secret_invalid(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            OidcClientAuthConfig(
                type="oidc",
                username="user1",
                password="pass1",  # pragma: allowlist secret
            )

    def test_token_plus_client_secret_invalid(self):
        with pytest.raises(ValueError, match="Only one of"):
            OidcClientAuthConfig(
                type="oidc",
                token="jwt",
                client_secret="secret",  # pragma: allowlist secret
                auth_discovery_url="https://idp/.well-known/openid-configuration",
                client_id="feast-client",
            )

    def test_token_env_var_with_discovery_url_invalid(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            OidcClientAuthConfig(
                type="oidc",
                token_env_var="MY_VAR",
                auth_discovery_url="https://idp/.well-known/openid-configuration",
                client_id="feast-client",
            )

    def test_token_with_discovery_url_invalid(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            OidcClientAuthConfig(
                type="oidc",
                token="eyJ.test",
                auth_discovery_url="https://idp/.well-known/openid-configuration",
                client_id="feast-client",
            )


# ---------------------------------------------------------------------------
#  Token manager
# ---------------------------------------------------------------------------


class TestOidcAuthClientManagerGetToken:
    def _make_manager(self, **kwargs) -> OidcAuthClientManager:
        cfg = OidcClientAuthConfig(type="oidc", **kwargs)
        return OidcAuthClientManager(cfg)

    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_token_returned_directly(self, mock_discovery_cls):
        mgr = self._make_manager(token="my-static-jwt")
        assert mgr.get_token() == "my-static-jwt"
        mock_discovery_cls.assert_not_called()

    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value=None,
    )
    def test_no_token_source_raises(self, _mock_sa):
        mgr = self._make_manager()
        with pytest.raises(PermissionError, match="No OIDC token source configured"):
            mgr.get_token()

    @patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "env-jwt-value"})
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_explicit_feast_env_var(self, mock_discovery_cls):
        mgr = self._make_manager(token_env_var="FEAST_OIDC_TOKEN")
        assert mgr.get_token() == "env-jwt-value"
        mock_discovery_cls.assert_not_called()

    @patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "fallback-jwt"})
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_bare_config_falls_back_to_well_known_env(self, mock_discovery_cls):
        mgr = self._make_manager()
        assert mgr.get_token() == "fallback-jwt"
        mock_discovery_cls.assert_not_called()

    @patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "should-not-win"}, clear=False)
    @patch("feast.permissions.client.oidc_authentication_client_manager.requests")
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_network_config_not_overridden_by_well_known_env(
        self, mock_discovery_cls, mock_requests
    ):
        mock_discovery_instance = MagicMock()
        mock_discovery_instance.get_token_url.return_value = "https://idp/token"
        mock_discovery_cls.return_value = mock_discovery_instance

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "idp-token"}
        mock_requests.post.return_value = mock_response

        mgr = self._make_manager(
            client_secret="secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert mgr.get_token() == "idp-token"

    @patch.dict(os.environ, {"CUSTOM_TOKEN_VAR": "custom-env-jwt"})
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_custom_env_var_read(self, mock_discovery_cls):
        mgr = self._make_manager(token_env_var="CUSTOM_TOKEN_VAR")
        assert mgr.get_token() == "custom-env-jwt"
        mock_discovery_cls.assert_not_called()

    @patch.dict(os.environ, {}, clear=False)
    @patch("feast.permissions.client.oidc_authentication_client_manager.requests")
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_fallthrough_to_client_credentials(self, mock_discovery_cls, mock_requests):
        os.environ.pop("FEAST_OIDC_TOKEN", None)

        mock_discovery_instance = MagicMock()
        mock_discovery_instance.get_token_url.return_value = "https://idp/token"
        mock_discovery_cls.return_value = mock_discovery_instance

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "network-token"}
        mock_requests.post.return_value = mock_response

        mgr = self._make_manager(
            client_secret="secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert mgr.get_token() == "network-token"
        mock_discovery_cls.assert_called_once()

    @patch.dict(os.environ, {}, clear=False)
    @patch("feast.permissions.client.oidc_authentication_client_manager.requests")
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_ropg_flow(self, mock_discovery_cls, mock_requests):
        os.environ.pop("FEAST_OIDC_TOKEN", None)

        mock_discovery_instance = MagicMock()
        mock_discovery_instance.get_token_url.return_value = "https://idp/token"
        mock_discovery_cls.return_value = mock_discovery_instance

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "ropg-token"}
        mock_requests.post.return_value = mock_response

        mgr = self._make_manager(
            username="user1",
            password="pass1",  # pragma: allowlist secret
            client_secret="secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert mgr.get_token() == "ropg-token"

        call_args = mock_requests.post.call_args
        assert call_args[1]["data"]["grant_type"] == "password"
        assert call_args[1]["data"]["username"] == "user1"

    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_token_takes_priority_over_env_var(self, mock_discovery_cls):
        with patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "env-token"}):
            mgr = self._make_manager(token="config-token")
            assert mgr.get_token() == "config-token"
            mock_discovery_cls.assert_not_called()

    @patch.dict(os.environ, {}, clear=False)
    def test_configured_env_var_missing_raises(self):
        os.environ.pop("MY_CUSTOM_VAR", None)
        mgr = self._make_manager(token_env_var="MY_CUSTOM_VAR")
        with pytest.raises(PermissionError, match="token_env_var='MY_CUSTOM_VAR'"):
            mgr.get_token()

    @patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "stale-token"}, clear=False)
    def test_configured_env_var_missing_does_not_fall_through(self):
        os.environ.pop("MY_CUSTOM_VAR", None)
        mgr = self._make_manager(token_env_var="MY_CUSTOM_VAR")
        with pytest.raises(PermissionError, match="token_env_var='MY_CUSTOM_VAR'"):
            mgr.get_token()

    # --- SA token file fallback tests ---

    @patch.dict(os.environ, {}, clear=False)
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value="sa-jwt-from-file",
    )
    def test_sa_token_file_read(self, _mock_sa):
        os.environ.pop("FEAST_OIDC_TOKEN", None)
        mgr = self._make_manager()
        assert mgr.get_token() == "sa-jwt-from-file"

    @patch.dict(os.environ, {}, clear=False)
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value=None,
    )
    def test_sa_token_file_missing_raises(self, _mock_sa):
        os.environ.pop("FEAST_OIDC_TOKEN", None)
        mgr = self._make_manager()
        with pytest.raises(PermissionError, match="No OIDC token source configured"):
            mgr.get_token()

    @patch.dict(os.environ, {"FEAST_OIDC_TOKEN": "env-token"}, clear=False)
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value="sa-jwt-from-file",
    )
    def test_feast_env_takes_priority_over_sa_token(self, _mock_sa):
        mgr = self._make_manager()
        assert mgr.get_token() == "env-token"
        _mock_sa.assert_not_called()

    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value="sa-jwt-from-file",
    )
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_explicit_token_skips_sa_file(self, mock_discovery_cls, _mock_sa):
        mgr = self._make_manager(token="my-explicit-token")
        assert mgr.get_token() == "my-explicit-token"
        _mock_sa.assert_not_called()

    @patch.dict(os.environ, {}, clear=False)
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OidcAuthClientManager._read_sa_token",
        return_value="sa-jwt-from-file",
    )
    @patch("feast.permissions.client.oidc_authentication_client_manager.requests")
    @patch(
        "feast.permissions.client.oidc_authentication_client_manager.OIDCDiscoveryService"
    )
    def test_client_secret_skips_sa_file(
        self, mock_discovery_cls, mock_requests, _mock_sa
    ):
        os.environ.pop("FEAST_OIDC_TOKEN", None)

        mock_discovery_instance = MagicMock()
        mock_discovery_instance.get_token_url.return_value = "https://idp/token"
        mock_discovery_cls.return_value = mock_discovery_instance

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "network-token"}
        mock_requests.post.return_value = mock_response

        mgr = self._make_manager(
            client_secret="secret",  # pragma: allowlist secret
            auth_discovery_url="https://idp/.well-known/openid-configuration",
            client_id="feast-client",
        )
        assert mgr.get_token() == "network-token"
        _mock_sa.assert_not_called()


# ---------------------------------------------------------------------------
#  Routing (RepoConfig.auth_config property)
# ---------------------------------------------------------------------------


class TestOidcClientRouting:
    def _make_repo_config(self, auth_dict: dict) -> RepoConfig:
        return RepoConfig(
            project="test_project",
            registry="data/registry.db",
            provider="local",
            auth=auth_dict,
        )

    def test_bare_oidc_routes_to_client(self):
        rc = self._make_repo_config({"type": "oidc"})
        assert isinstance(rc.auth_config, OidcClientAuthConfig)

    def test_token_routes_to_client(self):
        rc = self._make_repo_config({"type": "oidc", "token": "x"})
        assert isinstance(rc.auth_config, OidcClientAuthConfig)
        assert rc.auth_config.token == "x"

    def test_token_env_var_routes_to_client(self):
        rc = self._make_repo_config({"type": "oidc", "token_env_var": "MY_VAR"})
        assert isinstance(rc.auth_config, OidcClientAuthConfig)
        assert rc.auth_config.token_env_var == "MY_VAR"

    def test_server_config_routes_to_oidc_auth_config(self):
        from feast.permissions.auth_model import OidcAuthConfig

        rc = self._make_repo_config(
            {
                "type": "oidc",
                "auth_discovery_url": "https://idp/.well-known/openid-configuration",
                "client_id": "feast-server",
            }
        )
        assert isinstance(rc.auth_config, OidcAuthConfig)
        assert type(rc.auth_config) is OidcAuthConfig

    def test_ropg_routes_to_client(self):
        rc = self._make_repo_config(
            {
                "type": "oidc",
                "auth_discovery_url": "https://idp/.well-known/openid-configuration",
                "client_id": "feast-client",
                "client_secret": "secret",  # pragma: allowlist secret
                "username": "user1",
                "password": "pass1",  # pragma: allowlist secret
            }
        )
        assert isinstance(rc.auth_config, OidcClientAuthConfig)

    def test_incomplete_ropg_routes_to_client_with_actionable_error(self):
        with pytest.raises(
            ValueError, match="Incomplete configuration for 'client_credentials'"
        ):
            self._make_repo_config(
                {
                    "type": "oidc",
                    "auth_discovery_url": "https://idp/.well-known/openid-configuration",
                    "client_id": "feast-client",
                    "username": "user1",
                    "password": "pass1",  # pragma: allowlist secret
                }
            ).auth_config
