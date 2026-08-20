import base64
from unittest.mock import MagicMock, patch

import pytest

from feast.credentials import (
    ConnectionRef,
    CredentialProvider,
    CredentialResolutionError,
    EnvironmentProvider,
    KubernetesSecretProvider,
    VaultProvider,
    VaultProviderConfig,
    get_credential_provider,
    register_credential_provider,
    resolve_credentials,
)

# ---------------------------------------------------------------------------
# ConnectionRef serialization
# ---------------------------------------------------------------------------


class TestConnectionRefTags:
    def test_minimal_roundtrip(self):
        ref = ConnectionRef(provider="kubernetes", name="my-secret")
        tags = ref.to_tags()
        restored = ConnectionRef.from_tags(tags)
        assert restored == ref

    def test_full_roundtrip(self):
        ref = ConnectionRef(
            provider="vault",
            name="secret/data/feast/conn",
            namespace="ml-team",
            connection_type="snowflake.offline",
            auth_type="oauth2",
            params={"account": "xy12345", "warehouse": "COMPUTE_WH"},
        )
        tags = ref.to_tags()
        restored = ConnectionRef.from_tags(tags)
        assert restored == ref

    def test_from_tags_returns_none_when_no_provider(self):
        assert ConnectionRef.from_tags({"unrelated": "tag"}) is None

    def test_from_tags_returns_none_when_no_name(self):
        tags = {"feast.connection-ref.provider": "kubernetes"}
        assert ConnectionRef.from_tags(tags) is None

    def test_default_auth_type_not_serialized(self):
        ref = ConnectionRef(provider="env", name="AWS", auth_type="secret")
        tags = ref.to_tags()
        assert "feast.connection-ref.auth-type" not in tags

    def test_non_default_auth_type_serialized(self):
        ref = ConnectionRef(provider="env", name="AWS", auth_type="oauth2")
        tags = ref.to_tags()
        assert tags["feast.connection-ref.auth-type"] == "oauth2"

    def test_params_roundtrip(self):
        ref = ConnectionRef(
            provider="env",
            name="PG",
            params={"host": "localhost", "port": "5432"},
        )
        tags = ref.to_tags()
        assert tags["feast.connection-ref.param.host"] == "localhost"
        assert tags["feast.connection-ref.param.port"] == "5432"
        assert ConnectionRef.from_tags(tags) == ref

    def test_mixed_tags_ignored(self):
        tags = {
            "feast.connection-ref.provider": "env",
            "feast.connection-ref.name": "AWS",
            "team": "ml",
            "version": "2",
        }
        ref = ConnectionRef.from_tags(tags)
        assert ref == ConnectionRef(provider="env", name="AWS")

    def test_immutable(self):
        ref = ConnectionRef(provider="env", name="AWS")
        with pytest.raises(AttributeError):
            ref.provider = "vault"


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


class TestProviderRegistry:
    def test_unknown_provider_raises(self):
        with pytest.raises(CredentialResolutionError, match="No CredentialProvider"):
            get_credential_provider("nonexistent-provider-xyz")

    def test_builtin_providers_registered(self):
        assert get_credential_provider("env") is not None
        assert get_credential_provider("kubernetes") is not None
        assert get_credential_provider("vault") is not None

    def test_custom_provider_registration(self):
        class DummyProvider(CredentialProvider):
            def provider_type(self):
                return "dummy-test"

            def resolve(self, ref):
                return {"key": "value"}

        register_credential_provider(DummyProvider())
        provider = get_credential_provider("dummy-test")
        result = provider.resolve(ConnectionRef(provider="dummy-test", name="x"))
        assert result == {"key": "value"}


# ---------------------------------------------------------------------------
# EnvironmentProvider
# ---------------------------------------------------------------------------


class TestEnvironmentProvider:
    def test_prefix_filter(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA...")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
        monkeypatch.setenv("UNRELATED_VAR", "nope")

        provider = EnvironmentProvider()
        ref = ConnectionRef(provider="env", name="AWS")
        result = provider.resolve(ref)

        assert "AWS_ACCESS_KEY_ID" in result
        assert "AWS_SECRET_ACCESS_KEY" in result
        assert "UNRELATED_VAR" not in result

    def test_empty_prefix_returns_all(self, monkeypatch):
        monkeypatch.setenv("TEST_CRED_VAR", "yes")
        provider = EnvironmentProvider()
        ref = ConnectionRef(provider="env", name="*")
        result = provider.resolve(ref)
        assert "TEST_CRED_VAR" in result

    def test_no_match_returns_empty(self, monkeypatch):
        provider = EnvironmentProvider()
        ref = ConnectionRef(provider="env", name="NONEXISTENT_PREFIX_XYZ_")
        result = provider.resolve(ref)
        assert result == {}


# ---------------------------------------------------------------------------
# KubernetesSecretProvider
# ---------------------------------------------------------------------------


class TestKubernetesSecretProvider:
    @patch(
        "feast.credentials.KubernetesSecretProvider._current_namespace",
        return_value="default",
    )
    def test_resolve_reads_and_decodes_secret(self, _mock_ns):
        mock_secret = MagicMock()
        mock_secret.data = {
            "username": base64.b64encode(b"admin").decode(),
            "password": base64.b64encode(
                b"s3cret"
            ).decode(),  # pragma: allowlist secret
        }

        with patch("kubernetes.config.load_incluster_config"):
            with patch("kubernetes.client.CoreV1Api") as mock_api_cls:
                mock_api_cls.return_value.read_namespaced_secret.return_value = (
                    mock_secret
                )

                provider = KubernetesSecretProvider()
                ref = ConnectionRef(
                    provider="kubernetes", name="my-secret", namespace="test-ns"
                )
                result = provider.resolve(ref)

        assert result == {
            "username": "admin",
            "password": "s3cret",  # pragma: allowlist secret
        }
        mock_api_cls.return_value.read_namespaced_secret.assert_called_once_with(
            name="my-secret", namespace="test-ns"
        )

    @patch(
        "feast.credentials.KubernetesSecretProvider._current_namespace",
        return_value="default",
    )
    def test_resolve_uses_pod_namespace_when_empty(self, mock_ns):
        mock_secret = MagicMock()
        mock_secret.data = {"key": base64.b64encode(b"val").decode()}

        with patch("kubernetes.config.load_incluster_config"):
            with patch("kubernetes.client.CoreV1Api") as mock_api_cls:
                mock_api_cls.return_value.read_namespaced_secret.return_value = (
                    mock_secret
                )

                provider = KubernetesSecretProvider()
                ref = ConnectionRef(provider="kubernetes", name="my-secret")
                provider.resolve(ref)

        mock_api_cls.return_value.read_namespaced_secret.assert_called_once_with(
            name="my-secret", namespace="default"
        )

    @patch(
        "feast.credentials.KubernetesSecretProvider._current_namespace",
        return_value="default",
    )
    def test_resolve_raises_on_api_error(self, _mock_ns):
        from kubernetes.client.exceptions import ApiException

        with patch("kubernetes.config.load_incluster_config"):
            with patch("kubernetes.client.CoreV1Api") as mock_api_cls:
                mock_api_cls.return_value.read_namespaced_secret.side_effect = (
                    ApiException(status=403, reason="Forbidden")
                )

                provider = KubernetesSecretProvider()
                ref = ConnectionRef(
                    provider="kubernetes", name="forbidden-secret", namespace="ns"
                )
                with pytest.raises(CredentialResolutionError, match="Forbidden"):
                    provider.resolve(ref)

    @patch(
        "feast.credentials.KubernetesSecretProvider._current_namespace",
        return_value="default",
    )
    def test_resolve_handles_empty_secret_data(self, _mock_ns):
        mock_secret = MagicMock()
        mock_secret.data = None

        with patch("kubernetes.config.load_incluster_config"):
            with patch("kubernetes.client.CoreV1Api") as mock_api_cls:
                mock_api_cls.return_value.read_namespaced_secret.return_value = (
                    mock_secret
                )

                provider = KubernetesSecretProvider()
                ref = ConnectionRef(
                    provider="kubernetes", name="empty-secret", namespace="ns"
                )
                result = provider.resolve(ref)

        assert result == {}


# ---------------------------------------------------------------------------
# VaultProvider
# ---------------------------------------------------------------------------


try:
    import hvac  # noqa: F401

    _has_hvac = True
except ImportError:
    _has_hvac = False


@pytest.mark.skipif(not _has_hvac, reason="hvac not installed")
class TestVaultProvider:
    def test_resolve_reads_kv_v2_secret(self):
        mock_client_instance = MagicMock()
        mock_client_instance.is_authenticated.return_value = True
        mock_client_instance.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "api_key": "abc123",  # pragma: allowlist secret
                    "endpoint": "https://api.example.com",
                }
            }
        }

        with patch("hvac.Client", return_value=mock_client_instance):
            config = VaultProviderConfig(addr="https://vault:8200", token="s.token")
            provider = VaultProvider(config=config)
            ref = ConnectionRef(
                provider="vault", name="feast/my-conn", namespace="secret"
            )
            result = provider.resolve(ref)

        assert result == {
            "api_key": "abc123",  # pragma: allowlist secret
            "endpoint": "https://api.example.com",
        }
        mock_client_instance.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="feast/my-conn", mount_point="secret"
        )

    def test_resolve_defaults_mount_to_secret(self):
        mock_client_instance = MagicMock()
        mock_client_instance.is_authenticated.return_value = True
        mock_client_instance.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"k": "v"}}
        }

        with patch("hvac.Client", return_value=mock_client_instance):
            provider = VaultProvider(
                config=VaultProviderConfig(addr="http://vault", token="t")
            )
            ref = ConnectionRef(provider="vault", name="path/to/secret")
            provider.resolve(ref)

        mock_client_instance.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="path/to/secret", mount_point="secret"
        )

    def test_resolve_raises_when_not_authenticated(self):
        mock_client_instance = MagicMock()
        mock_client_instance.is_authenticated.return_value = False

        with patch("hvac.Client", return_value=mock_client_instance):
            provider = VaultProvider(
                config=VaultProviderConfig(addr="http://vault", token="bad")
            )
            ref = ConnectionRef(provider="vault", name="path")
            with pytest.raises(CredentialResolutionError, match="not authenticated"):
                provider.resolve(ref)

    def test_resolve_raises_on_vault_error(self):
        mock_client_instance = MagicMock()
        mock_client_instance.is_authenticated.return_value = True
        mock_client_instance.secrets.kv.v2.read_secret_version.side_effect = Exception(
            "permission denied"
        )

        with patch("hvac.Client", return_value=mock_client_instance):
            provider = VaultProvider(
                config=VaultProviderConfig(addr="http://vault", token="t")
            )
            ref = ConnectionRef(provider="vault", name="forbidden/path")
            with pytest.raises(CredentialResolutionError, match="permission denied"):
                provider.resolve(ref)


# ---------------------------------------------------------------------------
# resolve_credentials (end-to-end dispatch)
# ---------------------------------------------------------------------------


class TestResolveCredentials:
    def test_dispatches_to_correct_provider(self, monkeypatch):
        monkeypatch.setenv("MYAPP_KEY", "resolved-value")
        ref = ConnectionRef(provider="env", name="MYAPP_")
        result = resolve_credentials(ref)
        assert result["MYAPP_KEY"] == "resolved-value"

    def test_raises_for_unknown_provider(self):
        ref = ConnectionRef(provider="does-not-exist", name="x")
        with pytest.raises(CredentialResolutionError):
            resolve_credentials(ref)
