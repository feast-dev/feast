"""
External connection and credential resolution for Feast DataSources.

Provides a pluggable mechanism for DataSources to declare their full
connection identity — which backend to use, how to authenticate, and
where to connect — via a :class:`ConnectionRef` stored on each DataSource.

Credentials are resolved at runtime from external systems (Kubernetes
Secrets, HashiCorp Vault, cloud secret managers, environment variables)
instead of embedding them in ``feature_store.yaml``.

Usage::

    from feast.credentials import ConnectionRef

    # Minimal: just credentials (connection_type inferred from source class)
    source = FileSource(
        path="s3://bucket/features/",
        connection_ref=ConnectionRef(
            provider="kubernetes",
            name="my-s3-secret",
            namespace="ml-project",
        ),
    )

    # Full: explicit connection type + auth + params
    source = SnowflakeSource(
        table="USER_FEATURES",
        connection_ref=ConnectionRef(
            provider="kubernetes",
            name="snowflake-creds",
            namespace="ml-team",
            connection_type="snowflake.offline",
            auth_type="secret",
            params={"account": "xy12345", "warehouse": "COMPUTE_WH"},
        ),
    )

Providers are registered via :func:`register_credential_provider` and
resolved at runtime by :func:`resolve_credentials`.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ConnectionRef — the connection + credential reference stored on a DataSource
# ---------------------------------------------------------------------------

TAG_PREFIX = "feast.connection-ref."


@dataclass(frozen=True)
class ConnectionRef:
    """Immutable reference to an external connection and credential store.

    ``ConnectionRef`` is intended for credentials that are stored externally
    (K8s Secrets, Vault, etc.) and need to be resolved at runtime.  Auth
    methods that are handled natively by the cloud SDK credential chain
    (e.g., AWS IAM roles, IRSA, EKS Pod Identity, GCP Workload Identity)
    do **not** need a ``ConnectionRef`` — they are picked up automatically
    by the underlying client libraries (boto3, google-auth, etc.).

    Attributes:
        provider: Credential backend type — ``"kubernetes"``, ``"vault"``,
            ``"aws-secrets-manager"``, ``"gcp-secret-manager"``,
            ``"azure-key-vault"``, ``"env"``.
        name: Provider-specific identifier — K8s Secret name, Vault path,
            env-var prefix, etc.
        namespace: Optional scope qualifier — K8s namespace, Vault mount,
            AWS region, etc.  Defaults to ``""``.
        connection_type: Optional offline store class type (e.g.,
            ``"snowflake.offline"``, ``"bigquery"``, ``"spark"``).
            When empty, inferred from the DataSource class at runtime.
        auth_type: Authentication mechanism — ``"secret"`` (default),
            ``"oauth2"``, ``"basic"``, ``"sigv4"``.
        params: Optional non-sensitive connection parameters (e.g.,
            account, database, warehouse, endpoint URI).
    """

    provider: str
    name: str
    namespace: str = ""
    connection_type: str = ""
    auth_type: str = "secret"
    params: Dict[str, str] = field(default_factory=dict)

    # -- serialization to/from DataSource tags (backward-compatible) --------

    def to_tags(self) -> Dict[str, str]:
        """Serialize into DataSource ``tags`` dict entries."""
        tags: Dict[str, str] = {
            f"{TAG_PREFIX}provider": self.provider,
            f"{TAG_PREFIX}name": self.name,
        }
        if self.namespace:
            tags[f"{TAG_PREFIX}namespace"] = self.namespace
        if self.connection_type:
            tags[f"{TAG_PREFIX}connection-type"] = self.connection_type
        if self.auth_type and self.auth_type != "secret":
            tags[f"{TAG_PREFIX}auth-type"] = self.auth_type
        for key, value in self.params.items():
            tags[f"{TAG_PREFIX}param.{key}"] = value
        return tags

    @classmethod
    def from_tags(cls, tags: Dict[str, str]) -> Optional["ConnectionRef"]:
        """Deserialize from DataSource ``tags``.  Returns *None* when no
        connection-ref tags are present."""
        provider = tags.get(f"{TAG_PREFIX}provider")
        name = tags.get(f"{TAG_PREFIX}name")
        if not provider or not name:
            return None

        namespace = tags.get(f"{TAG_PREFIX}namespace", "")
        connection_type = tags.get(f"{TAG_PREFIX}connection-type", "")
        auth_type = tags.get(f"{TAG_PREFIX}auth-type", "secret")

        params: Dict[str, str] = {}
        param_prefix = f"{TAG_PREFIX}param."
        for key, value in tags.items():
            if key.startswith(param_prefix):
                param_key = key[len(param_prefix) :]
                params[param_key] = value

        return cls(
            provider=provider,
            name=name,
            namespace=namespace,
            connection_type=connection_type,
            auth_type=auth_type,
            params=params,
        )


# ---------------------------------------------------------------------------
# CredentialProvider — pluggable backend abstraction
# ---------------------------------------------------------------------------


class CredentialProvider(ABC):
    """Resolves a :class:`ConnectionRef` into key-value credential pairs."""

    @abstractmethod
    def provider_type(self) -> str:
        """Return the provider identifier this implementation handles."""
        ...

    @abstractmethod
    def resolve(self, ref: ConnectionRef) -> Dict[str, str]:
        """Return credential key-value pairs for *ref*.

        Raises:
            CredentialResolutionError: If the credentials cannot be resolved.
        """
        ...


class CredentialResolutionError(Exception):
    """Raised when a :class:`CredentialProvider` cannot resolve credentials."""


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

_PROVIDERS: Dict[str, CredentialProvider] = {}


def register_credential_provider(provider: CredentialProvider) -> None:
    """Register a :class:`CredentialProvider` for its declared type."""
    _PROVIDERS[provider.provider_type()] = provider


def get_credential_provider(provider_type: str) -> CredentialProvider:
    """Return the registered provider for *provider_type*.

    Raises:
        CredentialResolutionError: If no provider is registered.
    """
    if provider_type not in _PROVIDERS:
        raise CredentialResolutionError(
            f"No CredentialProvider registered for type '{provider_type}'. "
            f"Available: {list(_PROVIDERS.keys())}"
        )
    return _PROVIDERS[provider_type]


def resolve_credentials(ref: ConnectionRef) -> Dict[str, str]:
    """Convenience wrapper: look up the provider and resolve *ref*."""
    return get_credential_provider(ref.provider).resolve(ref)


# ---------------------------------------------------------------------------


def get_connection_config_override(data_source) -> Optional[Dict[str, str]]:
    """Get merged connection config from a DataSource's ``connection_ref``.

    Merges resolved credentials (secrets) with non-sensitive ``params``
    from the connection ref.  Returns ``None`` if no ``connection_ref`` is set.

    Offline stores can use this to override their global config::

        override = get_connection_config_override(data_source)
        if override:
            account = override.get("account", config.offline_store.account)
            ...
    """
    ref = getattr(data_source, "connection_ref", None)
    if ref is None:
        return None
    creds = resolve_credentials(ref)
    if ref.params:
        merged = dict(ref.params)
        merged.update(creds)
        return merged
    return creds


# ---------------------------------------------------------------------------
# Built-in provider: Environment variables (backward-compatible default)
# ---------------------------------------------------------------------------


class EnvironmentProvider(CredentialProvider):
    """Reads credentials from environment variables.

    ``ref.name`` is used as a prefix filter.  For example,
    ``ConnectionRef(provider="env", name="AWS")`` returns all env vars
    starting with ``AWS`` (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    ``AWS_DEFAULT_REGION``, …).

    If ``ref.name`` is empty or ``"*"``, all env vars are returned (use with
    care).
    """

    def provider_type(self) -> str:
        return "env"

    def resolve(self, ref: ConnectionRef) -> Dict[str, str]:
        prefix = ref.name
        if not prefix or prefix == "*":
            logger.warning(
                "EnvironmentProvider: resolving ALL environment variables "
                "(name='%s'). Restrict with a prefix to avoid exposing "
                "unrelated variables.",
                ref.name,
            )
            return dict(os.environ)
        return {k: v for k, v in os.environ.items() if k.startswith(prefix)}


# ---------------------------------------------------------------------------
# Built-in provider: Kubernetes Secrets
# ---------------------------------------------------------------------------


class KubernetesSecretProvider(CredentialProvider):
    """Reads credentials from Kubernetes Secrets via the K8s API.

    Requires the ``kubernetes`` Python package and a valid kubeconfig or
    in-cluster service account.

    ``ref.name`` is the Secret name.  ``ref.namespace`` is the K8s namespace
    (falls back to the Pod's own namespace when empty).
    """

    def provider_type(self) -> str:
        return "kubernetes"

    def resolve(self, ref: ConnectionRef) -> Dict[str, str]:
        try:
            from kubernetes import client
            from kubernetes import config as k8s_config
        except ImportError as exc:
            raise CredentialResolutionError(
                "kubernetes package is required for the 'kubernetes' "
                "credential provider.  Install it with: "
                "pip install kubernetes"
            ) from exc

        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            try:
                k8s_config.load_kube_config()
            except k8s_config.ConfigException as exc:
                raise CredentialResolutionError(
                    "Could not load Kubernetes configuration. "
                    "Ensure the Pod has a service account or a valid kubeconfig."
                ) from exc

        namespace = ref.namespace or self._current_namespace()
        v1 = client.CoreV1Api()
        try:
            secret = v1.read_namespaced_secret(name=ref.name, namespace=namespace)
        except client.exceptions.ApiException as exc:
            raise CredentialResolutionError(
                f"Failed to read Kubernetes Secret '{ref.name}' "
                f"in namespace '{namespace}': {exc.reason} (HTTP {exc.status})"
            ) from None

        import base64

        return {
            key: base64.b64decode(value).decode("utf-8")
            for key, value in (secret.data or {}).items()
        }

    @staticmethod
    def _current_namespace() -> str:
        """Return the namespace this Pod is running in."""
        ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        try:
            with open(ns_path) as f:
                return f.read().strip()
        except FileNotFoundError:
            return "default"


# ---------------------------------------------------------------------------
# Built-in provider: HashiCorp Vault (KV v2)
# ---------------------------------------------------------------------------


@dataclass
class VaultProviderConfig:
    """Configuration for the Vault credential provider."""

    addr: str = field(default_factory=lambda: os.environ.get("VAULT_ADDR", ""))
    token: str = field(default_factory=lambda: os.environ.get("VAULT_TOKEN", ""))
    role: str = field(default_factory=lambda: os.environ.get("VAULT_ROLE", ""))
    auth_method: str = field(
        default_factory=lambda: os.environ.get("VAULT_AUTH_METHOD", "token")
    )


class VaultProvider(CredentialProvider):
    """Reads credentials from HashiCorp Vault KV v2 secrets engine.

    ``ref.name`` is the Vault secret path (e.g. ``"secret/data/feast/my-conn"``).
    ``ref.namespace`` is the Vault mount point (defaults to ``"secret"``).

    Requires the ``hvac`` Python package.
    """

    def __init__(self, config: Optional[VaultProviderConfig] = None):
        self._config = config or VaultProviderConfig()

    def provider_type(self) -> str:
        return "vault"

    def resolve(self, ref: ConnectionRef) -> Dict[str, str]:
        try:
            import hvac
        except ImportError as exc:
            raise CredentialResolutionError(
                "hvac package is required for the 'vault' credential provider. "
                "Install it with: pip install hvac"
            ) from exc

        vault_client = hvac.Client(url=self._config.addr, token=self._config.token)
        if not vault_client.is_authenticated():
            raise CredentialResolutionError(
                "Vault client is not authenticated. "
                "Set VAULT_ADDR and VAULT_TOKEN, or configure auth_method."
            )

        mount_point = ref.namespace or "secret"
        try:
            response = vault_client.secrets.kv.v2.read_secret_version(
                path=ref.name, mount_point=mount_point
            )
        except Exception as exc:
            raise CredentialResolutionError(
                f"Failed to read Vault secret '{ref.name}' "
                f"at mount '{mount_point}': {type(exc).__name__}: {exc}"
            ) from None

        data = response.get("data", {}).get("data", {})
        return {k: str(v) for k, v in data.items()}


# ---------------------------------------------------------------------------
# Auto-register built-in providers on import
# ---------------------------------------------------------------------------

register_credential_provider(EnvironmentProvider())
register_credential_provider(KubernetesSecretProvider())
register_credential_provider(VaultProvider())
