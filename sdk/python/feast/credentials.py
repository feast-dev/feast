"""
External credential resolution for Feast DataSources.

Provides a pluggable mechanism for DataSources to reference credentials
stored in external systems (Kubernetes Secrets, HashiCorp Vault, cloud
secret managers) instead of embedding them in ``feature_store.yaml`` or
relying on ambient environment variables.

Usage::

    from feast.credentials import CredentialRef

    source = FileSource(
        path="s3://bucket/features/",
        credential_ref=CredentialRef(
            provider="kubernetes",
            name="my-s3-secret",
            namespace="ml-project",
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
# CredentialRef — the reference stored on a DataSource
# ---------------------------------------------------------------------------

TAG_PREFIX = "feast.credential-ref."


@dataclass(frozen=True)
class CredentialRef:
    """Immutable reference to an external credential store.

    Attributes:
        provider: Backend type — ``"kubernetes"``, ``"vault"``,
            ``"aws-secrets-manager"``, ``"gcp-secret-manager"``,
            ``"azure-key-vault"``, ``"env"``.
        name: Provider-specific identifier — K8s Secret name, Vault path,
            env-var prefix, etc.
        namespace: Optional scope qualifier — K8s namespace, Vault mount,
            AWS region, etc.  Defaults to ``""``.
    """

    provider: str
    name: str
    namespace: str = ""

    # -- serialization to/from DataSource tags (backward-compatible) --------

    def to_tags(self) -> Dict[str, str]:
        """Serialize into DataSource ``tags`` dict entries."""
        tags: Dict[str, str] = {
            f"{TAG_PREFIX}provider": self.provider,
            f"{TAG_PREFIX}name": self.name,
        }
        if self.namespace:
            tags[f"{TAG_PREFIX}namespace"] = self.namespace
        return tags

    @classmethod
    def from_tags(cls, tags: Dict[str, str]) -> Optional["CredentialRef"]:
        """Deserialize from DataSource ``tags``.  Returns *None* when no
        credential-ref tags are present."""
        provider = tags.get(f"{TAG_PREFIX}provider")
        name = tags.get(f"{TAG_PREFIX}name")
        if not provider or not name:
            return None
        return cls(
            provider=provider,
            name=name,
            namespace=tags.get(f"{TAG_PREFIX}namespace", ""),
        )


# ---------------------------------------------------------------------------
# CredentialProvider — pluggable backend abstraction
# ---------------------------------------------------------------------------


class CredentialProvider(ABC):
    """Resolves a :class:`CredentialRef` into key-value credential pairs."""

    @abstractmethod
    def provider_type(self) -> str:
        """Return the provider identifier this implementation handles."""
        ...

    @abstractmethod
    def resolve(self, ref: CredentialRef) -> Dict[str, str]:
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


def resolve_credentials(ref: CredentialRef) -> Dict[str, str]:
    """Convenience wrapper: look up the provider and resolve *ref*."""
    return get_credential_provider(ref.provider).resolve(ref)


# ---------------------------------------------------------------------------
# Built-in provider: Environment variables (backward-compatible default)
# ---------------------------------------------------------------------------


class EnvironmentProvider(CredentialProvider):
    """Reads credentials from environment variables.

    ``ref.name`` is used as a prefix filter.  For example,
    ``CredentialRef(provider="env", name="AWS")`` returns all env vars
    starting with ``AWS`` (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    ``AWS_DEFAULT_REGION``, …).

    If ``ref.name`` is empty or ``"*"``, all env vars are returned (use with
    care).
    """

    def provider_type(self) -> str:
        return "env"

    def resolve(self, ref: CredentialRef) -> Dict[str, str]:
        prefix = ref.name
        if not prefix or prefix == "*":
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

    def resolve(self, ref: CredentialRef) -> Dict[str, str]:
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
                f"in namespace '{namespace}': {exc.reason}"
            ) from exc

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

    def resolve(self, ref: CredentialRef) -> Dict[str, str]:
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
                f"at mount '{mount_point}': {exc}"
            ) from exc

        data = response.get("data", {}).get("data", {})
        return {k: str(v) for k, v in data.items()}


# ---------------------------------------------------------------------------
# Auto-register built-in providers on import
# ---------------------------------------------------------------------------

register_credential_provider(EnvironmentProvider())
register_credential_provider(KubernetesSecretProvider())
register_credential_provider(VaultProvider())
