from __future__ import annotations

from datetime import datetime
from logging import getLogger
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import SecretStr

try:
    import aerospike  # noqa: F401
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aerospike", str(e))

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig

logger = getLogger(__name__)


class AerospikeOnlineStoreConfig(FeastConfigBaseModel):
    """Aerospike configuration.

    Aerospike does not have a URI analogue; connections are established via a
    seed list of ``(host, port)`` or ``(host, port, tls_name)`` tuples. See the
    Aerospike Python client reference for the meaning of additional policies and
    TLS options surfaced below, and use ``client_kwargs`` for anything not
    explicitly modelled here.
    """

    type: Literal["aerospike"] = "aerospike"
    """Online store type selector"""

    hosts: List[Union[Tuple[str, int], Tuple[str, int, str]]] = [("localhost", 3000)]
    """Aerospike seed nodes.

    Each entry is either ``(host, port)`` or ``(host, port, tls_name)`` when TLS
    is enabled. At least one seed node is required.
    """

    namespace: str = "feast"
    """Aerospike namespace. Must be pre-configured on the cluster — namespaces
    cannot be created at runtime."""

    set_name_template: str = "{project}_{collection_suffix}"
    """Template for the per-project Aerospike set name. Available substitutions:
    ``{project}`` and ``{collection_suffix}``."""

    collection_suffix: str = "latest"
    """Suffix used by ``set_name_template`` to distinguish sets belonging to the
    same project (e.g. a future multi-version layout)."""

    user: Optional[str] = None
    """Optional username for Aerospike Enterprise authentication."""

    password: Optional[SecretStr] = None
    """Optional password for Aerospike Enterprise authentication."""

    auth_mode: Literal["internal", "external", "pki"] = "internal"
    """Authentication mode. ``internal`` for CE/EE user/password, ``external``
    for LDAP/Kerberos, ``pki`` for certificate-based auth."""

    tls: Optional[Dict[str, Any]] = None
    """TLS configuration, passed through verbatim to the Aerospike client.
    See the Aerospike Python client ``tls`` policy options."""

    ttl_seconds: Optional[int] = None
    """Record-level TTL, applied to every write. ``None`` uses the namespace
    default, ``0`` means never expire (mapped to the client's ``-1`` sentinel).
    No per-feature-view override in v1."""

    write_timeout_ms: int = 1_000
    """Per-call write timeout in milliseconds."""

    read_timeout_ms: int = 250
    """Per-call read timeout in milliseconds."""

    total_timeout_ms: int = 2_000
    """Total (including retries) timeout in milliseconds."""

    max_retries: int = 2
    """Maximum number of automatic retries on transient errors."""

    client_kwargs: Dict[str, Any] = {}
    """Escape hatch for any Aerospike client configuration not surfaced above.
    Merged into the client config passed to ``aerospike.client()``."""


class AerospikeOnlineStore(OnlineStore):
    """Aerospike implementation of the Feast :class:`OnlineStore`.

    This is a scaffold stub: the full read/write implementation lands in
    follow-up commits on this branch. See
    ``notes/aerospike-online-store-plan.md`` for the storage model, schema,
    and delivery plan.

    Planned layout (MongoDB-style, one set per project):

    * Namespace: ``config.online_store.namespace`` (server-configured)
    * Set:       ``{project}_{collection_suffix}``
    * Key:       ``serialize_entity_key(entity_key)`` (bytes)
    * Bins:

      * ``features``   — Map CDT ``{"<fv>": {"<feature>": <native_value>}}``
      * ``event_ts``   — Map CDT ``{"<fv>": <datetime>}``
      * ``created_ts`` — top-level datetime
    """

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        raise NotImplementedError(
            "AerospikeOnlineStore.online_write_batch is not implemented yet."
        )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        raise NotImplementedError(
            "AerospikeOnlineStore.online_read is not implemented yet."
        )

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        raise NotImplementedError("AerospikeOnlineStore.update is not implemented yet.")

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        raise NotImplementedError(
            "AerospikeOnlineStore.teardown is not implemented yet."
        )
