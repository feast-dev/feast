from __future__ import annotations

from datetime import datetime, timezone
from logging import getLogger
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import SecretStr

try:
    import aerospike
    from aerospike_helpers.batch.records import BatchRecords
    from aerospike_helpers.batch.records import Write as BatchWrite
    from aerospike_helpers.operations import map_operations as map_ops
    from aerospike_helpers.operations import operations as ops
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aerospike", str(e))

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.type_map import feast_value_type_to_python_type

logger = getLogger(__name__)


_AUTH_MODE_TO_CONSTANT: Dict[str, int] = {
    "internal": aerospike.AUTH_INTERNAL,
    "external": aerospike.AUTH_EXTERNAL,
    "pki": aerospike.AUTH_PKI,
}


def _datetime_to_epoch_ms(dt: datetime) -> int:
    """Convert a datetime to int64 epoch milliseconds.

    Aerospike has no native datetime type, so timestamps are stored as int
    bins. Per the OnlineStore contract, a tz-naive timestamp is treated as UTC.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _epoch_ms_to_datetime(value: Optional[int]) -> Optional[datetime]:
    """Inverse of :func:`_datetime_to_epoch_ms`. Returns a tz-aware UTC datetime."""
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)


def _resolve_ttl(ttl_seconds: Optional[int]) -> int:
    """Map the config's ``ttl_seconds`` to an Aerospike record-metadata TTL.

    * ``None`` -> use the namespace default (``TTL_NAMESPACE_DEFAULT``).
    * ``0``    -> never expire (``TTL_NEVER_EXPIRE``).
    * ``> 0``  -> that many seconds until expiry.
    """
    if ttl_seconds is None:
        return aerospike.TTL_NAMESPACE_DEFAULT
    if ttl_seconds == 0:
        return aerospike.TTL_NEVER_EXPIRE
    return int(ttl_seconds)


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

    Storage layout (MongoDB-style, one set per project):

    * Namespace: ``config.online_store.namespace`` (server-configured)
    * Set:       ``{project}_{collection_suffix}``
    * Key:       ``serialize_entity_key(entity_key)`` (bytes)
    * Bins:

      * ``features``   — Map CDT ``{"<fv>": {"<feature>": <native_value>}}``
      * ``event_ts``   — Map CDT ``{"<fv>": <epoch_ms_int>}``
      * ``created_ts`` — top-level ``<epoch_ms_int>``

    Timestamps are stored as int64 epoch milliseconds because Aerospike has no
    native datetime type. Tz-naive timestamps are treated as UTC per the
    :class:`OnlineStore` contract.
    """

    _client: Optional[aerospike.Client] = None

    # ------------------------------------------------------------------
    # Lifecycle / connection management
    # ------------------------------------------------------------------
    def _get_client(self, config: RepoConfig) -> aerospike.Client:
        """Lazily create and cache an Aerospike client on first use.

        The underlying C client maintains its own connection pool, so a single
        cached instance is safe to share across calls on this store.
        """
        if self._client is not None:
            return self._client

        if not isinstance(config.online_store, AerospikeOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be aerospike.")
        store_cfg = config.online_store

        client_config: Dict[str, Any] = {
            "hosts": [tuple(h) for h in store_cfg.hosts],
            "policies": {
                "read": {
                    "total_timeout": store_cfg.read_timeout_ms,
                    "max_retries": store_cfg.max_retries,
                },
                "write": {
                    "total_timeout": store_cfg.write_timeout_ms,
                    "max_retries": store_cfg.max_retries,
                },
                "batch": {"total_timeout": store_cfg.total_timeout_ms},
            },
            **store_cfg.client_kwargs,
        }
        if store_cfg.user:
            if store_cfg.password is None:
                raise ValueError(
                    "AerospikeOnlineStoreConfig.user is set but password is not."
                )
            client_config["user"] = store_cfg.user
            client_config["password"] = store_cfg.password.get_secret_value()
            client_config["auth_mode"] = _AUTH_MODE_TO_CONSTANT[store_cfg.auth_mode]
        if store_cfg.tls:
            client_config["tls"] = store_cfg.tls

        self._client = aerospike.client(client_config).connect()
        return self._client

    def _set_name(self, config: RepoConfig) -> str:
        """Render the per-project Aerospike set name from the configured template."""
        store_cfg = config.online_store
        return store_cfg.set_name_template.format(
            project=config.project,
            collection_suffix=store_cfg.collection_suffix,
        )

    def _aerospike_key(
        self, config: RepoConfig, entity_key: EntityKeyProto
    ) -> Tuple[str, str, bytes]:
        """Build a ``(namespace, set, user_key_bytes)`` tuple for an entity."""
        user_key = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        return (config.online_store.namespace, self._set_name(config), user_key)

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------
    @staticmethod
    def _build_batch_writes(
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        set_name: str,
    ) -> BatchRecords:
        """Build a :class:`BatchRecords` with one :class:`BatchWrite` per row.

        Each row becomes an atomic, server-side Map-put op list:

        * ``features[<fv>][<feature>] = <native>`` for every requested feature
          (single ``map_put_items`` op keyed by feature-view name).
        * ``event_ts[<fv>] = <epoch_ms>``.
        * ``created_ts = <epoch_ms>`` when provided.

        Using Map CDT ops rather than a full-record ``put`` means two writers
        touching different feature views on the same entity will not clobber
        each other, matching the MongoDB ``$set`` semantics.
        """
        ns = config.online_store.namespace
        ttl_meta = {"ttl": _resolve_ttl(config.online_store.ttl_seconds)}
        write_policy = {"key": aerospike.POLICY_KEY_SEND}

        batch = BatchRecords()
        for entity_key, proto_values, event_timestamp, created_timestamp in data:
            user_key = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            feature_map = {
                field: feast_value_type_to_python_type(val)
                for field, val in proto_values.items()
            }
            operations: List[Dict[str, Any]] = [
                map_ops.map_put_items("features", {table.name: feature_map}),
                map_ops.map_put(
                    "event_ts", table.name, _datetime_to_epoch_ms(event_timestamp)
                ),
            ]
            if created_timestamp is not None:
                operations.append(
                    ops.write("created_ts", _datetime_to_epoch_ms(created_timestamp))
                )
            batch.batch_records.append(
                BatchWrite(
                    key=(ns, set_name, user_key),
                    ops=operations,
                    meta=ttl_meta,
                    policy=write_policy,
                )
            )
        return batch

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Write a batch of feature rows using Aerospike's native batch-write API.

        Each row is upserted as a set of Map CDT operations on a single record,
        preserving data for other feature views that share the same entity key.
        """
        if not data:
            if progress:
                progress(0)
            return

        client = self._get_client(config)
        set_name = self._set_name(config)
        batch = self._build_batch_writes(config, table, data, set_name)
        if batch.batch_records:
            client.batch_write(batch)
        if progress:
            progress(len(data))

    # ------------------------------------------------------------------
    # Read / admin paths — implemented in subsequent chunks.
    # ------------------------------------------------------------------
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
