from __future__ import annotations

import asyncio
import functools
import importlib
import threading
from datetime import datetime, timezone
from logging import getLogger
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import SecretStr

try:
    import aerospike
    from aerospike_helpers import cdt_ctx
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
from feast.type_map import (
    feast_value_type_to_python_type,
    python_values_to_proto_values,
)

logger = getLogger(__name__)


# Aerospike per-record batch result codes we treat specially. Anything not
# listed here is surfaced as an exception so a transient server error (e.g.
# timeout, device overload) is never silently misreported as a missing feature.
# See https://aerospike.com/docs/server/reference/errors.
_AS_OK: int = 0
_AS_ERR_RECORD_NOT_FOUND: int = 2  # genuine "entity has never been written"
_AS_ERR_OP_NOT_APPLICABLE: int = 26  # map/list op targeted a missing CDT path


_AUTH_MODE_TO_CONSTANT: Dict[str, int] = {
    "internal": aerospike.AUTH_INTERNAL,
    "external": aerospike.AUTH_EXTERNAL,
    "pki": aerospike.AUTH_PKI,
}


# Type alias for the prewriting-hook signature. Hooks receive the rows about
# to be written and return a (possibly transformed) row list with the same
# schema. Defined as ``Any`` in the value position to keep the public type
# hint readable; the actual contract is documented on
# ``AerospikeOnlineStoreConfig.prewriting_hook``.
PrewritingHook = Callable[
    [
        "RepoConfig",
        "FeatureView",
        List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
    ],
    List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
]


# Create every Map CDT bin with an ordered map. map_get_by_key /
# map_remove_by_key on an ordered map are O(log N) in the map size instead of
# O(N), which matters on the update() background scan (which walks every
# record in the project's set) and on reads of wide feature views. The policy
# is applied on each put so map-creation on the first write picks up the
# ordering; subsequent puts keep it.
_ORDERED_MAP_POLICY: Dict[str, Any] = {"map_order": aerospike.MAP_KEY_ORDERED}

# Aerospike server ``batch-max-requests`` defaults to 5000 on many clusters (0
# means unlimited on newer releases). Stay well under that so materialization
# and wide feature-server requests do not trip BatchMaxRequestError (code 151).
_DEFAULT_BATCH_MAX_RECORDS: int = 1_000

_T = TypeVar("_T")


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
    """Default Aerospike namespace. Must be pre-configured on the cluster —
    namespaces cannot be created at runtime. Used for any feature view not
    listed in :attr:`namespace_overrides`."""

    set_name_template: str = "{project}_{collection_suffix}"
    """Template for the per-project Aerospike set name. Available substitutions:
    ``{project}`` and ``{collection_suffix}``. Used for any feature view not
    listed in :attr:`set_overrides`."""

    collection_suffix: str = "latest"
    """Suffix used by ``set_name_template`` to distinguish sets belonging to the
    same project (e.g. a future multi-version layout)."""

    namespace_overrides: Dict[str, str] = {}
    """Per-feature-view namespace overrides. Maps a feature view name to the
    Aerospike namespace that view should be stored in. Falls back to
    :attr:`namespace` for any feature view not listed.

    Lets a deployment pin hot small views to a RAM-only namespace and wider
    archival views to an SSD-backed one without having to split projects::

        namespace_overrides = {
            "driver_realtime_stats": "feast_ram",
            "driver_history_lookup": "feast_ssd",
        }

    Every namespace listed here MUST already exist on the cluster — Aerospike
    cannot create namespaces at runtime, and a missing namespace surfaces as
    an opaque ``AEROSPIKE_ERR_PARAM`` on the first read or write.
    """

    set_overrides: Dict[str, str] = {}
    """Per-feature-view set name overrides. Maps a feature view name to a
    fully-qualified Aerospike set name. Falls back to the rendering of
    :attr:`set_name_template` for any feature view not listed.

    Useful when a deployment wants each feature view in its own set so admin
    operations like ``truncate`` or the ``feast apply`` deletion path can
    target one view without scanning the records of the others. Tradeoff:
    multi-feature-view reads on the same entity become multiple round trips
    instead of one — only set this when the operational isolation is worth
    that cost.
    """

    prewriting_hook: Optional[str] = None
    """Optional import path of a callable applied to every batch of rows
    before they are written. Used for cross-cutting concerns like PII
    masking, encryption-at-rest, value coercion or dual-write fan-out.

    Format: ``"package.module.function_name"``. Resolved by import string
    (rather than a Python ``Callable``) so the config survives YAML / JSON
    serialisation and remote-feature-server transport.

    The hook signature is :data:`PrewritingHook`::

        def hook(
            config: RepoConfig,
            table: FeatureView,
            data: List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]],
        ) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
            ...

    The hook MUST return a row list with the same shape as its input.
    Hooks that raise will fail the whole batch — there is no per-row
    fallback. The resolved callable is cached on the store instance; if the
    configured import string changes between calls, it is re-resolved
    automatically on the next write.
    """

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
    """Per-call write total timeout in milliseconds. This is the hard deadline
    the client gives a single ``put`` / ``operate`` — including any retries —
    to return a response, after which the call fails."""

    read_timeout_ms: int = 250
    """Per-call read total timeout in milliseconds. Hard deadline for a
    single-record ``get`` — including any retries — after which the call
    fails."""

    batch_total_timeout_ms: int = 2_000
    """Total timeout in milliseconds for a whole ``batch_write`` /
    ``batch_operate`` call, including retries. Applies to every batch
    operation ``online_read`` and ``online_write_batch`` issue."""

    socket_timeout_ms: Optional[int] = None
    """Per-attempt socket timeout in milliseconds. This is the per-retry
    trigger that lets ``max_retries`` actually fire within the caller's
    overall ``*_timeout_ms`` budget — without it, a single attempt can
    consume the whole deadline and retries never run. Applied uniformly
    to ``read``, ``write`` and ``batch`` policies. ``None`` leaves the
    client default in place."""

    max_retries: int = 2
    """Maximum number of automatic retries on transient errors."""

    batch_max_records: int = _DEFAULT_BATCH_MAX_RECORDS
    """Maximum records per ``batch_write`` / ``batch_operate`` call.

    Aerospike enforces a per-node batch size via the server ``batch-max-requests``
    setting (historically 5000). Feast chunks read and write paths to this limit
    so large materializations and wide online-serving requests do not fail the
    whole batch when the server cap is exceeded.
    """

    client_kwargs: Dict[str, Any] = {}
    """Escape hatch for any Aerospike client configuration not surfaced above.
    Merged into the client config passed to ``aerospike.client()``."""


class AerospikeOnlineStore(OnlineStore):
    """Aerospike implementation of the Feast :class:`OnlineStore`.

    Storage layout (one set per project):

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

    def __init__(self) -> None:
        # Kept on the instance rather than the class so two ``AerospikeOnlineStore``
        # instances can't accidentally share a cached client through class state.
        self._client: Optional[aerospike.Client] = None
        # Resolved prewriting hook, cached on the instance so the import
        # cost is paid once. ``_prewriting_hook_spec`` records the import
        # string the cache was built from so a config swap (re-binding the
        # store to a different hook) re-resolves on next call.
        self._prewriting_hook: Optional[PrewritingHook] = None
        self._prewriting_hook_spec: Optional[str] = None
        self._client_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle / connection management
    # ------------------------------------------------------------------
    @staticmethod
    def _chunked(items: Sequence[_T], chunk_size: int) -> Iterator[Sequence[_T]]:
        """Yield slices of ``items`` no larger than ``chunk_size``."""
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        for start in range(0, len(items), chunk_size):
            yield items[start : start + chunk_size]

    def _batch_max_records(self, config: RepoConfig) -> int:
        store_cfg = config.online_store
        if not isinstance(store_cfg, AerospikeOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be aerospike.")
        return store_cfg.batch_max_records

    def _get_client(self, config: RepoConfig) -> aerospike.Client:
        """Lazily create and cache an Aerospike client on first use.

        The underlying C client maintains its own connection pool, so a single
        cached instance is safe to share across calls on this store. Creation
        is guarded by a lock so concurrent first callers in a threaded feature
        server do not leak extra connections.
        """
        if self._client is not None:
            return self._client

        with self._client_lock:
            if self._client is not None:
                return self._client

            if not isinstance(config.online_store, AerospikeOnlineStoreConfig):
                raise RuntimeError(
                    f"{config.online_store.type = }. It must be aerospike."
                )
            store_cfg = config.online_store

            read_policy: Dict[str, Any] = {
                "total_timeout": store_cfg.read_timeout_ms,
                "max_retries": store_cfg.max_retries,
            }
            write_policy: Dict[str, Any] = {
                "total_timeout": store_cfg.write_timeout_ms,
                "max_retries": store_cfg.max_retries,
            }
            batch_policy: Dict[str, Any] = {
                "total_timeout": store_cfg.batch_total_timeout_ms,
                "max_retries": store_cfg.max_retries,
            }
            if store_cfg.socket_timeout_ms is not None:
                # socket_timeout is the per-attempt deadline; without it,
                # total_timeout is the whole budget and retries never fire.
                read_policy["socket_timeout"] = store_cfg.socket_timeout_ms
                write_policy["socket_timeout"] = store_cfg.socket_timeout_ms
                batch_policy["socket_timeout"] = store_cfg.socket_timeout_ms

            client_config: Dict[str, Any] = {
                "hosts": [tuple(h) for h in store_cfg.hosts],
                "policies": {
                    "read": read_policy,
                    "write": write_policy,
                    "batch": batch_policy,
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

    def _set_name(self, config: RepoConfig, fv_name: Optional[str] = None) -> str:
        """Resolve the Aerospike set name for a feature view.

        Without ``fv_name`` returns the project-level default rendered from
        ``set_name_template``. With an ``fv_name`` returns the override from
        :attr:`AerospikeOnlineStoreConfig.set_overrides` if present, falling
        back to the project default. Calling without ``fv_name`` is preserved
        for callers (and tests) that want the project's default set without
        knowing which feature views might override it.
        """
        store_cfg = config.online_store
        overrides = getattr(store_cfg, "set_overrides", None) or {}
        if fv_name is not None and fv_name in overrides:
            return overrides[fv_name]
        return store_cfg.set_name_template.format(
            project=config.project,
            collection_suffix=store_cfg.collection_suffix,
        )

    def _namespace_for_fv(
        self, config: RepoConfig, fv_name: Optional[str] = None
    ) -> str:
        """Resolve the Aerospike namespace for a feature view.

        Mirrors :meth:`_set_name` semantics: without ``fv_name`` returns the
        store-default namespace; with one, returns the override from
        :attr:`AerospikeOnlineStoreConfig.namespace_overrides` if present.
        """
        store_cfg = config.online_store
        overrides = getattr(store_cfg, "namespace_overrides", None) or {}
        if fv_name is not None and fv_name in overrides:
            return overrides[fv_name]
        return store_cfg.namespace

    def _aerospike_key(
        self,
        config: RepoConfig,
        entity_key: EntityKeyProto,
        fv_name: Optional[str] = None,
    ) -> Tuple[str, str, bytearray]:
        """Build a ``(namespace, set, user_key)`` tuple for an entity.

        When ``fv_name`` is provided, the namespace and set name honour the
        per-feature-view overrides from
        :class:`AerospikeOnlineStoreConfig`. Without ``fv_name`` the
        store-level defaults are used.

        The user key is returned as a ``bytearray`` rather than ``bytes``:
        the Aerospike Python C client rejects ``bytes`` user keys
        (``calc_digest`` raises ``"Key is invalid"``), and ``batch_read`` /
        ``batch_operate`` silently hash only the first byte of a ``bytes``
        key. ``bytearray`` is the supported binary-key type.
        """
        user_key = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        )
        return (
            self._namespace_for_fv(config, fv_name),
            self._set_name(config, fv_name),
            bytearray(user_key),
        )

    def _resolve_prewriting_hook(self, config: RepoConfig) -> Optional[PrewritingHook]:
        """Resolve and cache the configured prewriting hook, if any.

        The hook is referenced by import string in the config (so it
        survives YAML/JSON round-trips and remote-feature-server transport),
        and resolved here on first use. The resolved callable is cached on
        the store instance — and re-resolved automatically if a subsequent
        config swap rebinds the store to a different hook.
        """
        spec = getattr(config.online_store, "prewriting_hook", None)
        if not spec:
            # A previously-set hook on this instance must be cleared if the
            # caller has since unset it on the config; otherwise we'd keep
            # applying a hook the user explicitly turned off.
            self._prewriting_hook = None
            self._prewriting_hook_spec = None
            return None
        if spec == self._prewriting_hook_spec and self._prewriting_hook is not None:
            return self._prewriting_hook

        module_path, _, fn_name = spec.rpartition(".")
        if not module_path or not fn_name:
            raise ValueError(
                "AerospikeOnlineStoreConfig.prewriting_hook must be a fully "
                f"qualified import path 'package.module.function', got: {spec!r}"
            )
        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ValueError(
                f"prewriting_hook {spec!r}: could not import module "
                f"{module_path!r}: {e}"
            ) from e
        try:
            hook = getattr(module, fn_name)
        except AttributeError:
            raise ValueError(
                f"prewriting_hook {spec!r}: module {module_path!r} has no "
                f"attribute {fn_name!r}"
            ) from None
        if not callable(hook):
            raise TypeError(
                f"prewriting_hook {spec!r} resolved to a non-callable "
                f"{type(hook).__name__}"
            )
        self._prewriting_hook = hook
        self._prewriting_hook_spec = spec
        return hook

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
        namespace: str,
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
        each other — each write only mutates its own slot in the outer map.

        The Map CDTs are created with ``MAP_KEY_ORDERED`` so key lookups on
        reads and the ``update()`` background scan stay O(log N) in the map
        size. Writes use the default ``POLICY_KEY_DIGEST`` — the serialized
        entity key itself is not stored on the server, saving per-record
        storage that the read path never consumes (result order is preserved
        by ``batch_operate`` and paired back via ``zip`` in ``online_read``).
        """
        ttl_meta = {"ttl": _resolve_ttl(config.online_store.ttl_seconds)}

        batch = BatchRecords()
        for entity_key, proto_values, event_timestamp, created_timestamp in data:
            user_key = bytearray(
                serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            )
            feature_map = {
                field: feast_value_type_to_python_type(val)
                for field, val in proto_values.items()
            }
            operations: List[Dict[str, Any]] = [
                map_ops.map_put_items(
                    "features",
                    {table.name: feature_map},
                    map_policy=_ORDERED_MAP_POLICY,
                ),
                map_ops.map_put(
                    "event_ts",
                    table.name,
                    _datetime_to_epoch_ms(event_timestamp),
                    map_policy=_ORDERED_MAP_POLICY,
                ),
            ]
            if created_timestamp is not None:
                operations.append(
                    ops.write("created_ts", _datetime_to_epoch_ms(created_timestamp))
                )
            batch.batch_records.append(
                BatchWrite(
                    key=(namespace, set_name, user_key),
                    ops=operations,
                    meta=ttl_meta,
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

        # Hook resolution happens before the early-data check to avoid wiring
        # work, but after the empty-batch check so a "no rows" call doesn't
        # pay the import cost. Hooks are allowed to mutate ``data`` in place
        # or return a new list; we always rebind to whatever they return.
        hook = self._resolve_prewriting_hook(config)
        if hook is not None:
            data = hook(config, table, data)
            if not data:
                if progress:
                    progress(0)
                return

        client = self._get_client(config)
        namespace = self._namespace_for_fv(config, table.name)
        set_name = self._set_name(config, table.name)
        chunk_size = self._batch_max_records(config)
        written = 0
        for chunk in self._chunked(data, chunk_size):
            batch = self._build_batch_writes(
                config, table, list(chunk), namespace, set_name
            )
            if batch.batch_records:
                client.batch_write(batch)
                # Per-record result codes must be inspected: client.batch_write
                # only raises if the whole request was rejected. A partial failure
                # (e.g. a single-partition timeout) is otherwise silent, which in
                # an online-serving path presents downstream as "model saw stale
                # features" weeks after the fact.
                self._raise_on_batch_errors(batch.batch_records, set_name, op="write")
            written += len(chunk)
            if progress:
                progress(written)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Read feature values for a batch of entities in a single round trip.

        Uses Aerospike's ``batch_operate`` with two server-side Map-get ops per
        record. When ``requested_features`` is provided, only those feature
        columns are shipped over the wire using ``map_get_by_key_list`` nested
        into the feature-view's Map CDT via ``cdt_ctx_map_key``. Otherwise the
        whole feature-view slot is returned.

        * features op (projected):
          ``map_get_by_key_list("features", requested_features,
          MAP_RETURN_KEY_VALUE, ctx=[cdt_ctx_map_key(<fv>)])``
        * features op (full):
          ``map_get_by_key("features", <fv>, MAP_RETURN_VALUE)``
        * event_ts op (always):
          ``map_get_by_key("event_ts", <fv>, MAP_RETURN_VALUE)``

        Per-record status codes are inspected so we can tell a genuine miss
        (``RECORD_NOT_FOUND``, or a nested ``OP_NOT_APPLICABLE`` when the
        feature-view slot is absent) apart from a transient server error,
        which is raised rather than silently returned as a null row. Output
        order matches ``entity_keys``.
        """
        if not entity_keys:
            return []

        client = self._get_client(config)
        ns = self._namespace_for_fv(config, table.name)
        set_name = self._set_name(config, table.name)
        read_ops = self._build_read_ops(table.name, requested_features)
        chunk_size = self._batch_max_records(config)

        # ``ids`` and ``docs`` use immutable ``bytes`` because ``bytearray`` is
        # unhashable and can't key a dict. Keys on the wire must stay
        # ``bytearray`` (see ``_aerospike_key``) — we only convert here for
        # lookup.
        ids: List[bytes] = []
        docs: Dict[bytes, Dict[str, Any]] = {}

        for entity_chunk in self._chunked(entity_keys, chunk_size):
            keys = [
                (
                    ns,
                    set_name,
                    bytearray(
                        serialize_entity_key(
                            k,
                            entity_key_serialization_version=config.entity_key_serialization_version,
                        )
                    ),
                )
                for k in entity_chunk
            ]
            batch = client.batch_operate(keys, read_ops)
            chunk_ids = [bytes(user_key) for _, _, user_key in keys]
            ids.extend(chunk_ids)
            # batch_operate preserves request order. We pair each response with
            # the original user-key rather than ``br.key[2]``: the Aerospike
            # client may return the key in a different representation (e.g. only
            # the first byte as a str when the write didn't use POLICY_KEY_SEND
            # for reads).
            for user_key, br in zip(chunk_ids, batch.batch_records):
                if br.result == _AS_ERR_RECORD_NOT_FOUND:
                    continue
                if br.result == _AS_ERR_OP_NOT_APPLICABLE:
                    # The record exists but the nested feature-view slot doesn't;
                    # treat as a miss to match the OnlineStore contract.
                    continue
                if br.result != _AS_OK:
                    raise RuntimeError(
                        f"Aerospike batch_operate returned a non-OK status for "
                        f"entity (ns={ns}, set={set_name}): result={br.result}"
                    )
                if br.record is None:
                    continue
                _, _, bins = br.record
                raw_features = bins.get("features") if bins else None
                fv_event_ts_ms = bins.get("event_ts") if bins else None
                fv_features = self._normalize_projected_features(raw_features)
                docs[user_key] = {
                    "features": {table.name: fv_features}
                    if fv_features is not None
                    else {},
                    "event_timestamps": {
                        table.name: _epoch_ms_to_datetime(fv_event_ts_ms)
                    },
                }

        return self._convert_raw_docs_to_proto(ids, docs, table)

    @staticmethod
    def _build_read_ops(
        fv_name: str, requested_features: Optional[List[str]]
    ) -> List[Dict[str, Any]]:
        """Build the per-record op list for an ``online_read`` call.

        Projects ``requested_features`` server-side via
        ``map_get_by_key_list`` + ``cdt_ctx_map_key`` when a projection list
        is provided. Without a projection, returns the whole feature-view
        submap.
        """
        if requested_features:
            features_op = map_ops.map_get_by_key_list(
                "features",
                list(requested_features),
                aerospike.MAP_RETURN_KEY_VALUE,
                ctx=[cdt_ctx.cdt_ctx_map_key(fv_name)],
            )
        else:
            features_op = map_ops.map_get_by_key(
                "features", fv_name, aerospike.MAP_RETURN_VALUE
            )
        return [
            features_op,
            map_ops.map_get_by_key("event_ts", fv_name, aerospike.MAP_RETURN_VALUE),
        ]

    @staticmethod
    def _normalize_projected_features(
        raw: Optional[Union[Dict[str, Any], List[Any]]],
    ) -> Optional[Dict[str, Any]]:
        """Convert an Aerospike features payload into a uniform ``{name: val}`` dict.

        The shape depends on which op produced the payload:

        * ``map_get_by_key("features", <fv>, MAP_RETURN_VALUE)`` returns a
          ``dict`` (the inner feature-view submap).
        * ``map_get_by_key_list("features", [...], MAP_RETURN_KEY_VALUE,
          ctx=...)`` returns a flat ``list`` of ``[k1, v1, k2, v2, ...]``
          containing only the requested keys that exist.
        """
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, list):
            if not raw:
                return {}
            return dict(zip(raw[0::2], raw[1::2]))
        return None

    @staticmethod
    def _raise_on_batch_errors(
        batch_records: Sequence[Any], set_name: str, op: str
    ) -> None:
        """Raise if any per-record result code signals a failed batch write/op.

        ``client.batch_write`` and ``client.batch_operate`` only raise when the
        overall request was rejected; partial failures (a single-partition
        timeout, a replica quorum miss, etc.) are surfaced per record via
        ``br.result`` and are otherwise silent. In an online-serving path
        those silent failures later present as missing features, so we fail
        loud here instead.
        """
        errors = [br.result for br in batch_records if br.result != _AS_OK]
        if errors:
            raise RuntimeError(
                f"Aerospike batch_{op} returned non-OK status codes for "
                f"{len(errors)} of {len(batch_records)} records "
                f"(set={set_name}): codes={errors[:10]}"
            )

    @staticmethod
    def _convert_raw_docs_to_proto(
        ids: List[bytes],
        docs: Dict[bytes, Dict[str, Any]],
        table: FeatureView,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Convert raw feature maps into ordered proto rows.

        The heavy lifting is done by
        :func:`feast.type_map.python_values_to_proto_values`, which is
        column-oriented and expects a list of values of a single type. This
        helper transforms the row-oriented Aerospike lookup result into
        columns, converts each column once, then reassembles rows — mirroring
        the MongoDB online store's reshape so we amortize the python→proto
        cost across the whole batch.

        Args:
            ids: serialized entity-key bytes, in the order requested.
            docs: ``{entity_id_bytes: {"features": {<fv>: {...}},
                "event_timestamps": {<fv>: datetime}}}``. Missing keys denote
                "record not found".
            table: FeatureView being read; provides feature name → type.

        Returns:
            A list of ``(event_timestamp, feature_dict)`` the same length as
            ``ids`` (``(None, None)`` for entities that had no data for this
            feature view).
        """
        feature_type_map = {
            feature.name: feature.dtype.to_value_type() for feature in table.features
        }

        raw_feature_columns: Dict[str, List[Any]] = {
            feature_name: [] for feature_name in feature_type_map
        }
        for entity_id in ids:
            doc = docs.get(entity_id)
            feature_dict = doc.get("features", {}).get(table.name, {}) if doc else {}
            for feature_name in feature_type_map:
                raw_feature_columns[feature_name].append(
                    feature_dict.get(feature_name, None)
                )

        proto_feature_columns = {
            feature_name: python_values_to_proto_values(
                raw_values, feature_type=feature_type_map[feature_name]
            )
            for feature_name, raw_values in raw_feature_columns.items()
        }

        results: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for i, entity_id in enumerate(ids):
            doc = docs.get(entity_id)
            if doc is None:
                results.append((None, None))
                continue

            fv_features = doc.get("features", {}).get(table.name)
            if fv_features is None:
                results.append((None, None))
                continue

            ts = doc.get("event_timestamps", {}).get(table.name)
            row_features = {
                feature_name: proto_feature_columns[feature_name][i]
                for feature_name in proto_feature_columns
            }
            results.append((ts, row_features))
        return results

    # ------------------------------------------------------------------
    # Async wrappers
    # ------------------------------------------------------------------
    # The Aerospike Python client is a synchronous C extension; there is no
    # native asyncio interface. Network calls do release the GIL, so we expose
    # a correct ``async`` surface by offloading each blocking call to the
    # default thread-pool executor. Callers that ``await`` these methods keep
    # the event loop responsive while the client talks to the cluster.

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            functools.partial(self.online_write_batch, config, table, data, progress),
        )

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            functools.partial(
                self.online_read, config, table, entity_keys, requested_features
            ),
        )

    async def initialize(self, config: RepoConfig) -> None:
        """Pre-warm the Aerospike client so the first request is hot.

        Feature servers typically call :meth:`initialize` during startup so the
        TCP + handshake latency is paid upfront rather than on the first
        ``online_read``.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, functools.partial(self._get_client, config))

    async def close(self) -> None:
        """Release the cached Aerospike client, if any."""
        with self._client_lock:
            if self._client is None:
                return
            client = self._client
            self._client = None
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, client.close)

    # ------------------------------------------------------------------
    # Admin paths (update / teardown)
    # ------------------------------------------------------------------
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ) -> None:
        """Reconcile per-feature-view data when a schema change is applied.

        Aerospike has no explicit schema, and records/sets are created lazily
        on first write, so there is nothing to do for ``tables_to_keep`` or
        either of the entity lists. For ``tables_to_delete`` we strip each
        feature-view's slot out of the ``features`` and ``event_ts`` Map
        CDTs on every record that contains it.

        Dropped feature views are grouped by their resolved
        ``(namespace, set)`` (per :attr:`namespace_overrides` /
        :attr:`set_overrides`), and each group is issued as one
        **background scan** with a combined op list — a single server-side
        pass per (ns, set), regardless of how many feature views in that
        group are being dropped. Without this grouping, a deployment that
        spreads feature views across multiple namespaces or sets would
        either issue blind cross-namespace scans (impossible) or scan one
        per dropped FV (wasteful). The scans run asynchronously server-side
        and return immediately; this matches the intent of ``feast apply``,
        after which the caller stops reading the dropped feature views
        anyway.
        """
        if not isinstance(config.online_store, AerospikeOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be aerospike.")
        if not tables_to_delete:
            return

        client = self._get_client(config)

        # Group dropped feature views by (namespace, set) so each scan only
        # touches the records that could plausibly contain the dropped slot.
        groups: Dict[Tuple[str, str], List[FeatureView]] = {}
        for fv in tables_to_delete:
            key = (
                self._namespace_for_fv(config, fv.name),
                self._set_name(config, fv.name),
            )
            groups.setdefault(key, []).append(fv)

        for (ns, set_name), fvs in groups.items():
            remove_ops: List[Dict[str, Any]] = []
            for fv in fvs:
                remove_ops.append(
                    map_ops.map_remove_by_key(
                        "features", fv.name, aerospike.MAP_RETURN_NONE
                    )
                )
                remove_ops.append(
                    map_ops.map_remove_by_key(
                        "event_ts", fv.name, aerospike.MAP_RETURN_NONE
                    )
                )

            scan = client.scan(ns, set_name)
            scan.add_ops(remove_ops)
            scan.execute_background()

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        """Truncate every (namespace, set) the project may have written to
        and close the cached client.

        Uses Aerospike's ``truncate(namespace, set, 0)`` — a set-scoped
        metadata operation that clears every record in O(1) client time,
        cheaper than Mongo's ``collection.drop()``. Passing ``0`` as the
        cutoff means "drop everything regardless of last-update time".

        Collects the unique ``(namespace, set)`` pairs from the project's
        store-level default plus every feature view in ``tables`` (each
        resolved through :attr:`namespace_overrides` /
        :attr:`set_overrides`). The default is always included so a
        teardown invoked with an empty ``tables`` list still clears the
        store-default location.

        Truncate on a non-existent set is a no-op, so calling ``teardown``
        on a project that never wrote data — or on a feature view that
        was never written to — is safe.
        """
        if not isinstance(config.online_store, AerospikeOnlineStoreConfig):
            raise RuntimeError(f"{config.online_store.type = }. It must be aerospike.")

        client = self._get_client(config)

        pairs: Set[Tuple[str, str]] = {
            (config.online_store.namespace, self._set_name(config))
        }
        for fv in tables:
            pairs.add(
                (
                    self._namespace_for_fv(config, fv.name),
                    self._set_name(config, fv.name),
                )
            )
        # sort to keep the truncation order deterministic for tests; the
        # operation is independent per (ns, set) so order has no semantic
        # meaning.
        for ns, set_name in sorted(pairs):
            client.truncate(ns, set_name, 0)
        with self._client_lock:
            if self._client is not None:
                self._client.close()
                self._client = None
