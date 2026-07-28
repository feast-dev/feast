# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import json
import logging
import math
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    ByteString,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr

from feast import Entity, FeatureView, RepoConfig, utils
from feast.infra.key_encoding_utils import deserialize_entity_key, serialize_entity_key
from feast.infra.online_stores.helpers import (
    _mmh3,
    _redis_key,
    _redis_key_prefix,
    compute_versioned_name,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.type_map import feast_value_type_to_python_type

try:
    from redis import Redis
    from redis import asyncio as redis_asyncio
    from redis.cluster import ClusterNode, RedisCluster
    from redis.sentinel import Sentinel
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("redis", str(e))

logger = logging.getLogger(__name__)


def _versioned_fv_name(table: FeatureView, config: RepoConfig) -> str:
    """Return the feature view name with version suffix when versioning is enabled."""
    return compute_versioned_name(
        table, config.registry.enable_online_feature_view_versioning
    )


class RedisType(str, Enum):
    redis = "redis"
    redis_cluster = "redis_cluster"
    redis_sentinel = "redis_sentinel"


class RedisOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """Online store config for Redis store"""

    type: Literal["redis"] = "redis"
    """Online store type selector"""

    redis_type: RedisType = RedisType.redis
    """Redis type: redis or redis_cluster"""

    sentinel_master: StrictStr = "mymaster"
    """Sentinel's master name"""

    connection_string: StrictStr = "localhost:6379"
    """Connection string containing the host, port, and configuration parameters for Redis
     format: host:port,parameter1,parameter2 eg. redis:6379,db=0 """

    key_ttl_seconds: Optional[int] = None
    """(Optional) redis key bin ttl (in seconds) for expiring entities"""

    full_scan_for_deletion: Optional[bool] = True
    """(Optional) whether to scan for deletion of features"""

    skip_dedup: bool = False
    """(Optional) Skip timestamp deduplication check on writes for higher throughput.
    When True, writes proceed in a single pipeline without reading existing timestamps first.
    This may cause older feature values to overwrite newer ones under concurrent writers."""


class RedisOnlineStore(OnlineStore):
    """
    Redis implementation of the online store interface.

    See https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md#redis-online-store-format
    for more details about the data model for this implementation.

    Attributes:
        _client: Redis connection.
    """

    _client: Optional[Union[Redis, RedisCluster]] = None
    _client_async: Optional[Union[redis_asyncio.Redis, redis_asyncio.RedisCluster]] = (
        None
    )
    _vadd_supported: Optional[bool] = None

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        return SupportedAsyncMethods(read=True, write=True)

    def _check_vadd_supported(self, client: Union[Redis, RedisCluster]) -> bool:
        """Check if the connected Redis server supports VADD (Redis 8+ Vector Sets)."""
        if self._vadd_supported is not None:
            return self._vadd_supported
        try:
            info = client.execute_command("COMMAND", "INFO", "VADD")
            self._vadd_supported = (
                info is not None and len(info) > 0 and info[0] is not None
            )
        except Exception:
            self._vadd_supported = False
        return self._vadd_supported

    @staticmethod
    def _vector_set_key(project: str, fv_name: str) -> str:
        """Return the Redis key used for the Vector Set of a feature view."""
        return f"vs:{project}:{fv_name}"

    @staticmethod
    def _vector_element_id(
        entity_key: EntityKeyProto, entity_key_serialization_version: int = 3
    ) -> str:
        """Create a unique string element ID from an entity key for use in VADD/VSIM."""
        return serialize_entity_key(
            entity_key,
            entity_key_serialization_version=entity_key_serialization_version,
        ).hex()

    @staticmethod
    def _normalize_vector(vector: Sequence[float]) -> List[float]:
        """Return a unit-length copy of *vector* for vector-set indexing/search."""
        floats = [float(v) for v in vector]
        norm = math.sqrt(sum(v * v for v in floats))
        if norm == 0:
            return floats
        return [v / norm for v in floats]

    @staticmethod
    def _json_safe_attr_value(value: Any) -> Any:
        """Convert Redis vector-set attributes into JSON-serializable values."""
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, list):
            return [RedisOnlineStore._json_safe_attr_value(v) for v in value]
        if isinstance(value, tuple):
            return [RedisOnlineStore._json_safe_attr_value(v) for v in value]
        if isinstance(value, set):
            return [RedisOnlineStore._json_safe_attr_value(v) for v in value]
        if isinstance(value, dict):
            return {
                str(k): RedisOnlineStore._json_safe_attr_value(v)
                for k, v in value.items()
            }
        return value

    def _vector_search_metric(
        self,
        config: RepoConfig,
        table: FeatureView,
        distance_metric: Optional[str] = None,
    ) -> str:
        """Resolve the vector-search metric for the feature view."""
        vector_fields = [f for f in table.features if getattr(f, "vector_index", None)]
        field_metric = (
            getattr(vector_fields[0], "vector_search_metric", None)
            if vector_fields
            else None
        )
        metric = (
            distance_metric
            or field_metric
            or getattr(config.online_store, "similarity", None)
        )
        metric = (metric or "COSINE").upper()
        if metric != "COSINE":
            raise ValueError(
                f"Unsupported distance metric {metric}. Redis online store only supports COSINE."
            )
        return metric

    def _vadd_vectors(
        self,
        client: Union[Redis, RedisCluster],
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
    ) -> None:
        """Index vector embeddings into a Redis Vector Set using VADD.

        Identifies the vector field from the FeatureView schema and issues
        VADD commands for each entity row that contains a vector value.
        Non-vector features and entity join-key values are stored as JSON
        attributes via SETATTR so they can be used with VSIM FILTER.
        """
        vector_fields = [f for f in table.features if getattr(f, "vector_index", None)]
        if not vector_fields:
            return
        vector_field = vector_fields[0]
        fv_name = _versioned_fv_name(table, config)
        vs_key = self._vector_set_key(config.project, fv_name)
        self._vector_search_metric(config, table)

        with client.pipeline(transaction=False) as pipe:
            for entity_key, values, _ts, _created in data:
                vec_val = values.get(vector_field.name)
                if vec_val is None:
                    continue
                python_vector = feast_value_type_to_python_type(vec_val)
                if not isinstance(python_vector, Sequence) or isinstance(
                    python_vector, (bytes, str)
                ):
                    continue
                floats = [float(v) for v in python_vector]
                if not floats:
                    continue
                floats = self._normalize_vector(floats)
                dim = len(floats)
                element_id = self._vector_element_id(
                    entity_key, config.entity_key_serialization_version
                )
                # Build attribute JSON for entity join keys
                attrs: Dict[str, Any] = {}
                for jk, ev in zip(entity_key.join_keys, entity_key.entity_values):
                    attrs[jk] = self._json_safe_attr_value(
                        feast_value_type_to_python_type(ev)
                    )
                attr_json = json.dumps(attrs)

                # VADD vs_key VALUES <dim> <v1> ... <vN> <element_id> SETATTR <attr_json>
                cmd_args: List[Any] = [vs_key, "VALUES", str(dim)]
                cmd_args.extend(str(f) for f in floats)
                cmd_args.append(element_id)
                cmd_args.extend(["SETATTR", attr_json])
                pipe.execute_command("VADD", *cmd_args)
            pipe.execute()

        key_ttl_seconds = getattr(config.online_store, "key_ttl_seconds", None)
        if key_ttl_seconds:
            client.expire(name=vs_key, time=key_ttl_seconds)

    def delete_entity_values(self, config: RepoConfig, join_keys: List[str]):
        client = self._get_client(config.online_store)
        deleted_count = 0
        prefix = _redis_key_prefix(join_keys)
        deleted_entity_ids: List[str] = []
        project_bytes = config.project.encode("utf8")
        can_delete_vectors = getattr(config.online_store, "vector_enabled", False)

        with client.pipeline(transaction=False) as pipe:
            for _k in client.scan_iter(
                b"".join([prefix, b"*", config.project.encode("utf8")])
            ):
                pipe.delete(_k)
                deleted_count += 1
                if can_delete_vectors and _k.endswith(project_bytes):
                    serialized_entity_key = _k[: -len(project_bytes)]
                    try:
                        entity_key = deserialize_entity_key(
                            serialized_entity_key,
                            entity_key_serialization_version=config.entity_key_serialization_version,
                        )
                        deleted_entity_ids.append(
                            self._vector_element_id(
                                entity_key, config.entity_key_serialization_version
                            )
                        )
                    except Exception:
                        # Keep deleting hash rows even if a key cannot be decoded.
                        continue
            pipe.execute()

        if (
            deleted_entity_ids
            and can_delete_vectors
            and self._check_vadd_supported(client)
        ):
            vector_set_keys = list(client.scan_iter(f"vs:{config.project}:*"))
            if vector_set_keys:
                with client.pipeline(transaction=False) as pipe:
                    for vs_key in vector_set_keys:
                        for entity_id in deleted_entity_ids:
                            pipe.execute_command("VREM", vs_key, entity_id)
                    pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for entity {', '.join(join_keys)}")

    def delete_table(self, config: RepoConfig, table: FeatureView):
        """
        Delete all rows in Redis for a specific feature view.

        Uses a two-phase pipelined approach to avoid an O(K) synchronous hkeys
        call inside the scan loop (N+1 pattern).

        Args:
            config: Feast config
            table: Feature view to delete
        """
        client = self._get_client(config.online_store)
        prefix = _redis_key_prefix(table.join_keys)

        fv_name = _versioned_fv_name(table, config)
        fv_name_bytes = bytes(fv_name, "utf8")
        redis_hash_keys = [_mmh3(f"{fv_name}:{f.name}") for f in table.features]
        redis_hash_keys.append(bytes(f"_ts:{fv_name}", "utf8"))

        # Clean up the Vector Set key unconditionally before the early-return
        vs_key = self._vector_set_key(config.project, fv_name)
        client.delete(vs_key)

        # Phase 1: collect all matching entity keys from SCAN (no per-key round trips)
        scan_pattern = b"".join([prefix, b"*", config.project.encode("utf8")])
        all_keys = list(client.scan_iter(scan_pattern))

        if not all_keys:
            logger.debug(f"Deleted 0 rows for feature view {fv_name}")
            return

        # Phase 2: pipeline hkeys for all collected entity keys (1 round trip)
        with client.pipeline(transaction=False) as pipe:
            for _k in all_keys:
                pipe.hkeys(_k)
            all_hkeys = pipe.execute()

        # Phase 3: pipeline all deletions based on phase 2 results (1 round trip)
        deleted_count = 0
        with client.pipeline(transaction=False) as pipe:
            for _k, field_names in zip(all_keys, all_hkeys):
                _tables = {_hk[4:] for _hk in field_names if _hk.startswith(b"_ts:")}
                if fv_name_bytes not in _tables:
                    continue
                if len(_tables) == 1:
                    pipe.delete(_k)
                else:
                    pipe.hdel(_k, *redis_hash_keys)
                deleted_count += 1
            pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for feature view {fv_name}")

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Delete data from feature views that are no longer in use.

        Args:
            config: Feast config
            tables_to_delete: Feature views to delete
            tables_to_keep: Feature views to keep
            entities_to_delete: Entities to delete
            entities_to_keep: Entities to keep
            partial: Whether to do a partial update
        """
        online_store_config = config.online_store

        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        if online_store_config.full_scan_for_deletion:
            for table in tables_to_delete:
                self.delete_table(config, table)

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        We delete the keys in redis for tables/views being removed.
        """
        client = self._get_client(config.online_store)
        join_keys_to_delete = set(tuple(table.join_keys) for table in tables)

        for join_keys in join_keys_to_delete:
            self.delete_entity_values(config, list(join_keys))

        # Clean up any Vector Set keys for the removed tables
        for table in tables:
            fv_name = _versioned_fv_name(table, config)
            vs_key = self._vector_set_key(config.project, fv_name)
            client.delete(vs_key)

    @staticmethod
    def _parse_connection_string(connection_string: str):
        """
        Reads Redis connections string using format
        for RedisCluster:
            redis1:6379,redis2:6379,skip_full_coverage_check=true,ssl=true,password=...
        for Redis:
            redis_master:6379,db=0,ssl=true,password=...
        """
        startup_nodes = [
            dict(zip(["host", "port"], c.split(":")))
            for c in connection_string.split(",")
            if "=" not in c
        ]
        params = {}
        for c in connection_string.split(","):
            if "=" in c:
                kv = c.split("=", 1)
                try:
                    kv[1] = json.loads(kv[1])
                except json.JSONDecodeError:
                    ...

                it = iter(kv)
                params.update(dict(zip(it, it)))

        return startup_nodes, params

    def _get_client(self, online_store_config: RedisOnlineStoreConfig):
        """
        Creates the Redis client RedisCluster or Redis depending on configuration
        """
        if not self._client:
            startup_nodes, kwargs = self._parse_connection_string(
                online_store_config.connection_string
            )
            if online_store_config.redis_type == RedisType.redis_cluster:
                kwargs["startup_nodes"] = [
                    ClusterNode(**node) for node in startup_nodes
                ]
                self._client = RedisCluster(**kwargs)
            elif online_store_config.redis_type == RedisType.redis_sentinel:
                sentinel_hosts = []

                for item in startup_nodes:
                    sentinel_hosts.append((item["host"], int(item["port"])))

                sentinel = Sentinel(sentinel_hosts, **kwargs)
                master = sentinel.master_for(online_store_config.sentinel_master)
                self._client = master
            else:
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client = Redis(**kwargs)
        return self._client

    async def _get_client_async(self, online_store_config: RedisOnlineStoreConfig):
        if not self._client_async:
            startup_nodes, kwargs = self._parse_connection_string(
                online_store_config.connection_string
            )
            if online_store_config.redis_type == RedisType.redis_cluster:
                kwargs["startup_nodes"] = [
                    redis_asyncio.cluster.ClusterNode(**node) for node in startup_nodes
                ]
                self._client_async = redis_asyncio.RedisCluster(**kwargs)
            elif online_store_config.redis_type == RedisType.redis_sentinel:
                sentinel_hosts = []
                for item in startup_nodes:
                    sentinel_hosts.append((item["host"], int(item["port"])))

                sentinel = redis_asyncio.Sentinel(sentinel_hosts, **kwargs)
                master = sentinel.master_for(online_store_config.sentinel_master)
                self._client_async = master
            else:
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client_async = redis_asyncio.Redis(**kwargs)
        return self._client_async

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        feature_view = _versioned_fv_name(table, config)
        ts_key = f"_ts:{feature_view}"
        vector_data_to_index: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ] = []

        if online_store_config.skip_dedup:
            # Single-pipeline fast path: no timestamp read, directly write all rows.
            # Reduces round trips from 2 to 1. Suitable for initial loads or
            # append-only pipelines where out-of-order writes are not a concern.
            with client.pipeline(transaction=False) as pipe:
                for entity_key, values, timestamp, created_ts in data:
                    redis_key_bin = _redis_key(
                        project,
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    aware_ts = utils.make_tzaware(timestamp)
                    ts = Timestamp()
                    ts.FromDatetime(aware_ts)
                    entity_hset: Dict[Any, Any] = {ts_key: ts.SerializeToString()}
                    for feature_name, val in values.items():
                        f_key = _mmh3(f"{feature_view}:{feature_name}")
                        entity_hset[f_key] = val.SerializeToString()
                    pipe.hset(redis_key_bin, mapping=entity_hset)
                    if online_store_config.key_ttl_seconds:
                        pipe.expire(
                            name=redis_key_bin,
                            time=online_store_config.key_ttl_seconds,
                        )
                    vector_data_to_index.append(
                        (entity_key, values, timestamp, created_ts)
                    )
                results = pipe.execute()
            if progress:
                progress(len(results))
            if (
                getattr(online_store_config, "vector_enabled", False)
                and vector_data_to_index
                and self._check_vadd_supported(client)
            ):
                self._vadd_vectors(client, config, table, vector_data_to_index)
            return

        keys = []
        # redis pipelining optimization: send multiple commands to redis server without waiting for every reply
        with client.pipeline(transaction=False) as pipe:
            # check if a previous record under the key bin exists
            # TODO: investigate if check and set is a better approach rather than pulling all entity ts and then setting
            # it may be significantly slower but avoids potential (rare) race conditions
            for entity_key, _, _, _ in data:
                redis_key_bin = _redis_key(
                    project,
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                keys.append(redis_key_bin)
                pipe.hmget(redis_key_bin, ts_key)
            prev_event_timestamps = pipe.execute()
            # flattening the list of lists. `hmget` does the lookup assuming a list of keys in the key bin
            prev_event_timestamps = [i[0] for i in prev_event_timestamps]

            for redis_key_bin, prev_event_time, (
                entity_key,
                values,
                timestamp,
                created_ts,
            ) in zip(keys, prev_event_timestamps, data):
                # Convert incoming timestamp to millisecond-aware datetime
                aware_ts = utils.make_tzaware(timestamp)
                # Build protobuf timestamp with nanos
                ts = Timestamp()
                ts.FromDatetime(aware_ts)
                # New timestamp in nanoseconds
                new_total_nanos = ts.seconds * 1_000_000_000 + ts.nanos
                # Compare against existing timestamp (nanosecond precision)
                if prev_event_time:
                    prev_ts = Timestamp()
                    prev_ts.ParseFromString(prev_event_time)
                    prev_total_nanos = prev_ts.seconds * 1_000_000_000 + prev_ts.nanos
                    # Skip only if older OR exact same instant
                    if prev_total_nanos and new_total_nanos <= prev_total_nanos:
                        if progress:
                            progress(1)
                        continue
                vector_data_to_index.append((entity_key, values, timestamp, created_ts))
                # Store full timestamp (seconds + nanos)
                entity_hset = {ts_key: ts.SerializeToString()}

                for feature_name, val in values.items():
                    f_key = _mmh3(f"{feature_view}:{feature_name}")
                    entity_hset[f_key] = val.SerializeToString()

                pipe.hset(redis_key_bin, mapping=entity_hset)

                if online_store_config.key_ttl_seconds:
                    pipe.expire(
                        name=redis_key_bin, time=online_store_config.key_ttl_seconds
                    )
            results = pipe.execute()
            if progress:
                progress(len(results))

        # Index vectors into Redis Vector Sets if vector_enabled
        if (
            getattr(online_store_config, "vector_enabled", False)
            and vector_data_to_index
            and self._check_vadd_supported(client)
        ):
            self._vadd_vectors(client, config, table, vector_data_to_index)

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Async version of online_write_batch using the async Redis client."""
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = await self._get_client_async(online_store_config)
        project = config.project

        feature_view = _versioned_fv_name(table, config)
        ts_key = f"_ts:{feature_view}"
        vector_data_to_index: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ] = []

        if online_store_config.skip_dedup:
            async with client.pipeline(transaction=False) as pipe:
                for entity_key, values, timestamp, created_ts in data:
                    redis_key_bin = _redis_key(
                        project,
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    aware_ts = utils.make_tzaware(timestamp)
                    ts = Timestamp()
                    ts.FromDatetime(aware_ts)
                    entity_hset: Dict[Any, Any] = {ts_key: ts.SerializeToString()}
                    for feature_name, val in values.items():
                        f_key = _mmh3(f"{feature_view}:{feature_name}")
                        entity_hset[f_key] = val.SerializeToString()
                    pipe.hset(redis_key_bin, mapping=entity_hset)
                    if online_store_config.key_ttl_seconds:
                        pipe.expire(
                            name=redis_key_bin,
                            time=online_store_config.key_ttl_seconds,
                        )
                    vector_data_to_index.append(
                        (entity_key, values, timestamp, created_ts)
                    )
                results = await pipe.execute()
            if progress:
                progress(len(results))
            if getattr(online_store_config, "vector_enabled", False):
                sync_client = self._get_client(online_store_config)
                if vector_data_to_index and self._check_vadd_supported(sync_client):
                    self._vadd_vectors(sync_client, config, table, vector_data_to_index)
            return

        keys = []
        async with client.pipeline(transaction=False) as pipe:
            for entity_key, _, _, _ in data:
                redis_key_bin = _redis_key(
                    project,
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                keys.append(redis_key_bin)
                pipe.hmget(redis_key_bin, ts_key)
            prev_event_timestamps = await pipe.execute()

        prev_event_timestamps = [i[0] for i in prev_event_timestamps]

        async with client.pipeline(transaction=False) as pipe:
            for redis_key_bin, prev_event_time, (
                entity_key,
                values,
                timestamp,
                created_ts,
            ) in zip(keys, prev_event_timestamps, data):
                aware_ts = utils.make_tzaware(timestamp)
                ts = Timestamp()
                ts.FromDatetime(aware_ts)
                new_total_nanos = ts.seconds * 1_000_000_000 + ts.nanos

                if prev_event_time:
                    prev_ts = Timestamp()
                    prev_ts.ParseFromString(prev_event_time)
                    prev_total_nanos = prev_ts.seconds * 1_000_000_000 + prev_ts.nanos
                    if prev_total_nanos and new_total_nanos <= prev_total_nanos:
                        if progress:
                            progress(1)
                        continue

                vector_data_to_index.append((entity_key, values, timestamp, created_ts))
                entity_hset = {ts_key: ts.SerializeToString()}
                for feature_name, val in values.items():
                    f_key = _mmh3(f"{feature_view}:{feature_name}")
                    entity_hset[f_key] = val.SerializeToString()

                pipe.hset(redis_key_bin, mapping=entity_hset)
                if online_store_config.key_ttl_seconds:
                    pipe.expire(
                        name=redis_key_bin, time=online_store_config.key_ttl_seconds
                    )

            results = await pipe.execute()
            if progress:
                progress(len(results))

        # Index vectors into Redis Vector Sets if vector_enabled (sync fallback)
        if (
            getattr(online_store_config, "vector_enabled", False)
            and vector_data_to_index
        ):
            sync_client = self._get_client(online_store_config)
            if self._check_vadd_supported(sync_client):
                self._vadd_vectors(sync_client, config, table, vector_data_to_index)

    def _generate_redis_keys_for_entities(
        self, config: RepoConfig, entity_keys: List[EntityKeyProto]
    ) -> List[bytes]:
        project = config.project
        version = config.entity_key_serialization_version
        project_bytes = project.encode("utf-8")
        return [
            serialize_entity_key(ek, entity_key_serialization_version=version)
            + project_bytes
            for ek in entity_keys
        ]

    def _generate_hset_keys_for_features(
        self,
        feature_view: FeatureView,
        requested_features: Optional[List[str]] = None,
        fv_name_override: Optional[str] = None,
    ) -> Tuple[List[str], List[str]]:
        if not requested_features:
            requested_features = [f.name for f in feature_view.features]

        fv_name = fv_name_override or feature_view.name
        hset_keys = [_mmh3(f"{fv_name}:{k}") for k in requested_features]

        ts_key = f"_ts:{fv_name}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)

        return requested_features, hset_keys

    def _convert_redis_values_to_protobuf(
        self,
        redis_values: List[List[ByteString]],
        feature_view: str,
        requested_features: List[str],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        return [
            self._get_features_for_entity(values, feature_view, requested_features)
            for values in redis_values
        ]

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = self._get_client(online_store_config)
        feature_view = table
        fv_name = _versioned_fv_name(table, config)

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features, fv_name_override=fv_name
        )
        keys = self._generate_redis_keys_for_entities(config, entity_keys)

        with client.pipeline(transaction=False) as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)

            redis_values = pipe.execute()

        return self._convert_redis_values_to_protobuf(
            redis_values, fv_name, requested_features
        )

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = await self._get_client_async(online_store_config)
        feature_view = table
        fv_name = _versioned_fv_name(table, config)

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features, fv_name_override=fv_name
        )
        keys = self._generate_redis_keys_for_entities(config, entity_keys)

        async with client.pipeline(transaction=False) as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)
            redis_values = await pipe.execute()

        return self._convert_redis_values_to_protobuf(
            redis_values, fv_name, requested_features
        )

    def _read_features_per_fv(
        self,
        config: RepoConfig,
        grouped_refs,
        join_key_values,
        entity_name_to_join_key_map,
        online_features_response,
        full_feature_names: bool,
        include_feature_view_version_metadata: bool,
    ) -> None:
        """Batch all per-FV HMGET commands into a single Redis pipeline."""
        work_items = []
        for table, requested_features in grouped_refs:
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(table_entity_values)
            fv_name = _versioned_fv_name(table, config)
            redis_keys = self._generate_redis_keys_for_entities(
                config, entity_key_protos
            )
            req_features, hset_keys = self._generate_hset_keys_for_features(
                table, requested_features, fv_name_override=fv_name
            )
            work_items.append(
                (table, req_features, fv_name, hset_keys, redis_keys, idxs, output_len)
            )

        if work_items:
            client = self._get_client(config.online_store)
            with client.pipeline(transaction=False) as pipe:
                for _, _, _, hset_keys, redis_keys, _, _ in work_items:
                    for redis_key in redis_keys:
                        pipe.hmget(redis_key, hset_keys)
                all_results = pipe.execute()

            offset = 0
            for (
                table,
                req_features,
                fv_name,
                _,
                redis_keys,
                idxs,
                output_len,
            ) in work_items:
                n = len(redis_keys)
                redis_values = all_results[offset : offset + n]
                offset += n

                read_rows = self._convert_redis_values_to_protobuf(
                    redis_values, fv_name, req_features
                )

                utils._populate_response_from_feature_data(
                    req_features,
                    read_rows,
                    idxs,
                    online_features_response,
                    full_feature_names,
                    table,
                    output_len,
                    include_feature_view_version_metadata,
                )

    async def _read_features_per_fv_async(
        self,
        config: RepoConfig,
        grouped_refs,
        join_key_values,
        entity_name_to_join_key_map,
        online_features_response,
        full_feature_names: bool,
        include_feature_view_version_metadata: bool,
    ) -> None:
        """Async version: batch all per-FV HMGET into a single async pipeline."""
        work_items = []
        for table, requested_features in grouped_refs:
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table, join_key_values, entity_name_to_join_key_map
            )
            entity_key_protos = utils._get_entity_key_protos(table_entity_values)
            fv_name = _versioned_fv_name(table, config)
            redis_keys = self._generate_redis_keys_for_entities(
                config, entity_key_protos
            )
            req_features, hset_keys = self._generate_hset_keys_for_features(
                table, requested_features, fv_name_override=fv_name
            )
            work_items.append(
                (table, req_features, fv_name, hset_keys, redis_keys, idxs, output_len)
            )

        if work_items:
            client = await self._get_client_async(config.online_store)
            async with client.pipeline(transaction=False) as pipe:
                for _, _, _, hset_keys, redis_keys, _, _ in work_items:
                    for redis_key in redis_keys:
                        pipe.hmget(redis_key, hset_keys)
                all_results = await pipe.execute()

            offset = 0
            for (
                table,
                req_features,
                fv_name,
                _,
                redis_keys,
                idxs,
                output_len,
            ) in work_items:
                n = len(redis_keys)
                redis_values = all_results[offset : offset + n]
                offset += n

                read_rows = self._convert_redis_values_to_protobuf(
                    redis_values, fv_name, req_features
                )

                utils._populate_response_from_feature_data(
                    req_features,
                    read_rows,
                    idxs,
                    online_features_response,
                    full_feature_names,
                    table,
                    output_len,
                    include_feature_view_version_metadata,
                )

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: str,
        requested_features: List[str],
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        res_val = dict(zip(requested_features, values))

        res_ts = Timestamp()
        ts_key = f"_ts:{feature_view}"
        ts_val = res_val.pop(ts_key)
        if ts_val:
            res_ts.ParseFromString(ts_val)

        res: Dict[str, ValueProto] = {}
        for feature_name, val_bin in res_val.items():
            val = ValueProto()
            if val_bin:
                val.ParseFromString(val_bin)
            res[feature_name] = val

        if not res:
            return None, None

        total_seconds = res_ts.seconds + res_ts.nanos / 1_000_000_000.0
        timestamp = datetime.fromtimestamp(total_seconds, tz=timezone.utc)

        return timestamp, res

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
        include_feature_view_version_metadata: bool = False,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """Retrieve documents via Redis 8 Vector Sets (VSIM).

        Uses the native ``VSIM`` command to find the *top_k* vectors closest
        to the provided *embedding*.  For each match, the full feature values
        are fetched from the existing Redis HSET storage and returned together
        with a synthetic ``distance`` key containing the similarity score.

        Args:
            config: Feast repo configuration.
            table: The FeatureView to search.
            requested_features: Feature names to include in the result.
            embedding: The query vector (list of floats).
            top_k: Number of nearest neighbours to return.
            distance_metric: Vector metric to use (COSINE or L2).
            query_string: Not supported; raises ValueError if provided.
            include_feature_view_version_metadata: Unused, kept for API compat.

        Returns:
            A list of ``(event_ts, entity_key_proto, feature_dict)`` tuples.
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        if not getattr(online_store_config, "vector_enabled", False):
            raise ValueError(
                "Vector search is not enabled in the online store config. "
                "Set vector_enabled=True in RedisOnlineStoreConfig."
            )
        if embedding is None:
            raise ValueError(
                "An embedding vector must be provided for Redis vector search."
            )
        if query_string is not None:
            raise ValueError(
                "Full-text search (query_string) is not supported by RedisOnlineStore. "
                "Use embedding-based vector search instead."
            )

        self._vector_search_metric(config, table, distance_metric)
        normalized_embedding = self._normalize_vector(embedding)

        client = self._get_client(online_store_config)

        if not self._check_vadd_supported(client):
            raise NotImplementedError(
                "The connected Redis server does not support Vector Sets (VADD/VSIM). "
                "Redis 8.0 or later is required for vector search."
            )

        fv_name = _versioned_fv_name(table, config)
        vs_key = self._vector_set_key(config.project, fv_name)
        dim = len(normalized_embedding)

        # Build VSIM command: VSIM key VALUES <dim> <v1..vN> WITHSCORES COUNT <top_k>
        cmd_args: List[Any] = [vs_key, "VALUES", str(dim)]
        cmd_args.extend(str(f) for f in normalized_embedding)
        cmd_args.extend(["WITHSCORES", "COUNT", str(top_k)])
        raw_results = client.execute_command("VSIM", *cmd_args)

        # raw_results is a flat list: [element_id, score, element_id, score, ...]
        if not raw_results:
            return []

        # Parse the flat response into (element_id, score) pairs
        pairs: List[Tuple[str, float]] = []
        for i in range(0, len(raw_results), 2):
            eid = raw_results[i]
            if isinstance(eid, bytes):
                eid = eid.decode("utf-8")
            score = float(raw_results[i + 1])
            pairs.append((eid, score))

        if not pairs:
            return []

        # Recover entity key protos and build HMGET pipeline to fetch features
        entity_key_protos: List[EntityKeyProto] = []
        redis_keys: List[bytes] = []
        for eid, _ in pairs:
            entity_key_bin = bytes.fromhex(eid)
            ek_proto = deserialize_entity_key(
                entity_key_bin,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            entity_key_protos.append(ek_proto)
            redis_keys.append(
                _redis_key(
                    config.project,
                    ek_proto,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            )

        # Build hash field keys for requested features + timestamp
        hset_keys = [_mmh3(f"{fv_name}:{feat}") for feat in requested_features]
        ts_hkey = f"_ts:{fv_name}"
        hset_keys.append(ts_hkey)

        with client.pipeline(transaction=False) as pipe:
            for rk in redis_keys:
                pipe.hmget(rk, hset_keys)
            all_values = pipe.execute()

        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        for idx, ((_eid, score), ek_proto) in enumerate(zip(pairs, entity_key_protos)):
            raw_vals = all_values[idx]
            feature_dict: Dict[str, ValueProto] = {}

            # Parse timestamp
            event_ts: Optional[datetime] = None
            ts_bin = raw_vals[-1]  # last element is the timestamp
            if ts_bin:
                ts_proto = Timestamp()
                ts_proto.ParseFromString(ts_bin)
                total_seconds = ts_proto.seconds + ts_proto.nanos / 1_000_000_000.0
                event_ts = datetime.fromtimestamp(total_seconds, tz=timezone.utc)

            # Parse feature values
            for feat_idx, feat_name in enumerate(requested_features):
                val = ValueProto()
                val_bin = raw_vals[feat_idx]
                if val_bin:
                    val.ParseFromString(val_bin)
                feature_dict[feat_name] = val

            distance = max(0.0, 1.0 - float(score))

            feature_dict["distance"] = ValueProto(float_val=distance)

            results.append((event_ts, ek_proto, feature_dict))

        return results
