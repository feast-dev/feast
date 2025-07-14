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
import json
import logging
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
from feast.infra.online_stores.helpers import _mmh3, _redis_key, _redis_key_prefix
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.utils import _build_retrieve_online_document_record, _get_feature_view_vector_field_metadata

try:
    from redis import Redis
    from redis import asyncio as redis_asyncio
    from redis.cluster import ClusterNode, RedisCluster
    from redis.sentinel import Sentinel
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("redis", str(e))

logger = logging.getLogger(__name__)

# Redis vector search distance metrics mapping
REDIS_DISTANCE_METRICS = {
    "L2": "L2",
    "IP": "IP",
    "COSINE": "COSINE",
    "cosine": "COSINE",
    "l2": "L2",
    "ip": "IP",
    "inner_product": "IP",
    "euclidean": "L2"
}


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

    # Vector search specific configurations
    vector_enabled: Optional[bool] = False
    """Whether to enable vector search capabilities"""

    vector_index_type: Optional[str] = "FLAT"
    """Vector index type: FLAT or HNSW"""

    vector_distance_metric: Optional[str] = "COSINE"
    """Distance metric for vector similarity: L2, IP, or COSINE"""

    vector_dim: Optional[int] = None
    """Vector dimension - must be specified if vector_enabled is True"""

    # HNSW specific parameters
    hnsw_m: Optional[int] = 16
    """Max number of outgoing edges for HNSW index"""

    hnsw_ef_construction: Optional[int] = 200
    """Max number of connected neighbors during HNSW graph building"""

    hnsw_ef_runtime: Optional[int] = 10
    """Max top candidates during HNSW KNN search"""


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

    def _create_vector_index(self, config: RepoConfig, table: FeatureView):
        """Create a vector index for the feature view if vector search is enabled."""
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        if not online_store_config.vector_enabled:
            return

        vector_field_metadata = _get_feature_view_vector_field_metadata(table)
        if not vector_field_metadata:
            return

        client = self._get_client(online_store_config)
        index_name = f"{config.project}:{table.name}:vector_index"

        # Check if index already exists
        try:
            client.execute_command("FT.INFO", index_name)
            logger.debug(f"Vector index {index_name} already exists")
            return
        except Exception:
            # Index doesn't exist, create it
            pass

        # Determine vector field configuration
        vector_dim = online_store_config.vector_dim or vector_field_metadata.vector_length
        if not vector_dim:
            raise ValueError(f"Vector dimension must be specified either in config.vector_dim or field.vector_length for table {table.name}")

        distance_metric = REDIS_DISTANCE_METRICS.get(
            online_store_config.vector_distance_metric or vector_field_metadata.vector_search_metric or "COSINE",
            "COSINE"
        )

        # Build vector field schema
        vector_field_name = f"vector_{vector_field_metadata.name}"

        if online_store_config.vector_index_type.upper() == "HNSW":
            vector_schema = [
                "TYPE", "FLOAT32",
                "DIM", str(vector_dim),
                "DISTANCE_METRIC", distance_metric,
                "M", str(online_store_config.hnsw_m),
                "EF_CONSTRUCTION", str(online_store_config.hnsw_ef_construction),
                "EF_RUNTIME", str(online_store_config.hnsw_ef_runtime)
            ]
        else:  # FLAT
            vector_schema = [
                "TYPE", "FLOAT32",
                "DIM", str(vector_dim),
                "DISTANCE_METRIC", distance_metric
            ]

        # Create the index
        try:
            cmd = [
                "FT.CREATE", index_name,
                "ON", "HASH",
                "PREFIX", "1", f"{config.project}:",
                "SCHEMA",
                vector_field_name, "VECTOR", online_store_config.vector_index_type.upper(),
                str(len(vector_schema))
            ] + vector_schema

            client.execute_command(*cmd)
            logger.info(f"Created vector index {index_name} for table {table.name}")
        except Exception as e:
            logger.error(f"Failed to create vector index {index_name}: {e}")
            raise

    def _extract_vector_from_value(self, val: ValueProto) -> Optional[bytes]:
        """Extract vector data from ValueProto and convert to bytes for Redis."""
        import numpy as np

        try:
            if val.HasField("float_list_val"):
                # Handle float list
                vector = np.array(val.float_list_val.val, dtype=np.float32)
                return vector.tobytes()
            elif val.HasField("double_list_val"):
                # Handle double list - convert to float32
                vector = np.array(val.double_list_val.val, dtype=np.float32)
                return vector.tobytes()
            elif val.HasField("int64_list_val"):
                # Handle int list - convert to float32
                vector = np.array(val.int64_list_val.val, dtype=np.float32)
                return vector.tobytes()
            else:
                logger.warning(f"Unsupported vector data type in ValueProto: {val}")
                return None
        except Exception as e:
            logger.error(f"Failed to extract vector from ValueProto: {e}")
            return None

    def delete_entity_values(self, config: RepoConfig, join_keys: List[str]):
        client = self._get_client(config.online_store)
        deleted_count = 0
        prefix = _redis_key_prefix(join_keys)

        with client.pipeline(transaction=False) as pipe:
            for _k in client.scan_iter(
                b"".join([prefix, b"*", config.project.encode("utf8")])
            ):
                pipe.delete(_k)
                deleted_count += 1
            pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for entity {', '.join(join_keys)}")

    def delete_table(self, config: RepoConfig, table: FeatureView):
        """
        Delete all rows in Redis for a specific feature view

        Args:
            config: Feast config
            table: Feature view to delete
        """
        client = self._get_client(config.online_store)
        deleted_count = 0
        prefix = _redis_key_prefix(table.join_keys)

        redis_hash_keys = [_mmh3(f"{table.name}:{f.name}") for f in table.features]
        redis_hash_keys.append(bytes(f"_ts:{table.name}", "utf8"))

        with client.pipeline(transaction=False) as pipe:
            for _k in client.scan_iter(
                b"".join([prefix, b"*", config.project.encode("utf8")])
            ):
                _tables = {
                    _hk[4:] for _hk in client.hgetall(_k) if _hk.startswith(b"_ts:")
                }
                if bytes(table.name, "utf8") not in _tables:
                    continue
                if len(_tables) == 1:
                    pipe.delete(_k)
                else:
                    pipe.hdel(_k, *redis_hash_keys)
                deleted_count += 1
            pipe.execute()

        logger.debug(f"Deleted {deleted_count} rows for feature view {table.name}")

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
        join_keys_to_delete = set(tuple(table.join_keys) for table in tables)

        for join_keys in join_keys_to_delete:
            self.delete_entity_values(config, list(join_keys))

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

        # Create vector index if vector search is enabled
        if online_store_config.vector_enabled:
            self._create_vector_index(config, table)

        client = self._get_client(online_store_config)
        project = config.project

        feature_view = table.name
        ts_key = f"_ts:{feature_view}"
        keys = []

        # Get vector field metadata if vector search is enabled
        vector_field_metadata = None
        if online_store_config.vector_enabled:
            vector_field_metadata = _get_feature_view_vector_field_metadata(table)
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

            for redis_key_bin, prev_event_time, (_, values, timestamp, _) in zip(
                keys, prev_event_timestamps, data
            ):
                event_time_seconds = int(utils.make_tzaware(timestamp).timestamp())

                # ignore if event_timestamp is before the event features that are currently in the feature store
                if prev_event_time:
                    prev_ts = Timestamp()
                    prev_ts.ParseFromString(prev_event_time)
                    if prev_ts.seconds and event_time_seconds <= prev_ts.seconds:
                        # TODO: somehow signal that it's not overwriting the current record?
                        if progress:
                            progress(1)
                        continue

                ts = Timestamp()
                ts.seconds = event_time_seconds
                entity_hset = dict()
                entity_hset[ts_key] = ts.SerializeToString()

                for feature_name, val in values.items():
                    f_key = _mmh3(f"{feature_view}:{feature_name}")
                    entity_hset[f_key] = val.SerializeToString()

                    # Handle vector data for vector search
                    if (vector_field_metadata and
                        feature_name == vector_field_metadata.name and
                        online_store_config.vector_enabled):
                        # Convert vector data to bytes for Redis vector search
                        vector_data = self._extract_vector_from_value(val)
                        if vector_data is not None:
                            vector_field_name = f"vector_{feature_name}"
                            entity_hset[vector_field_name] = vector_data

                pipe.hset(redis_key_bin, mapping=entity_hset)

                if online_store_config.key_ttl_seconds:
                    pipe.expire(
                        name=redis_key_bin, time=online_store_config.key_ttl_seconds
                    )
            results = pipe.execute()
            if progress:
                progress(len(results))

    def _generate_redis_keys_for_entities(
        self, config: RepoConfig, entity_keys: List[EntityKeyProto]
    ) -> List[bytes]:
        keys = []
        for entity_key in entity_keys:
            redis_key_bin = _redis_key(
                config.project,
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            keys.append(redis_key_bin)
        return keys

    def _generate_hset_keys_for_features(
        self,
        feature_view: FeatureView,
        requested_features: Optional[List[str]] = None,
    ) -> Tuple[List[str], List[str]]:
        if not requested_features:
            requested_features = [f.name for f in feature_view.features]

        hset_keys = [_mmh3(f"{feature_view.name}:{k}") for k in requested_features]

        ts_key = f"_ts:{feature_view.name}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)

        return requested_features, hset_keys

    def _convert_redis_values_to_protobuf(
        self,
        redis_values: List[List[ByteString]],
        feature_view: str,
        requested_features: List[str],
    ):
        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for values in redis_values:
            features = self._get_features_for_entity(
                values, feature_view, requested_features
            )
            result.append(features)
        return result

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

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features
        )
        keys = self._generate_redis_keys_for_entities(config, entity_keys)

        with client.pipeline(transaction=False) as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)

            redis_values = pipe.execute()

        return self._convert_redis_values_to_protobuf(
            redis_values, feature_view.name, requested_features
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

        requested_features, hset_keys = self._generate_hset_keys_for_features(
            feature_view, requested_features
        )
        keys = self._generate_redis_keys_for_entities(config, entity_keys)

        async with client.pipeline(transaction=False) as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)
            redis_values = await pipe.execute()

        return self._convert_redis_values_to_protobuf(
            redis_values, feature_view.name, requested_features
        )

    def _get_features_for_entity(
        self,
        values: List[ByteString],
        feature_view: str,
        requested_features: List[str],
    ) -> Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]:
        res_val = dict(zip(requested_features, values))

        res_ts = Timestamp()
        ts_val = res_val.pop(f"_ts:{feature_view}")
        if ts_val:
            res_ts.ParseFromString(bytes(ts_val))

        res = {}
        for feature_name, val_bin in res_val.items():
            val = ValueProto()
            if val_bin:
                val.ParseFromString(bytes(val_bin))
            res[feature_name] = val

        if not res:
            return None, None
        else:
            timestamp = datetime.fromtimestamp(res_ts.seconds, tz=timezone.utc)
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
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieve documents using vector similarity search from Redis.

        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_features: List of requested features to retrieve
            embedding: Query embedding to search for (optional)
            top_k: Number of items to return
            distance_metric: Distance metric to use (optional)
            query_string: The query string to search for using keyword search (optional)

        Returns:
            List of tuples containing the event timestamp, entity key, and feature values
        """
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        if not online_store_config.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        if embedding is None and query_string is None:
            raise ValueError("Either embedding or query_string must be provided")

        if query_string is not None:
            raise NotImplementedError("Text search is not yet implemented for Redis vector store")

        vector_field_metadata = _get_feature_view_vector_field_metadata(table)
        if not vector_field_metadata:
            raise ValueError(f"No vector field found in feature view {table.name}")

        client = self._get_client(online_store_config)
        index_name = f"{config.project}:{table.name}:vector_index"

        # Prepare the vector query
        import numpy as np
        query_vector = np.array(embedding, dtype=np.float32).tobytes()

        # Determine distance metric
        search_distance_metric = (
            distance_metric or
            online_store_config.vector_distance_metric or
            vector_field_metadata.vector_search_metric or
            "COSINE"
        )
        redis_distance_metric = REDIS_DISTANCE_METRICS.get(search_distance_metric.upper(), "COSINE")

        # Build the search query
        vector_field_name = f"vector_{vector_field_metadata.name}"

        try:
            # Perform vector search using FT.SEARCH
            search_cmd = [
                "FT.SEARCH", index_name,
                f"*=>[KNN {top_k} @{vector_field_name} $BLOB AS vector_score]",
                "PARAMS", "2", "BLOB", query_vector,
                "SORTBY", "vector_score",
                "LIMIT", "0", str(top_k),
                "DIALECT", "2"
            ]

            search_results = client.execute_command(*search_cmd)

            # Parse search results
            results = []
            if len(search_results) > 1:
                num_results = search_results[0]
                for i in range(1, len(search_results), 2):
                    if i + 1 < len(search_results):
                        redis_key = search_results[i]
                        field_values = search_results[i + 1]

                        # Extract entity key from redis key
                        entity_key = self._extract_entity_key_from_redis_key(redis_key, config)

                        # Convert field values to feature dict
                        feature_dict = self._convert_search_result_to_features(
                            field_values, table, requested_features
                        )

                        # Extract timestamp
                        timestamp = self._extract_timestamp_from_features(feature_dict, table.name)

                        results.append((timestamp, entity_key, feature_dict))

            return results

        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            raise

    def _extract_entity_key_from_redis_key(self, redis_key: bytes, config: RepoConfig) -> Optional[EntityKeyProto]:
        """Extract entity key from Redis key."""
        try:
            # Redis key format: project:entity_key_hash
            key_str = redis_key.decode('utf-8')
            project_prefix = f"{config.project}:"
            if key_str.startswith(project_prefix):
                # For now, return None as entity key extraction requires more complex logic
                # This would need to be implemented based on the specific entity key serialization
                return None
            return None
        except Exception as e:
            logger.warning(f"Failed to extract entity key from Redis key {redis_key}: {e}")
            return None

    def _convert_search_result_to_features(
        self,
        field_values: List,
        table: FeatureView,
        requested_features: List[str]
    ) -> Optional[Dict[str, ValueProto]]:
        """Convert Redis search result field values to feature dictionary."""
        try:
            # field_values is a list of [field_name, field_value, field_name, field_value, ...]
            field_dict = {}
            for i in range(0, len(field_values), 2):
                if i + 1 < len(field_values):
                    field_name = field_values[i].decode('utf-8') if isinstance(field_values[i], bytes) else field_values[i]
                    field_value = field_values[i + 1]
                    field_dict[field_name] = field_value

            # Convert to ValueProto format
            feature_dict = {}
            for feature_name in requested_features:
                # Look for the feature in the field dict using the hashed key
                f_key = _mmh3(f"{table.name}:{feature_name}")
                f_key_str = f_key.decode('utf-8') if isinstance(f_key, bytes) else str(f_key)

                if f_key_str in field_dict:
                    val = ValueProto()
                    val.ParseFromString(field_dict[f_key_str])
                    feature_dict[feature_name] = val

            return feature_dict if feature_dict else None

        except Exception as e:
            logger.error(f"Failed to convert search result to features: {e}")
            return None

    def _extract_timestamp_from_features(self, feature_dict: Optional[Dict[str, ValueProto]], table_name: str) -> Optional[datetime]:
        """Extract timestamp from feature dictionary."""
        try:
            if not feature_dict:
                return None

            ts_key = f"_ts:{table_name}"
            # The timestamp is stored separately in Redis, not in the search results
            # For now, return current time as a placeholder
            return datetime.now(tz=timezone.utc)

        except Exception as e:
            logger.warning(f"Failed to extract timestamp: {e}")
            return None
