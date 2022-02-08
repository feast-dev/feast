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
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    ByteString,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureView, RepoConfig, utils
from feast.infra.online_stores.helpers import _mmh3, _redis_key, _redis_key_prefix
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.usage import log_exceptions_and_usage, tracing_span

try:
    from redis import Redis
    from redis.cluster import RedisCluster
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("redis", str(e))

logger = logging.getLogger(__name__)


class RedisType(str, Enum):
    redis = "redis"
    redis_cluster = "redis_cluster"


class RedisOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Redis store"""

    type: Literal["redis"] = "redis"
    """Online store type selector"""

    redis_type: RedisType = RedisType.redis
    """Redis type: redis or redis_cluster"""

    connection_string: StrictStr = "localhost:6379"
    """Connection string containing the host, port, and configuration parameters for Redis
     format: host:port,parameter1,parameter2 eg. redis:6379,db=0 """


class RedisOnlineStore(OnlineStore):
    _client: Optional[Union[Redis, RedisCluster]] = None

    def delete_entity_values(self, config: RepoConfig, join_keys: List[str]):
        client = self._get_client(config.online_store)
        deleted_count = 0
        pipeline = client.pipeline(transaction=False)
        prefix = _redis_key_prefix(join_keys)

        for _k in client.scan_iter(
            b"".join([prefix, b"*", config.project.encode("utf8")])
        ):
            pipeline.delete(_k)
            deleted_count += 1
        pipeline.execute()

        logger.debug(f"Deleted {deleted_count} rows for entity {', '.join(join_keys)}")

    @log_exceptions_and_usage(online_store="redis")
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
        Look for join_keys (list of entities) that are not in use anymore
        (usually this happens when the last feature view that was using specific compound key is deleted)
        and remove all features attached to this "join_keys".
        """
        join_keys_to_keep = set(tuple(table.entities) for table in tables_to_keep)

        join_keys_to_delete = set(tuple(table.entities) for table in tables_to_delete)

        for join_keys in join_keys_to_delete - join_keys_to_keep:
            self.delete_entity_values(config, list(join_keys))

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        We delete the keys in redis for tables/views being removed.
        """
        join_keys_to_delete = set(tuple(table.entities) for table in tables)

        for join_keys in join_keys_to_delete:
            self.delete_entity_values(config, list(join_keys))

    @staticmethod
    def _parse_connection_string(connection_string: str):
        """
        Reads Redis connections string using format
        for RedisCluster:
            redis1:6379,redis2:6379,decode_responses=true,skip_full_coverage_check=true,ssl=true,password=...
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
                kwargs["startup_nodes"] = startup_nodes
                self._client = RedisCluster(**kwargs)
            else:
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client = Redis(**kwargs)
        return self._client

    @log_exceptions_and_usage(online_store="redis")
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

        feature_view = table.name
        ts_key = f"_ts:{feature_view}"
        keys = []
        # redis pipelining optimization: send multiple commands to redis server without waiting for every reply
        with client.pipeline(transaction=False) as pipe:
            # check if a previous record under the key bin exists
            # TODO: investigate if check and set is a better approach rather than pulling all entity ts and then setting
            # it may be significantly slower but avoids potential (rare) race conditions
            for entity_key, _, _, _ in data:
                redis_key_bin = _redis_key(project, entity_key)
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

                pipe.hset(redis_key_bin, mapping=entity_hset)
                # TODO: support expiring the entity / features in Redis
                # otherwise entity features remain in redis until cleaned up in separate process
                # client.expire redis_key_bin based a ttl setting
            results = pipe.execute()
            if progress:
                progress(len(results))

    @log_exceptions_and_usage(online_store="redis")
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
        feature_view = table.name
        project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        if not requested_features:
            requested_features = [f.name for f in table.features]

        hset_keys = [_mmh3(f"{feature_view}:{k}") for k in requested_features]

        ts_key = f"_ts:{feature_view}"
        hset_keys.append(ts_key)
        requested_features.append(ts_key)

        keys = []
        for entity_key in entity_keys:
            redis_key_bin = _redis_key(project, entity_key)
            keys.append(redis_key_bin)
        with client.pipeline(transaction=False) as pipe:
            for redis_key_bin in keys:
                pipe.hmget(redis_key_bin, hset_keys)
            with tracing_span(name="remote_call"):
                redis_values = pipe.execute()
        for values in redis_values:
            features = self._get_features_for_entity(
                values, feature_view, requested_features
            )
            result.append(features)
        return result

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
            timestamp = datetime.fromtimestamp(res_ts.seconds)
            return timestamp, res
