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
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureTable, FeatureView, RepoConfig, utils
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel

try:
    from redis import Redis
    from rediscluster import RedisCluster
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("redis", str(e))

EX_SECONDS = 253402300799


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

    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        There's currently no setup done for Redis.
        """
        pass

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ):
        """
        There's currently no teardown done for Redis.
        """
        pass

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
            if online_store_config.type == RedisType.redis_cluster:
                kwargs["startup_nodes"] = startup_nodes
                self._client = RedisCluster(**kwargs)
            else:
                kwargs["host"] = startup_nodes[0]["host"]
                kwargs["port"] = startup_nodes[0]["port"]
                self._client = Redis(**kwargs)
        return self._client

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = self._get_client(online_store_config)
        project = config.project

        entity_hset = {}
        feature_view = table.name

        ex = Timestamp()
        ex.seconds = EX_SECONDS
        ex_str = ex.SerializeToString()

        for entity_key, values, timestamp, created_ts in data:
            redis_key_bin = _redis_key(project, entity_key)
            ts = Timestamp()
            ts.seconds = int(utils.make_tzaware(timestamp).timestamp())
            entity_hset[f"_ts:{feature_view}"] = ts.SerializeToString()
            entity_hset[f"_ex:{feature_view}"] = ex_str

            for feature_name, val in values.items():
                f_key = _mmh3(f"{feature_view}:{feature_name}")
                entity_hset[f_key] = val.SerializeToString()

            client.hset(redis_key_bin, mapping=entity_hset)
            if progress:
                progress(1)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
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

        for entity_key in entity_keys:
            redis_key_bin = _redis_key(project, entity_key)
            hset_keys = [_mmh3(f"{feature_view}:{k}") for k in requested_features]
            ts_key = f"_ts:{feature_view}"
            hset_keys.append(ts_key)
            values = client.hmget(redis_key_bin, hset_keys)
            requested_features.append(ts_key)
            res_val = dict(zip(requested_features, values))

            res_ts = Timestamp()
            ts_val = res_val.pop(ts_key)
            if ts_val:
                res_ts.ParseFromString(ts_val)

            res = {}
            for feature_name, val_bin in res_val.items():
                val = ValueProto()
                if val_bin:
                    val.ParseFromString(val_bin)
                res[feature_name] = val

            if not res:
                result.append((None, None))
            else:
                timestamp = datetime.fromtimestamp(res_ts.seconds)
                result.append((timestamp, res))
        return result
