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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from google.protobuf.timestamp_pb2 import Timestamp

from feast import FeatureTable, FeatureView, RepoConfig
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RedisOnlineStoreConfig, RedisType

try:
    from redis import Redis
    from rediscluster import RedisCluster
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("redis", str(e))


class RedisOnlineStore(OnlineStore):
    @classmethod
    def _parse_connection_string(cls, connection_string: str):
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
                kv = c.split("=")
                try:
                    kv[1] = json.loads(kv[1])
                except json.JSONDecodeError:
                    ...

                it = iter(kv)
                params.update(dict(zip(it, it)))

        return startup_nodes, params

    @classmethod
    def _get_client(cls, online_store_config: RedisOnlineStoreConfig):
        """
        Creates the Redis client RedisCluster or Redis depending on configuration
        """
        startup_nodes, kwargs = cls._parse_connection_string(
            online_store_config.connection_string
        )
        if online_store_config.type == RedisType.redis_cluster:
            kwargs["startup_nodes"] = startup_nodes
            return RedisCluster(**kwargs)
        else:
            kwargs["host"] = startup_nodes[0]["host"]
            kwargs["port"] = startup_nodes[0]["port"]
            return Redis(**kwargs)

    @classmethod
    def online_write_batch(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        pass

    @classmethod
    def online_read(
        cls,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_store_config = config.online_store
        assert isinstance(online_store_config, RedisOnlineStoreConfig)

        client = cls._get_client(online_store_config)
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
