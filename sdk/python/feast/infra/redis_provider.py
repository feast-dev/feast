import json
import os
import struct
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import mmh3
import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from redis import Redis
from rediscluster import RedisCluster

from feast import FeatureTable, utils
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.helpers import get_offline_store_from_sources
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.storage.Redis_pb2 import RedisKeyV2 as RedisKeyProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RedisOnlineStoreConfig, RepoConfig

EX_SECONDS = 253402300799


class RedisProvider(Provider):
    _db_path: Path

    def __init__(self, config: RepoConfig):
        assert isinstance(config.online_store, RedisOnlineStoreConfig)

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        pass

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        # according to the repos_operations.py we can delete the whole project
        client = self._get_client()

        tables_join_keys = [[e for e in t.entities] for t in tables]
        for table_join_keys in tables_join_keys:
            redis_key_bin = _redis_key(
                project, EntityKeyProto(join_keys=table_join_keys)
            )
            keys = client.keys(f"{redis_key_bin}*")
            if keys:
                client.unlink(*keys)

    def online_write_batch(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        client = self._get_client()

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

    def online_read(
        self,
        project: str,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        client = self._get_client()
        feature_view = table.name

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

    def materialize_single_feature_view(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
    ) -> None:
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            event_timestamp_column,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        start_date = utils.make_tzaware(start_date)
        end_date = utils.make_tzaware(end_date)

        offline_store = get_offline_store_from_sources([feature_view.input])
        table = offline_store.pull_latest_from_table_or_query(
            data_source=feature_view.input,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        if feature_view.input.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.input.field_mapping)

        join_keys = [entity.join_key for entity in entities]
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        self.online_write_batch(project, feature_view, rows_to_write, None)

        feature_view.materialization_intervals.append((start_date, end_date))
        registry.apply_feature_view(feature_view, project)

    def _get_cs(self):
        """
        Reads Redis connections string using format
        for RedisCluster:
            redis1:6379,redis2:6379,decode_responses=true,skip_full_coverage_check=true,ssl=true,password=...
        for Redis:
            redis_master:6379,db=0,ssl=true,password=...
        """
        connection_string = os.environ["REDIS_CONNECTION_STRING"]
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

    def _get_client(self):
        """
        Creates the Redis client RedisCluster or Redis depending on configuration
        """
        startup_nodes, kwargs = self._get_cs()

        if os.environ["REDIS_TYPE"] == "REDIS_CLUSTER":
            kwargs["startup_nodes"] = startup_nodes
            return RedisCluster(**kwargs)
        else:
            kwargs["host"] = startup_nodes[0]["host"]
            kwargs["port"] = startup_nodes[0]["port"]
            return Redis(**kwargs)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        offline_store = get_offline_store_from_sources(
            [feature_view.input for feature_view in feature_views]
        )
        return offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
        )


def _redis_key(project: str, entity_key: EntityKeyProto):
    redis_key = RedisKeyProto(
        project=project,
        entity_names=entity_key.join_keys,
        entity_values=entity_key.entity_values,
    )
    # key = _mmh3(serialize_entity_key(entity_key))
    return redis_key.SerializeToString()


def _mmh3(key: str):
    """
    Calculate murmur3_32 hash which is equal to scala version which is using little endian:
        https://stackoverflow.com/questions/29932956/murmur3-hash-different-result-between-python-and-java-implementation
        https://stackoverflow.com/questions/13141787/convert-decimal-int-to-little-endian-string-x-x
    """
    key_hash = mmh3.hash(key, signed=False)
    return bytes.fromhex(struct.pack("<Q", key_hash).hex().rstrip("0"))
