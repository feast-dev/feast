import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import mmh3
import pandas as pd
from redis import Redis
from rediscluster import RedisCluster

from feast import FeatureTable, utils
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.helpers import get_offline_store_from_sources
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import RedisOnlineStoreConfig, RepoConfig


class RedisProvider(Provider):
    _db_path: Path

    def __init__(self, config: RepoConfig):
        assert isinstance(config.online_store, RedisOnlineStoreConfig)

    def _get_client(self):
        if os.environ["REDIS_TYPE"] == "REDIS_CLUSTER":
            return RedisCluster(
                host=os.environ["REDIS_HOST"],
                port=os.environ["REDIS_PORT"],
                decode_responses=True,
            )
        else:
            return Redis(
                host=os.environ["REDIS_HOST"], port=os.environ["REDIS_PORT"], db=0
            )

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        client = self._get_client()
        # TODO

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        # according to the repos_operations.py we can delete the whole project
        client = self._get_client()
        keys = client.keys("{project}:*")
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

        for entity_key, values, timestamp, created_ts in data:
            redis_key_bin = _redis_key(project, entity_key)
            timestamp = utils.make_tzaware(timestamp).strftime("%Y-%m-%d %H:%M:%S")
            entity_hset[f"_ts:{feature_view}"] = timestamp

            if created_ts is not None:
                created_ts = utils.make_tzaware(created_ts).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                entity_hset[f"_created_ts:{feature_view}"] = created_ts

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

        for entity_key in entity_keys:
            redis_key_bin = _redis_key(project, entity_key)
            hset_keys = [_mmh3(f"{feature_view}:{k}") for k in requested_features]
            ts_key = f"_ts:{feature_view}"
            hset_keys.append(ts_key)
            values = client.hmget(redis_key_bin, hset_keys)

            requested_features.append(ts_key)
            res_val = dict(zip(requested_features, values))
            res_ts = res_val.pop(ts_key)

            res = {}
            for feature_name, val_bin in res_val.items():
                val = ValueProto()
                val.ParseFromString(val_bin)
                res[feature_name] = val

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
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


def _redis_key(project: str, entity_key: EntityKeyProto) -> str:
    key = _mmh3(serialize_entity_key(entity_key))
    return f"{project}:{key}"


def _mmh3(key: str) -> str:
    return mmh3.hash_bytes(key).hex()
