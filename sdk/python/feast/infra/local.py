from datetime import datetime
from time import sleep
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import pytz
from tqdm import tqdm

from feast import FeatureTable, KinesisSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.offline_stores.helpers import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
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
from feast.repo_config import RepoConfig


class LocalProvider(Provider):
    def __init__(self, config: RepoConfig):
        assert config is not None
        self.config = config
        self.offline_store = get_offline_store_from_config(config.offline_store)
        self.online_store = get_online_store_from_config(config.online_store)

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        self.online_store.update(
            self.config,
            tables_to_delete,
            tables_to_keep,
            entities_to_delete,
            entities_to_keep,
            partial,
        )

        streaming_tables_to_keep = [
            _table
            for _table in tables_to_keep
            if isinstance(_table, FeatureView) and _table.stream_source is not None
        ]

        streaming_tables_to_delete = [
            _table
            for _table in tables_to_delete
            if isinstance(_table, FeatureView) and _table.stream_source is not None
        ]

        if streaming_tables_to_keep or streaming_tables_to_delete:
            import boto3
            import ray

            from feast.infra.consumers.kinesis import KinesisRayConsumer

            ray.init(namespace="feast", address="auto", ignore_reinit_error=True)
            s = boto3.session.Session()

            consumers = []
            for table_to_keep in streaming_tables_to_keep:
                if isinstance(table_to_keep.stream_source, KinesisSource):
                    print(f"Creating actor for {table_to_keep.name}")
                    try:
                        ray.get_actor(table_to_keep.name)
                        print(f"Actor for {table_to_keep.name} exists. Continuing...")
                        continue
                    except ValueError:
                        # Actor doesn't exist, so create it.
                        pass

                    k = KinesisRayConsumer.options(  # type: ignore
                        name=table_to_keep.name, lifetime="detached"
                    ).remote(s, table_to_keep.stream_source.kinesis_options)
                    consumers.append(k.consume.remote())

            # Ensure that actors get created by sleeping for a couple of seconds.
            sleep(2)

            for table_to_delete in streaming_tables_to_delete:
                if isinstance(table_to_delete.stream_source, KinesisSource):
                    print(f"Killing actor for {table_to_delete.name}")
                    ray.kill(ray.get_actor(table_to_delete.name))

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[Union[FeatureTable, FeatureView]],
        entities: Sequence[Entity],
    ) -> None:
        self.online_store.teardown(self.config, tables, entities)
        streaming_tables_to_delete = [
            _table
            for _table in tables
            if isinstance(_table, FeatureView) and _table.stream_source is not None
        ]

        if streaming_tables_to_delete:
            import ray

            ray.init(namespace="feast", address="auto", ignore_reinit_error=True)

            for table in streaming_tables_to_delete:
                print(f"Terminating Actor for {table.name}")
                try:
                    a = ray.get_actor(table.name)
                    ray.kill(a)
                except ValueError:
                    # The Actor doesn't exist so we continue.
                    pass

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        self.online_store.online_write_batch(config, table, data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result = self.online_store.online_read(config, table, entity_keys)

        return result

    def materialize_single_feature_view(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
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

        table = self.offline_store.pull_latest_from_table_or_query(
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

        with tqdm_builder(len(rows_to_write)) as pbar:
            self.online_write_batch(
                self.config, feature_view, rows_to_write, lambda x: pbar.update(x)
            )

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        return self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
        )


def _table_id(project: str, table: Union[FeatureTable, FeatureView]) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime):
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
