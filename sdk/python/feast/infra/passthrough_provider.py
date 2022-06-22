from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import pyarrow
import pyarrow as pa
from tqdm import tqdm

from feast import FeatureService
from feast.entity import Entity
from feast.feature_logging import FeatureServiceLoggingSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.provider import (
    Provider,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import BaseRegistry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDataset
from feast.usage import RatioSampler, log_exceptions_and_usage, set_usage_attribute
from feast.utils import make_tzaware

DEFAULT_BATCH_SIZE = 10_000


class PassthroughProvider(Provider):
    """
    The Passthrough provider delegates all operations to the underlying online and offline stores.
    """

    def __init__(self, config: RepoConfig):
        super().__init__(config)

        self.repo_config = config
        self._offline_store = None
        self._online_store = None

    @property
    def online_store(self):
        if not self._online_store and self.repo_config.online_store:
            self._online_store = get_online_store_from_config(
                self.repo_config.online_store
            )
        return self._online_store

    @property
    def offline_store(self):
        if not self._offline_store:
            self._offline_store = get_offline_store_from_config(
                self.repo_config.offline_store
            )
        return self._offline_store

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        set_usage_attribute("provider", self.__class__.__name__)

        # Call update only if there is an online store
        if self.online_store:
            self.online_store.update(
                config=self.repo_config,
                tables_to_delete=tables_to_delete,
                tables_to_keep=tables_to_keep,
                entities_to_keep=entities_to_keep,
                entities_to_delete=entities_to_delete,
                partial=partial,
            )

    def teardown_infra(
        self, project: str, tables: Sequence[FeatureView], entities: Sequence[Entity],
    ) -> None:
        set_usage_attribute("provider", self.__class__.__name__)
        if self.online_store:
            self.online_store.teardown(self.repo_config, tables, entities)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        set_usage_attribute("provider", self.__class__.__name__)
        if self.online_store:
            self.online_store.online_write_batch(config, table, data, progress)

    def offline_write_batch(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        data: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        set_usage_attribute("provider", self.__class__.__name__)

        if self.offline_store:
            self.offline_store.__class__.offline_write_batch(
                config, feature_view, data, progress
            )

    @log_exceptions_and_usage(sampler=RatioSampler(ratio=0.001))
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: List[str] = None,
    ) -> List:
        set_usage_attribute("provider", self.__class__.__name__)
        result = []
        if self.online_store:
            result = self.online_store.online_read(
                config, table, entity_keys, requested_features
            )
        return result

    def ingest_df(
        self, feature_view: FeatureView, entities: List[Entity], df: pd.DataFrame,
    ):
        set_usage_attribute("provider", self.__class__.__name__)
        table = pa.Table.from_pandas(df)

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_keys = {entity.join_key: entity.value_type for entity in entities}
        rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        self.online_write_batch(
            self.repo_config, feature_view, rows_to_write, progress=None
        )

    def ingest_df_to_offline_store(self, feature_view: FeatureView, table: pa.Table):
        set_usage_attribute("provider", self.__class__.__name__)

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        self.offline_write_batch(self.repo_config, feature_view, table, None)

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: BaseRegistry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        set_usage_attribute("provider", self.__class__.__name__)

        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        offline_job = self.offline_store.pull_latest_from_table_or_query(
            config=config,
            data_source=feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_date,
            end_date=end_date,
        )

        table = offline_job.to_arrow()

        if feature_view.batch_source.field_mapping is not None:
            table = _run_field_mapping(table, feature_view.batch_source.field_mapping)

        join_key_to_value_type = {
            entity.name: entity.dtype.to_value_type()
            for entity in feature_view.entity_columns
        }

        with tqdm_builder(table.num_rows) as pbar:
            for batch in table.to_batches(DEFAULT_BATCH_SIZE):
                rows_to_write = _convert_arrow_to_proto(
                    batch, feature_view, join_key_to_value_type
                )
                self.online_write_batch(
                    self.repo_config,
                    feature_view,
                    rows_to_write,
                    lambda x: pbar.update(x),
                )

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool,
    ) -> RetrievalJob:
        set_usage_attribute("provider", self.__class__.__name__)

        job = self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )

        return job

    def retrieve_saved_dataset(
        self, config: RepoConfig, dataset: SavedDataset
    ) -> RetrievalJob:
        set_usage_attribute("provider", self.__class__.__name__)

        feature_name_columns = [
            ref.replace(":", "__") if dataset.full_feature_names else ref.split(":")[1]
            for ref in dataset.features
        ]

        # ToDo: replace hardcoded value
        event_ts_column = "event_timestamp"

        return self.offline_store.pull_all_from_table_or_query(
            config=config,
            data_source=dataset.storage.to_data_source(),
            join_key_columns=dataset.join_keys,
            feature_name_columns=feature_name_columns,
            timestamp_field=event_ts_column,
            start_date=make_tzaware(dataset.min_event_timestamp),  # type: ignore
            end_date=make_tzaware(dataset.max_event_timestamp + timedelta(seconds=1)),  # type: ignore
        )

    def write_feature_service_logs(
        self,
        feature_service: FeatureService,
        logs: Union[pyarrow.Table, str],
        config: RepoConfig,
        registry: BaseRegistry,
    ):
        assert (
            feature_service.logging_config is not None
        ), "Logging should be configured for the feature service before calling this function"

        self.offline_store.write_logged_features(
            config=config,
            data=logs,
            source=FeatureServiceLoggingSource(feature_service, config.project),
            logging_config=feature_service.logging_config,
            registry=registry,
        )

    def retrieve_feature_service_logs(
        self,
        feature_service: FeatureService,
        start_date: datetime,
        end_date: datetime,
        config: RepoConfig,
        registry: BaseRegistry,
    ) -> RetrievalJob:
        assert (
            feature_service.logging_config is not None
        ), "Logging should be configured for the feature service before calling this function"

        logging_source = FeatureServiceLoggingSource(feature_service, config.project)
        schema = logging_source.get_schema(registry)
        logging_config = feature_service.logging_config
        ts_column = logging_source.get_log_timestamp_column()
        columns = list(set(schema.names) - {ts_column})

        return self.offline_store.pull_all_from_table_or_query(
            config=config,
            data_source=logging_config.destination.to_data_source(),
            join_key_columns=[],
            feature_name_columns=columns,
            timestamp_field=ts_column,
            start_date=make_tzaware(start_date),
            end_date=make_tzaware(end_date),
        )
