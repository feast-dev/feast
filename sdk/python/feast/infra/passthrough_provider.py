from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow as pa
from tqdm import tqdm

from feast import OnDemandFeatureView, importer
from feast.base_feature_view import BaseFeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_logging import FeatureServiceLoggingSource
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.common.materialization_job import (
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.compute_engines.base import (
    ComputeEngine,
)
from feast.infra.infra_object import Infra, InfraObject
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
from feast.infra.online_stores.helpers import get_online_store_from_config
from feast.infra.provider import Provider
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.supported_async_methods import ProviderAsyncMethods
from feast.online_response import OnlineResponse
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import BATCH_ENGINE_CLASS_FOR_TYPE, RepoConfig
from feast.saved_dataset import SavedDataset
from feast.stream_feature_view import StreamFeatureView
from feast.utils import (
    _convert_arrow_to_proto,
    _run_pyarrow_field_mapping,
    make_tzaware,
)

DEFAULT_BATCH_SIZE = 10_000


class PassthroughProvider(Provider):
    """
    The passthrough provider delegates all operations to the underlying online and offline stores.
    """

    def __init__(self, config: RepoConfig):
        self.repo_config = config
        self._offline_store = None
        self._online_store = None
        self._batch_engine: Optional[ComputeEngine] = None

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

    @property
    def async_supported(self) -> ProviderAsyncMethods:
        return ProviderAsyncMethods(
            online=self.online_store.async_supported,
        )

    @property
    def batch_engine(self) -> ComputeEngine:
        if self._batch_engine:
            return self._batch_engine
        else:
            engine_config = self.repo_config.batch_engine_config
            config_is_dict = False
            if isinstance(engine_config, str):
                engine_config_type = engine_config
            elif isinstance(engine_config, Dict):
                if "type" not in engine_config:
                    raise ValueError("engine_config needs to have a `type` specified.")
                engine_config_type = engine_config["type"]
                config_is_dict = True
            else:
                raise RuntimeError(
                    f"Invalid config type specified for batch_engine: {type(engine_config)}"
                )

            if engine_config_type in BATCH_ENGINE_CLASS_FOR_TYPE:
                engine_config_type = BATCH_ENGINE_CLASS_FOR_TYPE[engine_config_type]
            engine_module, engine_class_name = engine_config_type.rsplit(".", 1)
            engine_class = importer.import_class(engine_module, engine_class_name)

            if config_is_dict:
                _batch_engine = engine_class(
                    repo_config=self.repo_config,
                    offline_store=self.offline_store,
                    online_store=self.online_store,
                    **engine_config,
                )
            else:
                _batch_engine = engine_class(
                    repo_config=self.repo_config,
                    offline_store=self.offline_store,
                    online_store=self.online_store,
                )
            self._batch_engine = _batch_engine
            return _batch_engine

    def plan_infra(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> Infra:
        infra = Infra()
        if self.online_store:
            infra_objects: List[InfraObject] = self.online_store.plan(
                config, desired_registry_proto
            )
            infra.infra_objects += infra_objects
        return infra

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[Union[FeatureView, OnDemandFeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        # Call update only if there is an online store
        if self.online_store:
            tables_to_keep_online = [
                fv
                for fv in tables_to_keep
                if not hasattr(fv, "online") or (hasattr(fv, "online") and fv.online)
            ]

            self.online_store.update(
                config=self.repo_config,
                tables_to_delete=tables_to_delete,
                tables_to_keep=tables_to_keep_online,
                entities_to_keep=entities_to_keep,
                entities_to_delete=entities_to_delete,
                partial=partial,
            )
        if self.batch_engine:
            self.batch_engine.update(
                project,
                tables_to_delete,
                tables_to_keep,
                entities_to_delete,
                entities_to_keep,
            )

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ) -> None:
        if self.online_store:
            self.online_store.teardown(self.repo_config, tables, entities)
        if self.batch_engine:
            self.batch_engine.teardown_infra(project, tables, entities)

    def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureView, BaseFeatureView, OnDemandFeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if self.online_store:
            self.online_store.online_write_batch(config, table, data, progress)

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: Union[FeatureView, BaseFeatureView, OnDemandFeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if self.online_store:
            await self.online_store.online_write_batch_async(
                config, table, data, progress
            )

    def offline_write_batch(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        data: pa.Table,
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        if self.offline_store:
            self.offline_store.__class__.offline_write_batch(
                config, feature_view, data, progress
            )

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List:
        result = []
        if self.online_store:
            result = self.online_store.online_read(
                config, table, entity_keys, requested_features
            )
        return result

    def get_online_features(
        self,
        config: RepoConfig,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[ValueProto], RepeatedValue]],
        ],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> OnlineResponse:
        return self.online_store.get_online_features(
            config=config,
            features=features,
            entity_rows=entity_rows,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )

    async def get_online_features_async(
        self,
        config: RepoConfig,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[ValueProto], RepeatedValue]],
        ],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> OnlineResponse:
        return await self.online_store.get_online_features_async(
            config=config,
            features=features,
            entity_rows=entity_rows,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
        )

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List:
        result = []
        if self.online_store:
            result = await self.online_store.online_read_async(
                config, table, entity_keys, requested_features
            )
        return result

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: Optional[List[str]],
        query: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List:
        result = []
        if self.online_store:
            result = self.online_store.retrieve_online_documents(
                config,
                table,
                requested_features,
                query,
                top_k,
                distance_metric,
            )
        return result

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: Optional[List[str]],
        query: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
    ) -> List:
        result = []
        if self.online_store:
            result = self.online_store.retrieve_online_documents_v2(
                config,
                table,
                requested_features,
                query,
                top_k,
                distance_metric,
                query_string,
            )
        return result

    @staticmethod
    def _prep_rows_to_write_for_ingestion(
        feature_view: Union[BaseFeatureView, FeatureView, OnDemandFeatureView],
        df: pd.DataFrame,
        field_mapping: Optional[Dict] = None,
    ):
        table = pa.Table.from_pandas(df)
        if isinstance(feature_view, OnDemandFeatureView):
            if not field_mapping:
                field_mapping = {}
            table = _run_pyarrow_field_mapping(table, field_mapping)
            join_keys = {
                entity.name: entity.dtype.to_value_type()
                for entity in feature_view.entity_columns
            }
            rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)
        else:
            if hasattr(feature_view, "entity_columns"):
                join_keys = {
                    entity.name: entity.dtype.to_value_type()
                    for entity in feature_view.entity_columns
                }
            else:
                join_keys = {}

            # Note: A dictionary mapping of column names in this data
            #   source to feature names in a feature table or view. Only used for feature
            #   columns, not entity or timestamp columns.
            if hasattr(feature_view, "batch_source"):
                if feature_view.batch_source.field_mapping is not None:
                    table = _run_pyarrow_field_mapping(
                        table, feature_view.batch_source.field_mapping
                    )
            else:
                table = _run_pyarrow_field_mapping(table, {})

            if not isinstance(feature_view, BaseFeatureView):
                for entity in feature_view.entity_columns:
                    join_keys[entity.name] = entity.dtype.to_value_type()
            rows_to_write = _convert_arrow_to_proto(table, feature_view, join_keys)

        return rows_to_write

    def ingest_df(
        self,
        feature_view: Union[BaseFeatureView, FeatureView, OnDemandFeatureView],
        df: pd.DataFrame,
        field_mapping: Optional[Dict] = None,
    ):
        rows_to_write = self._prep_rows_to_write_for_ingestion(
            feature_view=feature_view,
            df=df,
            field_mapping=field_mapping,
        )
        self.online_write_batch(
            self.repo_config, feature_view, rows_to_write, progress=None
        )

    async def ingest_df_async(
        self,
        feature_view: Union[BaseFeatureView, FeatureView, OnDemandFeatureView],
        df: pd.DataFrame,
        field_mapping: Optional[Dict] = None,
    ):
        rows_to_write = self._prep_rows_to_write_for_ingestion(
            feature_view=feature_view,
            df=df,
            field_mapping=field_mapping,
        )
        await self.online_write_batch_async(
            self.repo_config, feature_view, rows_to_write, progress=None
        )

    def ingest_df_to_offline_store(self, feature_view: FeatureView, table: pa.Table):
        if feature_view.batch_source.field_mapping is not None:
            table = _run_pyarrow_field_mapping(
                table, feature_view.batch_source.field_mapping
            )

        self.offline_write_batch(self.repo_config, feature_view, table, None)

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: Union[FeatureView, OnDemandFeatureView],
        start_date: datetime,
        end_date: datetime,
        registry: BaseRegistry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
        disable_event_timestamp: bool = False,
    ) -> None:
        if isinstance(feature_view, OnDemandFeatureView):
            if not feature_view.write_to_online_store:
                raise ValueError(
                    f"OnDemandFeatureView {feature_view.name} does not have write_to_online_store enabled"
                )
            return
        assert (
            isinstance(feature_view, BatchFeatureView)
            or isinstance(feature_view, StreamFeatureView)
            or isinstance(feature_view, FeatureView)
            or isinstance(feature_view, OnDemandFeatureView)
        ), f"Unexpected type for {feature_view.name}: {type(feature_view)}"
        task = MaterializationTask(
            project=project,
            feature_view=feature_view,
            start_time=start_date,
            end_time=end_date,
            tqdm_builder=tqdm_builder,
            disable_event_timestamp=disable_event_timestamp,
        )
        jobs = self.batch_engine.materialize(registry, task)
        assert len(jobs) == 1
        if jobs[0].status() == MaterializationJobStatus.ERROR and jobs[0].error():
            e = jobs[0].error()
            assert e
            raise e

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[Union[FeatureView, OnDemandFeatureView]],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool,
        **kwargs,
    ) -> RetrievalJob:
        job = self.offline_store.get_historical_features(
            config=config,
            feature_views=feature_views,
            feature_refs=feature_refs,
            entity_df=entity_df,
            registry=registry,
            project=project,
            full_feature_names=full_feature_names,
            **kwargs,
        )

        return job

    def retrieve_saved_dataset(
        self, config: RepoConfig, dataset: SavedDataset
    ) -> RetrievalJob:
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
        logs: Union[pa.Table, str],
        config: RepoConfig,
        registry: BaseRegistry,
    ):
        assert feature_service.logging_config is not None, (
            "Logging should be configured for the feature service before calling this function"
        )

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
        assert feature_service.logging_config is not None, (
            "Logging should be configured for the feature service before calling this function"
        )

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

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        self.offline_store.validate_data_source(config=config, data_source=data_source)

    def get_table_column_names_and_types_from_data_source(
        self, config: RepoConfig, data_source: DataSource
    ) -> Iterable[Tuple[str, str]]:
        return self.offline_store.get_table_column_names_and_types_from_data_source(
            config=config, data_source=data_source
        )

    async def initialize(self, config: RepoConfig) -> None:
        await self.online_store.initialize(config)

    async def close(self) -> None:
        await self.online_store.close()
