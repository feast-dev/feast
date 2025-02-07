from datetime import datetime
from pathlib import Path
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

import pandas
import pyarrow
from tqdm import tqdm

from feast import Entity, FeatureService, FeatureView, RepoConfig
from feast.data_source import DataSource
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.provider import Provider
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.supported_async_methods import (
    ProviderAsyncMethods,
    SupportedAsyncMethods,
)
from feast.online_response import OnlineResponse
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.saved_dataset import SavedDataset


class FooProvider(Provider):
    @staticmethod
    def with_async_support(online_read=False, online_write=False):
        class _FooProvider(FooProvider):
            @property
            def async_supported(self):
                return ProviderAsyncMethods(
                    online=SupportedAsyncMethods(
                        read=online_read,
                        write=online_write,
                    )
                )

        return _FooProvider(None)

    def __init__(self, config: RepoConfig):
        pass

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        pass

    def teardown_infra(
        self,
        project: str,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        pass

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        pass

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
        pass

    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        return RetrievalJob()

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        return []

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        return []

    def retrieve_saved_dataset(self, config: RepoConfig, dataset: SavedDataset):
        pass

    def write_feature_service_logs(
        self,
        feature_service: FeatureService,
        logs: Union[pyarrow.Table, Path],
        config: RepoConfig,
        registry: BaseRegistry,
    ):
        pass

    def retrieve_feature_service_logs(
        self,
        feature_service: FeatureService,
        start_date: datetime,
        end_date: datetime,
        config: RepoConfig,
        registry: BaseRegistry,
    ) -> RetrievalJob:
        return RetrievalJob()

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: str,
        requested_features: Optional[List[str]],
        query: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        return []

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        query: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        return []

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        pass

    def get_table_column_names_and_types_from_data_source(
        self, config: RepoConfig, data_source: DataSource
    ) -> Iterable[Tuple[str, str]]:
        return []

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
        pass

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
        pass

    async def online_write_batch_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        pass

    async def initialize(self, config: RepoConfig) -> None:
        pass

    async def close(self) -> None:
        pass
