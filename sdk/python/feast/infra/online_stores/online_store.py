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
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from feast import Entity, utils
from feast.batch_feature_view import BatchFeatureView
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import InfraObject
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.supported_async_methods import SupportedAsyncMethods
from feast.online_response import OnlineResponse
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView


class OnlineStore(ABC):
    """
    The interface that Feast uses to interact with the storage system that handles online features.
    """

    @property
    def async_supported(self) -> SupportedAsyncMethods:
        return SupportedAsyncMethods()

    @abstractmethod
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """
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
        """
        Writes a batch of feature rows to the online store asynchronously.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online write batch async"
        )

    @abstractmethod
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        pass

    async def online_read_async(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys asynchronously.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online read async"
        )

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
        if isinstance(entity_rows, list):
            columnar: Dict[str, List[Any]] = {k: [] for k in entity_rows[0].keys()}
            for entity_row in entity_rows:
                for key, value in entity_row.items():
                    try:
                        columnar[key].append(value)
                    except KeyError as e:
                        raise ValueError(
                            "All entity_rows must have the same keys."
                        ) from e

            entity_rows = columnar

        (
            join_key_values,
            grouped_refs,
            entity_name_to_join_key_map,
            requested_on_demand_feature_views,
            feature_refs,
            requested_result_row_names,
            online_features_response,
        ) = utils._prepare_entities_to_read_from_online_store(
            registry=registry,
            project=project,
            features=features,
            entity_values=entity_rows,
            full_feature_names=full_feature_names,
            native_entity_values=True,
        )

        for table, requested_features in grouped_refs:
            # Get the correct set of entity values with the correct join keys.
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table,
                join_key_values,
                entity_name_to_join_key_map,
            )

            entity_key_protos = utils._get_entity_key_protos(table_entity_values)

            # Fetch data for Entities.
            read_rows = self.online_read(
                config=config,
                table=table,
                entity_keys=entity_key_protos,
                requested_features=requested_features,
            )

            feature_data = utils._convert_rows_to_protobuf(
                requested_features, read_rows
            )

            # Populate the result_rows with the Features from the OnlineStore inplace.
            utils._populate_response_from_feature_data(
                feature_data,
                idxs,
                online_features_response,
                full_feature_names,
                requested_features,
                table,
                output_len,
            )

        if requested_on_demand_feature_views:
            utils._augment_response_with_on_demand_transforms(
                online_features_response,
                feature_refs,
                requested_on_demand_feature_views,
                full_feature_names,
            )

        utils._drop_unneeded_columns(
            online_features_response, requested_result_row_names
        )
        return OnlineResponse(online_features_response)

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
        if isinstance(entity_rows, list):
            columnar: Dict[str, List[Any]] = {k: [] for k in entity_rows[0].keys()}
            for entity_row in entity_rows:
                for key, value in entity_row.items():
                    try:
                        columnar[key].append(value)
                    except KeyError as e:
                        raise ValueError(
                            "All entity_rows must have the same keys."
                        ) from e

            entity_rows = columnar

        (
            join_key_values,
            grouped_refs,
            entity_name_to_join_key_map,
            requested_on_demand_feature_views,
            feature_refs,
            requested_result_row_names,
            online_features_response,
        ) = utils._prepare_entities_to_read_from_online_store(
            registry=registry,
            project=project,
            features=features,
            entity_values=entity_rows,
            full_feature_names=full_feature_names,
            native_entity_values=True,
        )

        async def query_table(table, requested_features):
            # Get the correct set of entity values with the correct join keys.
            table_entity_values, idxs, output_len = utils._get_unique_entities(
                table,
                join_key_values,
                entity_name_to_join_key_map,
            )

            entity_key_protos = utils._get_entity_key_protos(table_entity_values)

            # Fetch data for Entities.
            read_rows = await self.online_read_async(
                config=config,
                table=table,
                entity_keys=entity_key_protos,
                requested_features=requested_features,
            )

            return idxs, read_rows, output_len

        all_responses = await asyncio.gather(
            *[
                query_table(table, requested_features)
                for table, requested_features in grouped_refs
            ]
        )

        for (idxs, read_rows, output_len), (table, requested_features) in zip(
            all_responses, grouped_refs
        ):
            feature_data = utils._convert_rows_to_protobuf(
                requested_features, read_rows
            )

            # Populate the result_rows with the Features from the OnlineStore inplace.
            utils._populate_response_from_feature_data(
                feature_data,
                idxs,
                online_features_response,
                full_feature_names,
                requested_features,
                table,
                output_len,
            )

        if requested_on_demand_feature_views:
            utils._augment_response_with_on_demand_transforms(
                online_features_response,
                feature_refs,
                requested_on_demand_feature_views,
                full_feature_names,
            )

        utils._drop_unneeded_columns(
            online_features_response, requested_result_row_names
        )
        return OnlineResponse(online_features_response)

    @abstractmethod
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconciles cloud resources with the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables_to_delete: Feature views whose corresponding infrastructure should be deleted.
            tables_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            partial: If true, tables_to_delete and tables_to_keep are not exhaustive lists, so
                infrastructure corresponding to other feature views should be not be touched.
        """
        pass

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        """
        Returns the set of InfraObjects required to support the desired registry.

        Args:
            config: The config for the current feature store.
            desired_registry_proto: The desired registry, in proto form.
        """
        return []

    @abstractmethod
    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources for the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        pass

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        """
        Retrieves online feature values for the specified embeddings.

        Args:
            distance_metric: distance metric to use for retrieval.
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            requested_features: The list of features whose embeddings should be used for retrieval.
            embedding: The embeddings to use for retrieval.
            top_k: The number of documents to retrieve.

        Returns:
            object: A list of top k closest documents to the specified embedding. Each item in the list is a tuple
            where the first item is the event timestamp for the row, and the second item is a dict of feature
            name to embeddings.
        """
        if not requested_features:
            raise ValueError("Requested_features must be specified")
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online retrieval"
        )

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
        Retrieves online feature values for the specified embeddings.

        Args:
            distance_metric: distance metric to use for retrieval.
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            requested_features: The list of features whose embeddings should be used for retrieval.
            embedding: The embeddings to use for retrieval (optional)
            top_k: The number of documents to retrieve.
            query_string: The query string to search for using keyword search (bm25) (optional)

        Returns:
            object: A list of top k closest documents to the specified embedding. Each item in the list is a tuple
            where the first item is the event timestamp for the row, and the second item is a dict of feature
            name to embeddings.
        """
        assert embedding is not None or query_string is not None, (
            "Either embedding or query_string must be specified"
        )
        raise NotImplementedError(
            f"Online store {self.__class__.__name__} does not support online retrieval"
        )

    async def initialize(self, config: RepoConfig) -> None:
        pass

    async def close(self) -> None:
        pass
