# Copyright 2019 The Feast Authors
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
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.provider import Provider, get_provider
from feast.offline_store import (
    RetrievalJob,
    get_offline_store,
    get_offline_store_for_retrieval,
)
from feast.online_response import OnlineResponse, _infer_online_entity_rows
from feast.protos.feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import (
    LocalOnlineStoreConfig,
    OnlineStoreConfig,
    RepoConfig,
    load_repo_config,
)
from feast.type_map import python_value_to_proto_value


class FeatureStore:
    """
    A FeatureStore object is used to define, create, and retrieve features.
    """

    config: RepoConfig
    repo_path: Optional[str]
    _registry: Registry

    def __init__(
        self, repo_path: Optional[str] = None, config: Optional[RepoConfig] = None,
    ):
        self.repo_path = repo_path
        if repo_path is not None and config is not None:
            raise ValueError("You cannot specify both repo_path and config")
        if config is not None:
            self.config = config
        elif repo_path is not None:
            self.config = load_repo_config(Path(repo_path))
        else:
            self.config = RepoConfig(
                registry="./registry.db",
                project="default",
                provider="local",
                online_store=OnlineStoreConfig(
                    local=LocalOnlineStoreConfig(path="online_store.db")
                ),
            )

        registry_config = self.config.get_registry_config()
        self._registry = Registry(
            registry_path=registry_config.path,
            cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
        )

    @property
    def project(self) -> str:
        return self.config.project

    def _get_provider(self) -> Provider:
        return get_provider(self.config)

    def refresh_registry(self):
        """Fetches and caches a copy of the feature registry in memory.

        Explicitly calling this method allows for direct control of the state of the registry cache. Every time this
        method is called the complete registry state will be retrieved from the remote registry store backend
        (e.g., GCS, S3), and the cache timer will be reset. If refresh_registry() is run before get_online_features()
        is called, then get_online_feature() will use the cached registry instead of retrieving (and caching) the
        registry itself.

        Additionally, the TTL for the registry cache can be set to infinity (by setting it to 0), which means that
        refresh_registry() will become the only way to update the cached registry. If the TTL is set to a value
        greater than 0, then once the cache becomes stale (more time than the TTL has passed), a new cache will be
        downloaded synchronously, which may increase latencies if the triggering method is get_online_features()
        """

        registry_config = self.config.get_registry_config()
        self._registry = Registry(
            registry_path=registry_config.path,
            cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
        )
        self._registry.refresh()

    def list_entities(self) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Returns:
            List of entities
        """
        return self._registry.list_entities(self.project)

    def list_feature_views(self) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Returns:
            List of feature views
        """
        return self._registry.list_feature_views(self.project)

    def get_entity(self, name: str) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        return self._registry.get_entity(name, self.project)

    def get_feature_view(self, name: str) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        return self._registry.get_feature_view(name, self.project)

    def delete_feature_view(self, name: str):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
        """
        return self._registry.delete_feature_view(name, self.project)

    def apply(self, objects: List[Union[FeatureView, Entity]]):
        """Register objects to metadata store and update related infrastructure.

        The apply method registers one or more definitions (e.g., Entity, FeatureView) and registers or updates these
        objects in the Feast registry. Once the registry has been updated, the apply method will update related
        infrastructure (e.g., create tables in an online store) in order to reflect these new definitions. All
        operations are idempotent, meaning they can safely be rerun.

        Args: objects (List[Union[FeatureView, Entity]]): A list of FeatureView or Entity objects that should be
            registered

        Examples:
            Register a single Entity and FeatureView.
            >>> from feast.feature_store import FeatureStore
            >>> from feast import Entity, FeatureView, Feature, ValueType, FileSource
            >>> from datetime import timedelta
            >>>
            >>> fs = FeatureStore()
            >>> customer_entity = Entity(name="customer", value_type=ValueType.INT64, description="customer entity")
            >>> customer_feature_view = FeatureView(
            >>>     name="customer_fv",
            >>>     entities=["customer"],
            >>>     features=[Feature(name="age", dtype=ValueType.INT64)],
            >>>     input=FileSource(path="file.parquet", event_timestamp_column="timestamp"),
            >>>     ttl=timedelta(days=1)
            >>> )
            >>> fs.apply([customer_entity, customer_feature_view])
        """

        # TODO: Add locking
        # TODO: Optimize by only making a single call (read/write)

        views_to_update = []
        for ob in objects:
            if isinstance(ob, FeatureView):
                self._registry.apply_feature_view(ob, project=self.config.project)
                views_to_update.append(ob)
            elif isinstance(ob, Entity):
                self._registry.apply_entity(ob, project=self.config.project)
            else:
                raise ValueError(
                    f"Unknown object type ({type(ob)}) provided as part of apply() call"
                )
        self._get_provider().update_infra(
            project=self.config.project,
            tables_to_delete=[],
            tables_to_keep=views_to_update,
            partial=True,
        )

    def get_historical_features(
        self, entity_df: Union[pd.DataFrame, str], feature_refs: List[str],
    ) -> RetrievalJob:
        """Enrich an entity dataframe with historical feature values for either training or batch scoring.

        This method joins historical feature data from one or more feature views to an entity dataframe by using a time
        travel join.

        Each feature view is joined to the entity dataframe using all entities configured for the respective feature
        view. All configured entities must be available in the entity dataframe. Therefore, the entity dataframe must
        contain all entities found in all feature views, but the individual feature views can have different entities.

        Time travel is based on the configured TTL for each feature view. A shorter TTL will limit the
        amount of scanning that will be done in order to find feature data for a specific entity key. Setting a short
        TTL may result in null values being returned.

        Args:
            entity_df (Union[pd.DataFrame, str]): An entity dataframe is a collection of rows containing all entity
                columns (e.g., customer_id, driver_id) on which features need to be joined, as well as a event_timestamp
                column used to ensure point-in-time correctness. Either a Pandas DataFrame can be provided or a string
                SQL query. The query must be of a format supported by the configured offline store (e.g., BigQuery)
            feature_refs: A list of features that should be retrieved from the offline store. Feature references are of
                the format "feature_view:feature", e.g., "customer_fv:daily_transactions".

        Returns:
            RetrievalJob which can be used to materialize the results.

        Examples:
            Retrieve historical features using a BigQuery SQL entity dataframe
            >>> from feast.feature_store import FeatureStore
            >>>
            >>> fs = FeatureStore(config=RepoConfig(provider="gcp"))
            >>> retrieval_job = fs.get_historical_features(
            >>>     entity_df="SELECT event_timestamp, order_id, customer_id from gcp_project.my_ds.customer_orders",
            >>>     feature_refs=["customer:age", "customer:avg_orders_1d", "customer:avg_orders_7d"]
            >>> )
            >>> feature_data = job.to_df()
            >>> model.fit(feature_data) # insert your modeling framework here.
        """

        all_feature_views = self._registry.list_feature_views(
            project=self.config.project
        )
        feature_views = _get_requested_feature_views(feature_refs, all_feature_views)
        offline_store = get_offline_store_for_retrieval(feature_views)
        job = offline_store.get_historical_features(
            self.config, feature_views, feature_refs, entity_df
        )
        return job

    def materialize_incremental(
        self, feature_views: Optional[List[str]], end_date: datetime,
    ) -> None:
        """
        Materialize incremental new data from the offline store into the online store.

        This method loads incremental new feature data up to the specified end time from either
        the specified feature views, or all feature views if none are specified,
        into the online store where it is available for online serving. The start time of
        the interval materialized is either the most recent end time of a prior materialization or
        (now - ttl) if no such prior materialization exists.

        Args:
            feature_views (List[str]): Optional list of feature view names. If selected, will only run
                materialization for the specified feature views.
            end_date (datetime): End date for time range of data to materialize into the online store

        Examples:
            Materialize all features into the online store up to 5 minutes ago.
            >>> from datetime import datetime, timedelta
            >>> from feast.feature_store import FeatureStore
            >>>
            >>> fs = FeatureStore(config=RepoConfig(provider="gcp"))
            >>> fs.materialize_incremental(
            >>>     end_date=datetime.utcnow() - timedelta(minutes=5)
            >>> )
        """
        feature_views_to_materialize = []
        if feature_views is None:
            feature_views_to_materialize = self._registry.list_feature_views(
                self.config.project
            )
        else:
            for name in feature_views:
                feature_view = self._registry.get_feature_view(
                    name, self.config.project
                )
                feature_views_to_materialize.append(feature_view)

        # TODO paging large loads
        for feature_view in feature_views_to_materialize:
            start_date = feature_view.most_recent_end_time
            if start_date is None:
                if feature_view.ttl is None:
                    raise Exception(
                        f"No start time found for feature view {feature_view.name}. materialize_incremental() requires either a ttl to be set or for materialize() to have been run at least once."
                    )
                start_date = datetime.utcnow() - feature_view.ttl
            self._materialize_single_feature_view(feature_view, start_date, end_date)

    def materialize(
        self,
        feature_views: Optional[List[str]],
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        """
        Materialize data from the offline store into the online store.

        This method loads feature data in the specified interval from either
        the specified feature views, or all feature views if none are specified,
        into the online store where it is available for online serving.

        Args:
            feature_views (List[str]): Optional list of feature view names. If selected, will only run
                materialization for the specified feature views.
            start_date (datetime): Start date for time range of data to materialize into the online store
            end_date (datetime): End date for time range of data to materialize into the online store

        Examples:
            Materialize all features into the online store over the interval
            from 3 hours ago to 10 minutes ago.
            >>> from datetime import datetime, timedelta
            >>> from feast.feature_store import FeatureStore
            >>>
            >>> fs = FeatureStore(config=RepoConfig(provider="gcp"))
            >>> fs.materialize(
            >>>     start_date=datetime.utcnow() - timedelta(hours=3),
            >>>     end_date=datetime.utcnow() - timedelta(minutes=10)
            >>> )
        """
        feature_views_to_materialize = []
        if feature_views is None:
            feature_views_to_materialize = self._registry.list_feature_views(
                self.config.project
            )
        else:
            for name in feature_views:
                feature_view = self._registry.get_feature_view(
                    name, self.config.project
                )
                feature_views_to_materialize.append(feature_view)

        # TODO paging large loads
        for feature_view in feature_views_to_materialize:
            self._materialize_single_feature_view(feature_view, start_date, end_date)

    def _materialize_single_feature_view(
        self, feature_view: FeatureView, start_date: datetime, end_date: datetime
    ) -> None:
        (
            entity_names,
            feature_names,
            event_timestamp_column,
            created_timestamp_column,
        ) = _run_reverse_field_mapping(feature_view)

        offline_store = get_offline_store(self.config)
        table = offline_store.pull_latest_from_table_or_query(
            feature_view.input,
            entity_names,
            feature_names,
            event_timestamp_column,
            created_timestamp_column,
            start_date,
            end_date,
        )

        if feature_view.input.field_mapping is not None:
            table = _run_forward_field_mapping(table, feature_view.input.field_mapping)

        rows_to_write = _convert_arrow_to_proto(table, feature_view)

        provider = self._get_provider()
        provider.online_write_batch(
            self.config.project, feature_view, rows_to_write, None
        )

        feature_view.materialization_intervals.append((start_date, end_date))
        self.apply([feature_view])

    def get_online_features(
        self, feature_refs: List[str], entity_rows: List[Dict[str, Any]],
    ) -> OnlineResponse:
        """
        Retrieves the latest online feature data.

        Note: This method will download the full feature registry the first time it is run. If you are using a
        remote registry like GCS or S3 then that may take a few seconds. The registry remains cached up to a TTL
        duration (which can be set to infinitey). If the cached registry is stale (more time than the TTL has
        passed), then a new registry will be downloaded synchronously by this method. This download may
        introduce latency to online feature retrieval. In order to avoid synchronous downloads, please call
        refresh_registry() prior to the TTL being reached. Remember it is possible to set the cache TTL to
        infinity (cache forever).

        Args:
            feature_refs: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
                Only the feature name is required.
            entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
        Returns:
            OnlineResponse containing the feature data in records.
        Examples:
            >>> from feast import FeatureStore
            >>>
            >>> store = FeatureStore(repo_path="...")
            >>> feature_refs = ["sales:daily_transactions"]
            >>> entity_rows = [{"customer_id": 0},{"customer_id": 1}]
            >>>
            >>> online_response = store.get_online_features(
            >>>     feature_refs, entity_rows, project="my_project")
            >>> online_response_dict = online_response.to_dict()
            >>> print(online_response_dict)
            {'sales:daily_transactions': [1.1,1.2], 'sales:customer_id': [0,1]}
        """

        response = self._get_online_features(
            feature_refs=feature_refs,
            entity_rows=_infer_online_entity_rows(entity_rows),
            project=self.config.project,
        )

        return OnlineResponse(response)

    def _get_online_features(
        self,
        entity_rows: List[GetOnlineFeaturesRequestV2.EntityRow],
        feature_refs: List[str],
        project: str,
    ) -> GetOnlineFeaturesResponse:

        provider = self._get_provider()

        entity_keys = []
        result_rows: List[GetOnlineFeaturesResponse.FieldValues] = []

        for row in entity_rows:
            entity_keys.append(_entity_row_to_key(row))
            result_rows.append(_entity_row_to_field_values(row))

        all_feature_views = self._registry.list_feature_views(
            project=self.config.project, allow_cache=True
        )

        grouped_refs = _group_refs(feature_refs, all_feature_views)
        for table, requested_features in grouped_refs:
            read_rows = provider.online_read(
                project=project, table=table, entity_keys=entity_keys,
            )
            for row_idx, read_row in enumerate(read_rows):
                row_ts, feature_data = read_row
                result_row = result_rows[row_idx]

                if feature_data is None:
                    for feature_name in requested_features:
                        feature_ref = f"{table.name}:{feature_name}"
                        result_row.statuses[
                            feature_ref
                        ] = GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND
                else:
                    for feature_name in feature_data:
                        feature_ref = f"{table.name}:{feature_name}"
                        if feature_name in requested_features:
                            result_row.fields[feature_ref].CopyFrom(
                                feature_data[feature_name]
                            )
                            result_row.statuses[
                                feature_ref
                            ] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

        return GetOnlineFeaturesResponse(field_values=result_rows)


def _entity_row_to_key(row: GetOnlineFeaturesRequestV2.EntityRow) -> EntityKeyProto:
    names, values = zip(*row.fields.items())
    return EntityKeyProto(entity_names=names, entity_values=values)  # type: ignore


def _entity_row_to_field_values(
    row: GetOnlineFeaturesRequestV2.EntityRow,
) -> GetOnlineFeaturesResponse.FieldValues:
    result = GetOnlineFeaturesResponse.FieldValues()
    for k in row.fields:
        result.fields[k].CopyFrom(row.fields[k])
        result.statuses[k] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

    return result


def _group_refs(
    feature_refs: List[str], all_feature_views: List[FeatureView]
) -> List[Tuple[FeatureView, List[str]]]:
    """ Get list of feature views and corresponding feature names based on feature references"""

    # view name to view proto
    view_index = {view.name: view for view in all_feature_views}

    # view name to feature names
    views_features = defaultdict(list)

    for ref in feature_refs:
        view_name, feat_name = ref.split(":")
        if view_name not in view_index:
            raise ValueError(f"Could not find feature view from reference {ref}")
        views_features[view_name].append(feat_name)

    result = []
    for view_name, feature_names in views_features.items():
        result.append((view_index[view_name], feature_names))
    return result


def _run_reverse_field_mapping(
    feature_view: FeatureView,
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the entity names,
    feature names, event timestamp column, and created timestamp column
    to get the names of the relevant columns in the BigQuery table.

    Args:
        feature_view: FeatureView object containing the field mapping
            as well as the names to reverse-map.
    Returns:
        Tuple containing the list of reverse-mapped entity names,
        reverse-mapped feature names, reverse-mapped event timestamp column,
        and reverse-mapped created timestamp column that will be passed into
        the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    event_timestamp_column = feature_view.input.event_timestamp_column
    entity_names = [entity for entity in feature_view.entities]
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.input.created_timestamp_column
    if feature_view.input.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        event_timestamp_column = (
            reverse_field_mapping[event_timestamp_column]
            if event_timestamp_column in reverse_field_mapping.keys()
            else event_timestamp_column
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column
            and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        entity_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in entity_names
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]
    return (
        entity_names,
        feature_names,
        event_timestamp_column,
        created_timestamp_column,
    )


def _run_forward_field_mapping(
    table: pyarrow.Table, field_mapping: Dict[str, str],
) -> pyarrow.Table:
    # run field mapping in the forward direction
    cols = table.column_names
    mapped_cols = [
        field_mapping[col] if col in field_mapping.keys() else col for col in cols
    ]
    table = table.rename_columns(mapped_cols)
    return table


def _convert_arrow_to_proto(
    table: pyarrow.Table, feature_view: FeatureView
) -> List[Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]]:
    rows_to_write = []

    def _coerce_datetime(ts):
        """
        Depending on underlying time resolution, arrow to_pydict() sometimes returns pandas
        timestamp type (for nanosecond resolution), and sometimes you get standard python datetime
        (for microsecond resolution).

        While pandas timestamp class is a subclass of python datetime, it doesn't always behave the
        same way. We convert it to normal datetime so that consumers downstream don't have to deal
        with these quirks.
        """

        if isinstance(ts, pd.Timestamp):
            return ts.to_pydatetime()
        else:
            return ts

    for row in zip(*table.to_pydict().values()):
        entity_key = EntityKeyProto()
        for entity_name in feature_view.entities:
            entity_key.entity_names.append(entity_name)
            idx = table.column_names.index(entity_name)
            value = python_value_to_proto_value(row[idx])
            entity_key.entity_values.append(value)
        feature_dict = {}
        for feature in feature_view.features:
            idx = table.column_names.index(feature.name)
            value = python_value_to_proto_value(row[idx])
            feature_dict[feature.name] = value
        event_timestamp_idx = table.column_names.index(
            feature_view.input.event_timestamp_column
        )
        event_timestamp = _coerce_datetime(row[event_timestamp_idx])

        if feature_view.input.created_timestamp_column is not None:
            created_timestamp_idx = table.column_names.index(
                feature_view.input.created_timestamp_column
            )
            created_timestamp = _coerce_datetime(row[created_timestamp_idx])
        else:
            created_timestamp = None

        rows_to_write.append(
            (entity_key, feature_dict, event_timestamp, created_timestamp)
        )
    return rows_to_write


def _get_requested_feature_views(
    feature_refs: List[str], all_feature_views: List[FeatureView]
) -> List[FeatureView]:
    """Get list of feature views based on feature references"""
    return list(view for view, _ in _group_refs(feature_refs, all_feature_views))
