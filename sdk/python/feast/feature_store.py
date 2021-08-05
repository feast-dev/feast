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
import os
import warnings
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
from colorama import Fore, Style
from tqdm import tqdm

from feast import utils
from feast.entity import Entity
from feast.errors import (
    EntityNotFoundException,
    FeatureNameCollisionError,
    FeatureViewNotFoundException,
)
from feast.feature_service import FeatureService
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.inference import (
    update_data_sources_with_inferred_event_timestamp_col,
    update_entities_with_inferred_types_from_feature_views,
)
from feast.infra.provider import Provider, RetrievalJob, get_provider
from feast.online_response import OnlineResponse, _infer_online_entity_rows
from feast.protos.feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.registry import Registry
from feast.repo_config import RepoConfig, load_repo_config
from feast.usage import log_exceptions, log_exceptions_and_usage
from feast.version import get_version

warnings.simplefilter("once", DeprecationWarning)


class FeatureStore:
    """
    A FeatureStore object is used to define, create, and retrieve features.

    Args:
        repo_path (optional): Path to a `feature_store.yaml` used to configure the
            feature store.
        config (optional): Configuration object used to configure the feature store.
    """

    config: RepoConfig
    repo_path: Path
    _registry: Registry

    @log_exceptions
    def __init__(
        self, repo_path: Optional[str] = None, config: Optional[RepoConfig] = None,
    ):
        """
        Creates a FeatureStore object.

        Raises:
            ValueError: If both or neither of repo_path and config are specified.
        """
        if repo_path is not None and config is not None:
            raise ValueError("You cannot specify both repo_path and config.")
        if config is not None:
            self.repo_path = Path(os.getcwd())
            self.config = config
        elif repo_path is not None:
            self.repo_path = Path(repo_path)
            self.config = load_repo_config(Path(repo_path))
        else:
            raise ValueError("Please specify one of repo_path or config.")

        registry_config = self.config.get_registry_config()
        self._registry = Registry(
            registry_path=registry_config.path,
            repo_path=self.repo_path,
            cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
        )

    @log_exceptions
    def version(self) -> str:
        """Returns the version of the current Feast SDK/CLI."""
        return get_version()

    @property
    def registry(self) -> Registry:
        """Gets the registry of this feature store."""
        return self._registry

    @property
    def project(self) -> str:
        """Gets the project of this feature store."""
        return self.config.project

    def _get_provider(self) -> Provider:
        # TODO: Bake self.repo_path into self.config so that we dont only have one interface to paths
        return get_provider(self.config, self.repo_path)

    @log_exceptions_and_usage
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
            repo_path=self.repo_path,
            cache_ttl=timedelta(seconds=registry_config.cache_ttl_seconds),
        )
        self._registry.refresh()

    @log_exceptions_and_usage
    def list_entities(self, allow_cache: bool = False) -> List[Entity]:
        """
        Retrieves the list of entities from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.

        Returns:
            A list of entities.
        """
        return self._registry.list_entities(self.project, allow_cache=allow_cache)

    @log_exceptions_and_usage
    def list_feature_services(self) -> List[FeatureService]:
        """
        Retrieves the list of feature services from the registry.

        Returns:
            A list of feature services.
        """
        return self._registry.list_feature_services(self.project)

    @log_exceptions_and_usage
    def list_feature_views(self) -> List[FeatureView]:
        """
        Retrieves the list of feature views from the registry.

        Returns:
            A list of feature views.
        """
        return self._registry.list_feature_views(self.project)

    @log_exceptions_and_usage
    def get_entity(self, name: str) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity.

        Returns:
            The specified entity.

        Raises:
            EntityNotFoundException: The entity could not be found.
        """
        return self._registry.get_entity(name, self.project)

    @log_exceptions_and_usage
    def get_feature_service(self, name: str) -> FeatureService:
        """
        Retrieves a feature service.

        Args:
            name: Name of feature service.

        Returns:
            The specified feature service.

        Raises:
            FeatureServiceNotFoundException: The feature service could not be found.
        """
        return self._registry.get_feature_service(name, self.project)

    @log_exceptions_and_usage
    def get_feature_view(self, name: str) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view.

        Returns:
            The specified feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._registry.get_feature_view(name, self.project)

    @log_exceptions_and_usage
    def delete_feature_view(self, name: str):
        """
        Deletes a feature view.

        Args:
            name: Name of feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._registry.delete_feature_view(name, self.project)

    @log_exceptions_and_usage
    def delete_feature_service(self, name: str):
        """
            Deletes a feature service.

            Args:
                name: Name of feature service.

            Raises:
                FeatureServiceNotFoundException: The feature view could not be found.
            """
        return self._registry.delete_feature_service(name, self.project)

    def _get_features(
        self,
        features: Optional[Union[List[str], FeatureService]],
        feature_refs: Optional[List[str]],
    ) -> List[str]:
        _features = features or feature_refs
        if not _features:
            raise ValueError("No features specified for retrieval")

        _feature_refs: List[str]
        if isinstance(_features, FeatureService):
            # Get the latest value of the feature service, in case the object passed in has been updated underneath us.
            _feature_refs = _get_feature_refs_from_feature_services(
                self.get_feature_service(_features.name)
            )
        else:
            _feature_refs = _features
        return _feature_refs

    @log_exceptions_and_usage
    def apply(
        self,
        objects: Union[
            Entity,
            FeatureView,
            FeatureService,
            List[Union[FeatureView, Entity, FeatureService]],
        ],
        commit: bool = True,
    ):
        """Register objects to metadata store and update related infrastructure.

        The apply method registers one or more definitions (e.g., Entity, FeatureView) and registers or updates these
        objects in the Feast registry. Once the registry has been updated, the apply method will update related
        infrastructure (e.g., create tables in an online store) in order to reflect these new definitions. All
        operations are idempotent, meaning they can safely be rerun.

        Args:
            objects: A single object, or a list of objects that should be registered with the Feature Store.
            commit: whether to commit changes to the registry

        Raises:
            ValueError: The 'objects' parameter could not be parsed properly.

        Examples:
            Register an Entity and a FeatureView.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> fs = FeatureStore(config=RepoConfig(registry="feature_repo/data/registry.db", project="feature_repo", provider="local"))
            >>> driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id")
            >>> driver_hourly_stats = FileSource(
            ...     path="feature_repo/data/driver_stats.parquet",
            ...     event_timestamp_column="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=["driver_id"],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     batch_source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
        """
        # TODO: Add locking

        if not isinstance(objects, Iterable):
            objects = [objects]

        assert isinstance(objects, list)

        views_to_update = [ob for ob in objects if isinstance(ob, FeatureView)]
        _validate_feature_views(views_to_update)
        entities_to_update = [ob for ob in objects if isinstance(ob, Entity)]
        services_to_update = [ob for ob in objects if isinstance(ob, FeatureService)]

        # Make inferences
        update_entities_with_inferred_types_from_feature_views(
            entities_to_update, views_to_update, self.config
        )

        update_data_sources_with_inferred_event_timestamp_col(
            [view.batch_source for view in views_to_update], self.config
        )

        for view in views_to_update:
            view.infer_features_from_batch_source(self.config)

        if len(views_to_update) + len(entities_to_update) + len(
            services_to_update
        ) != len(objects):
            raise ValueError("Unknown object type provided as part of apply() call")

        for view in views_to_update:
            self._registry.apply_feature_view(view, project=self.project, commit=False)
        for ent in entities_to_update:
            self._registry.apply_entity(ent, project=self.project, commit=False)
        for feature_service in services_to_update:
            self._registry.apply_feature_service(feature_service, project=self.project)

        self._get_provider().update_infra(
            project=self.project,
            tables_to_delete=[],
            tables_to_keep=views_to_update,
            entities_to_delete=[],
            entities_to_keep=entities_to_update,
            partial=True,
        )

        if commit:
            self._registry.commit()

    @log_exceptions_and_usage
    def teardown(self):
        """Tears down all local and cloud resources for the feature store."""
        tables: List[Union[FeatureView, FeatureTable]] = []
        feature_views = self.list_feature_views()
        feature_tables = self._registry.list_feature_tables(self.project)

        tables.extend(feature_views)
        tables.extend(feature_tables)

        entities = self.list_entities()

        self._get_provider().teardown_infra(self.project, tables, entities)
        self._registry.teardown()

    @log_exceptions_and_usage
    def get_historical_features(
        self,
        entity_df: Union[pd.DataFrame, str],
        features: Optional[Union[List[str], FeatureService]] = None,
        feature_refs: Optional[List[str]] = None,
        full_feature_names: bool = False,
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
            features: A list of features, that should be retrieved from the offline store.
                Either a list of string feature references can be provided or a FeatureService object.
                Feature references are of the format "feature_view:feature", e.g., "customer_fv:daily_transactions".
            full_feature_names: A boolean that provides the option to add the feature view prefixes to the feature names,
                changing them from the format "feature" to "feature_view__feature" (e.g., "daily_transactions" changes to
                "customer_fv__daily_transactions"). By default, this value is set to False.

        Returns:
            RetrievalJob which can be used to materialize the results.

        Raises:
            ValueError: Both or neither of features and feature_refs are specified.

        Examples:
            Retrieve historical features from a local offline store.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> import pandas as pd
            >>> fs = FeatureStore(config=RepoConfig(registry="feature_repo/data/registry.db", project="feature_repo", provider="local"))
            >>> # Before retrieving historical features, we must register the appropriate entity and featureview.
            >>> driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id")
            >>> driver_hourly_stats = FileSource(
            ...     path="feature_repo/data/driver_stats.parquet",
            ...     event_timestamp_column="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=["driver_id"],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     features=[
            ...         Feature(name="conv_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="acc_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="avg_daily_trips", dtype=ValueType.INT64),
            ...     ],
            ...     batch_source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
            >>> entity_df = pd.DataFrame.from_dict(
            ...     {
            ...         "driver_id": [1001, 1002],
            ...         "event_timestamp": [
            ...             datetime(2021, 4, 12, 10, 59, 42),
            ...             datetime(2021, 4, 12, 8, 12, 10),
            ...         ],
            ...     }
            ... )
            >>> retrieval_job = fs.get_historical_features(
            ...     entity_df=entity_df,
            ...     features=[
            ...         "driver_hourly_stats:conv_rate",
            ...         "driver_hourly_stats:acc_rate",
            ...         "driver_hourly_stats:avg_daily_trips",
            ...     ],
            ... )
            >>> feature_data = retrieval_job.to_df()
        """
        if (features is not None and feature_refs is not None) or (
            features is None and feature_refs is None
        ):
            raise ValueError(
                "You must specify exactly one of features and feature_refs."
            )

        if feature_refs:
            warnings.warn(
                (
                    "The argument 'feature_refs' is being deprecated. Please use 'features' "
                    "instead. Feast 0.13 and onwards will not support the argument 'feature_refs'."
                ),
                DeprecationWarning,
            )

        _feature_refs = self._get_features(features, feature_refs)

        all_feature_views = self._registry.list_feature_views(project=self.project)
        feature_views = list(
            view for view, _ in _group_feature_refs(_feature_refs, all_feature_views)
        )

        _validate_feature_refs(_feature_refs, full_feature_names)

        provider = self._get_provider()

        job = provider.get_historical_features(
            self.config,
            feature_views,
            _feature_refs,
            entity_df,
            self._registry,
            self.project,
            full_feature_names,
        )

        return job

    @log_exceptions_and_usage
    def materialize_incremental(
        self, end_date: datetime, feature_views: Optional[List[str]] = None,
    ) -> None:
        """
        Materialize incremental new data from the offline store into the online store.

        This method loads incremental new feature data up to the specified end time from either
        the specified feature views, or all feature views if none are specified,
        into the online store where it is available for online serving. The start time of
        the interval materialized is either the most recent end time of a prior materialization or
        (now - ttl) if no such prior materialization exists.

        Args:
            end_date (datetime): End date for time range of data to materialize into the online store
            feature_views (List[str]): Optional list of feature view names. If selected, will only run
                materialization for the specified feature views.

        Raises:
            Exception: A feature view being materialized does not have a TTL set.

        Examples:
            Materialize all features into the online store up to 5 minutes ago.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> fs = FeatureStore(config=RepoConfig(registry="feature_repo/data/registry.db", project="feature_repo", provider="local"))
            >>> # Before materializing, we must register the appropriate entity and featureview.
            >>> driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)
            >>> driver_hourly_stats = FileSource(
            ...     path="feature_repo/data/driver_stats.parquet",
            ...     event_timestamp_column="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=["driver_id"],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     features=[
            ...         Feature(name="conv_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="acc_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="avg_daily_trips", dtype=ValueType.INT64),
            ...     ],
            ...     batch_source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
            >>> fs.materialize_incremental(end_date=datetime.utcnow() - timedelta(minutes=5))
            Materializing...
            <BLANKLINE>
            ...
        """
        feature_views_to_materialize = []
        if feature_views is None:
            feature_views_to_materialize = self._registry.list_feature_views(
                self.project
            )
        else:
            for name in feature_views:
                feature_view = self._registry.get_feature_view(name, self.project)
                feature_views_to_materialize.append(feature_view)

        _print_materialization_log(
            None,
            end_date,
            len(feature_views_to_materialize),
            self.config.online_store.type,
        )
        # TODO paging large loads
        for feature_view in feature_views_to_materialize:
            start_date = feature_view.most_recent_end_time
            if start_date is None:
                if feature_view.ttl is None:
                    raise Exception(
                        f"No start time found for feature view {feature_view.name}. materialize_incremental() requires"
                        f" either a ttl to be set or for materialize() to have been run at least once."
                    )
                start_date = datetime.utcnow() - feature_view.ttl
            provider = self._get_provider()
            print(
                f"{Style.BRIGHT + Fore.GREEN}{feature_view.name}{Style.RESET_ALL}"
                f" from {Style.BRIGHT + Fore.GREEN}{start_date.replace(microsecond=0).astimezone()}{Style.RESET_ALL}"
                f" to {Style.BRIGHT + Fore.GREEN}{end_date.replace(microsecond=0).astimezone()}{Style.RESET_ALL}:"
            )

            def tqdm_builder(length):
                return tqdm(total=length, ncols=100)

            start_date = utils.make_tzaware(start_date)
            end_date = utils.make_tzaware(end_date)

            provider.materialize_single_feature_view(
                config=self.config,
                feature_view=feature_view,
                start_date=start_date,
                end_date=end_date,
                registry=self._registry,
                project=self.project,
                tqdm_builder=tqdm_builder,
            )

            self._registry.apply_materialization(
                feature_view, self.project, start_date, end_date
            )

    @log_exceptions_and_usage
    def materialize(
        self,
        start_date: datetime,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
    ) -> None:
        """
        Materialize data from the offline store into the online store.

        This method loads feature data in the specified interval from either
        the specified feature views, or all feature views if none are specified,
        into the online store where it is available for online serving.

        Args:
            start_date (datetime): Start date for time range of data to materialize into the online store
            end_date (datetime): End date for time range of data to materialize into the online store
            feature_views (List[str]): Optional list of feature view names. If selected, will only run
                materialization for the specified feature views.

        Examples:
            Materialize all features into the online store over the interval
            from 3 hours ago to 10 minutes ago.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> fs = FeatureStore(config=RepoConfig(registry="feature_repo/data/registry.db", project="feature_repo", provider="local"))
            >>> # Before materializing, we must register the appropriate entity and featureview.
            >>> driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)
            >>> driver_hourly_stats = FileSource(
            ...     path="feature_repo/data/driver_stats.parquet",
            ...     event_timestamp_column="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=["driver_id"],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     features=[
            ...         Feature(name="conv_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="acc_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="avg_daily_trips", dtype=ValueType.INT64),
            ...     ],
            ...     batch_source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
            >>> fs.materialize(
            ...     start_date=datetime.utcnow() - timedelta(hours=3), end_date=datetime.utcnow() - timedelta(minutes=10)
            ... )
            Materializing...
            <BLANKLINE>
            ...
        """
        if utils.make_tzaware(start_date) > utils.make_tzaware(end_date):
            raise ValueError(
                f"The given start_date {start_date} is greater than the given end_date {end_date}."
            )

        feature_views_to_materialize = []
        if feature_views is None:
            feature_views_to_materialize = self._registry.list_feature_views(
                self.project
            )
        else:
            for name in feature_views:
                feature_view = self._registry.get_feature_view(name, self.project)
                feature_views_to_materialize.append(feature_view)

        _print_materialization_log(
            start_date,
            end_date,
            len(feature_views_to_materialize),
            self.config.online_store.type,
        )
        # TODO paging large loads
        for feature_view in feature_views_to_materialize:
            provider = self._get_provider()
            print(f"{Style.BRIGHT + Fore.GREEN}{feature_view.name}{Style.RESET_ALL}:")

            def tqdm_builder(length):
                return tqdm(total=length, ncols=100)

            start_date = utils.make_tzaware(start_date)
            end_date = utils.make_tzaware(end_date)

            provider.materialize_single_feature_view(
                config=self.config,
                feature_view=feature_view,
                start_date=start_date,
                end_date=end_date,
                registry=self._registry,
                project=self.project,
                tqdm_builder=tqdm_builder,
            )

            self._registry.apply_materialization(
                feature_view, self.project, start_date, end_date
            )

    @log_exceptions_and_usage
    def get_online_features(
        self,
        features: Union[List[str], FeatureService],
        entity_rows: List[Dict[str, Any]],
        feature_refs: Optional[List[str]] = None,
        full_feature_names: bool = False,
    ) -> OnlineResponse:
        """
        Retrieves the latest online feature data.

        Note: This method will download the full feature registry the first time it is run. If you are using a
        remote registry like GCS or S3 then that may take a few seconds. The registry remains cached up to a TTL
        duration (which can be set to infinity). If the cached registry is stale (more time than the TTL has
        passed), then a new registry will be downloaded synchronously by this method. This download may
        introduce latency to online feature retrieval. In order to avoid synchronous downloads, please call
        refresh_registry() prior to the TTL being reached. Remember it is possible to set the cache TTL to
        infinity (cache forever).

        Args:
            features: List of feature references that will be returned for each entity.
                Each feature reference should have the following format:
                "feature_table:feature" where "feature_table" & "feature" refer to
                the feature and feature table names respectively.
                Only the feature name is required.
            entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.

        Returns:
            OnlineResponse containing the feature data in records.

        Raises:
            Exception: No entity with the specified name exists.

        Examples:
            Materialize all features into the online store over the interval
            from 3 hours ago to 10 minutes ago, and then retrieve these online features.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> import pandas as pd
            >>> fs = FeatureStore(config=RepoConfig(registry="feature_repo/data/registry.db", project="feature_repo", provider="local"))
            >>> # Before getting online features, we must register the appropriate entity and featureview and then materialize the features.
            >>> driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id",)
            >>> driver_hourly_stats = FileSource(
            ...     path="feature_repo/data/driver_stats.parquet",
            ...     event_timestamp_column="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=["driver_id"],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     features=[
            ...         Feature(name="conv_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="acc_rate", dtype=ValueType.FLOAT),
            ...         Feature(name="avg_daily_trips", dtype=ValueType.INT64),
            ...     ],
            ...     batch_source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
            >>> fs.materialize(
            ...     start_date=datetime.utcnow() - timedelta(hours=3), end_date=datetime.utcnow() - timedelta(minutes=10)
            ... )
            Materializing...
            <BLANKLINE>
            ...
            >>> online_response = fs.get_online_features(
            ...     features=[
            ...         "driver_hourly_stats:conv_rate",
            ...         "driver_hourly_stats:acc_rate",
            ...         "driver_hourly_stats:avg_daily_trips",
            ...     ],
            ...     entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}, {"driver_id": 1003}, {"driver_id": 1004}],
            ... )
            >>> online_response_dict = online_response.to_dict()
        """
        _feature_refs = self._get_features(features, feature_refs)

        provider = self._get_provider()
        entities = self.list_entities(allow_cache=True)
        entity_name_to_join_key_map = {}
        for entity in entities:
            entity_name_to_join_key_map[entity.name] = entity.join_key

        join_key_rows = []
        for row in entity_rows:
            join_key_row = {}
            for entity_name, entity_value in row.items():
                try:
                    join_key = entity_name_to_join_key_map[entity_name]
                except KeyError:
                    raise EntityNotFoundException(entity_name, self.project)
                join_key_row[join_key] = entity_value
            join_key_rows.append(join_key_row)

        entity_row_proto_list = _infer_online_entity_rows(join_key_rows)

        union_of_entity_keys = []
        result_rows: List[GetOnlineFeaturesResponse.FieldValues] = []

        for entity_row_proto in entity_row_proto_list:
            union_of_entity_keys.append(_entity_row_to_key(entity_row_proto))
            result_rows.append(_entity_row_to_field_values(entity_row_proto))

        all_feature_views = self._registry.list_feature_views(
            project=self.project, allow_cache=True
        )

        _validate_feature_refs(_feature_refs, full_feature_names)
        grouped_refs = _group_feature_refs(_feature_refs, all_feature_views)
        for table, requested_features in grouped_refs:
            entity_keys = _get_table_entity_keys(
                table, union_of_entity_keys, entity_name_to_join_key_map
            )
            read_rows = provider.online_read(
                config=self.config,
                table=table,
                entity_keys=entity_keys,
                requested_features=requested_features,
            )
            for row_idx, read_row in enumerate(read_rows):
                row_ts, feature_data = read_row
                result_row = result_rows[row_idx]

                if feature_data is None:
                    for feature_name in requested_features:
                        feature_ref = (
                            f"{table.name}__{feature_name}"
                            if full_feature_names
                            else feature_name
                        )
                        result_row.statuses[
                            feature_ref
                        ] = GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND
                else:
                    for feature_name in feature_data:
                        feature_ref = (
                            f"{table.name}__{feature_name}"
                            if full_feature_names
                            else feature_name
                        )
                        if feature_name in requested_features:
                            result_row.fields[feature_ref].CopyFrom(
                                feature_data[feature_name]
                            )
                            result_row.statuses[
                                feature_ref
                            ] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

        return OnlineResponse(GetOnlineFeaturesResponse(field_values=result_rows))


def _entity_row_to_key(row: GetOnlineFeaturesRequestV2.EntityRow) -> EntityKeyProto:
    names, values = zip(*row.fields.items())
    return EntityKeyProto(join_keys=names, entity_values=values)


def _entity_row_to_field_values(
    row: GetOnlineFeaturesRequestV2.EntityRow,
) -> GetOnlineFeaturesResponse.FieldValues:
    result = GetOnlineFeaturesResponse.FieldValues()
    for k in row.fields:
        result.fields[k].CopyFrom(row.fields[k])
        result.statuses[k] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

    return result


def _validate_feature_refs(feature_refs: List[str], full_feature_names: bool = False):
    collided_feature_refs = []

    if full_feature_names:
        collided_feature_refs = [
            ref for ref, occurrences in Counter(feature_refs).items() if occurrences > 1
        ]
    else:
        feature_names = [ref.split(":")[1] for ref in feature_refs]
        collided_feature_names = [
            ref
            for ref, occurrences in Counter(feature_names).items()
            if occurrences > 1
        ]

        for feature_name in collided_feature_names:
            collided_feature_refs.extend(
                [ref for ref in feature_refs if ref.endswith(":" + feature_name)]
            )

    if len(collided_feature_refs) > 0:
        raise FeatureNameCollisionError(collided_feature_refs, full_feature_names)


def _group_feature_refs(
    features: Union[List[str], FeatureService], all_feature_views: List[FeatureView]
) -> List[Tuple[FeatureView, List[str]]]:
    """ Get list of feature views and corresponding feature names based on feature references"""

    # view name to view proto
    view_index = {view.name: view for view in all_feature_views}

    # view name to feature names
    views_features = defaultdict(list)

    if isinstance(features, list) and isinstance(features[0], str):
        for ref in features:
            view_name, feat_name = ref.split(":")
            if view_name not in view_index:
                raise FeatureViewNotFoundException(view_name)
            views_features[view_name].append(feat_name)
    elif isinstance(features, FeatureService):
        for feature_projection in features.features:
            projected_features = feature_projection.features
            views_features[feature_projection.name].extend(
                [f.name for f in projected_features]
            )

    result = []
    for view_name, feature_names in views_features.items():
        result.append((view_index[view_name], feature_names))
    return result


def _get_feature_refs_from_feature_services(
    feature_service: FeatureService,
) -> List[str]:
    feature_refs = []
    for projection in feature_service.features:
        feature_refs.extend(
            [f"{projection.name}:{f.name}" for f in projection.features]
        )
    return feature_refs


def _get_table_entity_keys(
    table: FeatureView, entity_keys: List[EntityKeyProto], join_key_map: Dict[str, str],
) -> List[EntityKeyProto]:
    table_join_keys = [join_key_map[entity_name] for entity_name in table.entities]
    required_entities = OrderedDict.fromkeys(sorted(table_join_keys))
    entity_key_protos = []
    for entity_key in entity_keys:
        required_entities_to_values = required_entities.copy()
        for i in range(len(entity_key.join_keys)):
            entity_name = entity_key.join_keys[i]
            entity_value = entity_key.entity_values[i]

            if entity_name in required_entities_to_values:
                if required_entities_to_values[entity_name] is not None:
                    raise ValueError(
                        f"Duplicate entity keys detected. Table {table.name} expects {table_join_keys}. The entity "
                        f"{entity_name} was provided at least twice"
                    )
                required_entities_to_values[entity_name] = entity_value

        entity_names = []
        entity_values = []
        for entity_name, entity_value in required_entities_to_values.items():
            if entity_value is None:
                raise ValueError(
                    f"Table {table.name} expects entity field {table_join_keys}. No entity value was found for "
                    f"{entity_name}"
                )
            entity_names.append(entity_name)
            entity_values.append(entity_value)
        entity_key_protos.append(
            EntityKeyProto(join_keys=entity_names, entity_values=entity_values)
        )
    return entity_key_protos


def _print_materialization_log(
    start_date, end_date, num_feature_views: int, online_store: str
):
    if start_date:
        print(
            f"Materializing {Style.BRIGHT + Fore.GREEN}{num_feature_views}{Style.RESET_ALL} feature views"
            f" from {Style.BRIGHT + Fore.GREEN}{start_date.replace(microsecond=0).astimezone()}{Style.RESET_ALL}"
            f" to {Style.BRIGHT + Fore.GREEN}{end_date.replace(microsecond=0).astimezone()}{Style.RESET_ALL}"
            f" into the {Style.BRIGHT + Fore.GREEN}{online_store}{Style.RESET_ALL} online store.\n"
        )
    else:
        print(
            f"Materializing {Style.BRIGHT + Fore.GREEN}{num_feature_views}{Style.RESET_ALL} feature views"
            f" to {Style.BRIGHT + Fore.GREEN}{end_date.replace(microsecond=0).astimezone()}{Style.RESET_ALL}"
            f" into the {Style.BRIGHT + Fore.GREEN}{online_store}{Style.RESET_ALL} online store.\n"
        )


def _validate_feature_views(feature_views: List[FeatureView]):
    """ Verify feature views have unique names"""
    name_to_fv_dict = {}
    for fv in feature_views:
        if fv.name in name_to_fv_dict:
            raise ValueError(
                f"More than one feature view with name {fv.name} found. Please ensure that all feature view names are unique. It may be necessary to ignore certain files in your feature repository by using a .feastignore file."
            )
        else:
            name_to_fv_dict[fv.name] = fv
