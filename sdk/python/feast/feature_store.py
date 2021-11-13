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
import copy
import itertools
import os
import warnings
from collections import Counter, OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union, cast

import pandas as pd
from colorama import Fore, Style
from tqdm import tqdm

from feast import feature_server, flags, flags_helper, utils
from feast.base_feature_view import BaseFeatureView
from feast.entity import Entity
from feast.errors import (
    EntityNotFoundException,
    ExperimentalFeatureNotEnabled,
    FeatureNameCollisionError,
    FeatureViewNotFoundException,
    RequestDataNotFoundInEntityDfException,
    RequestDataNotFoundInEntityRowsException,
)
from feast.feature_service import FeatureService
from feast.feature_table import FeatureTable
from feast.feature_view import (
    DUMMY_ENTITY_ID,
    DUMMY_ENTITY_NAME,
    DUMMY_ENTITY_VAL,
    FeatureView,
)
from feast.inference import (
    update_data_sources_with_inferred_event_timestamp_col,
    update_entities_with_inferred_types_from_feature_views,
)
from feast.infra.provider import Provider, RetrievalJob, get_provider
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.online_response import OnlineResponse, _infer_online_entity_rows
from feast.protos.feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequestV2,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.registry import Registry
from feast.repo_config import RepoConfig, load_repo_config
from feast.request_feature_view import RequestFeatureView
from feast.type_map import python_value_to_proto_value
from feast.usage import log_exceptions, log_exceptions_and_usage, set_usage_attribute
from feast.value_type import ValueType
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
    _provider: Provider

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
        self._registry = Registry(registry_config, repo_path=self.repo_path)
        self._provider = get_provider(self.config, self.repo_path)

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
        return self._provider

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
        self._registry = Registry(registry_config, repo_path=self.repo_path)
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
        return self._list_entities(allow_cache)

    def _list_entities(
        self, allow_cache: bool = False, hide_dummy_entity: bool = True
    ) -> List[Entity]:
        all_entities = self._registry.list_entities(
            self.project, allow_cache=allow_cache
        )
        return [
            entity
            for entity in all_entities
            if entity.name != DUMMY_ENTITY_NAME or not hide_dummy_entity
        ]

    @log_exceptions_and_usage
    def list_feature_services(self) -> List[FeatureService]:
        """
        Retrieves the list of feature services from the registry.

        Returns:
            A list of feature services.
        """
        return self._registry.list_feature_services(self.project)

    @log_exceptions_and_usage
    def list_feature_views(self, allow_cache: bool = False) -> List[FeatureView]:
        """
        Retrieves the list of feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.

        Returns:
            A list of feature views.
        """
        return self._list_feature_views(allow_cache)

    @log_exceptions_and_usage
    def list_request_feature_views(
        self, allow_cache: bool = False
    ) -> List[RequestFeatureView]:
        """
        Retrieves the list of feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.

        Returns:
            A list of feature views.
        """
        return self._registry.list_request_feature_views(
            self.project, allow_cache=allow_cache
        )

    def _list_feature_views(
        self, allow_cache: bool = False, hide_dummy_entity: bool = True,
    ) -> List[FeatureView]:
        feature_views = []
        for fv in self._registry.list_feature_views(
            self.project, allow_cache=allow_cache
        ):
            if hide_dummy_entity and fv.entities[0] == DUMMY_ENTITY_NAME:
                fv.entities = []
            feature_views.append(fv)
        return feature_views

    @log_exceptions_and_usage
    def list_on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        """
        Retrieves the list of on demand feature views from the registry.

        Returns:
            A list of on demand feature views.
        """
        return self._registry.list_on_demand_feature_views(self.project)

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
        return self._get_feature_view(name)

    def _get_feature_view(
        self, name: str, hide_dummy_entity: bool = True
    ) -> FeatureView:
        feature_view = self._registry.get_feature_view(name, self.project)
        if hide_dummy_entity and feature_view.entities[0] == DUMMY_ENTITY_NAME:
            feature_view.entities = []
        return feature_view

    @log_exceptions_and_usage
    def get_on_demand_feature_view(self, name: str) -> OnDemandFeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view.

        Returns:
            The specified feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._registry.get_on_demand_feature_view(name, self.project)

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

        _feature_refs = []
        if isinstance(_features, FeatureService):
            feature_service_from_registry = self.get_feature_service(_features.name)
            if feature_service_from_registry != _features:
                warnings.warn(
                    "The FeatureService object that has been passed in as an argument is"
                    "inconsistent with the version from Registry. Potentially a newer version"
                    "of the FeatureService has been applied to the registry."
                )
            for projection in feature_service_from_registry.feature_view_projections:
                _feature_refs.extend(
                    [
                        f"{projection.name_to_use()}:{f.name}"
                        for f in projection.features
                    ]
                )
        else:
            assert isinstance(_features, list)
            _feature_refs = _features
        return _feature_refs

    @log_exceptions_and_usage
    def apply(
        self,
        objects: Union[
            Entity,
            FeatureView,
            OnDemandFeatureView,
            RequestFeatureView,
            FeatureService,
            FeatureTable,
            List[
                Union[
                    FeatureView,
                    OnDemandFeatureView,
                    RequestFeatureView,
                    Entity,
                    FeatureService,
                    FeatureTable,
                ]
            ],
        ],
        objects_to_delete: List[
            Union[
                FeatureView,
                OnDemandFeatureView,
                RequestFeatureView,
                Entity,
                FeatureService,
                FeatureTable,
            ]
        ] = [],
        partial: bool = True,
        commit: bool = True,
    ):
        """Register objects to metadata store and update related infrastructure.

        The apply method registers one or more definitions (e.g., Entity, FeatureView) and registers or updates these
        objects in the Feast registry. Once the registry has been updated, the apply method will update related
        infrastructure (e.g., create tables in an online store) in order to reflect these new definitions. All
        operations are idempotent, meaning they can safely be rerun.

        Args:
            objects: A single object, or a list of objects that should be registered with the Feature Store.
            objects_to_delete: A list of objects to be deleted from the registry and removed from the
                provider's infrastructure. This deletion will only be performed if partial is set to False.
            partial: If True, apply will only handle the specified objects; if False, apply will also delete
                all the objects in objects_to_delete, and tear down any associated cloud resources.
            commit: whether to commit changes to the registry

        Raises:
            ValueError: The 'objects' parameter could not be parsed properly.

        Examples:
            Register an Entity and a FeatureView.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, ValueType, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> fs = FeatureStore(repo_path="feature_repo")
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

        # Separate all objects into entities, feature services, and different feature view types.
        entities_to_update = [ob for ob in objects if isinstance(ob, Entity)]
        views_to_update = [ob for ob in objects if isinstance(ob, FeatureView)]
        request_views_to_update = [
            ob for ob in objects if isinstance(ob, RequestFeatureView)
        ]
        odfvs_to_update = [ob for ob in objects if isinstance(ob, OnDemandFeatureView)]
        services_to_update = [ob for ob in objects if isinstance(ob, FeatureService)]
        tables_to_update = [ob for ob in objects if isinstance(ob, FeatureTable)]

        if len(entities_to_update) + len(views_to_update) + len(
            request_views_to_update
        ) + len(odfvs_to_update) + len(services_to_update) + len(
            tables_to_update
        ) != len(
            objects
        ):
            raise ValueError("Unknown object type provided as part of apply() call")

        # Validate all types of feature views.
        if (
            not flags_helper.enable_on_demand_feature_views(self.config)
            and len(odfvs_to_update) > 0
        ):
            raise ExperimentalFeatureNotEnabled(flags.FLAG_ON_DEMAND_TRANSFORM_NAME)

        set_usage_attribute("odfv", bool(odfvs_to_update))

        _validate_feature_views(
            [*views_to_update, *odfvs_to_update, *request_views_to_update]
        )

        # Make inferences
        update_entities_with_inferred_types_from_feature_views(
            entities_to_update, views_to_update, self.config
        )

        update_data_sources_with_inferred_event_timestamp_col(
            [view.batch_source for view in views_to_update], self.config
        )

        for view in views_to_update:
            view.infer_features_from_batch_source(self.config)

        for odfv in odfvs_to_update:
            odfv.infer_features()

        # Handle all entityless feature views by using DUMMY_ENTITY as a placeholder entity.
        DUMMY_ENTITY = Entity(
            name=DUMMY_ENTITY_NAME,
            join_key=DUMMY_ENTITY_ID,
            value_type=ValueType.INT32,
        )
        entities_to_update.append(DUMMY_ENTITY)

        # Add all objects to the registry and update the provider's infrastructure.
        for view in itertools.chain(
            views_to_update, odfvs_to_update, request_views_to_update
        ):
            self._registry.apply_feature_view(view, project=self.project, commit=False)
        for ent in entities_to_update:
            self._registry.apply_entity(ent, project=self.project, commit=False)
        for feature_service in services_to_update:
            self._registry.apply_feature_service(feature_service, project=self.project)
        for table in tables_to_update:
            self._registry.apply_feature_table(table, project=self.project)

        if not partial:
            # Delete all registry objects that should not exist.
            entities_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, Entity)
            ]
            views_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, FeatureView)
            ]
            request_views_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, RequestFeatureView)
            ]
            odfvs_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, OnDemandFeatureView)
            ]
            services_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, FeatureService)
            ]
            tables_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, FeatureTable)
            ]

            for entity in entities_to_delete:
                self._registry.delete_entity(
                    entity.name, project=self.project, commit=False
                )
            for view in views_to_delete:
                self._registry.delete_feature_view(
                    view.name, project=self.project, commit=False
                )
            for request_view in request_views_to_delete:
                self._registry.delete_feature_view(
                    request_view.name, project=self.project, commit=False
                )
            for odfv in odfvs_to_delete:
                self._registry.delete_feature_view(
                    odfv.name, project=self.project, commit=False
                )
            for service in services_to_delete:
                self._registry.delete_feature_service(
                    service.name, project=self.project, commit=False
                )
            for table in tables_to_delete:
                self._registry.delete_feature_table(
                    table.name, project=self.project, commit=False
                )

        self._get_provider().update_infra(
            project=self.project,
            tables_to_delete=views_to_delete + tables_to_delete if not partial else [],
            tables_to_keep=views_to_update + tables_to_update,
            entities_to_delete=entities_to_delete if not partial else [],
            entities_to_keep=entities_to_update,
            partial=partial,
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

            >>> from feast import FeatureStore, RepoConfig
            >>> import pandas as pd
            >>> fs = FeatureStore(repo_path="feature_repo")
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
        (
            all_feature_views,
            all_request_feature_views,
            all_on_demand_feature_views,
        ) = self._get_feature_views_to_use(features)

        # TODO(achal): _group_feature_refs returns the on demand feature views, but it's no passed into the provider.
        # This is a weird interface quirk - we should revisit the `get_historical_features` to
        # pass in the on demand feature views as well.
        fvs, odfvs, request_fvs, request_fv_refs = _group_feature_refs(
            _feature_refs,
            all_feature_views,
            all_request_feature_views,
            all_on_demand_feature_views,
        )
        feature_views = list(view for view, _ in fvs)
        on_demand_feature_views = list(view for view, _ in odfvs)
        request_feature_views = list(view for view, _ in request_fvs)

        set_usage_attribute("odfv", bool(on_demand_feature_views))
        set_usage_attribute("request_fv", bool(request_feature_views))

        # Check that the right request data is present in the entity_df
        if type(entity_df) == pd.DataFrame:
            entity_pd_df = cast(pd.DataFrame, entity_df)
            for fv in request_feature_views:
                for feature in fv.features:
                    if feature.name not in entity_pd_df.columns:
                        raise RequestDataNotFoundInEntityDfException(
                            feature_name=feature.name, feature_view_name=fv.name
                        )
            for odfv in on_demand_feature_views:
                odfv_request_data_schema = odfv.get_request_data_schema()
                for feature_name in odfv_request_data_schema.keys():
                    if feature_name not in entity_pd_df.columns:
                        raise RequestDataNotFoundInEntityDfException(
                            feature_name=feature_name, feature_view_name=odfv.name,
                        )

        _validate_feature_refs(_feature_refs, full_feature_names)
        # Drop refs that refer to RequestFeatureViews since they don't need to be fetched and
        # already exist in the entity_df
        _feature_refs = [ref for ref in _feature_refs if ref not in request_fv_refs]
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

            >>> from feast import FeatureStore, RepoConfig
            >>> from datetime import datetime, timedelta
            >>> fs = FeatureStore(repo_path="feature_repo")
            >>> fs.materialize_incremental(end_date=datetime.utcnow() - timedelta(minutes=5))
            Materializing...
            <BLANKLINE>
            ...
        """
        feature_views_to_materialize: List[FeatureView] = []
        if feature_views is None:
            feature_views_to_materialize = self._list_feature_views(
                hide_dummy_entity=False
            )
        else:
            for name in feature_views:
                feature_view = self._get_feature_view(name, hide_dummy_entity=False)
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

            >>> from feast import FeatureStore, RepoConfig
            >>> from datetime import datetime, timedelta
            >>> fs = FeatureStore(repo_path="feature_repo")
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

        feature_views_to_materialize: List[FeatureView] = []
        if feature_views is None:
            feature_views_to_materialize = self._list_feature_views(
                hide_dummy_entity=False
            )
        else:
            for name in feature_views:
                feature_view = self._get_feature_view(name, hide_dummy_entity=False)
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
    def write_to_online_store(
        self, feature_view_name: str, df: pd.DataFrame,
    ):
        """
        ingests data directly into the Online store
        """
        if not flags_helper.enable_direct_ingestion_to_online_store(self.config):
            raise ExperimentalFeatureNotEnabled(
                flags.FLAG_DIRECT_INGEST_TO_ONLINE_STORE
            )

        # TODO: restrict this to work with online StreamFeatureViews and validate the FeatureView type
        feature_view = self._registry.get_feature_view(feature_view_name, self.project)
        entities = []
        for entity_name in feature_view.entities:
            entities.append(self._registry.get_entity(entity_name, self.project))
        provider = self._get_provider()
        provider.ingest_df(feature_view, entities, df)

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

            >>> from feast import FeatureStore, RepoConfig
            >>> fs = FeatureStore(repo_path="feature_repo")
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
        (
            requested_feature_views,
            requested_request_feature_views,
            requested_on_demand_feature_views,
        ) = self._get_feature_views_to_use(
            features=features, allow_cache=True, hide_dummy_entity=False
        )

        _validate_feature_refs(_feature_refs, full_feature_names)
        (
            grouped_refs,
            grouped_odfv_refs,
            grouped_request_fv_refs,
            _,
        ) = _group_feature_refs(
            _feature_refs,
            requested_feature_views,
            requested_request_feature_views,
            requested_on_demand_feature_views,
        )
        set_usage_attribute("odfv", bool(grouped_odfv_refs))
        set_usage_attribute("request_fv", bool(grouped_request_fv_refs))

        feature_views = list(view for view, _ in grouped_refs)
        entityless_case = DUMMY_ENTITY_NAME in [
            entity_name
            for feature_view in feature_views
            for entity_name in feature_view.entities
        ]

        provider = self._get_provider()
        entities = self._list_entities(allow_cache=True, hide_dummy_entity=False)
        entity_name_to_join_key_map: Dict[str, str] = {}
        for entity in entities:
            entity_name_to_join_key_map[entity.name] = entity.join_key
        for feature_view in requested_feature_views:
            for entity_name in feature_view.entities:
                entity = self._registry.get_entity(
                    entity_name, self.project, allow_cache=True
                )
                # User directly uses join_key as the entity reference in the entity_rows for the
                # entity mapping case.
                entity_name = feature_view.projection.join_key_map.get(
                    entity.join_key, entity.name
                )
                join_key = feature_view.projection.join_key_map.get(
                    entity.join_key, entity.join_key
                )
                entity_name_to_join_key_map[entity_name] = join_key

        needed_request_data, needed_request_fv_features = self.get_needed_request_data(
            grouped_odfv_refs, grouped_request_fv_refs
        )

        join_key_rows = []
        request_data_features: Dict[str, List[Any]] = {}
        # Entity rows may be either entities or request data.
        for row in entity_rows:
            join_key_row = {}
            for entity_name, entity_value in row.items():
                # Found request data
                if (
                    entity_name in needed_request_data
                    or entity_name in needed_request_fv_features
                ):
                    if entity_name not in request_data_features:
                        request_data_features[entity_name] = []
                    request_data_features[entity_name].append(entity_value)
                    continue
                try:
                    join_key = entity_name_to_join_key_map[entity_name]
                except KeyError:
                    raise EntityNotFoundException(entity_name, self.project)
                join_key_row[join_key] = entity_value
                if entityless_case:
                    join_key_row[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
            if len(join_key_row) > 0:
                # May be empty if this entity row was request data
                join_key_rows.append(join_key_row)

        self.ensure_request_data_values_exist(
            needed_request_data, needed_request_fv_features, request_data_features
        )

        entity_row_proto_list = _infer_online_entity_rows(join_key_rows)

        union_of_entity_keys: List[EntityKeyProto] = []
        result_rows: List[GetOnlineFeaturesResponse.FieldValues] = []

        for entity_row_proto in entity_row_proto_list:
            # Create a list of entity keys to filter down for each feature view at lookup time.
            union_of_entity_keys.append(_entity_row_to_key(entity_row_proto))
            # Also create entity values to append to the result
            result_rows.append(_entity_row_to_field_values(entity_row_proto))

        for table, requested_features in grouped_refs:
            table_join_keys = [
                entity_name_to_join_key_map[entity_name]
                for entity_name in table.entities
            ]
            self._populate_result_rows_from_feature_view(
                table_join_keys,
                full_feature_names,
                provider,
                requested_features,
                result_rows,
                table,
                union_of_entity_keys,
            )

        requested_result_row_names = self._get_requested_result_fields(
            result_rows, needed_request_fv_features
        )
        self._populate_odfv_dependencies(
            entity_name_to_join_key_map,
            full_feature_names,
            grouped_odfv_refs,
            provider,
            request_data_features,
            result_rows,
            union_of_entity_keys,
        )

        initial_response = OnlineResponse(
            GetOnlineFeaturesResponse(field_values=result_rows)
        )
        return self._augment_response_with_on_demand_transforms(
            _feature_refs,
            requested_result_row_names,
            requested_on_demand_feature_views,
            full_feature_names,
            initial_response,
            result_rows,
        )

    def _get_requested_result_fields(
        self,
        result_rows: List[GetOnlineFeaturesResponse.FieldValues],
        needed_request_fv_features: Set[str],
    ):
        # Get requested feature values so we can drop odfv dependencies that aren't requested
        requested_result_row_names: Set[str] = set()
        for result_row in result_rows:
            for feature_name in result_row.fields.keys():
                requested_result_row_names.add(feature_name)
        # Request feature view values are also request data features that should be in the
        # final output
        requested_result_row_names.update(needed_request_fv_features)
        return requested_result_row_names

    def _populate_odfv_dependencies(
        self,
        entity_name_to_join_key_map: Dict[str, str],
        full_feature_names: bool,
        grouped_odfv_refs: List[Tuple[OnDemandFeatureView, List[str]]],
        provider: Provider,
        request_data_features: Dict[str, List[Any]],
        result_rows: List[GetOnlineFeaturesResponse.FieldValues],
        union_of_entity_keys: List[EntityKeyProto],
    ):
        # Add more feature values to the existing result rows for the request data features
        for feature_name, feature_values in request_data_features.items():
            for row_idx, feature_value in enumerate(feature_values):
                result_row = result_rows[row_idx]
                result_row.fields[feature_name].CopyFrom(
                    python_value_to_proto_value(feature_value)
                )
                result_row.statuses[
                    feature_name
                ] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

        # Add data if odfv requests specific feature views as dependencies
        if len(grouped_odfv_refs) > 0:
            for odfv, _ in grouped_odfv_refs:
                for fv in odfv.input_feature_views.values():
                    table_join_keys = [
                        entity_name_to_join_key_map[entity_name]
                        for entity_name in fv.entities
                    ]
                    self._populate_result_rows_from_feature_view(
                        table_join_keys,
                        full_feature_names,
                        provider,
                        [feature.name for feature in fv.features],
                        result_rows,
                        fv,
                        union_of_entity_keys,
                    )

    def get_needed_request_data(
        self,
        grouped_odfv_refs: List[Tuple[OnDemandFeatureView, List[str]]],
        grouped_request_fv_refs: List[Tuple[RequestFeatureView, List[str]]],
    ) -> Tuple[Set[str], Set[str]]:
        needed_request_data: Set[str] = set()
        needed_request_fv_features: Set[str] = set()
        for odfv, _ in grouped_odfv_refs:
            odfv_request_data_schema = odfv.get_request_data_schema()
            needed_request_data.update(odfv_request_data_schema.keys())
        for request_fv, _ in grouped_request_fv_refs:
            for feature in request_fv.features:
                needed_request_fv_features.add(feature.name)
        return needed_request_data, needed_request_fv_features

    def ensure_request_data_values_exist(
        self,
        needed_request_data: Set[str],
        needed_request_fv_features: Set[str],
        request_data_features: Dict[str, List[Any]],
    ):
        if len(needed_request_data) + len(needed_request_fv_features) != len(
            request_data_features.keys()
        ):
            missing_features = [
                x
                for x in itertools.chain(
                    needed_request_data, needed_request_fv_features
                )
                if x not in request_data_features
            ]
            raise RequestDataNotFoundInEntityRowsException(
                feature_names=missing_features
            )

    def _populate_result_rows_from_feature_view(
        self,
        table_join_keys: List[str],
        full_feature_names: bool,
        provider: Provider,
        requested_features: List[str],
        result_rows: List[GetOnlineFeaturesResponse.FieldValues],
        table: FeatureView,
        union_of_entity_keys: List[EntityKeyProto],
    ):
        entity_keys = _get_table_entity_keys(
            table, union_of_entity_keys, table_join_keys
        )
        read_rows = provider.online_read(
            config=self.config,
            table=table,
            entity_keys=entity_keys,
            requested_features=requested_features,
        )
        # Each row is a set of features for a given entity key
        for row_idx, read_row in enumerate(read_rows):
            row_ts, feature_data = read_row
            result_row = result_rows[row_idx]

            if feature_data is None:
                for feature_name in requested_features:
                    feature_ref = (
                        f"{table.projection.name_to_use()}__{feature_name}"
                        if full_feature_names
                        else feature_name
                    )
                    result_row.statuses[
                        feature_ref
                    ] = GetOnlineFeaturesResponse.FieldStatus.NOT_FOUND
            else:
                for feature_name in feature_data:
                    feature_ref = (
                        f"{table.projection.name_to_use()}__{feature_name}"
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

    def _augment_response_with_on_demand_transforms(
        self,
        feature_refs: List[str],
        requested_result_row_names: Set[str],
        odfvs: List[OnDemandFeatureView],
        full_feature_names: bool,
        initial_response: OnlineResponse,
        result_rows: List[GetOnlineFeaturesResponse.FieldValues],
    ) -> OnlineResponse:
        all_on_demand_feature_views = {view.name: view for view in odfvs}
        all_odfv_feature_names = all_on_demand_feature_views.keys()

        if len(all_on_demand_feature_views) == 0:
            return initial_response
        initial_response_df = initial_response.to_df()

        odfv_feature_refs = defaultdict(list)
        for feature_ref in feature_refs:
            view_name, feature_name = feature_ref.split(":")
            if view_name in all_odfv_feature_names:
                odfv_feature_refs[view_name].append(feature_name)

        # Apply on demand transformations
        odfv_result_names = set()
        for odfv_name, _feature_refs in odfv_feature_refs.items():
            odfv = all_on_demand_feature_views[odfv_name]
            transformed_features_df = odfv.get_transformed_features_df(
                initial_response_df
            )
            for row_idx in range(len(result_rows)):
                result_row = result_rows[row_idx]

                selected_subset = [
                    f for f in transformed_features_df.columns if f in _feature_refs
                ]

                for transformed_feature in selected_subset:
                    transformed_feature_name = (
                        f"{odfv.projection.name_to_use()}__{transformed_feature}"
                        if full_feature_names
                        else transformed_feature
                    )
                    odfv_result_names.add(transformed_feature_name)
                    proto_value = python_value_to_proto_value(
                        transformed_features_df[transformed_feature].values[row_idx]
                    )
                    result_row.fields[transformed_feature_name].CopyFrom(proto_value)
                    result_row.statuses[
                        transformed_feature_name
                    ] = GetOnlineFeaturesResponse.FieldStatus.PRESENT

        # Drop values that aren't needed
        unneeded_features = [
            val
            for val in result_rows[0].fields
            if val not in requested_result_row_names and val not in odfv_result_names
        ]
        for row_idx in range(len(result_rows)):
            result_row = result_rows[row_idx]
            for unneeded_feature in unneeded_features:
                result_row.fields.pop(unneeded_feature)
                result_row.statuses.pop(unneeded_feature)

        return OnlineResponse(GetOnlineFeaturesResponse(field_values=result_rows))

    def _get_feature_views_to_use(
        self,
        features: Optional[Union[List[str], FeatureService]],
        allow_cache=False,
        hide_dummy_entity: bool = True,
    ) -> Tuple[List[FeatureView], List[RequestFeatureView], List[OnDemandFeatureView]]:

        fvs = {
            fv.name: fv
            for fv in self._list_feature_views(allow_cache, hide_dummy_entity)
        }

        request_fvs = {
            fv.name: fv
            for fv in self._registry.list_request_feature_views(
                project=self.project, allow_cache=allow_cache
            )
        }

        od_fvs = {
            fv.name: fv
            for fv in self._registry.list_on_demand_feature_views(
                project=self.project, allow_cache=allow_cache
            )
        }

        if isinstance(features, FeatureService):
            fvs_to_use, request_fvs_to_use, od_fvs_to_use = [], [], []
            for fv_name, projection in [
                (projection.name, projection)
                for projection in features.feature_view_projections
            ]:
                if fv_name in fvs:
                    fvs_to_use.append(
                        fvs[fv_name].with_projection(copy.copy(projection))
                    )
                elif fv_name in request_fvs:
                    request_fvs_to_use.append(
                        request_fvs[fv_name].with_projection(copy.copy(projection))
                    )
                elif fv_name in od_fvs:
                    od_fvs_to_use.append(
                        od_fvs[fv_name].with_projection(copy.copy(projection))
                    )
                else:
                    raise ValueError(
                        f"The provided feature service {features.name} contains a reference to a feature view"
                        f"{fv_name} which doesn't exist. Please make sure that you have created the feature view"
                        f'{fv_name} and that you have registered it by running "apply".'
                    )
            views_to_use = (fvs_to_use, request_fvs_to_use, od_fvs_to_use)
        else:
            views_to_use = (
                [*fvs.values()],
                [*request_fvs.values()],
                [*od_fvs.values()],
            )

        return views_to_use

    @log_exceptions_and_usage
    def serve(self, port: int) -> None:
        """Start the feature consumption server locally on a given port."""
        if not flags_helper.enable_python_feature_server(self.config):
            raise ExperimentalFeatureNotEnabled(flags.FLAG_PYTHON_FEATURE_SERVER_NAME)

        feature_server.start_server(self, port)

    @log_exceptions_and_usage
    def get_feature_server_endpoint(self) -> Optional[str]:
        """Returns endpoint for the feature server, if it exists."""
        return self._provider.get_feature_server_endpoint()

    @log_exceptions_and_usage
    def serve_transformations(self, port: int) -> None:
        """Start the feature transformation server locally on a given port."""
        if not flags_helper.enable_python_feature_server(self.config):
            raise ExperimentalFeatureNotEnabled(flags.FLAG_PYTHON_FEATURE_SERVER_NAME)
        if not flags_helper.enable_on_demand_feature_views(self.config):
            raise ExperimentalFeatureNotEnabled(flags.FLAG_ON_DEMAND_TRANSFORM_NAME)

        from feast import transformation_server

        transformation_server.start_server(self, port)


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
        feature_names = [
            ref.split(":")[1] if ":" in ref else ref for ref in feature_refs
        ]
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
    features: List[str],
    all_feature_views: List[FeatureView],
    all_request_feature_views: List[RequestFeatureView],
    all_on_demand_feature_views: List[OnDemandFeatureView],
) -> Tuple[
    List[Tuple[FeatureView, List[str]]],
    List[Tuple[OnDemandFeatureView, List[str]]],
    List[Tuple[RequestFeatureView, List[str]]],
    Set[str],
]:
    """ Get list of feature views and corresponding feature names based on feature references"""

    # view name to view proto
    view_index = {view.projection.name_to_use(): view for view in all_feature_views}

    # request view name to proto
    request_view_index = {
        view.projection.name_to_use(): view for view in all_request_feature_views
    }

    # on demand view to on demand view proto
    on_demand_view_index = {
        view.projection.name_to_use(): view for view in all_on_demand_feature_views
    }

    # view name to feature names
    views_features = defaultdict(list)
    request_views_features = defaultdict(list)
    request_view_refs = set()

    # on demand view name to feature names
    on_demand_view_features = defaultdict(list)

    for ref in features:
        view_name, feat_name = ref.split(":")
        if view_name in view_index:
            views_features[view_name].append(feat_name)
        elif view_name in on_demand_view_index:
            on_demand_view_features[view_name].append(feat_name)
        elif view_name in request_view_index:
            request_views_features[view_name].append(feat_name)
            request_view_refs.add(ref)
        else:
            raise FeatureViewNotFoundException(view_name)

    fvs_result: List[Tuple[FeatureView, List[str]]] = []
    odfvs_result: List[Tuple[OnDemandFeatureView, List[str]]] = []
    request_fvs_result: List[Tuple[RequestFeatureView, List[str]]] = []

    for view_name, feature_names in views_features.items():
        fvs_result.append((view_index[view_name], feature_names))
    for view_name, feature_names in request_views_features.items():
        request_fvs_result.append((request_view_index[view_name], feature_names))
    for view_name, feature_names in on_demand_view_features.items():
        odfvs_result.append((on_demand_view_index[view_name], feature_names))
    return fvs_result, odfvs_result, request_fvs_result, request_view_refs


def _get_table_entity_keys(
    table: FeatureView, entity_keys: List[EntityKeyProto], table_join_keys: List[str]
) -> List[EntityKeyProto]:
    reverse_join_key_map = {
        alias: original for original, alias in table.projection.join_key_map.items()
    }
    required_entities = OrderedDict.fromkeys(sorted(table_join_keys))
    entity_key_protos = []
    for entity_key in entity_keys:
        required_entities_to_values = required_entities.copy()
        for i in range(len(entity_key.join_keys)):
            entity_name = reverse_join_key_map.get(
                entity_key.join_keys[i], entity_key.join_keys[i]
            )
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


def _validate_feature_views(feature_views: List[BaseFeatureView]):
    """ Verify feature views have case-insensitively unique names"""
    fv_names = set()
    for fv in feature_views:
        case_insensitive_fv_name = fv.name.lower()
        if case_insensitive_fv_name in fv_names:
            raise ValueError(
                f"More than one feature view with name {case_insensitive_fv_name} found. Please ensure that all feature view names are case-insensitively unique. It may be necessary to ignore certain files in your feature repository by using a .feastignore file."
            )
        else:
            fv_names.add(case_insensitive_fv_name)
