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
import itertools
import logging
import os
import warnings
from datetime import datetime, timedelta
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
    cast,
)

import pandas as pd
import pyarrow as pa
from colorama import Fore, Style
from google.protobuf.timestamp_pb2 import Timestamp
from tqdm import tqdm

from feast import feature_server, flags_helper, ui_server, utils
from feast.base_feature_view import BaseFeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.data_source import (
    DataSource,
    KafkaSource,
    KinesisSource,
    PushMode,
    PushSource,
)
from feast.diff.infra_diff import InfraDiff, diff_infra_protos
from feast.diff.registry_diff import RegistryDiff, apply_diff_to_registry, diff_between
from feast.dqm.errors import ValidationFailed
from feast.entity import Entity
from feast.errors import (
    DataFrameSerializationError,
    DataSourceRepeatNamesException,
    FeatureViewNotFoundException,
    PushSourceNotFoundException,
    RequestDataNotFoundInEntityDfException,
)
from feast.feast_object import FeastObject
from feast.feature_service import FeatureService
from feast.feature_view import (
    DUMMY_ENTITY,
    DUMMY_ENTITY_NAME,
    FeatureView,
)
from feast.inference import (
    update_data_sources_with_inferred_event_timestamp_col,
    update_feature_views_with_inferred_features_and_entities,
)
from feast.infra.infra_object import Infra
from feast.infra.provider import Provider, RetrievalJob, get_provider
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.registry.registry import Registry
from feast.infra.registry.sql import SqlRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.online_response import OnlineResponse
from feast.permissions.decision import DecisionStrategy
from feast.permissions.permission import Permission
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import RepeatedValue, Value
from feast.repo_config import RepoConfig, load_repo_config
from feast.repo_contents import RepoContents
from feast.saved_dataset import SavedDataset, SavedDatasetStorage, ValidationReference
from feast.stream_feature_view import StreamFeatureView
from feast.utils import _utc_now
from feast.version import get_version

warnings.simplefilter("once", DeprecationWarning)


class FeatureStore:
    """
    A FeatureStore object is used to define, create, and retrieve features.

    Attributes:
        config: The config for the feature store.
        repo_path: The path to the feature repo.
        _registry: The registry for the feature store.
        _provider: The provider for the feature store.
    """

    config: RepoConfig
    repo_path: Path
    _registry: BaseRegistry
    _provider: Provider

    def __init__(
        self,
        repo_path: Optional[str] = None,
        config: Optional[RepoConfig] = None,
        fs_yaml_file: Optional[Path] = None,
    ):
        """
        Creates a FeatureStore object.

        Args:
            repo_path (optional): Path to the feature repo. Defaults to the current working directory.
            config (optional): Configuration object used to configure the feature store.
            fs_yaml_file (optional): Path to the `feature_store.yaml` file used to configure the feature store.
                At most one of 'fs_yaml_file' and 'config' can be set.

        Raises:
            ValueError: If both or neither of repo_path and config are specified.
        """
        if fs_yaml_file is not None and config is not None:
            raise ValueError("You cannot specify both fs_yaml_file and config.")

        if repo_path:
            self.repo_path = Path(repo_path)
        else:
            self.repo_path = Path(os.getcwd())

        # If config is specified, or fs_yaml_file is specified, those take precedence over
        # the default feature_store.yaml location under repo_path.
        if config is not None:
            self.config = config
        elif fs_yaml_file is not None:
            self.config = load_repo_config(self.repo_path, fs_yaml_file)
        else:
            self.config = load_repo_config(
                self.repo_path, utils.get_default_yaml_file_path(self.repo_path)
            )

        registry_config = self.config.registry
        if registry_config.registry_type == "sql":
            self._registry = SqlRegistry(registry_config, self.config.project, None)
        elif registry_config.registry_type == "snowflake.registry":
            from feast.infra.registry.snowflake import SnowflakeRegistry

            self._registry = SnowflakeRegistry(
                registry_config, self.config.project, None
            )
        elif registry_config and registry_config.registry_type == "remote":
            from feast.infra.registry.remote import RemoteRegistry

            self._registry = RemoteRegistry(
                registry_config, self.config.project, None, self.config.auth_config
            )
        else:
            r = Registry(
                self.config.project,
                registry_config,
                repo_path=self.repo_path,
                auth_config=self.config.auth_config,
            )
            r._initialize_registry(self.config.project)
            self._registry = r

        self._provider = get_provider(self.config)

    def version(self) -> str:
        """Returns the version of the current Feast SDK/CLI."""
        return get_version()

    @property
    def registry(self) -> BaseRegistry:
        """Gets the registry of this feature store."""
        return self._registry

    @property
    def project(self) -> str:
        """Gets the project of this feature store."""
        return self.config.project

    def _get_provider(self) -> Provider:
        # TODO: Bake self.repo_path into self.config so that we dont only have one interface to paths
        return self._provider

    def refresh_registry(self):
        """Fetches and caches a copy of the feature registry in memory.

        Explicitly calling this method allows for direct control of the state of the registry cache. Every time this
        method is called the complete registry state will be retrieved from the remote registry store backend
        (e.g., GCS, S3), and the cache timer will be reset. If refresh_registry() is run before get_online_features()
        is called, then get_online_features() will use the cached registry instead of retrieving (and caching) the
        registry itself.

        Additionally, the TTL for the registry cache can be set to infinity (by setting it to 0), which means that
        refresh_registry() will become the only way to update the cached registry. If the TTL is set to a value
        greater than 0, then once the cache becomes stale (more time than the TTL has passed), a new cache will be
        downloaded synchronously, which may increase latencies if the triggering method is get_online_features().
        """
        registry_config = self.config.registry
        registry = Registry(
            self.config.project,
            registry_config,
            repo_path=self.repo_path,
            auth_config=self.config.auth_config,
        )
        registry.refresh(self.config.project)

        self._registry = registry

    def list_entities(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[Entity]:
        """
        Retrieves the list of entities from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of entities.
        """
        return self._list_entities(allow_cache, tags=tags)

    def _list_entities(
        self,
        allow_cache: bool = False,
        hide_dummy_entity: bool = True,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        all_entities = self._registry.list_entities(
            self.project, allow_cache=allow_cache, tags=tags
        )
        return [
            entity
            for entity in all_entities
            if entity.name != DUMMY_ENTITY_NAME or not hide_dummy_entity
        ]

    def list_feature_services(
        self, tags: Optional[dict[str, str]] = None
    ) -> List[FeatureService]:
        """
        Retrieves the list of feature services from the registry.

        Args:
            tags: Filter by tags.

        Returns:
            A list of feature services.
        """
        return self._registry.list_feature_services(self.project, tags=tags)

    def list_all_feature_views(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[Union[FeatureView, StreamFeatureView, OnDemandFeatureView]]:
        """
        Retrieves the list of feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.

        Returns:
            A list of feature views.
        """
        return self._list_all_feature_views(allow_cache, tags=tags)

    def list_feature_views(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[FeatureView]:
        """
        Retrieves the list of feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of feature views.
        """
        logging.warning(
            "list_feature_views will make breaking changes. Please use list_batch_feature_views instead. "
            "list_feature_views will behave like list_all_feature_views in the future."
        )
        return utils._list_feature_views(
            self._registry, self.project, allow_cache, tags=tags
        )

    def list_batch_feature_views(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[FeatureView]:
        """
        Retrieves the list of feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of feature views.
        """
        return self._list_batch_feature_views(allow_cache=allow_cache, tags=tags)

    def _list_all_feature_views(
        self,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Union[FeatureView, StreamFeatureView, OnDemandFeatureView]]:
        all_feature_views = (
            utils._list_feature_views(
                self._registry, self.project, allow_cache, tags=tags
            )
            + self._list_stream_feature_views(allow_cache, tags=tags)
            + self.list_on_demand_feature_views(allow_cache, tags=tags)
        )
        return all_feature_views

    def _list_feature_views(
        self,
        allow_cache: bool = False,
        hide_dummy_entity: bool = True,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        logging.warning(
            "_list_feature_views will make breaking changes. Please use _list_batch_feature_views instead. "
            "_list_feature_views will behave like _list_all_feature_views in the future."
        )
        feature_views = []
        for fv in self._registry.list_feature_views(
            self.project, allow_cache=allow_cache, tags=tags
        ):
            if (
                hide_dummy_entity
                and fv.entities
                and fv.entities[0] == DUMMY_ENTITY_NAME
            ):
                fv.entities = []
                fv.entity_columns = []
            feature_views.append(fv)
        return feature_views

    def _list_batch_feature_views(
        self,
        allow_cache: bool = False,
        hide_dummy_entity: bool = True,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        feature_views = []
        for fv in self._registry.list_feature_views(
            self.project, allow_cache=allow_cache, tags=tags
        ):
            if (
                hide_dummy_entity
                and fv.entities
                and fv.entities[0] == DUMMY_ENTITY_NAME
            ):
                fv.entities = []
                fv.entity_columns = []
            feature_views.append(fv)
        return feature_views

    def _list_stream_feature_views(
        self,
        allow_cache: bool = False,
        hide_dummy_entity: bool = True,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        stream_feature_views = []
        for sfv in self._registry.list_stream_feature_views(
            self.project, allow_cache=allow_cache, tags=tags
        ):
            if hide_dummy_entity and sfv.entities[0] == DUMMY_ENTITY_NAME:
                sfv.entities = []
                sfv.entity_columns = []
            stream_feature_views.append(sfv)
        return stream_feature_views

    def list_on_demand_feature_views(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[OnDemandFeatureView]:
        """
        Retrieves the list of on demand feature views from the registry.

        Args:
            allow_cache: Whether to allow returning entities from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of on demand feature views.
        """
        return self._registry.list_on_demand_feature_views(
            self.project, allow_cache=allow_cache, tags=tags
        )

    def list_stream_feature_views(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[StreamFeatureView]:
        """
        Retrieves the list of stream feature views from the registry.

        Returns:
            A list of stream feature views.
        """
        return self._list_stream_feature_views(allow_cache, tags=tags)

    def list_data_sources(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[DataSource]:
        """
        Retrieves the list of data sources from the registry.

        Args:
            allow_cache: Whether to allow returning data sources from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of data sources.
        """
        return self._registry.list_data_sources(
            self.project, allow_cache=allow_cache, tags=tags
        )

    def get_entity(self, name: str, allow_registry_cache: bool = False) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity.
            allow_registry_cache: (Optional) Whether to allow returning this entity from a cached registry

        Returns:
            The specified entity.

        Raises:
            EntityNotFoundException: The entity could not be found.
        """
        return self._registry.get_entity(
            name, self.project, allow_cache=allow_registry_cache
        )

    def get_feature_service(
        self, name: str, allow_cache: bool = False
    ) -> FeatureService:
        """
        Retrieves a feature service.

        Args:
            name: Name of feature service.
            allow_cache: Whether to allow returning feature services from a cached registry.

        Returns:
            The specified feature service.

        Raises:
            FeatureServiceNotFoundException: The feature service could not be found.
        """
        return self._registry.get_feature_service(name, self.project, allow_cache)

    def get_feature_view(
        self, name: str, allow_registry_cache: bool = False
    ) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view.
            allow_registry_cache: (Optional) Whether to allow returning this entity from a cached registry

        Returns:
            The specified feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._get_feature_view(name, allow_registry_cache=allow_registry_cache)

    def _get_feature_view(
        self,
        name: str,
        hide_dummy_entity: bool = True,
        allow_registry_cache: bool = False,
    ) -> FeatureView:
        feature_view = self._registry.get_feature_view(
            name, self.project, allow_cache=allow_registry_cache
        )
        if hide_dummy_entity and feature_view.entities[0] == DUMMY_ENTITY_NAME:
            feature_view.entities = []
        return feature_view

    def get_stream_feature_view(
        self, name: str, allow_registry_cache: bool = False
    ) -> StreamFeatureView:
        """
        Retrieves a stream feature view.

        Args:
            name: Name of stream feature view.
            allow_registry_cache: (Optional) Whether to allow returning this entity from a cached registry

        Returns:
            The specified stream feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._get_stream_feature_view(
            name, allow_registry_cache=allow_registry_cache
        )

    def _get_stream_feature_view(
        self,
        name: str,
        hide_dummy_entity: bool = True,
        allow_registry_cache: bool = False,
    ) -> StreamFeatureView:
        stream_feature_view = self._registry.get_stream_feature_view(
            name, self.project, allow_cache=allow_registry_cache
        )
        if hide_dummy_entity and stream_feature_view.entities[0] == DUMMY_ENTITY_NAME:
            stream_feature_view.entities = []
        return stream_feature_view

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

    def get_data_source(self, name: str) -> DataSource:
        """
        Retrieves the list of data sources from the registry.

        Args:
            name: Name of the data source.

        Returns:
            The specified data source.

        Raises:
            DataSourceObjectNotFoundException: The data source could not be found.
        """
        return self._registry.get_data_source(name, self.project)

    def delete_feature_view(self, name: str):
        """
        Deletes a feature view.

        Args:
            name: Name of feature view.

        Raises:
            FeatureViewNotFoundException: The feature view could not be found.
        """
        return self._registry.delete_feature_view(name, self.project)

    def delete_feature_service(self, name: str):
        """
        Deletes a feature service.

        Args:
            name: Name of feature service.

        Raises:
            FeatureServiceNotFoundException: The feature view could not be found.
        """
        return self._registry.delete_feature_service(name, self.project)

    def _should_use_plan(self):
        """Returns True if plan and _apply_diffs should be used, False otherwise."""
        # Currently only the local provider with sqlite online store supports plan and _apply_diffs.
        return self.config.provider == "local" and (
            self.config.online_store and self.config.online_store.type == "sqlite"
        )

    def _validate_all_feature_views(
        self,
        views_to_update: List[FeatureView],
        odfvs_to_update: List[OnDemandFeatureView],
        sfvs_to_update: List[StreamFeatureView],
    ):
        """Validates all feature views."""
        if len(odfvs_to_update) > 0 and not flags_helper.is_test():
            warnings.warn(
                "On demand feature view is an experimental feature. "
                "This API is stable, but the functionality does not scale well for offline retrieval",
                RuntimeWarning,
            )
        _validate_feature_views(
            [
                *views_to_update,
                *odfvs_to_update,
                *sfvs_to_update,
            ]
        )

    def _make_inferences(
        self,
        data_sources_to_update: List[DataSource],
        entities_to_update: List[Entity],
        views_to_update: List[FeatureView],
        odfvs_to_update: List[OnDemandFeatureView],
        sfvs_to_update: List[StreamFeatureView],
        feature_services_to_update: List[FeatureService],
    ):
        """Makes inferences for entities, feature views, odfvs, and feature services."""
        update_data_sources_with_inferred_event_timestamp_col(
            data_sources_to_update, self.config
        )

        update_data_sources_with_inferred_event_timestamp_col(
            [view.batch_source for view in views_to_update], self.config
        )

        update_data_sources_with_inferred_event_timestamp_col(
            [view.batch_source for view in sfvs_to_update], self.config
        )

        # New feature views may reference previously applied entities.
        entities = self._list_entities()
        update_feature_views_with_inferred_features_and_entities(
            views_to_update, entities + entities_to_update, self.config
        )
        update_feature_views_with_inferred_features_and_entities(
            sfvs_to_update, entities + entities_to_update, self.config
        )
        # TODO(kevjumba): Update schema inferrence
        for sfv in sfvs_to_update:
            if not sfv.schema:
                raise ValueError(
                    f"schema inference not yet supported for stream feature views. please define schema for stream feature view: {sfv.name}"
                )

        for odfv in odfvs_to_update:
            odfv.infer_features()

        fvs_to_update_map = {
            view.name: view for view in [*views_to_update, *sfvs_to_update]
        }
        for feature_service in feature_services_to_update:
            feature_service.infer_features(fvs_to_update=fvs_to_update_map)

    def _get_feature_views_to_materialize(
        self,
        feature_views: Optional[List[str]],
    ) -> List[FeatureView]:
        """
        Returns the list of feature views that should be materialized.

        If no feature views are specified, all feature views will be returned.

        Args:
            feature_views: List of names of feature views to materialize.

        Raises:
            FeatureViewNotFoundException: One of the specified feature views could not be found.
            ValueError: One of the specified feature views is not configured for materialization.
        """
        feature_views_to_materialize: List[FeatureView] = []

        if feature_views is None:
            feature_views_to_materialize = utils._list_feature_views(
                self._registry, self.project, hide_dummy_entity=False
            )
            feature_views_to_materialize = [
                fv for fv in feature_views_to_materialize if fv.online
            ]
            stream_feature_views_to_materialize = self._list_stream_feature_views(
                hide_dummy_entity=False
            )
            feature_views_to_materialize += [
                sfv for sfv in stream_feature_views_to_materialize if sfv.online
            ]
        else:
            for name in feature_views:
                try:
                    feature_view = self._get_feature_view(name, hide_dummy_entity=False)
                except FeatureViewNotFoundException:
                    feature_view = self._get_stream_feature_view(
                        name, hide_dummy_entity=False
                    )

                if not feature_view.online:
                    raise ValueError(
                        f"FeatureView {feature_view.name} is not configured to be served online."
                    )
                feature_views_to_materialize.append(feature_view)

        return feature_views_to_materialize

    def plan(
        self, desired_repo_contents: RepoContents
    ) -> Tuple[RegistryDiff, InfraDiff, Infra]:
        """Dry-run registering objects to metadata store.

        The plan method dry-runs registering one or more definitions (e.g., Entity, FeatureView), and produces
        a list of all the changes the that would be introduced in the feature repo. The changes computed by the plan
        command are for informational purposes, and are not actually applied to the registry.

        Args:
            desired_repo_contents: The desired repo state.

        Raises:
            ValueError: The 'objects' parameter could not be parsed properly.

        Examples:
            Generate a plan adding an Entity and a FeatureView.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, FileSource, RepoConfig
            >>> from feast.feature_store import RepoContents
            >>> from datetime import timedelta
            >>> fs = FeatureStore(repo_path="project/feature_repo")
            >>> driver = Entity(name="driver_id", description="driver id")
            >>> driver_hourly_stats = FileSource(
            ...     path="project/feature_repo/data/driver_stats.parquet",
            ...     timestamp_field="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=[driver],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     source=driver_hourly_stats,
            ... )
            >>> registry_diff, infra_diff, new_infra = fs.plan(RepoContents(
            ...     data_sources=[driver_hourly_stats],
            ...     feature_views=[driver_hourly_stats_view],
            ...     on_demand_feature_views=list(),
            ...     stream_feature_views=list(),
            ...     entities=[driver],
            ...     feature_services=list(),
            ...     permissions=list())) # register entity and feature view
        """
        # Validate and run inference on all the objects to be registered.
        self._validate_all_feature_views(
            desired_repo_contents.feature_views,
            desired_repo_contents.on_demand_feature_views,
            desired_repo_contents.stream_feature_views,
        )
        _validate_data_sources(desired_repo_contents.data_sources)
        self._make_inferences(
            desired_repo_contents.data_sources,
            desired_repo_contents.entities,
            desired_repo_contents.feature_views,
            desired_repo_contents.on_demand_feature_views,
            desired_repo_contents.stream_feature_views,
            desired_repo_contents.feature_services,
        )

        # Compute the desired difference between the current objects in the registry and
        # the desired repo state.
        registry_diff = diff_between(
            self._registry, self.project, desired_repo_contents
        )

        # Compute the desired difference between the current infra, as stored in the registry,
        # and the desired infra.
        self._registry.refresh(project=self.project)
        current_infra_proto = InfraProto()
        current_infra_proto.CopyFrom(self._registry.proto().infra)
        desired_registry_proto = desired_repo_contents.to_registry_proto()
        new_infra = self._provider.plan_infra(self.config, desired_registry_proto)
        new_infra_proto = new_infra.to_proto()
        infra_diff = diff_infra_protos(current_infra_proto, new_infra_proto)

        return registry_diff, infra_diff, new_infra

    def _apply_diffs(
        self, registry_diff: RegistryDiff, infra_diff: InfraDiff, new_infra: Infra
    ):
        """Applies the given diffs to the metadata store and infrastructure.

        Args:
            registry_diff: The diff between the current registry and the desired registry.
            infra_diff: The diff between the current infra and the desired infra.
            new_infra: The desired infra.
        """
        infra_diff.update()
        apply_diff_to_registry(
            self._registry, registry_diff, self.project, commit=False
        )

        self._registry.update_infra(new_infra, self.project, commit=True)

    def apply(
        self,
        objects: Union[
            DataSource,
            Entity,
            FeatureView,
            OnDemandFeatureView,
            BatchFeatureView,
            StreamFeatureView,
            FeatureService,
            ValidationReference,
            Permission,
            List[FeastObject],
        ],
        objects_to_delete: Optional[List[FeastObject]] = None,
        partial: bool = True,
    ):
        """Register objects to metadata store and update related infrastructure.

        The apply method registers one or more definitions (e.g., Entity, FeatureView) and registers or updates these
        objects in the Feast registry. Once the apply method has updated the infrastructure (e.g., create tables in
        an online store), it will commit the updated registry. All operations are idempotent, meaning they can safely
        be rerun.

        Args:
            objects: A single object, or a list of objects that should be registered with the Feature Store.
            objects_to_delete: A list of objects to be deleted from the registry and removed from the
                provider's infrastructure. This deletion will only be performed if partial is set to False.
            partial: If True, apply will only handle the specified objects; if False, apply will also delete
                all the objects in objects_to_delete, and tear down any associated cloud resources.

        Raises:
            ValueError: The 'objects' parameter could not be parsed properly.

        Examples:
            Register an Entity and a FeatureView.

            >>> from feast import FeatureStore, Entity, FeatureView, Feature, FileSource, RepoConfig
            >>> from datetime import timedelta
            >>> fs = FeatureStore(repo_path="project/feature_repo")
            >>> driver = Entity(name="driver_id", description="driver id")
            >>> driver_hourly_stats = FileSource(
            ...     path="project/feature_repo/data/driver_stats.parquet",
            ...     timestamp_field="event_timestamp",
            ...     created_timestamp_column="created",
            ... )
            >>> driver_hourly_stats_view = FeatureView(
            ...     name="driver_hourly_stats",
            ...     entities=[driver],
            ...     ttl=timedelta(seconds=86400 * 1),
            ...     source=driver_hourly_stats,
            ... )
            >>> fs.apply([driver_hourly_stats_view, driver]) # register entity and feature view
        """
        # TODO: Add locking
        if not isinstance(objects, Iterable):
            objects = [objects]
        assert isinstance(objects, list)

        if not objects_to_delete:
            objects_to_delete = []

        # Separate all objects into entities, feature services, and different feature view types.
        entities_to_update = [ob for ob in objects if isinstance(ob, Entity)]
        views_to_update = [
            ob
            for ob in objects
            if
            (
                # BFVs are not handled separately from FVs right now.
                (isinstance(ob, FeatureView) or isinstance(ob, BatchFeatureView))
                and not isinstance(ob, StreamFeatureView)
            )
        ]
        sfvs_to_update = [ob for ob in objects if isinstance(ob, StreamFeatureView)]
        odfvs_to_update = [ob for ob in objects if isinstance(ob, OnDemandFeatureView)]
        services_to_update = [ob for ob in objects if isinstance(ob, FeatureService)]
        data_sources_set_to_update = {
            ob for ob in objects if isinstance(ob, DataSource)
        }
        validation_references_to_update = [
            ob for ob in objects if isinstance(ob, ValidationReference)
        ]
        permissions_to_update = [ob for ob in objects if isinstance(ob, Permission)]

        batch_sources_to_add: List[DataSource] = []
        for data_source in data_sources_set_to_update:
            if (
                isinstance(data_source, PushSource)
                or isinstance(data_source, KafkaSource)
                or isinstance(data_source, KinesisSource)
            ):
                assert data_source.batch_source
                batch_sources_to_add.append(data_source.batch_source)
        for batch_source in batch_sources_to_add:
            data_sources_set_to_update.add(batch_source)

        for fv in itertools.chain(views_to_update, sfvs_to_update):
            data_sources_set_to_update.add(fv.batch_source)
            if fv.stream_source:
                data_sources_set_to_update.add(fv.stream_source)

        for odfv in odfvs_to_update:
            for v in odfv.source_request_sources.values():
                data_sources_set_to_update.add(v)

        data_sources_to_update = list(data_sources_set_to_update)

        # Handle all entityless feature views by using DUMMY_ENTITY as a placeholder entity.
        entities_to_update.append(DUMMY_ENTITY)

        # Validate all feature views and make inferences.
        self._validate_all_feature_views(
            views_to_update, odfvs_to_update, sfvs_to_update
        )
        self._make_inferences(
            data_sources_to_update,
            entities_to_update,
            views_to_update,
            odfvs_to_update,
            sfvs_to_update,
            services_to_update,
        )

        # Add all objects to the registry and update the provider's infrastructure.
        for ds in data_sources_to_update:
            self._registry.apply_data_source(ds, project=self.project, commit=False)
        for view in itertools.chain(views_to_update, odfvs_to_update, sfvs_to_update):
            self._registry.apply_feature_view(view, project=self.project, commit=False)
        for ent in entities_to_update:
            self._registry.apply_entity(ent, project=self.project, commit=False)
        for feature_service in services_to_update:
            self._registry.apply_feature_service(
                feature_service, project=self.project, commit=False
            )
        for validation_references in validation_references_to_update:
            self._registry.apply_validation_reference(
                validation_references, project=self.project, commit=False
            )
        for permission in permissions_to_update:
            self._registry.apply_permission(
                permission, project=self.project, commit=False
            )

        entities_to_delete = []
        views_to_delete = []
        sfvs_to_delete = []
        permissions_to_delete = []
        if not partial:
            # Delete all registry objects that should not exist.
            entities_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, Entity)
            ]
            views_to_delete = [
                ob
                for ob in objects_to_delete
                if (
                    (isinstance(ob, FeatureView) or isinstance(ob, BatchFeatureView))
                    and not isinstance(ob, StreamFeatureView)
                )
            ]
            odfvs_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, OnDemandFeatureView)
            ]
            sfvs_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, StreamFeatureView)
            ]
            services_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, FeatureService)
            ]
            data_sources_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, DataSource)
            ]
            validation_references_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, ValidationReference)
            ]
            permissions_to_delete = [
                ob for ob in objects_to_delete if isinstance(ob, Permission)
            ]

            for data_source in data_sources_to_delete:
                self._registry.delete_data_source(
                    data_source.name, project=self.project, commit=False
                )
            for entity in entities_to_delete:
                self._registry.delete_entity(
                    entity.name, project=self.project, commit=False
                )
            for view in views_to_delete:
                self._registry.delete_feature_view(
                    view.name, project=self.project, commit=False
                )
            for odfv in odfvs_to_delete:
                self._registry.delete_feature_view(
                    odfv.name, project=self.project, commit=False
                )
            for sfv in sfvs_to_delete:
                self._registry.delete_feature_view(
                    sfv.name, project=self.project, commit=False
                )
            for service in services_to_delete:
                self._registry.delete_feature_service(
                    service.name, project=self.project, commit=False
                )
            for validation_references in validation_references_to_delete:
                self._registry.delete_validation_reference(
                    validation_references.name, project=self.project, commit=False
                )
            for permission in permissions_to_delete:
                self._registry.delete_permission(
                    permission.name, project=self.project, commit=False
                )

        tables_to_delete: List[FeatureView] = (
            views_to_delete + sfvs_to_delete if not partial else []  # type: ignore
        )
        tables_to_keep: List[FeatureView] = views_to_update + sfvs_to_update  # type: ignore

        self._get_provider().update_infra(
            project=self.project,
            tables_to_delete=tables_to_delete,
            tables_to_keep=tables_to_keep,
            entities_to_delete=entities_to_delete if not partial else [],
            entities_to_keep=entities_to_update,
            partial=partial,
        )

        self._registry.commit()

    def teardown(self):
        """Tears down all local and cloud resources for the feature store."""
        tables: List[FeatureView] = []
        feature_views = self.list_feature_views()

        tables.extend(feature_views)

        entities = self.list_entities()

        self._get_provider().teardown_infra(self.project, tables, entities)
        self._registry.teardown()

    def get_historical_features(
        self,
        entity_df: Union[pd.DataFrame, str],
        features: Union[List[str], FeatureService],
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
            features: The list of features that should be retrieved from the offline store. These features can be
                specified either as a list of string feature references or as a feature service. String feature
                references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Returns:
            RetrievalJob which can be used to materialize the results.

        Raises:
            ValueError: Both or neither of features and feature_refs are specified.

        Examples:
            Retrieve historical features from a local offline store.

            >>> from feast import FeatureStore, RepoConfig
            >>> import pandas as pd
            >>> fs = FeatureStore(repo_path="project/feature_repo")
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
        _feature_refs = utils._get_features(self._registry, self.project, features)
        (
            all_feature_views,
            all_on_demand_feature_views,
        ) = utils._get_feature_views_to_use(self._registry, self.project, features)

        # TODO(achal): _group_feature_refs returns the on demand feature views, but it's not passed into the provider.
        # This is a weird interface quirk - we should revisit the `get_historical_features` to
        # pass in the on demand feature views as well.
        fvs, odfvs = utils._group_feature_refs(
            _feature_refs,
            all_feature_views,
            all_on_demand_feature_views,
        )
        feature_views = list(view for view, _ in fvs)
        on_demand_feature_views = list(view for view, _ in odfvs)

        # Check that the right request data is present in the entity_df
        if type(entity_df) == pd.DataFrame:
            if self.config.coerce_tz_aware:
                entity_df = utils.make_df_tzaware(cast(pd.DataFrame, entity_df))
            for odfv in on_demand_feature_views:
                odfv_request_data_schema = odfv.get_request_data_schema()
                for feature_name in odfv_request_data_schema.keys():
                    if feature_name not in entity_df.columns:
                        raise RequestDataNotFoundInEntityDfException(
                            feature_name=feature_name,
                            feature_view_name=odfv.name,
                        )

        utils._validate_feature_refs(_feature_refs, full_feature_names)
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

    def create_saved_dataset(
        self,
        from_: RetrievalJob,
        name: str,
        storage: SavedDatasetStorage,
        tags: Optional[Dict[str, str]] = None,
        feature_service: Optional[FeatureService] = None,
        allow_overwrite: bool = False,
    ) -> SavedDataset:
        """
        Execute provided retrieval job and persist its outcome in given storage.
        Storage type (eg, BigQuery or Redshift) must be the same as globally configured offline store.
        After data successfully persisted saved dataset object with dataset metadata is committed to the registry.
        Name for the saved dataset should be unique within project, since it's possible to overwrite previously stored dataset
        with the same name.

        Args:
            from_: The retrieval job whose result should be persisted.
            name: The name of the saved dataset.
            storage: The saved dataset storage object indicating where the result should be persisted.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            feature_service (optional): The feature service that should be associated with this saved dataset.
            allow_overwrite (optional): If True, the persisted result can overwrite an existing table or file.

        Returns:
            SavedDataset object with attached RetrievalJob

        Raises:
            ValueError if given retrieval job doesn't have metadata
        """
        if not flags_helper.is_test():
            warnings.warn(
                "Saving dataset is an experimental feature. "
                "This API is unstable and it could and most probably will be changed in the future. "
                "We do not guarantee that future changes will maintain backward compatibility.",
                RuntimeWarning,
            )

        if not from_.metadata:
            raise ValueError(
                f"The RetrievalJob {type(from_)} must implement the metadata property."
            )

        dataset = SavedDataset(
            name=name,
            features=from_.metadata.features,
            join_keys=from_.metadata.keys,
            full_feature_names=from_.full_feature_names,
            storage=storage,
            tags=tags,
            feature_service_name=feature_service.name if feature_service else None,
        )

        dataset.min_event_timestamp = from_.metadata.min_event_timestamp
        dataset.max_event_timestamp = from_.metadata.max_event_timestamp

        from_.persist(storage=storage, allow_overwrite=allow_overwrite)

        dataset = dataset.with_retrieval_job(
            self._get_provider().retrieve_saved_dataset(
                config=self.config, dataset=dataset
            )
        )

        self._registry.apply_saved_dataset(dataset, self.project, commit=True)
        return dataset

    def get_saved_dataset(self, name: str) -> SavedDataset:
        """
        Find a saved dataset in the registry by provided name and
        create a retrieval job to pull whole dataset from storage (offline store).

        If dataset couldn't be found by provided name SavedDatasetNotFound exception will be raised.

        Data will be retrieved from globally configured offline store.

        Returns:
            SavedDataset with RetrievalJob attached

        Raises:
            SavedDatasetNotFound
        """
        if not flags_helper.is_test():
            warnings.warn(
                "Retrieving datasets is an experimental feature. "
                "This API is unstable and it could and most probably will be changed in the future. "
                "We do not guarantee that future changes will maintain backward compatibility.",
                RuntimeWarning,
            )

        dataset = self._registry.get_saved_dataset(name, self.project)
        provider = self._get_provider()

        retrieval_job = provider.retrieve_saved_dataset(
            config=self.config, dataset=dataset
        )
        return dataset.with_retrieval_job(retrieval_job)

    def materialize_incremental(
        self,
        end_date: datetime,
        feature_views: Optional[List[str]] = None,
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
            >>> fs = FeatureStore(repo_path="project/feature_repo")
            >>> fs.materialize_incremental(end_date=_utc_now() - timedelta(minutes=5))
            Materializing...
            <BLANKLINE>
            ...
        """
        feature_views_to_materialize = self._get_feature_views_to_materialize(
            feature_views
        )
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
                elif feature_view.ttl.total_seconds() > 0:
                    start_date = _utc_now() - feature_view.ttl
                else:
                    # TODO(felixwang9817): Find the earliest timestamp for this specific feature
                    # view from the offline store, and set the start date to that timestamp.
                    print(
                        f"Since the ttl is 0 for feature view {Style.BRIGHT + Fore.GREEN}{feature_view.name}{Style.RESET_ALL}, "
                        "the start date will be set to 1 year before the current time."
                    )
                    start_date = _utc_now() - timedelta(weeks=52)
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
                feature_view,
                self.project,
                start_date,
                end_date,
            )

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
            >>> fs = FeatureStore(repo_path="project/feature_repo")
            >>> fs.materialize(
            ...     start_date=_utc_now() - timedelta(hours=3), end_date=_utc_now() - timedelta(minutes=10)
            ... )
            Materializing...
            <BLANKLINE>
            ...
        """
        if utils.make_tzaware(start_date) > utils.make_tzaware(end_date):
            raise ValueError(
                f"The given start_date {start_date} is greater than the given end_date {end_date}."
            )

        feature_views_to_materialize = self._get_feature_views_to_materialize(
            feature_views
        )
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
                feature_view,
                self.project,
                start_date,
                end_date,
            )

    def push(
        self,
        push_source_name: str,
        df: pd.DataFrame,
        allow_registry_cache: bool = True,
        to: PushMode = PushMode.ONLINE,
    ):
        """
        Push features to a push source. This updates all the feature views that have the push source as stream source.

        Args:
            push_source_name: The name of the push source we want to push data to.
            df: The data being pushed.
            allow_registry_cache: Whether to allow cached versions of the registry.
            to: Whether to push to online or offline store. Defaults to online store only.
        """
        from feast.data_source import PushSource

        all_fvs = self.list_feature_views(allow_cache=allow_registry_cache)
        all_fvs += self.list_stream_feature_views(allow_cache=allow_registry_cache)

        fvs_with_push_sources = {
            fv
            for fv in all_fvs
            if (
                fv.stream_source is not None
                and isinstance(fv.stream_source, PushSource)
                and fv.stream_source.name == push_source_name
            )
        }

        if not fvs_with_push_sources:
            raise PushSourceNotFoundException(push_source_name)

        for fv in fvs_with_push_sources:
            if to == PushMode.ONLINE or to == PushMode.ONLINE_AND_OFFLINE:
                self.write_to_online_store(
                    fv.name, df, allow_registry_cache=allow_registry_cache
                )
            if to == PushMode.OFFLINE or to == PushMode.ONLINE_AND_OFFLINE:
                self.write_to_offline_store(
                    fv.name, df, allow_registry_cache=allow_registry_cache
                )

    def write_to_online_store(
        self,
        feature_view_name: str,
        df: Optional[pd.DataFrame] = None,
        inputs: Optional[Union[Dict[str, List[Any]], pd.DataFrame]] = None,
        allow_registry_cache: bool = True,
    ):
        """
        Persists a dataframe to the online store.

        Args:
            feature_view_name: The feature view to which the dataframe corresponds.
            df: The dataframe to be persisted.
            inputs: Optional the dictionary object to be written
            allow_registry_cache (optional): Whether to allow retrieving feature views from a cached registry.
        """
        # TODO: restrict this to work with online StreamFeatureViews and validate the FeatureView type
        try:
            feature_view: FeatureView = self.get_stream_feature_view(
                feature_view_name, allow_registry_cache=allow_registry_cache
            )
        except FeatureViewNotFoundException:
            feature_view = self.get_feature_view(
                feature_view_name, allow_registry_cache=allow_registry_cache
            )
        if df is not None and inputs is not None:
            raise ValueError("Both df and inputs cannot be provided at the same time.")
        if df is None and inputs is not None:
            if isinstance(inputs, dict):
                try:
                    df = pd.DataFrame(inputs)
                except Exception as _:
                    raise DataFrameSerializationError(inputs)
            elif isinstance(inputs, pd.DataFrame):
                pass
            else:
                raise ValueError("inputs must be a dictionary or a pandas DataFrame.")
        provider = self._get_provider()
        provider.ingest_df(feature_view, df)

    def write_to_offline_store(
        self,
        feature_view_name: str,
        df: pd.DataFrame,
        allow_registry_cache: bool = True,
        reorder_columns: bool = True,
    ):
        """
        Persists the dataframe directly into the batch data source for the given feature view.

        Fails if the dataframe columns do not match the columns of the batch data source. Optionally
        reorders the columns of the dataframe to match.
        """
        # TODO: restrict this to work with online StreamFeatureViews and validate the FeatureView type
        try:
            feature_view: FeatureView = self.get_stream_feature_view(
                feature_view_name, allow_registry_cache=allow_registry_cache
            )
        except FeatureViewNotFoundException:
            feature_view = self.get_feature_view(
                feature_view_name, allow_registry_cache=allow_registry_cache
            )

        # Get columns of the batch source and the input dataframe.
        column_names_and_types = (
            feature_view.batch_source.get_table_column_names_and_types(self.config)
        )
        source_columns = [column for column, _ in column_names_and_types]
        input_columns = df.columns.values.tolist()

        if set(input_columns) != set(source_columns):
            raise ValueError(
                f"The input dataframe has columns {set(input_columns)} but the batch source has columns {set(source_columns)}."
            )

        if reorder_columns:
            df = df.reindex(columns=source_columns)

        table = pa.Table.from_pandas(df)
        provider = self._get_provider()
        provider.ingest_df_to_offline_store(feature_view, table)

    def get_online_features(
        self,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[Value], RepeatedValue]],
        ],
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
            features: The list of features that should be retrieved from the online store. These features can be
                specified either as a list of string feature references or as a feature service. String feature
                references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
            entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Returns:
            OnlineResponse containing the feature data in records.

        Raises:
            Exception: No entity with the specified name exists.

        Examples:
            Retrieve online features from an online store.

            >>> from feast import FeatureStore, RepoConfig
            >>> fs = FeatureStore(repo_path="project/feature_repo")
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
        provider = self._get_provider()

        return provider.get_online_features(
            config=self.config,
            features=features,
            entity_rows=entity_rows,
            registry=self._registry,
            project=self.project,
            full_feature_names=full_feature_names,
        )

    async def get_online_features_async(
        self,
        features: Union[List[str], FeatureService],
        entity_rows: Union[
            List[Dict[str, Any]],
            Mapping[str, Union[Sequence[Any], Sequence[Value], RepeatedValue]],
        ],
        full_feature_names: bool = False,
    ) -> OnlineResponse:
        """
        [Alpha] Retrieves the latest online feature data asynchronously.

        Note: This method will download the full feature registry the first time it is run. If you are using a
        remote registry like GCS or S3 then that may take a few seconds. The registry remains cached up to a TTL
        duration (which can be set to infinity). If the cached registry is stale (more time than the TTL has
        passed), then a new registry will be downloaded synchronously by this method. This download may
        introduce latency to online feature retrieval. In order to avoid synchronous downloads, please call
        refresh_registry() prior to the TTL being reached. Remember it is possible to set the cache TTL to
        infinity (cache forever).

        Args:
            features: The list of features that should be retrieved from the online store. These features can be
                specified either as a list of string feature references or as a feature service. String feature
                references must have format "feature_view:feature", e.g. "customer_fv:daily_transactions".
            entity_rows: A list of dictionaries where each key-value is an entity-name, entity-value pair.
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Returns:
            OnlineResponse containing the feature data in records.

        Raises:
            Exception: No entity with the specified name exists.
        """
        provider = self._get_provider()

        return await provider.get_online_features_async(
            config=self.config,
            features=features,
            entity_rows=entity_rows,
            registry=self._registry,
            project=self.project,
            full_feature_names=full_feature_names,
        )

    def retrieve_online_documents(
        self,
        feature: str,
        query: Union[str, List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> OnlineResponse:
        """
        Retrieves the top k closest document features. Note, embeddings are a subset of features.

        Args:
            feature: The list of document features that should be retrieved from the online document store. These features can be
                specified either as a list of string document feature references or as a feature service. String feature
                references must have format "feature_view:feature", e.g, "document_fv:document_embeddings".
            query: The query to retrieve the closest document features for.
            top_k: The number of closest document features to retrieve.
            distance_metric: The distance metric to use for retrieval.
        """
        if isinstance(query, str):
            raise ValueError(
                "Using embedding functionality is not supported for document retrieval. Please embed the query before calling retrieve_online_documents."
            )
        (
            available_feature_views,
            _,
        ) = utils._get_feature_views_to_use(
            registry=self._registry,
            project=self.project,
            features=[feature],
            allow_cache=True,
            hide_dummy_entity=False,
        )
        requested_feature_view_name = (
            feature.split(":")[0] if isinstance(feature, str) else feature
        )
        for feature_view in available_feature_views:
            if feature_view.name == requested_feature_view_name:
                requested_feature_view = feature_view
        if not requested_feature_view:
            raise ValueError(
                f"Feature view {requested_feature_view} not found in the registry."
            )
        requested_feature = (
            feature.split(":")[1] if isinstance(feature, str) else feature
        )
        provider = self._get_provider()
        document_features = self._retrieve_from_online_store(
            provider,
            requested_feature_view,
            requested_feature,
            query,
            top_k,
            distance_metric,
        )

        # TODO Refactor to better way of populating result
        # TODO populate entity in the response after returning entity in document_features is supported
        # TODO currently not return the vector value since it is same as feature value, if embedding is supported,
        # the feature value can be raw text before embedded
        document_feature_vals = [feature[2] for feature in document_features]
        document_feature_distance_vals = [feature[4] for feature in document_features]
        online_features_response = GetOnlineFeaturesResponse(results=[])
        utils._populate_result_rows_from_columnar(
            online_features_response=online_features_response,
            data={requested_feature: document_feature_vals},
        )
        utils._populate_result_rows_from_columnar(
            online_features_response=online_features_response,
            data={"distance": document_feature_distance_vals},
        )
        return OnlineResponse(online_features_response)

    def _retrieve_from_online_store(
        self,
        provider: Provider,
        table: FeatureView,
        requested_feature: str,
        query: List[float],
        top_k: int,
        distance_metric: Optional[str],
    ) -> List[Tuple[Timestamp, "FieldStatus.ValueType", Value, Value, Value]]:
        """
        Search and return document features from the online document store.
        """
        documents = provider.retrieve_online_documents(
            config=self.config,
            table=table,
            requested_feature=requested_feature,
            query=query,
            top_k=top_k,
            distance_metric=distance_metric,
        )

        read_row_protos = []
        row_ts_proto = Timestamp()

        for row_ts, feature_val, vector_value, distance_val in documents:
            # Reset timestamp to default or update if row_ts is not None
            if row_ts is not None:
                row_ts_proto.FromDatetime(row_ts)

            if feature_val is None or vector_value is None or distance_val is None:
                feature_val = Value()
                vector_value = Value()
                distance_val = Value()
                status = FieldStatus.NOT_FOUND
            else:
                status = FieldStatus.PRESENT

            read_row_protos.append(
                (row_ts_proto, status, feature_val, vector_value, distance_val)
            )
        return read_row_protos

    def serve(
        self,
        host: str,
        port: int,
        type_: str = "http",
        no_access_log: bool = True,
        workers: int = 1,
        metrics: bool = False,
        keep_alive_timeout: int = 30,
        registry_ttl_sec: int = 2,
    ) -> None:
        """Start the feature consumption server locally on a given port."""
        type_ = type_.lower()
        if type_ != "http":
            raise ValueError(
                f"Python server only supports 'http'. Got '{type_}' instead."
            )
        # Start the python server
        feature_server.start_server(
            self,
            host=host,
            port=port,
            no_access_log=no_access_log,
            workers=workers,
            metrics=metrics,
            keep_alive_timeout=keep_alive_timeout,
            registry_ttl_sec=registry_ttl_sec,
        )

    def get_feature_server_endpoint(self) -> Optional[str]:
        """Returns endpoint for the feature server, if it exists."""
        return self._provider.get_feature_server_endpoint()

    def serve_ui(
        self,
        host: str,
        port: int,
        get_registry_dump: Callable,
        registry_ttl_sec: int,
        root_path: str = "",
    ) -> None:
        """Start the UI server locally"""
        if flags_helper.is_test():
            warnings.warn(
                "The Feast UI is an experimental feature. "
                "We do not guarantee that future changes will maintain backward compatibility.",
                RuntimeWarning,
            )
        ui_server.start_server(
            self,
            host=host,
            port=port,
            get_registry_dump=get_registry_dump,
            project_id=self.config.project,
            registry_ttl_sec=registry_ttl_sec,
            root_path=root_path,
        )

    def serve_registry(self, port: int) -> None:
        """Start registry server locally on a given port."""
        from feast import registry_server

        registry_server.start_server(self, port)

    def serve_offline(self, host: str, port: int) -> None:
        """Start offline server locally on a given port."""
        from feast import offline_server

        offline_server.start_server(self, host, port)

    def serve_transformations(self, port: int) -> None:
        """Start the feature transformation server locally on a given port."""
        warnings.warn(
            "On demand feature view is an experimental feature. "
            "This API is stable, but the functionality does not scale well for offline retrieval",
            RuntimeWarning,
        )

        from feast import transformation_server

        transformation_server.start_server(self, port)

    def write_logged_features(
        self, logs: Union[pa.Table, Path], source: FeatureService
    ):
        """
        Write logs produced by a source (currently only feature service is supported as a source)
        to an offline store.

        Args:
            logs: Arrow Table or path to parquet dataset directory on disk
            source: Object that produces logs
        """
        if not isinstance(source, FeatureService):
            raise ValueError("Only feature service is currently supported as a source")

        assert (
            source.logging_config is not None
        ), "Feature service must be configured with logging config in order to use this functionality"

        assert isinstance(logs, (pa.Table, Path))

        self._get_provider().write_feature_service_logs(
            feature_service=source,
            logs=logs,
            config=self.config,
            registry=self._registry,
        )

    def validate_logged_features(
        self,
        source: FeatureService,
        start: datetime,
        end: datetime,
        reference: ValidationReference,
        throw_exception: bool = True,
        cache_profile: bool = True,
    ) -> Optional[ValidationFailed]:
        """
        Load logged features from an offline store and validate them against provided validation reference.

        Args:
            source: Logs source object (currently only feature services are supported)
            start: lower bound for loading logged features
            end:  upper bound for loading logged features
            reference: validation reference
            throw_exception: throw exception or return it as a result
            cache_profile: store cached profile in Feast registry

        Returns:
            Throw or return (depends on parameter) ValidationFailed exception if validation was not successful
            or None if successful.

        """
        if not flags_helper.is_test():
            warnings.warn(
                "Logged features validation is an experimental feature. "
                "This API is unstable and it could and most probably will be changed in the future. "
                "We do not guarantee that future changes will maintain backward compatibility.",
                RuntimeWarning,
            )

        if not isinstance(source, FeatureService):
            raise ValueError("Only feature service is currently supported as a source")

        j = self._get_provider().retrieve_feature_service_logs(
            feature_service=source,
            start_date=start,
            end_date=end,
            config=self.config,
            registry=self.registry,
        )

        # read and run validation
        try:
            t = j.to_arrow(validation_reference=reference)
        except ValidationFailed as exc:
            if throw_exception:
                raise

            return exc
        else:
            print(f"{t.shape[0]} rows were validated.")

        if cache_profile:
            self.apply(reference)

        return None

    def get_validation_reference(
        self, name: str, allow_cache: bool = False
    ) -> ValidationReference:
        """
        Retrieves a validation reference.

        Raises:
            ValidationReferenceNotFoundException: The validation reference could not be found.
        """
        ref = self._registry.get_validation_reference(
            name, project=self.project, allow_cache=allow_cache
        )
        ref._dataset = self.get_saved_dataset(ref.dataset_name)
        return ref

    def list_validation_references(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[ValidationReference]:
        """
        Retrieves the list of validation references from the registry.

        Args:
            allow_cache: Whether to allow returning validation references from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of validation references.
        """
        return self._registry.list_validation_references(
            self.project, allow_cache=allow_cache, tags=tags
        )

    def list_permissions(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[Permission]:
        """
        Retrieves the list of permissions from the registry.

        Args:
            allow_cache: Whether to allow returning permissions from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of permissions.
        """
        return self._registry.list_permissions(
            self.project, allow_cache=allow_cache, tags=tags
        )

    def get_permission(self, name: str) -> Permission:
        """
        Retrieves a permission from the registry.

        Args:
            name: Name of the permission.

        Returns:
            The specified permission.

        Raises:
            PermissionObjectNotFoundException: The permission could not be found.
        """
        return self._registry.get_permission(name, self.project)

    def list_saved_datasets(
        self, allow_cache: bool = False, tags: Optional[dict[str, str]] = None
    ) -> List[SavedDataset]:
        """
        Retrieves the list of saved datasets from the registry.

        Args:
            allow_cache: Whether to allow returning saved datasets from a cached registry.
            tags: Filter by tags.

        Returns:
            A list of saved datasets.
        """
        return self._registry.list_saved_datasets(
            self.project, allow_cache=allow_cache, tags=tags
        )

    def set_decision_strategy(self, decision_strategy: DecisionStrategy):
        """
        Set the project decision strategy.

        Args:
            decision_strategy: The decision strategy to set to.
        """
        return self._registry.set_decision_strategy(self.project, decision_strategy)

    def get_decision_strategy(self, project: str) -> DecisionStrategy:
        """
        Get the project decision strategy.

        Args:
            project: The project to get the decision strategy for.

        Returns:
            The decision strategy or None.
        """
        return self._registry.get_decision_strategy(project)


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
    """Verify feature views have case-insensitively unique names"""
    fv_names = set()
    for fv in feature_views:
        case_insensitive_fv_name = fv.name.lower()
        if case_insensitive_fv_name in fv_names:
            raise ValueError(
                f"More than one feature view with name {case_insensitive_fv_name} found. "
                f"Please ensure that all feature view names are case-insensitively unique. "
                f"It may be necessary to ignore certain files in your feature repository by using a .feastignore file."
            )
        else:
            fv_names.add(case_insensitive_fv_name)


def _validate_data_sources(data_sources: List[DataSource]):
    """Verify data sources have case-insensitively unique names."""
    ds_names = set()
    for ds in data_sources:
        case_insensitive_ds_name = ds.name.lower()
        if case_insensitive_ds_name in ds_names:
            raise DataSourceRepeatNamesException(case_insensitive_ds_name)
        else:
            ds_names.add(case_insensitive_ds_name)
