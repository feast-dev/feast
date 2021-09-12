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

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set
from urllib.parse import urlparse

from feast import importer
from feast.entity import Entity
from feast.errors import (
    ConflictingFeatureViewNames,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureTableNotFoundException,
    FeatureViewNotFoundException,
    OnDemandFeatureViewNotFoundException,
)
from feast.feature_service import FeatureService
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.repo_config import RegistryConfig

REGISTRY_SCHEMA_VERSION = "1"


REGISTRY_STORE_CLASS_FOR_TYPE = {
    "GCSRegistryStore": "feast.infra.gcp.GCSRegistryStore",
    "S3RegistryStore": "feast.infra.aws.S3RegistryStore",
    "LocalRegistryStore": "feast.infra.local.LocalRegistryStore",
}

REGISTRY_STORE_CLASS_FOR_SCHEME = {
    "gs": "GCSRegistryStore",
    "s3": "S3RegistryStore",
    "file": "LocalRegistryStore",
    "": "LocalRegistryStore",
}


def get_registry_store_class_from_type(registry_store_type: str):
    if not registry_store_type.endswith("RegistryStore"):
        raise Exception('Registry store class name should end with "RegistryStore"')
    if registry_store_type in REGISTRY_STORE_CLASS_FOR_TYPE:
        registry_store_type = REGISTRY_STORE_CLASS_FOR_TYPE[registry_store_type]
    module_name, registry_store_class_name = registry_store_type.rsplit(".", 1)

    return importer.get_class_from_type(
        module_name, registry_store_class_name, "RegistryStore"
    )


def get_registry_store_class_from_scheme(registry_path: str):
    uri = urlparse(registry_path)
    if uri.scheme not in REGISTRY_STORE_CLASS_FOR_SCHEME:
        raise Exception(
            f"Registry path {registry_path} has unsupported scheme {uri.scheme}. "
            f"Supported schemes are file, s3 and gs."
        )
    else:
        registry_store_type = REGISTRY_STORE_CLASS_FOR_SCHEME[uri.scheme]
        return get_registry_store_class_from_type(registry_store_type)


class Registry:
    """
    Registry: A registry allows for the management and persistence of feature definitions and related metadata.
    """

    # The cached_registry_proto object is used for both reads and writes. In particular,
    # all write operations refresh the cache and modify it in memory; the write must
    # then be persisted to the underlying RegistryStore with a call to commit().
    cached_registry_proto: Optional[RegistryProto] = None
    cached_registry_proto_created: Optional[datetime] = None
    cached_registry_proto_ttl: timedelta
    cache_being_updated: bool = False

    def __init__(self, registry_config: RegistryConfig, repo_path: Path):
        """
        Create the Registry object.

        Args:
            registry_config: RegistryConfig object containing the destination path and cache ttl,
            repo_path: Path to the base of the Feast repository
            or where it will be created if it does not exist yet.
        """
        registry_store_type = registry_config.registry_store_type
        registry_path = registry_config.path
        if registry_store_type is None:
            cls = get_registry_store_class_from_scheme(registry_path)
        else:
            cls = get_registry_store_class_from_type(str(registry_store_type))

        self._registry_store = cls(registry_config, repo_path)
        self.cached_registry_proto_ttl = timedelta(
            seconds=registry_config.cache_ttl_seconds
            if registry_config.cache_ttl_seconds is not None
            else 0
        )

    def _initialize_registry(self):
        """Explicitly initializes the registry with an empty proto if it doesn't exist."""
        try:
            self._get_registry_proto()
        except FileNotFoundError:
            registry_proto = RegistryProto()
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
            self._registry_store.update_registry_proto(registry_proto)

    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        entity.is_valid()
        entity_proto = entity.to_proto()
        entity_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_entity_proto in enumerate(
            self.cached_registry_proto.entities
        ):
            if (
                existing_entity_proto.spec.name == entity_proto.spec.name
                and existing_entity_proto.spec.project == project
            ):
                del self.cached_registry_proto.entities[idx]
                break

        self.cached_registry_proto.entities.append(entity_proto)
        if commit:
            self.commit()

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of entities
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        entities = []
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.project == project:
                entities.append(Entity.from_proto(entity_proto))
        return entities

    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        """
        Registers a single feature service with Feast

        Args:
            feature_service: A feature service that will be registered
            project: Feast project that this entity belongs to
        """
        feature_service_proto = feature_service.to_proto()
        feature_service_proto.spec.project = project

        registry = self._prepare_registry_for_changes()

        for idx, existing_feature_service_proto in enumerate(registry.feature_services):
            if (
                existing_feature_service_proto.spec.name
                == feature_service_proto.spec.name
                and existing_feature_service_proto.spec.project == project
            ):
                del registry.feature_services[idx]
        registry.feature_services.append(feature_service_proto)
        if commit:
            self.commit()

    def list_feature_services(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureService]:
        """
        Retrieve a list of feature services from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of feature services
        """

        registry = self._get_registry_proto(allow_cache=allow_cache)
        feature_services = []
        for feature_service_proto in registry.feature_services:
            if feature_service_proto.spec.project == project:
                feature_services.append(
                    FeatureService.from_proto(feature_service_proto)
                )
        return feature_services

    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
        """
        Retrieves a feature service.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to

        Returns:
            Returns either the specified feature service, or raises an exception if
            none is found
        """
        registry = self._get_registry_proto(allow_cache=allow_cache)

        for feature_service_proto in registry.feature_services:
            if (
                feature_service_proto.spec.project == project
                and feature_service_proto.spec.name == name
            ):
                return FeatureService.from_proto(feature_service_proto)
        raise FeatureServiceNotFoundException(name, project=project)

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.name == name and entity_proto.spec.project == project:
                return Entity.from_proto(entity_proto)
        raise EntityNotFoundException(name, project=project)

    def apply_feature_table(
        self, feature_table: FeatureTable, project: str, commit: bool = True
    ):
        """
        Registers a single feature table with Feast

        Args:
            feature_table: Feature table that will be registered
            project: Feast project that this feature table belongs to
            commit: Whether the change should be persisted immediately
        """
        feature_table.is_valid()
        feature_table_proto = feature_table.to_proto()
        feature_table_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_table_proto in enumerate(
            self.cached_registry_proto.feature_tables
        ):
            if (
                existing_feature_table_proto.spec.name == feature_table_proto.spec.name
                and existing_feature_table_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_tables[idx]
                break

        self.cached_registry_proto.feature_tables.append(feature_table_proto)
        if commit:
            self.commit()

    def apply_feature_view(
        self, feature_view: FeatureView, project: str, commit: bool = True
    ):
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        feature_view.is_valid()
        feature_view_proto = feature_view.to_proto()
        feature_view_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        if feature_view.name in self._get_existing_on_demand_feature_view_names():
            raise ConflictingFeatureViewNames(feature_view.name)

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == feature_view_proto.spec.name
                and existing_feature_view_proto.spec.project == project
            ):
                if FeatureView.from_proto(existing_feature_view_proto) == feature_view:
                    return
                else:
                    del self.cached_registry_proto.feature_views[idx]
                    break

        self.cached_registry_proto.feature_views.append(feature_view_proto)
        if commit:
            self.commit()

    def apply_on_demand_feature_view(
        self,
        on_demand_feature_view: OnDemandFeatureView,
        project: str,
        commit: bool = True,
    ):
        """
        Registers a single on demand feature view with Feast

        Args:
            on_demand_feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        on_demand_feature_view_proto = on_demand_feature_view.to_proto()
        on_demand_feature_view_proto.spec.project = project
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        if on_demand_feature_view.name in self._get_existing_feature_view_names():
            raise ConflictingFeatureViewNames(on_demand_feature_view.name)

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.on_demand_feature_views
        ):
            if (
                existing_feature_view_proto.spec.name
                == on_demand_feature_view_proto.spec.name
                and existing_feature_view_proto.spec.project == project
            ):
                if (
                    OnDemandFeatureView.from_proto(existing_feature_view_proto)
                    == on_demand_feature_view
                ):
                    return
                else:
                    del self.cached_registry_proto.on_demand_feature_views[idx]
                    break

        self.cached_registry_proto.on_demand_feature_views.append(
            on_demand_feature_view_proto
        )
        if commit:
            self.commit()

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[OnDemandFeatureView]:
        """
        Retrieve a list of on demand feature views from the registry

        Args:
            allow_cache: Whether to allow returning on demand feature views from a cached registry
            project: Filter on demand feature views based on project name

        Returns:
            List of on demand feature views
        """

        registry = self._get_registry_proto(allow_cache=allow_cache)
        on_demand_feature_views = []
        for on_demand_feature_view in registry.on_demand_feature_views:
            if on_demand_feature_view.spec.project == project:
                on_demand_feature_views.append(
                    OnDemandFeatureView.from_proto(on_demand_feature_view)
                )
        return on_demand_feature_views

    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
        """
        Retrieves an on demand feature view.

        Args:
            name: Name of on demand feature view
            project: Feast project that this on demand feature  belongs to

        Returns:
            Returns either the specified on demand feature view, or raises an exception if
            none is found
        """
        registry = self._get_registry_proto(allow_cache=allow_cache)

        for on_demand_feature_view in registry.on_demand_feature_views:
            if (
                on_demand_feature_view.spec.project == project
                and on_demand_feature_view.spec.name == name
            ):
                return OnDemandFeatureView.from_proto(on_demand_feature_view)
        raise OnDemandFeatureViewNotFoundException(name, project=project)

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ):
        """
        Updates materialization intervals tracked for a single feature view in Feast

        Args:
            feature_view: Feature view that will be updated with an additional materialization interval tracked
            project: Feast project that this feature view belongs to
            start_date (datetime): Start date of the materialization interval to track
            end_date (datetime): End date of the materialization interval to track
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == feature_view.name
                and existing_feature_view_proto.spec.project == project
            ):
                existing_feature_view = FeatureView.from_proto(
                    existing_feature_view_proto
                )
                existing_feature_view.materialization_intervals.append(
                    (start_date, end_date)
                )
                feature_view_proto = existing_feature_view.to_proto()
                feature_view_proto.spec.project = project
                del self.cached_registry_proto.feature_views[idx]
                self.cached_registry_proto.feature_views.append(feature_view_proto)
                if commit:
                    self.commit()
                return

        raise FeatureViewNotFoundException(feature_view.name, project)

    def list_feature_tables(self, project: str) -> List[FeatureTable]:
        """
        Retrieve a list of feature tables from the registry

        Args:
            project: Filter feature tables based on project name

        Returns:
            List of feature tables
        """
        registry_proto = self._get_registry_proto()
        feature_tables = []
        for feature_table_proto in registry_proto.feature_tables:
            if feature_table_proto.spec.project == project:
                feature_tables.append(FeatureTable.from_proto(feature_table_proto))
        return feature_tables

    def list_feature_views(
        self, project: str, allow_cache: bool = False
    ) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature tables based on project name

        Returns:
            List of feature views
        """
        registry_proto = self._get_registry_proto(allow_cache=allow_cache)
        feature_views = []
        for feature_view_proto in registry_proto.feature_views:
            if feature_view_proto.spec.project == project:
                feature_views.append(FeatureView.from_proto(feature_view_proto))
        return feature_views

    def get_feature_table(self, name: str, project: str) -> FeatureTable:
        """
        Retrieves a feature table.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to

        Returns:
            Returns either the specified feature table, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto()
        for feature_table_proto in registry_proto.feature_tables:
            if (
                feature_table_proto.spec.name == name
                and feature_table_proto.spec.project == project
            ):
                return FeatureTable.from_proto(feature_table_proto)
        raise FeatureTableNotFoundException(name, project)

    def get_feature_view(self, name: str, project: str) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        registry_proto = self._get_registry_proto()
        for feature_view_proto in registry_proto.feature_views:
            if (
                feature_view_proto.spec.name == name
                and feature_view_proto.spec.project == project
            ):
                return FeatureView.from_proto(feature_view_proto)
        raise FeatureViewNotFoundException(name, project)

    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature service or raises an exception if not found.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, feature_service_proto in enumerate(
            self.cached_registry_proto.feature_services
        ):
            if (
                feature_service_proto.spec.name == name
                and feature_service_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_services[idx]
                if commit:
                    self.commit()
                return
        raise FeatureServiceNotFoundException(name, project)

    def delete_feature_table(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature table or raises an exception if not found.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_table_proto in enumerate(
            self.cached_registry_proto.feature_tables
        ):
            if (
                existing_feature_table_proto.spec.name == name
                and existing_feature_table_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_tables[idx]
                if commit:
                    self.commit()
                return

        raise FeatureTableNotFoundException(name, project)

    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        self._prepare_registry_for_changes()
        assert self.cached_registry_proto

        for idx, existing_feature_view_proto in enumerate(
            self.cached_registry_proto.feature_views
        ):
            if (
                existing_feature_view_proto.spec.name == name
                and existing_feature_view_proto.spec.project == project
            ):
                del self.cached_registry_proto.feature_views[idx]
                if commit:
                    self.commit()
                return

        raise FeatureViewNotFoundException(name, project)

    def commit(self):
        """Commits the state of the registry cache to the remote registry store."""
        if self.cached_registry_proto:
            self._registry_store.update_registry_proto(self.cached_registry_proto)

    def refresh(self):
        """Refreshes the state of the registry cache by fetching the registry state from the remote registry store."""
        self._get_registry_proto(allow_cache=False)

    def teardown(self):
        """Tears down (removes) the registry."""
        self._registry_store.teardown()

    def _prepare_registry_for_changes(self):
        """Prepares the Registry for changes by refreshing the cache if necessary."""
        try:
            self._get_registry_proto(allow_cache=True)
        except FileNotFoundError:
            registry_proto = RegistryProto()
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
            self.cached_registry_proto = registry_proto
            self.cached_registry_proto_created = datetime.now()
        return self.cached_registry_proto

    def _get_registry_proto(self, allow_cache: bool = False) -> RegistryProto:
        """Returns the cached or remote registry state

        Args:
            allow_cache: Whether to allow the use of the registry cache when fetching the RegistryProto

        Returns: Returns a RegistryProto object which represents the state of the registry
        """
        expired = (
            self.cached_registry_proto is None
            or self.cached_registry_proto_created is None
        ) or (
            self.cached_registry_proto_ttl.total_seconds() > 0  # 0 ttl means infinity
            and (
                datetime.now()
                > (self.cached_registry_proto_created + self.cached_registry_proto_ttl)
            )
        )
        if allow_cache and (not expired or self.cache_being_updated):
            assert isinstance(self.cached_registry_proto, RegistryProto)
            return self.cached_registry_proto

        try:
            self.cache_being_updated = True
            registry_proto = self._registry_store.get_registry_proto()
            self.cached_registry_proto = registry_proto
            self.cached_registry_proto_created = datetime.now()
        except Exception as e:
            raise e
        finally:
            self.cache_being_updated = False
        return registry_proto

    def _get_existing_feature_view_names(self) -> Set[str]:
        assert self.cached_registry_proto
        return set([fv.spec.name for fv in self.cached_registry_proto.feature_views])

    def _get_existing_on_demand_feature_view_names(self) -> Set[str]:
        assert self.cached_registry_proto
        return set(
            [
                odfv.spec.name
                for odfv in self.cached_registry_proto.on_demand_feature_views
            ]
        )
