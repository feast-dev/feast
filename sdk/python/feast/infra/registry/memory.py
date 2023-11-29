import copy
from typing import Optional, Dict, List, Any, Tuple, Union
from pathlib import Path
from datetime import datetime

from feast import usage

# Feast Resources
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView, BaseFeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.request_feature_view import RequestFeatureView
from feast.project_metadata import ProjectMetadata
from feast.infra.infra_object import Infra
from feast.saved_dataset import SavedDataset, ValidationReference

from feast.repo_config import RegistryConfig

from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.infra.registry.base_registry import BaseRegistry

from feast.errors import (
    ConflictingFeatureViewNames,
    FeatureServiceNotFoundException,
    FeatureServiceNameCollisionException,
    FeatureViewNotFoundException,
    ValidationReferenceNotFound,
    RegistryNotBuiltException,
    EntityNameCollisionException,
    EntityNotFoundException,
    DataSourceRepeatNamesException,
    DataSourceObjectNotFoundException,
    SavedDatasetNotFound,
    DuplicateValidationReference,
    SavedDatasetCollisionException,
    MissingProjectMetadataException
)

TimeDependentObject = Union[
    BaseFeatureView,
    FeatureView,
    StreamFeatureView,
    OnDemandFeatureView,
    RequestFeatureView,
    Entity,
    FeatureService,
    SavedDataset
]

FeastResource = Union[
    TimeDependentObject, DataSource, SavedDataset, ValidationReference, ProjectMetadata, Infra
]


def project_key(project: str, key: str) -> str:
    # maps (project, key) pair to a single string, called a `projected key`
    return f"{project}:{key}"


def invert_projected_key(projected_key: str) -> Tuple[str, str]:
    # inverse of `project_key`
    s = projected_key.split(":")
    if len(s) != 2:
        raise ValueError(f"Invalid projected key {projected_key}. Key must follow the format `project:name`.")
    return s[0], s[1]


def list_registry_dict(project: str, registry: Dict[str, FeastResource]) -> List[FeastResource]:
    # returns all values in registry that belong to `project`
    return [
        v for k, v in registry.items() if invert_projected_key(k)[0] == project
    ]


class InMemoryRegistry(BaseRegistry):
    def __init__(
        self,
        registry_config: Optional[RegistryConfig],
        repo_path: Optional[Path],
        is_feast_apply: bool = False
    ) -> None:

        # unused
        self.repo_path = repo_path

        # flag signaling that the registry has been populated; this should be set after a Feast apply operation
        self.is_built = False
        self.is_feast_apply = is_feast_apply

        self.infra: Dict[str, Infra] = {}
        self.entities: Dict[str, Entity] = {}
        self.feature_services: Dict[str, FeatureService] = {}
        self.project_metadata: Dict[str, ProjectMetadata] = {}
        self.validation_references: Dict[str, ValidationReference] = {}

        self.data_sources: Dict[str, DataSource] = {}
        self.saved_datasets: Dict[str, SavedDataset] = {}

        self.stream_feature_views: Dict[str, StreamFeatureView] = {}
        self.feature_views: Dict[str, FeatureView] = {}
        self.on_demand_feature_views: Dict[str, OnDemandFeatureView] = {}
        self.request_feature_views: Dict[str, RequestFeatureView] = {}

        self.feature_view_registries = [
            self.stream_feature_views,
            self.feature_views,
            self.on_demand_feature_views,
            self.request_feature_views
        ]

        # recomputing `RegistryProto` is expensive, cache unless changed
        self.cached_proto: Optional[RegistryProto] = None

    def enter_apply_context(self):
        self.is_feast_apply = True

    def exit_apply_context(self):
        self.is_feast_apply = False
        self.proto()
        # if this flag is not set, `get_*` operations of the registry will fail; this flag is subtly different from
        # `is_feast_apply` in that `is_built` remains True if set at least once.
        self.is_built = True

    def _get_feature_view_registry(self, feature_view: BaseFeatureView) -> Dict[str, BaseFeatureView]:
        # returns the sub-registry that aligns with `type(feature_view)`, or an exception if the type is unknown
        if isinstance(feature_view, StreamFeatureView):
            return self.stream_feature_views
        if isinstance(feature_view, FeatureView):
            return self.feature_views
        if isinstance(feature_view, OnDemandFeatureView):
            return self.on_demand_feature_views
        if isinstance(feature_view, RequestFeatureView):
            return self.request_feature_views
        raise FeatureViewNotFoundException(feature_view)

    def _maybe_init_project_metadata(self, project: str) -> None:
        # updates `usage` project uuid to match requested project
        metadata = self.project_metadata.setdefault(project, ProjectMetadata(project_name=project))
        usage.set_current_project_uuid(metadata.project_uuid)

    def _maybe_reset_proto_registry(self) -> None:
        # set cached proto registry to `None` if write operation is applied and registry is built
        if self.is_built:
            self.cached_proto = None

    def _delete_object(
        self, name: str, project: str, registry: Dict[str, FeastResource], on_miss_exc: Exception
    ) -> None:
        # deletes a key from `registry`, or `on_miss_exc` is raised if the object doesn't exist in the registry
        self._maybe_init_project_metadata(project)
        key = project_key(project, name)
        if key not in registry:
            raise on_miss_exc
        del registry[key]
        self._maybe_reset_proto_registry()

    def _get_object(
        self, name: str, project: str, registry: Dict[str, FeastResource], on_miss_exc: Exception
    ) -> FeastResource:
        # returns a `FeastResource` from the registry, or `on_miss_exc` if the object doesn't exist in the registry
        self._maybe_init_project_metadata(project)
        if not self.is_built:
            raise RegistryNotBuiltException(registry_name=self.__class__.__name__)
        key = project_key(project, name)
        if key not in registry:
            raise on_miss_exc
        return registry[key]

    def _update_object_ts(self, obj: TimeDependentObject) -> TimeDependentObject:
        # updates the `created_timestamp` and `last_updated_timestamp` attributes of a `TimeDependentObject`
        # WARNING: this is an in-place operation!
        now = datetime.utcnow()
        if not obj.created_timestamp:
            obj.created_timestamp = now
        obj.last_updated_timestamp = now
        return obj

    def apply_entity(self, entity: Entity, project: str, commit: bool = True) -> None:
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        self._maybe_init_project_metadata(project)

        entity.is_valid()
        key = project_key(project, entity.name)
        if key in self.entities and self.entities[key] != entity:
            raise EntityNameCollisionException(entity.name, project)

        self.entities[key] = self._update_object_ts(entity)
        self._maybe_reset_proto_registry()

    def delete_entity(self, name: str, project: str, commit: bool = True) -> None:
        """
        Deletes an entity or raises an exception if not found.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        self._delete_object(
            name=name, project=project, registry=self.entities, on_miss_exc=EntityNotFoundException(name, project)
        )

    def get_entity(self, name: str, project: str, allow_cache: bool = False) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to
            allow_cache: Whether to allow returning this entity from a cached registry

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        exc = EntityNotFoundException(name, project)
        return self._get_object(name=name, project=project, registry=self.entities, on_miss_exc=exc)

    def list_entities(self, project: str, allow_cache: bool = False) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of entities
        """
        return list_registry_dict(project=project, registry=self.entities)

    def apply_data_source(self, data_source: DataSource, project: str, commit: bool = True) -> None:
        """
        Registers a single data source with Feast

        Args:
            data_source: A data source that will be registered
            project: Feast project that this data source belongs to
            commit: Whether to immediately commit to the registry
        """
        self._maybe_init_project_metadata(project)
        key = project_key(project, data_source.name)
        if key in self.data_sources and self.data_sources[key] != data_source:
            raise DataSourceRepeatNamesException(data_source.name)
        self.data_sources[key] = data_source
        self._maybe_reset_proto_registry()

    def delete_data_source(self, name: str, project: str, commit: bool = True) -> None:
        """
        Deletes a data source or raises an exception if not found.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            commit: Whether the change should be persisted immediately
        """
        exc = DataSourceObjectNotFoundException(name=name, project=project)
        self._delete_object(name=name, project=project, registry=self.data_sources, on_miss_exc=exc)

    def get_data_source(self, name: str, project: str, allow_cache: bool = False) -> DataSource:
        """
        Retrieves a data source.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            allow_cache: Whether to allow returning this data source from a cached registry

        Returns:
            Returns either the specified data source, or raises an exception if none is found
        """
        exc = DataSourceObjectNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.data_sources, on_miss_exc=exc)

    def list_data_sources(self, project: str, allow_cache: bool = False) -> List[DataSource]:
        """
        Retrieve a list of data sources from the registry

        Args:
            project: Filter data source based on project name
            allow_cache: Whether to allow returning data sources from a cached registry

        Returns:
            List of data sources
        """
        return list_registry_dict(project=project, registry=self.data_sources)

    def apply_feature_service(self, feature_service: FeatureService, project: str, commit: bool = True) -> None:
        """
        Registers a single feature service with Feast

        Args:
            feature_service: A feature service that will be registered
            project: Feast project that this entity belongs to
        """
        self._maybe_init_project_metadata(project)
        key = project_key(project, feature_service.name)
        if key in self.feature_services and self.feature_services[key] != feature_service:
            raise FeatureServiceNameCollisionException(service_name=feature_service.name, project=project)
        self.feature_services[key] = self._update_object_ts(feature_service)
        self._maybe_reset_proto_registry()

    def delete_feature_service(self, name: str, project: str, commit: bool = True) -> None:
        """
        Deletes a feature service or raises an exception if not found.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            commit: Whether the change should be persisted immediately
        """
        exc = FeatureServiceNotFoundException(name=name, project=project)
        self._delete_object(name=name, project=project, registry=self.feature_services, on_miss_exc=exc)

    def get_feature_service(self, name: str, project: str, allow_cache: bool = False) -> FeatureService:
        """
        Retrieves a feature service.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            allow_cache: Whether to allow returning this feature service from a cached registry

        Returns:
            Returns either the specified feature service, or raises an exception if
            none is found
        """
        exc = FeatureServiceNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.feature_services, on_miss_exc=exc)

    def list_feature_services(self, project: str, allow_cache: bool = False) -> List[FeatureService]:
        """
        Retrieve a list of feature services from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name

        Returns:
            List of feature services
        """
        return list_registry_dict(project, self.feature_services)

    def apply_feature_view(self, feature_view: BaseFeatureView, project: str, commit: bool = True) -> None:
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        self._maybe_init_project_metadata(project)
        feature_view.ensure_valid()

        registry = self._get_feature_view_registry(feature_view)
        key = project_key(project, feature_view.name)
        if key in registry and registry[key] != feature_view:
            raise ConflictingFeatureViewNames(feature_view.name)
        registry[key] = self._update_object_ts(feature_view)
        self._maybe_reset_proto_registry()

    def delete_feature_view(self, name: str, project: str, commit: bool = True) -> None:
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        self._maybe_init_project_metadata(project=project)
        key = project_key(project, name)
        for registry in self.feature_view_registries:
            if key in registry:
                del registry[key]
                self._maybe_reset_proto_registry()
                return
        raise FeatureViewNotFoundException(name=name, project=project)

    def get_stream_feature_view(self, name: str, project: str, allow_cache: bool = False) -> StreamFeatureView:
        """
        Retrieves a stream feature view.

        Args:
            name: Name of stream feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        exc = FeatureViewNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.stream_feature_views, on_miss_exc=exc)

    def list_stream_feature_views(
        self, project: str, allow_cache: bool = False, ignore_udfs: bool = False
    ) -> List[StreamFeatureView]:
        """
        Retrieve a list of stream feature views from the registry

        Args:
            project: Filter stream feature views based on project name
            allow_cache: Whether to allow returning stream feature views from a cached registry
            ignore_udfs: Whether a feast apply operation is being executed. Determines whether environment
                sensitive commands, such as dill.loads(), are skipped and 'None' is set as their results.
        Returns:
            List of stream feature views
        """
        return list_registry_dict(project, self.stream_feature_views)

    def get_on_demand_feature_view(self, name: str, project: str, allow_cache: bool = False) -> OnDemandFeatureView:
        """
        Retrieves an on demand feature view.

        Args:
            name: Name of on demand feature view
            project: Feast project that this on demand feature view belongs to
            allow_cache: Whether to allow returning this on demand feature view from a cached registry

        Returns:
            Returns either the specified on demand feature view, or raises an exception if
            none is found
        """
        exc = FeatureViewNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.on_demand_feature_views, on_miss_exc=exc)

    def list_on_demand_feature_views(
        self, project: str, allow_cache: bool = False, ignore_udfs: bool = False
    ) -> List[OnDemandFeatureView]:
        """
        Retrieve a list of on demand feature views from the registry

        Args:
            project: Filter on demand feature views based on project name
            allow_cache: Whether to allow returning on demand feature views from a cached registry
            ignore_udfs: Whether a feast apply operation is being executed. Determines whether environment
                         sensitive commands, such as dill.loads(), are skipped and 'None' is set as their results.
        Returns:
            List of on demand feature views
        """
        return list_registry_dict(project, self.on_demand_feature_views)

    def get_feature_view(self, name: str, project: str, allow_cache: bool = False) -> FeatureView:
        """
        Retrieves a feature view.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        exc = FeatureViewNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.feature_views, on_miss_exc=exc)

    def list_feature_views(self, project: str, allow_cache: bool = False) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of feature views
        """
        return list_registry_dict(project, self.feature_views)

    def get_request_feature_view(self, name: str, project: str) -> RequestFeatureView:
        """
        Retrieves a request feature view.

        Args:
            name: Name of request feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        exc = FeatureViewNotFoundException(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.request_feature_views, on_miss_exc=exc)

    def list_request_feature_views(self, project: str, allow_cache: bool = False) -> List[RequestFeatureView]:
        """
        Retrieve a list of request feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of request feature views
        """
        return list_registry_dict(project, self.request_feature_views)

    def apply_materialization(
        self,
        feature_view: FeatureView,
        project: str,
        start_date: datetime,
        end_date: datetime,
        commit: bool = True,
    ) -> None:
        self._maybe_init_project_metadata(project)
        key = project_key(project, feature_view.name)
        for registry in [self.feature_views, self.stream_feature_views]:
            if key in registry:
                fv = registry[key]
                fv.materialization_intervals.append((start_date, end_date))
                fv.last_updated_timestamp = datetime.utcnow()
                self._maybe_reset_proto_registry()
                return
        raise FeatureViewNotFoundException(feature_view.name, project)

    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ) -> None:
        """
        Stores a saved dataset metadata with Feast

        Args:
            saved_dataset: SavedDataset that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        self._maybe_init_project_metadata(project)
        key = project_key(project, saved_dataset.name)
        if key in self.saved_datasets and self.saved_datasets[key] != saved_dataset:
            raise SavedDatasetCollisionException(project=project, name=saved_dataset.name)
        self.saved_datasets[key] = self._update_object_ts(saved_dataset)
        self._maybe_reset_proto_registry()

    def get_saved_dataset(
        self, name: str, project: str, allow_cache: bool = False
    ) -> SavedDataset:
        """
        Retrieves a saved dataset.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified SavedDataset, or raises an exception if
            none is found
        """
        exc = SavedDatasetNotFound(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.saved_datasets, on_miss_exc=exc)

    def delete_saved_dataset(self, name: str, project: str, allow_cache: bool = False):
        """
        Delete a saved dataset.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified SavedDataset, or raises an exception if
            none is found
        """
        exc = SavedDatasetNotFound(name=name, project=project)
        self._delete_object(name=name, project=project, registry=self.saved_datasets, on_miss_exc=exc)

    def list_saved_datasets(self, project: str, allow_cache: bool = False) -> List[SavedDataset]:
        """
        Retrieves a list of all saved datasets in specified project

        Args:
            project: Feast project
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns the list of SavedDatasets
        """
        return list_registry_dict(project=project, registry=self.saved_datasets)

    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ) -> None:
        """
        Persist a validation reference

        Args:
            validation_reference: ValidationReference that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        self._maybe_init_project_metadata(project)
        key = project_key(project, validation_reference.name)
        if key in self.validation_references and self.validation_references[key] != validation_reference:
            raise DuplicateValidationReference(name=validation_reference.name, project=project)
        self.validation_references[key] = validation_reference
        self._maybe_reset_proto_registry()

    def delete_validation_reference(self, name: str, project: str, commit: bool = True) -> None:
        """
        Deletes a validation reference or raises an exception if not found.

        Args:
            name: Name of validation reference
            project: Feast project that this object belongs to
            commit: Whether the change should be persisted immediately
        """
        exc = ValidationReferenceNotFound(name=name, project=project)
        self._delete_object(name=name, project=project, registry=self.validation_references, on_miss_exc=exc)

    def get_validation_reference(
        self, name: str, project: str, allow_cache: bool = False
    ) -> ValidationReference:
        """
        Retrieves a validation reference.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            allow_cache: Whether to allow returning this dataset from a cached registry

        Returns:
            Returns either the specified ValidationReference, or raises an exception if
            none is found
        """
        exc = ValidationReferenceNotFound(name=name, project=project)
        return self._get_object(name=name, project=project, registry=self.validation_references, on_miss_exc=exc)

    def list_validation_references(self, project: str, allow_cache: bool = False) -> List[ValidationReference]:
        """
        Retrieve a list of validation references from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name

        Returns:
            List of request feature views
        """
        return list_registry_dict(project=project, registry=self.validation_references)

    def list_project_metadata(self, project: str, allow_cache: bool = False) -> List[ProjectMetadata]:
        """
        Retrieves project metadata

        Args:
            project: Filter metadata based on project name
            allow_cache: Allow returning feature views from the cached registry

        Returns:
            List of project metadata
        """
        if project not in self.project_metadata:
            raise MissingProjectMetadataException(project=project)
        return [self.project_metadata[project]]

    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        """
        Updates the stored Infra object.

        Args:
            infra: The new Infra object to be stored.
            project: Feast project that the Infra object refers to
            commit: Whether the change should be persisted immediately
        """
        self.infra[project] = infra
        self._maybe_reset_proto_registry()

    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        """
        Retrieves the stored Infra object.

        Args:
            project: Feast project that the Infra object refers to
            allow_cache: Whether to allow returning this entity from a cached registry

        Returns:
            The stored Infra object.
        """
        if project not in self.infra:
            return Infra()
        return self.infra[project]

    def apply_user_metadata(self, project: str, feature_view: BaseFeatureView, metadata_bytes: Optional[bytes]) -> None:
        # not supported for BaseFeatureView in-memory objects
        pass

    def get_user_metadata(self, project: str, feature_view: BaseFeatureView) -> Optional[bytes]:
        # not supported for BaseFeatureView in-memory objects
        pass

    def proto(self) -> RegistryProto:
        if self.cached_proto:
            return self.cached_proto

        r = RegistryProto()
        for project in self.project_metadata:
            for lister, registry_proto_field in [
                (self.list_entities, r.entities),
                (self.list_feature_views, r.feature_views),
                (self.list_data_sources, r.data_sources),
                (self.list_on_demand_feature_views, r.on_demand_feature_views),
                (self.list_request_feature_views, r.request_feature_views),
                (self.list_stream_feature_views, r.stream_feature_views),
                (self.list_feature_services, r.feature_services),
                (self.list_saved_datasets, r.saved_datasets),
                (self.list_validation_references, r.validation_references),
                (self.list_project_metadata, r.project_metadata),
            ]:
                objs: List[Any] = lister(project)
                if objs:
                    registry_proto_field_data = []
                    for obj in objs:
                        object_proto = obj.to_proto()
                        # Overriding project when missing, this is to handle failures when the registry is cached
                        if getattr(object_proto, 'spec', None) and object_proto.spec.project == '':
                            object_proto.spec.project = project
                        registry_proto_field_data.append(object_proto)

                    registry_proto_field.extend(registry_proto_field_data)
            r.infra.CopyFrom(self.get_infra(project).to_proto())
        if self.is_built:
            self.cached_proto = r
        return r

    def commit(self) -> None:
        # This is a noop because transactions are not supported
        pass

    def refresh(self, project: Optional[str] = None) -> None:
        self.proto()
        if project:
            self._maybe_init_project_metadata(project)

    def teardown(self) -> None:
        pass
