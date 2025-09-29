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
import json
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message

from feast.base_feature_view import BaseFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.infra.infra_object import Infra
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.permission import Permission
from feast.project import Project
from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.Permission_pb2 import Permission as PermissionProto
from feast.protos.feast.core.Project_pb2 import Project as ProjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.saved_dataset import SavedDataset, ValidationReference
from feast.stream_feature_view import StreamFeatureView
from feast.transformation.pandas_transformation import PandasTransformation
from feast.transformation.substrait_transformation import SubstraitTransformation


class BaseRegistry(ABC):
    """
    The interface that Feast uses to apply, list, retrieve, and delete Feast objects (e.g. entities,
    feature views, and data sources).
    """

    # Entity operations
    @abstractmethod
    def apply_entity(self, entity: Entity, project: str, commit: bool = True):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def delete_entity(self, name: str, project: str, commit: bool = True):
        """
        Deletes an entity or raises an exception if not found.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def list_entities(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name
            tags: Filter by tags

        Returns:
            List of entities
        """
        raise NotImplementedError

    # Data source operations
    @abstractmethod
    def apply_data_source(
        self, data_source: DataSource, project: str, commit: bool = True
    ):
        """
        Registers a single data source with Feast

        Args:
            data_source: A data source that will be registered
            project: Feast project that this data source belongs to
            commit: Whether to immediately commit to the registry
        """
        raise NotImplementedError

    @abstractmethod
    def delete_data_source(self, name: str, project: str, commit: bool = True):
        """
        Deletes a data source or raises an exception if not found.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def get_data_source(
        self, name: str, project: str, allow_cache: bool = False
    ) -> DataSource:
        """
        Retrieves a data source.

        Args:
            name: Name of data source
            project: Feast project that this data source belongs to
            allow_cache: Whether to allow returning this data source from a cached registry

        Returns:
            Returns either the specified data source, or raises an exception if none is found
        """
        raise NotImplementedError

    @abstractmethod
    def list_data_sources(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[DataSource]:
        """
        Retrieve a list of data sources from the registry

        Args:
            project: Filter data source based on project name
            allow_cache: Whether to allow returning data sources from a cached registry
            tags: Filter by tags

        Returns:
            List of data sources
        """
        raise NotImplementedError

    # Feature service operations
    @abstractmethod
    def apply_feature_service(
        self, feature_service: FeatureService, project: str, commit: bool = True
    ):
        """
        Registers a single feature service with Feast

        Args:
            feature_service: A feature service that will be registered
            project: Feast project that this entity belongs to
        """
        raise NotImplementedError

    @abstractmethod
    def delete_feature_service(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature service or raises an exception if not found.

        Args:
            name: Name of feature service
            project: Feast project that this feature service belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def get_feature_service(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureService:
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
        raise NotImplementedError

    @abstractmethod
    def list_feature_services(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureService]:
        """
        Retrieve a list of feature services from the registry

        Args:
            allow_cache: Whether to allow returning entities from a cached registry
            project: Filter entities based on project name
            tags: Filter by tags

        Returns:
            List of feature services
        """
        raise NotImplementedError

    # Feature view operations
    @abstractmethod
    def apply_feature_view(
        self, feature_view: BaseFeatureView, project: str, commit: bool = True
    ):
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def delete_feature_view(self, name: str, project: str, commit: bool = True):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    # stream feature view operations
    @abstractmethod
    def get_stream_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> StreamFeatureView:
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
        raise NotImplementedError

    @abstractmethod
    def list_stream_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[StreamFeatureView]:
        """
        Retrieve a list of stream feature views from the registry

        Args:
            project: Filter stream feature views based on project name
            allow_cache: Whether to allow returning stream feature views from a cached registry
            tags: Filter by tags

        Returns:
            List of stream feature views
        """
        raise NotImplementedError

    # on demand feature view operations
    @abstractmethod
    def get_on_demand_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> OnDemandFeatureView:
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
        raise NotImplementedError

    @abstractmethod
    def list_on_demand_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[OnDemandFeatureView]:
        """
        Retrieve a list of on demand feature views from the registry

        Args:
            project: Filter on demand feature views based on project name
            allow_cache: Whether to allow returning on demand feature views from a cached registry
            tags: Filter by tags

        Returns:
            List of on demand feature views
        """
        raise NotImplementedError

    # regular feature view operations
    @abstractmethod
    def get_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> FeatureView:
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
        raise NotImplementedError

    @abstractmethod
    def list_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name
            tags: Filter by tags

        Returns:
            List of feature views
        """
        raise NotImplementedError

    @abstractmethod
    def get_any_feature_view(
        self, name: str, project: str, allow_cache: bool = False
    ) -> BaseFeatureView:
        """
        Retrieves a feature view of any type.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
            allow_cache: Allow returning feature view from the cached registry

        Returns:
            Returns either the specified feature view, or raises an exception if
            none is found
        """
        raise NotImplementedError

    @abstractmethod
    def list_all_feature_views(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[BaseFeatureView]:
        """
        Retrieve a list of feature views of all types from the registry

        Args:
            allow_cache: Allow returning feature views from the cached registry
            project: Filter feature views based on project name
            tags: Filter by tags

        Returns:
            List of feature views
        """
        raise NotImplementedError

    @abstractmethod
    def apply_materialization(
        self,
        feature_view: Union[FeatureView, OnDemandFeatureView],
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
        raise NotImplementedError

    # Saved dataset operations
    @abstractmethod
    def apply_saved_dataset(
        self,
        saved_dataset: SavedDataset,
        project: str,
        commit: bool = True,
    ):
        """
        Stores a saved dataset metadata with Feast

        Args:
            saved_dataset: SavedDataset that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    def delete_saved_dataset(self, name: str, project: str, commit: bool = True):
        """
        Delete a saved dataset.

        Args:
            name: Name of dataset
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def list_saved_datasets(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[SavedDataset]:
        """
        Retrieves a list of all saved datasets in specified project

        Args:
            project: Feast project
            allow_cache: Whether to allow returning this dataset from a cached registry
            tags: Filter by tags

        Returns:
            Returns the list of SavedDatasets
        """
        raise NotImplementedError

    # Validation reference operations
    @abstractmethod
    def apply_validation_reference(
        self,
        validation_reference: ValidationReference,
        project: str,
        commit: bool = True,
    ):
        """
        Persist a validation reference

        Args:
            validation_reference: ValidationReference that will be added / updated to registry
            project: Feast project that this dataset belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def delete_validation_reference(self, name: str, project: str, commit: bool = True):
        """
        Deletes a validation reference or raises an exception if not found.

        Args:
            name: Name of validation reference
            project: Feast project that this object belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    # TODO: Needs to be implemented.
    def list_validation_references(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[ValidationReference]:
        """
        Retrieve a list of validation references from the registry

        Args:
            project: Filter validation references based on project name
            allow_cache: Allow returning validation references from the cached registry
            tags: Filter by tags

        Returns:
            List of request validation references
        """
        raise NotImplementedError

    @abstractmethod
    def list_project_metadata(
        self, project: str, allow_cache: bool = False
    ) -> List[ProjectMetadata]:
        """
        Retrieves project metadata

        Args:
            project: Filter metadata based on project name
            allow_cache: Allow returning feature views from the cached registry

        Returns:
            List of project metadata
        """
        raise NotImplementedError

    @abstractmethod
    def get_project_metadata(self, project: str, key: str) -> Optional[str]:
        """
        Retrieves a custom project metadata value by key.

        Args:
            project: Feast project name
            key: Metadata key

        Returns:
            The metadata value as a string, or None if not found.
        """
        raise NotImplementedError

    @abstractmethod
    def update_infra(self, infra: Infra, project: str, commit: bool = True):
        """
        Updates the stored Infra object.

        Args:
            infra: The new Infra object to be stored.
            project: Feast project that the Infra object refers to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def get_infra(self, project: str, allow_cache: bool = False) -> Infra:
        """
        Retrieves the stored Infra object.

        Args:
            project: Feast project that the Infra object refers to
            allow_cache: Whether to allow returning this entity from a cached registry

        Returns:
            The stored Infra object.
        """
        raise NotImplementedError

    @abstractmethod
    def apply_user_metadata(
        self,
        project: str,
        feature_view: BaseFeatureView,
        metadata_bytes: Optional[bytes],
    ): ...

    @abstractmethod
    def get_user_metadata(
        self, project: str, feature_view: BaseFeatureView
    ) -> Optional[bytes]: ...

    # Permission operations
    @abstractmethod
    def apply_permission(
        self, permission: Permission, project: str, commit: bool = True
    ):
        """
        Registers a single permission with Feast

        Args:
            permission: A permission that will be registered
            project: Feast project that this permission belongs to
            commit: Whether to immediately commit to the registry
        """
        raise NotImplementedError

    @abstractmethod
    def delete_permission(self, name: str, project: str, commit: bool = True):
        """
        Deletes a permission or raises an exception if not found.

        Args:
            name: Name of permission
            project: Feast project that this permission belongs to
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def get_permission(
        self, name: str, project: str, allow_cache: bool = False
    ) -> Permission:
        """
        Retrieves a permission.

        Args:
            name: Name of permission
            project: Feast project that this permission belongs to
            allow_cache: Whether to allow returning this permission from a cached registry

        Returns:
            Returns either the specified permission, or raises an exception if none is found
        """
        raise NotImplementedError

    @abstractmethod
    def list_permissions(
        self,
        project: str,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Permission]:
        """
        Retrieve a list of permissions from the registry

        Args:
            project: Filter permission based on project name
            allow_cache: Whether to allow returning permissions from a cached registry

        Returns:
            List of permissions
        """
        raise NotImplementedError

    @abstractmethod
    def apply_project(
        self,
        project: Project,
        commit: bool = True,
    ):
        """
        Registers a project with Feast

        Args:
            project: A project that will be registered
            commit: Whether to immediately commit to the registry
        """
        raise NotImplementedError

    @abstractmethod
    def delete_project(
        self,
        name: str,
        commit: bool = True,
    ):
        """
        Deletes a project or raises an ProjectNotFoundException exception if not found.

        Args:
            project: Feast project name that needs to be deleted
            commit: Whether the change should be persisted immediately
        """
        raise NotImplementedError

    @abstractmethod
    def get_project(
        self,
        name: str,
        allow_cache: bool = False,
    ) -> Project:
        """
        Retrieves a project.

        Args:
            name: Feast project name
            allow_cache: Whether to allow returning this permission from a cached registry

        Returns:
            Returns either the specified project, or raises ProjectObjectNotFoundException exception if none is found
        """
        raise NotImplementedError

    @abstractmethod
    def list_projects(
        self,
        allow_cache: bool = False,
        tags: Optional[dict[str, str]] = None,
    ) -> List[Project]:
        """
        Retrieve a list of projects from the registry

        Args:
            allow_cache: Whether to allow returning permissions from a cached registry

        Returns:
            List of project
        """
        raise NotImplementedError

    @abstractmethod
    def proto(self) -> RegistryProto:
        """
        Retrieves a proto version of the registry.

        Returns:
            The registry proto object.
        """
        raise NotImplementedError

    @abstractmethod
    def commit(self):
        """Commits the state of the registry cache to the remote registry store."""
        raise NotImplementedError

    @abstractmethod
    def refresh(self, project: Optional[str] = None):
        """Refreshes the state of the registry cache by fetching the registry state from the remote registry store."""
        raise NotImplementedError

    # Lineage operations
    def get_registry_lineage(
        self,
        project: str,
        allow_cache: bool = False,
        filter_object_type: Optional[str] = None,
        filter_object_name: Optional[str] = None,
    ) -> tuple[List[Any], List[Any]]:
        """
        Get complete registry lineage with relationships and indirect relationships.
        Args:
            project: Feast project name
            allow_cache: Whether to allow returning data from a cached registry
            filter_object_type: Optional filter by object type (dataSource, entity, featureView, featureService)
            filter_object_name: Optional filter by object name
        Returns:
            Tuple of (direct_relationships, indirect_relationships)
        """
        from feast.lineage.registry_lineage import RegistryLineageGenerator

        # Create a registry proto with all objects
        registry_proto = self._build_registry_proto(project, allow_cache)

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry_proto
        )

        # Apply filtering if specified
        if filter_object_type and filter_object_name:
            relationships = [
                rel
                for rel in relationships
                if (
                    (
                        rel.source.type.value == filter_object_type
                        and rel.source.name == filter_object_name
                    )
                    or (
                        rel.target.type.value == filter_object_type
                        and rel.target.name == filter_object_name
                    )
                )
            ]
            indirect_relationships = [
                rel
                for rel in indirect_relationships
                if (
                    (
                        rel.source.type.value == filter_object_type
                        and rel.source.name == filter_object_name
                    )
                    or (
                        rel.target.type.value == filter_object_type
                        and rel.target.name == filter_object_name
                    )
                )
            ]

        return relationships, indirect_relationships

    def get_object_relationships(
        self,
        project: str,
        object_type: str,
        object_name: str,
        include_indirect: bool = False,
        allow_cache: bool = False,
    ) -> List[Any]:
        """
        Get relationships for a specific object.
        Args:
            project: Feast project name
            object_type: Type of object (dataSource, entity, featureView, featureService, feature)
            object_name: Name of the object
            include_indirect: Whether to include indirect relationships
            allow_cache: Whether to allow returning data from a cached registry
        Returns:
            List of relationships involving the specified object
        """
        from feast.lineage.registry_lineage import (
            RegistryLineageGenerator,
        )

        registry_proto = self._build_registry_proto(project, allow_cache)
        lineage_generator = RegistryLineageGenerator()
        relationships = lineage_generator.get_object_relationships(
            registry_proto, object_type, object_name, include_indirect=include_indirect
        )
        return relationships

    def _build_registry_proto(
        self, project: str, allow_cache: bool = False
    ) -> RegistryProto:
        """Helper method to build a registry proto with all objects."""
        registry = RegistryProto()

        # Add all entities
        entities = self.list_entities(project=project, allow_cache=allow_cache)
        for entity in entities:
            registry.entities.append(entity.to_proto())

        # Add all data sources
        data_sources = self.list_data_sources(project=project, allow_cache=allow_cache)
        for data_source in data_sources:
            registry.data_sources.append(data_source.to_proto())

        # Add all feature views
        feature_views = self.list_feature_views(
            project=project, allow_cache=allow_cache
        )
        for feature_view in feature_views:
            registry.feature_views.append(feature_view.to_proto())

        # Add all stream feature views
        stream_feature_views = self.list_stream_feature_views(
            project=project, allow_cache=allow_cache
        )
        for stream_feature_view in stream_feature_views:
            registry.stream_feature_views.append(stream_feature_view.to_proto())

        # Add all on-demand feature views
        on_demand_feature_views = self.list_on_demand_feature_views(
            project=project, allow_cache=allow_cache
        )
        for on_demand_feature_view in on_demand_feature_views:
            registry.on_demand_feature_views.append(on_demand_feature_view.to_proto())

        # Add all feature services
        feature_services = self.list_feature_services(
            project=project, allow_cache=allow_cache
        )
        for feature_service in feature_services:
            registry.feature_services.append(feature_service.to_proto())

        return registry

    @staticmethod
    def _message_to_sorted_dict(message: Message) -> Dict[str, Any]:
        return json.loads(MessageToJson(message, sort_keys=True))

    def to_dict(self, project: str) -> Dict[str, List[Any]]:
        """Returns a dictionary representation of the registry contents for the specified project.
        For each list in the dictionary, the elements are sorted by name, so this
        method can be used to compare two registries.
        Args:
            project: Feast project to convert to a dict
        """
        registry_dict: Dict[str, Any] = defaultdict(list)
        registry_dict["project"] = project
        for project_metadata in sorted(self.list_project_metadata(project=project)):
            registry_dict["projectMetadata"].append(
                self._message_to_sorted_dict(project_metadata.to_proto())
            )
        for data_source in sorted(
            self.list_data_sources(project=project), key=lambda ds: ds.name
        ):
            registry_dict["dataSources"].append(
                self._message_to_sorted_dict(data_source.to_proto())
            )
        for entity in sorted(
            self.list_entities(project=project),
            key=lambda entity: entity.name,
        ):
            registry_dict["entities"].append(
                self._message_to_sorted_dict(entity.to_proto())
            )
        for feature_view in sorted(
            self.list_feature_views(project=project),
            key=lambda feature_view: feature_view.name,
        ):
            registry_dict["featureViews"].append(
                self._message_to_sorted_dict(feature_view.to_proto())
            )
        for feature_service in sorted(
            self.list_feature_services(project=project),
            key=lambda feature_service: feature_service.name,
        ):
            registry_dict["featureServices"].append(
                self._message_to_sorted_dict(feature_service.to_proto())
            )
        for on_demand_feature_view in sorted(
            self.list_on_demand_feature_views(project=project),
            key=lambda on_demand_feature_view: on_demand_feature_view.name,
        ):
            odfv_dict = self._message_to_sorted_dict(on_demand_feature_view.to_proto())
            # We are logging a warning because the registry object may be read from a proto that is not updated
            # i.e., we have to submit dual writes but in order to ensure the read behavior succeeds we have to load
            # both objects to compare any changes in the registry
            warnings.warn(
                "We will be deprecating the usage of spec.userDefinedFunction in a future release please upgrade cautiously.",
                DeprecationWarning,
            )
            if on_demand_feature_view.feature_transformation:
                if isinstance(
                    on_demand_feature_view.feature_transformation, PandasTransformation
                ):
                    if "userDefinedFunction" not in odfv_dict["spec"]:
                        odfv_dict["spec"]["userDefinedFunction"] = {}
                    odfv_dict["spec"]["userDefinedFunction"]["body"] = (
                        on_demand_feature_view.feature_transformation.udf_string
                    )
                    odfv_dict["spec"]["featureTransformation"]["userDefinedFunction"][
                        "body"
                    ] = on_demand_feature_view.feature_transformation.udf_string
                elif isinstance(
                    on_demand_feature_view.feature_transformation,
                    SubstraitTransformation,
                ):
                    odfv_dict["spec"]["featureTransformation"]["substraitPlan"][
                        "body"
                    ] = on_demand_feature_view.feature_transformation.substrait_plan
                else:
                    odfv_dict["spec"]["featureTransformation"]["userDefinedFunction"][
                        "body"
                    ] = None
                    odfv_dict["spec"]["featureTransformation"]["substraitPlan"][
                        "body"
                    ] = None
                registry_dict["onDemandFeatureViews"].append(odfv_dict)
        for stream_feature_view in sorted(
            self.list_stream_feature_views(project=project),
            key=lambda stream_feature_view: stream_feature_view.name,
        ):
            sfv_dict = self._message_to_sorted_dict(stream_feature_view.to_proto())

            sfv_dict["spec"]["userDefinedFunction"]["body"] = (
                stream_feature_view.udf_string
            )
            registry_dict["streamFeatureViews"].append(sfv_dict)

        for saved_dataset in sorted(
            self.list_saved_datasets(project=project), key=lambda item: item.name
        ):
            registry_dict["savedDatasets"].append(
                self._message_to_sorted_dict(saved_dataset.to_proto())
            )
        for infra_object in sorted(self.get_infra(project=project).infra_objects):
            registry_dict["infra"].append(
                self._message_to_sorted_dict(infra_object.to_proto())
            )
        for permission in sorted(
            self.list_permissions(project=project), key=lambda ds: ds.name
        ):
            registry_dict["permissions"].append(
                self._message_to_sorted_dict(permission.to_proto())
            )

        return registry_dict

    @staticmethod
    def deserialize_registry_values(serialized_proto, feast_obj_type) -> Any:
        if feast_obj_type == Entity:
            return EntityProto.FromString(serialized_proto)
        if feast_obj_type == SavedDataset:
            return SavedDatasetProto.FromString(serialized_proto)
        if feast_obj_type == FeatureView:
            return FeatureViewProto.FromString(serialized_proto)
        if feast_obj_type == StreamFeatureView:
            return StreamFeatureViewProto.FromString(serialized_proto)
        if feast_obj_type == OnDemandFeatureView:
            return OnDemandFeatureViewProto.FromString(serialized_proto)
        if feast_obj_type == FeatureService:
            return FeatureServiceProto.FromString(serialized_proto)
        if feast_obj_type == Permission:
            return PermissionProto.FromString(serialized_proto)
        if feast_obj_type == Project:
            return ProjectProto.FromString(serialized_proto)
        if issubclass(feast_obj_type, DataSource):
            return DataSourceProto.FromString(serialized_proto)
        return None
