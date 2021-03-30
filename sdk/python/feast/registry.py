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

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryFile
from typing import Callable, List
from urllib.parse import urlparse

from google.auth.exceptions import DefaultCredentialsError

from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto

REGISTRY_SCHEMA_VERSION = "1"


class Registry:
    """
    Registry: A registry allows for the management and persistence of feature definitions and related metadata.
    """

    def __init__(self, registry_path: str):
        """
        Create the Registry object.

        Args:
            registry_path: filepath or GCS URI that is the location of the object store registry,
            or where it will be created if it does not exist yet.
        """
        uri = urlparse(registry_path)
        if uri.scheme == "gs":
            self._registry_store: RegistryStore = GCSRegistryStore(registry_path)
        elif uri.scheme == "file" or uri.scheme == "":
            self._registry_store = LocalRegistryStore(registry_path)
        else:
            raise Exception(
                f"Registry path {registry_path} has unsupported scheme {uri.scheme}. Supported schemes are file and gs."
            )
        return

    def apply_entity(self, entity: Entity, project: str):
        """
        Registers a single entity with Feast

        Args:
            entity: Entity that will be registered
            project: Feast project that this entity belongs to
        """
        entity.is_valid()
        entity_proto = entity.to_proto()
        entity_proto.spec.project = project

        def updater(registry_proto: RegistryProto):
            for idx, existing_entity_proto in enumerate(registry_proto.entities):
                if (
                    existing_entity_proto.spec.name == entity_proto.spec.name
                    and existing_entity_proto.spec.project == project
                ):
                    del registry_proto.entities[idx]
                    registry_proto.entities.append(entity_proto)
                    return registry_proto
            registry_proto.entities.append(entity_proto)
            return registry_proto

        self._registry_store.update_registry(updater)
        return

    def list_entities(self, project: str) -> List[Entity]:
        """
        Retrieve a list of entities from the registry

        Args:
            project: Filter entities based on project name

        Returns:
            List of entities
        """
        registry_proto = self._registry_store.get_registry()
        entities = []
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.project == project:
                entities.append(Entity.from_proto(entity_proto))
        return entities

    def get_entity(self, name: str, project: str) -> Entity:
        """
        Retrieves an entity.

        Args:
            name: Name of entity
            project: Feast project that this entity belongs to

        Returns:
            Returns either the specified entity, or raises an exception if
            none is found
        """
        registry_proto = self._registry_store.get_registry()
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.name == name and entity_proto.spec.project == project:
                return Entity.from_proto(entity_proto)
        raise Exception(f"Entity {name} does not exist in project {project}")

    def apply_feature_table(self, feature_table: FeatureTable, project: str):
        """
        Registers a single feature table with Feast

        Args:
            feature_table: Feature table that will be registered
            project: Feast project that this feature table belongs to
        """
        feature_table.is_valid()
        feature_table_proto = feature_table.to_proto()
        feature_table_proto.spec.project = project

        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_table_proto in enumerate(
                registry_proto.feature_tables
            ):
                if (
                    existing_feature_table_proto.spec.name
                    == feature_table_proto.spec.name
                    and existing_feature_table_proto.spec.project == project
                ):
                    del registry_proto.feature_tables[idx]
                    registry_proto.feature_tables.append(feature_table_proto)
                    return registry_proto
            registry_proto.feature_tables.append(feature_table_proto)
            return registry_proto

        self._registry_store.update_registry(updater)
        return

    def apply_feature_view(self, feature_view: FeatureView, project: str):
        """
        Registers a single feature view with Feast

        Args:
            feature_view: Feature view that will be registered
            project: Feast project that this feature view belongs to
        """
        feature_view.is_valid()
        feature_view_proto = feature_view.to_proto()
        feature_view_proto.spec.project = project

        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_view_proto in enumerate(
                registry_proto.feature_views
            ):
                if (
                    existing_feature_view_proto.spec.name
                    == feature_view_proto.spec.name
                    and existing_feature_view_proto.spec.project == project
                ):
                    del registry_proto.feature_views[idx]
                    registry_proto.feature_views.append(feature_view_proto)
                    return registry_proto
            registry_proto.feature_views.append(feature_view_proto)
            return registry_proto

        self._registry_store.update_registry(updater)

    def list_feature_tables(self, project: str) -> List[FeatureTable]:
        """
        Retrieve a list of feature tables from the registry

        Args:
            project: Filter feature tables based on project name

        Returns:
            List of feature tables
        """
        registry_proto = self._registry_store.get_registry()
        feature_tables = []
        for feature_table_proto in registry_proto.feature_tables:
            if feature_table_proto.spec.project == project:
                feature_tables.append(FeatureTable.from_proto(feature_table_proto))
        return feature_tables

    def list_feature_views(self, project: str) -> List[FeatureView]:
        """
        Retrieve a list of feature views from the registry

        Args:
            project: Filter feature tables based on project name

        Returns:
            List of feature views
        """
        registry_proto = self._registry_store.get_registry()
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
        registry_proto = self._registry_store.get_registry()
        for feature_table_proto in registry_proto.feature_tables:
            if (
                feature_table_proto.spec.name == name
                and feature_table_proto.spec.project == project
            ):
                return FeatureTable.from_proto(feature_table_proto)
        raise Exception(f"Feature table {name} does not exist in project {project}")

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
        registry_proto = self._registry_store.get_registry()
        for feature_view_proto in registry_proto.feature_views:
            if (
                feature_view_proto.spec.name == name
                and feature_view_proto.spec.project == project
            ):
                return FeatureView.from_proto(feature_view_proto)
        raise Exception(f"Feature view {name} does not exist in project {project}")

    def delete_feature_table(self, name: str, project: str):
        """
        Deletes a feature table or raises an exception if not found.

        Args:
            name: Name of feature table
            project: Feast project that this feature table belongs to
        """

        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_table_proto in enumerate(
                registry_proto.feature_tables
            ):
                if (
                    existing_feature_table_proto.spec.name == name
                    and existing_feature_table_proto.spec.project == project
                ):
                    del registry_proto.feature_tables[idx]
                    return registry_proto
            raise Exception(f"Feature table {name} does not exist in project {project}")

        self._registry_store.update_registry(updater)
        return

    def delete_feature_view(self, name: str, project: str):
        """
        Deletes a feature view or raises an exception if not found.

        Args:
            name: Name of feature view
            project: Feast project that this feature view belongs to
        """

        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_view_proto in enumerate(
                registry_proto.feature_views
            ):
                if (
                    existing_feature_view_proto.spec.name == name
                    and existing_feature_view_proto.spec.project == project
                ):
                    del registry_proto.feature_views[idx]
                    return registry_proto
            raise Exception(f"Feature view {name} does not exist in project {project}")

        self._registry_store.update_registry(updater)


class RegistryStore(ABC):
    """
    RegistryStore: abstract base class implemented by specific backends (local file system, GCS)
    containing lower level methods used by the Registry class that are backend-specific.
    """

    @abstractmethod
    def get_registry(self):
        """
        Retrieves the registry proto from the registry path. If there is no file at that path,
        returns an empty registry proto.

        Returns:
            Returns either the registry proto stored at the registry path, or an empty registry proto.
        """
        pass

    @abstractmethod
    def update_registry(self, updater: Callable[[RegistryProto], RegistryProto]):
        """
        Updates the registry using the function passed in. If the registry proto has not been created yet
        this method will create it. This method writes to the registry path.

        Args:
            updater: function that takes in the current registry proto and outputs the desired registry proto
        """
        pass


class LocalRegistryStore(RegistryStore):
    def __init__(self, filepath: str):
        self._filepath = Path(filepath)
        return

    def get_registry(self):
        registry_proto = RegistryProto()
        if self._filepath.exists():
            registry_proto.ParseFromString(self._filepath.read_bytes())
        else:
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
        return registry_proto

    def update_registry(self, updater: Callable[[RegistryProto], RegistryProto]):
        registry_proto = self.get_registry()
        registry_proto = updater(registry_proto)
        self._write_registry(registry_proto)
        return

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        self._filepath.write_bytes(registry_proto.SerializeToString())
        return


class GCSRegistryStore(RegistryStore):
    def __init__(self, uri: str):
        try:
            from google.cloud import storage
        except ImportError:
            # TODO: Ensure versioning depends on requirements.txt/setup.py and isn't hardcoded
            raise ImportError(
                "Install package google-cloud-storage==1.20.* for gcs support"
                "run ```pip install google-cloud-storage==1.20.*```"
            )
        try:
            self.gcs_client = storage.Client()
        except DefaultCredentialsError:
            self.gcs_client = storage.Client.create_anonymous_client()
        self._uri = urlparse(uri)
        self._bucket = self._uri.hostname
        self._blob = self._uri.path.lstrip("/")
        return

    def get_registry(self):
        from google.cloud import storage
        from google.cloud.exceptions import NotFound

        file_obj = TemporaryFile()
        registry_proto = RegistryProto()
        try:
            bucket = self.gcs_client.get_bucket(self._bucket)
        except NotFound:
            raise Exception(
                f"No bucket named {self._bucket} exists; please create it first."
            )
        if storage.Blob(bucket=bucket, name=self._blob).exists(self.gcs_client):
            self.gcs_client.download_blob_to_file(self._uri.geturl(), file_obj)
            file_obj.seek(0)
            registry_proto.ParseFromString(file_obj.read())
        else:
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
        return registry_proto

    def update_registry(self, updater: Callable[[RegistryProto], RegistryProto]):
        registry_proto = self.get_registry()
        registry_proto = updater(registry_proto)
        self._write_registry(registry_proto)
        return

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version_id = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        # we have already checked the bucket exists so no need to do it again
        gs_bucket = self.gcs_client.get_bucket(self._bucket)
        blob = gs_bucket.blob(self._blob)
        file_obj = TemporaryFile()
        file_obj.write(registry_proto.SerializeToString())
        file_obj.seek(0)
        blob.upload_from_file(file_obj)
        return
