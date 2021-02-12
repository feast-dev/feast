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

from abc import ABC, abstractmethod
from datetime import datetime
from feast.core.Registry_pb2 import Registry as RegistryProto
from feast.entity import Entity
from feast.feature_table import FeatureTable
from pathlib import Path
from typing import Callable
import uuid

REGISTRY_SCHEMA_VERSION = 1


class Registry:
    def __init__(self, registry_path: str):
        self._registry_store = LocalRegistryStore(registry_path)
        return

    def apply_entity(self, entity: Entity):
        entity.is_valid()
        entity_proto = entity.to_proto()
        def updater(registry_proto: RegistryProto):
            for idx, existing_entity_proto in enumerate(registry_proto.entities):
                if existing_entity_proto.spec.name == entity_proto.spec.name:
                    registry_proto.entities[idx] = entity_proto
                    return registry_proto
            registry_proto.entities.append(entity_proto)
            return registry_proto
        self._registry_store.update_registry(updater)
        return

    def list_entities(self):
        registry_proto = self._registry_store.get_registry()
        entities = []
        for entity_proto in registry_proto.entities:
            entities.append(Entity.from_proto(entity_proto))
        return entities

    def get_entity(self, name: str):
        registry_proto = self._registry_store.get_registry()
        for entity_proto in registry_proto.entities:
            if entity_proto.spec.name == name:
                return Entity.from_proto(entity_proto)
        raise Exception(f"Entity {name} does not exist")

    def apply_feature_table(self, feature_table: FeatureTable):
        feature_table.is_valid()
        feature_table_proto = feature_table.to_proto()
        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_table_proto in enumerate(registry_proto.feature_tables):
                if existing_feature_table_proto.spec.name == feature_table_proto.spec.name:
                    registry_proto.feature_tables[idx] = feature_table_proto
                    return registry_proto
            registry_proto.feature_tables.append(feature_table_proto)
            return registry_proto
        self._registry_store.update_registry(updater)
        return

    def list_feature_tables(self):
        registry_proto = self._registry_store.get_registry()
        feature_tables = []
        for feature_table_proto in registry_proto.feature_tables:
            feature_tables.append(FeatureTable.from_proto(feature_table_proto))
        return feature_tables

    def get_feature_table(self, name: str):
        registry_proto = self._registry_store.get_registry()
        for feature_table_proto in registry_proto.feature_tables:
            if feature_table_proto.spec.name == name:
                return FeatureTable.from_proto(feature_table_proto)
        raise Exception(f"Feature table {name} does not exist")

    def delete_feature_table(self, name: str):
        def updater(registry_proto: RegistryProto):
            for idx, existing_feature_table_proto in enumerate(registry_proto.feature_tables):
                if existing_feature_table_proto.spec.name == name:
                    del registry_proto.feature_tables[idx]
                    return registry_proto
            raise Exception(f"Feature table {name} does not exist")
        self._registry_store.update_registry(updater)
        return


class RegistryStore(ABC):
    @abstractmethod
    def get_registry(self):
        pass

    @abstractmethod
    def update_registry(self, updater: Callable[[RegistryProto], RegistryProto]):
        pass


class LocalRegistryStore(RegistryStore):
    def __init__(self, filepath: str):
        self._filepath = Path(filepath)
        if not self._filepath.exists():
            registry_proto = RegistryProto()
            registry_proto.registry_schema_version = REGISTRY_SCHEMA_VERSION
            self._write_registry(registry_proto)
        return

    def get_registry(self):
        registry_proto = RegistryProto()
        registry_proto.ParseFromString(self._filepath.read_bytes())
        return registry_proto

    def update_registry(self, updater: Callable[[RegistryProto], RegistryProto]):
        registry_proto = self.get_registry()
        registry_proto = updater(registry_proto)
        self._write_registry(registry_proto)
        return

    def _write_registry(self, registry_proto: RegistryProto):
        registry_proto.version = str(uuid.uuid4())
        registry_proto.last_updated.FromDatetime(datetime.utcnow())
        self._filepath.write_bytes(registry_proto.SerializeToString())
        return
