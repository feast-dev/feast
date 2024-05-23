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

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import InfraObject
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig


class OnlineStore(ABC):
    """
    The interface that Feast uses to interact with the storage system that handles online features.
    """

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

    @abstractmethod
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
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
