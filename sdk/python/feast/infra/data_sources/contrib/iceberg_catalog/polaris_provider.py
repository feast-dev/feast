# Copyright 2026 The Feast Authors
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

"""Polaris Provider: records Feast metadata on Polaris tables.

Detects :class:`PolarisSource` in feature views during ``feast apply`` and sets
``feast.*`` table properties through the standard Iceberg REST ``updateTable``
operation. Tables are never dropped; when a feature view is deleted the
properties are updated to mark the table as no longer a feature table.

Usage in feature_store.yaml::

    provider: polaris
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Sequence

from feast.base_feature_view import BaseFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.data_sources.contrib.iceberg_catalog.polaris_source import (
    PolarisSource,
)
from feast.infra.passthrough_provider import PassthroughProvider

logger = logging.getLogger(__name__)

TAG_PREFIX = "feast.tag."


class PolarisProvider(PassthroughProvider):
    """Provider that syncs Feast metadata to Polaris on ``feast apply``."""

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[BaseFeatureView],
        tables_to_keep: Sequence[BaseFeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        super().update_infra(
            project,
            tables_to_delete,
            tables_to_keep,
            entities_to_delete,
            entities_to_keep,
            partial,
        )
        self._sync_polaris_properties(project, tables_to_keep)
        self._mark_polaris_tables_removed(tables_to_delete)

    @staticmethod
    def _polaris_source(view: BaseFeatureView):
        if not isinstance(view, FeatureView):
            return None
        source = view.batch_source
        if isinstance(source, PolarisSource) and source.sync_metadata:
            return source
        return None

    @staticmethod
    def build_properties(project: str, view: FeatureView) -> Dict[str, str]:
        properties: Dict[str, str] = {
            "feast.is_feature_table": "true",
            "feast.project": project,
            "feast.feature_view": view.name,
        }
        if view.owner:
            properties["feast.owner"] = view.owner
        if view.description:
            properties["feast.description"] = view.description
        for key, value in (view.tags or {}).items():
            properties[f"{TAG_PREFIX}{key}"] = value
        return properties

    def _sync_polaris_properties(
        self, project: str, views: Sequence[BaseFeatureView]
    ) -> None:
        for view in views:
            source = self._polaris_source(view)
            if source is None:
                continue
            assert isinstance(view, FeatureView)
            logger.info("Syncing Feast metadata for %s to %s", view.name, source.fqn)
            client = source.get_catalog_client()
            client.set_table_properties(
                source.namespace,
                source.iceberg_table,
                self.build_properties(project, view),
            )

    def _mark_polaris_tables_removed(self, views: Sequence[BaseFeatureView]) -> None:
        for view in views:
            source = self._polaris_source(view)
            if source is None:
                continue
            logger.info(
                "Feature view %s deleted; keeping Polaris table %s and marking it",
                view.name,
                source.fqn,
            )
            client = source.get_catalog_client()
            client.set_table_properties(
                source.namespace,
                source.iceberg_table,
                {
                    "feast.is_feature_table": "false",
                    "feast.deleted_at": datetime.now(timezone.utc).isoformat(),
                },
            )
