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

"""Unity Catalog Provider — Feast lifecycle governance hooks.

Detects UnityCatalogSource in feature views during ``feast apply`` and:
1. Registers/updates feature tables in Unity Catalog
2. Syncs lineage from source tables to feature tables
3. Sets Feast metadata as UC table properties

Usage in feature_store.yaml:
    provider: feast.infra.data_sources.contrib.iceberg_catalog.uc_provider.UnityCatalogProvider
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

from feast.base_feature_view import BaseFeatureView
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.infra.data_sources.contrib.iceberg_catalog.unity_catalog_source import (
    UnityCatalogSource,
)
from feast.infra.passthrough_provider import PassthroughProvider

logger = logging.getLogger(__name__)


class UnityCatalogProvider(PassthroughProvider):
    """Provider that hooks into Feast lifecycle to sync with Unity Catalog.

    Extends PassthroughProvider to detect UnityCatalogSource in feature views
    and trigger UC-specific governance operations during feast apply/teardown.
    Standard offline/online store operations are delegated to the configured stores.
    """

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

        self._register_uc_feature_tables(project, tables_to_keep)
        self._cleanup_uc_feature_tables(project, tables_to_delete)

    def _register_uc_feature_tables(
        self, project: str, feature_views: Sequence[BaseFeatureView]
    ):
        for fv in feature_views:
            if not isinstance(fv, FeatureView):
                continue
            source = fv.batch_source
            if not isinstance(source, UnityCatalogSource):
                continue
            if not source.register_as_feature_table:
                continue

            logger.info(f"Registering UC feature table for: {fv.name} -> {source.fqn}")

            uc = source.get_uc_client()

            entity_columns = []
            if hasattr(fv, "entity_columns"):
                entity_columns = [col.name for col in fv.entity_columns]
            elif hasattr(fv, "entities"):
                entity_columns = list(fv.entities)

            columns = []
            if hasattr(fv, "schema") and fv.schema:
                columns = [
                    {"name": field.name, "type": str(field.dtype)}
                    for field in fv.schema
                ]

            properties: Dict[str, str] = {
                "feast.project": project,
                "feast.feature_view": fv.name,
            }
            if fv.owner:
                properties["feast.owner"] = fv.owner
            if fv.description:
                properties["feast.description"] = fv.description
            if fv.tags:
                for k, v in fv.tags.items():
                    properties[f"feast.tag.{k}"] = v

            success = uc.register_feature_table(
                fqn=source.fqn,
                primary_keys=entity_columns,
                columns=columns,
                properties=properties,
            )

            if success and source.sync_lineage:
                self._sync_lineage(uc, source, fv)

    def _sync_lineage(self, uc: Any, source: UnityCatalogSource, fv: FeatureView):
        source_fqns: List[str] = []

        if hasattr(fv, "stream_source") and fv.stream_source:
            if isinstance(fv.stream_source, UnityCatalogSource):
                stream_fqn = fv.stream_source.fqn
                if stream_fqn != source.fqn:
                    source_fqns.append(stream_fqn)

        for input_fv in getattr(fv, "source_feature_view_projections", {}).values():
            input_source = getattr(input_fv, "batch_source", None)
            if isinstance(input_source, UnityCatalogSource):
                if input_source.fqn != source.fqn:
                    source_fqns.append(input_source.fqn)

        if source_fqns:
            uc.record_lineage(
                target_fqn=source.fqn,
                source_fqns=source_fqns,
            )

    def _cleanup_uc_feature_tables(
        self, project: str, feature_views: Sequence[BaseFeatureView]
    ):
        for fv in feature_views:
            if not isinstance(fv, FeatureView):
                continue
            source = fv.batch_source
            if not isinstance(source, UnityCatalogSource):
                continue
            if not source.register_as_feature_table:
                continue

            logger.info(
                f"Feature view {fv.name} deleted. UC table {source.fqn} metadata "
                f"will be preserved (table not dropped)."
            )
            uc = source.get_uc_client()
            uc.set_table_properties(
                fqn=source.fqn,
                properties={
                    "feast.is_feature_table": "false",
                    "feast.deleted_at": datetime.now(timezone.utc).isoformat(),
                },
            )
