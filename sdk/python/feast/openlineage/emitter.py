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

"""
Feast OpenLineage Emitter.

This module provides high-level functions for emitting OpenLineage events
from Feast operations like materialization, feature retrieval, and registry changes.
"""

import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from feast import FeatureService, FeatureView
    from feast.infra.registry.base_registry import BaseRegistry
    from feast.on_demand_feature_view import OnDemandFeatureView
    from feast.stream_feature_view import StreamFeatureView

from feast.openlineage.client import FeastOpenLineageClient
from feast.openlineage.config import OpenLineageConfig

try:
    from openlineage.client.event_v2 import (
        InputDataset,
        OutputDataset,
        RunState,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False

logger = logging.getLogger(__name__)


class FeastOpenLineageEmitter:
    """
    High-level emitter for Feast OpenLineage events.

    This class provides methods for emitting lineage events for various
    Feast operations including:
    - Registry apply (feature view, feature service definitions)
    - Materialization (batch and incremental)
    - Feature retrieval (online and historical)
    - Data source relationships

    Example:
        from feast import FeatureStore
        from feast.openlineage import FeastOpenLineageEmitter, OpenLineageConfig

        config = OpenLineageConfig(transport_type="http", transport_url="http://localhost:5000")
        emitter = FeastOpenLineageEmitter(config)

        fs = FeatureStore(repo_path="feature_repo")

        # Emit lineage for registry
        emitter.emit_registry_lineage(fs.registry, fs.project)
    """

    def __init__(
        self,
        config: Optional[OpenLineageConfig] = None,
        client: Optional[FeastOpenLineageClient] = None,
    ):
        """
        Initialize the Feast OpenLineage Emitter.

        Args:
            config: OpenLineage configuration
            client: Optional pre-configured FeastOpenLineageClient
        """
        self._config = config or OpenLineageConfig.from_env()
        self._client = client or FeastOpenLineageClient(self._config)

    @property
    def is_enabled(self) -> bool:
        """Check if the emitter is enabled."""
        return self._client.is_enabled

    @property
    def namespace(self) -> str:
        """Get the default namespace."""
        return self._config.namespace

    def _get_namespace(self, project: str) -> str:
        """
        Get the OpenLineage namespace for a project.

        By default, uses the Feast project name as the namespace.
        If an explicit namespace is configured (not the default "feast"),
        it will be used as a prefix: {namespace}/{project}

        Args:
            project: Feast project name

        Returns:
            OpenLineage namespace string
        """
        # If namespace is default "feast", just use project name
        if self._config.namespace == "feast":
            return project
        # If custom namespace is configured, use it as prefix
        return f"{self._config.namespace}/{project}"

    def emit_registry_lineage(
        self,
        registry: "BaseRegistry",
        project: str,
        allow_cache: bool = True,
    ) -> List[bool]:
        """
        Emit lineage events for all objects in a Feast registry.

        This method emits JobEvents for feature views and feature services,
        and DatasetEvents for data sources and entities.

        Args:
            registry: Feast registry
            project: Project name
            allow_cache: Whether to use cached registry data

        Returns:
            List of success/failure indicators for each event
        """
        if not self.is_enabled:
            return []

        results = []

        # Emit events for feature views
        try:
            feature_views = registry.list_feature_views(
                project=project, allow_cache=allow_cache
            )
            for fv in feature_views:
                result = self.emit_feature_view_lineage(fv, project)
                results.append(result)
        except Exception as e:
            logger.error(f"Error emitting feature view lineage: {e}")

        # Emit events for stream feature views
        try:
            stream_fvs = registry.list_stream_feature_views(
                project=project, allow_cache=allow_cache
            )
            for sfv in stream_fvs:
                result = self.emit_stream_feature_view_lineage(sfv, project)
                results.append(result)
        except Exception as e:
            logger.error(f"Error emitting stream feature view lineage: {e}")

        # Emit events for on-demand feature views
        try:
            odfvs = registry.list_on_demand_feature_views(
                project=project, allow_cache=allow_cache
            )
            for odfv in odfvs:
                result = self.emit_on_demand_feature_view_lineage(odfv, project)
                results.append(result)
        except Exception as e:
            logger.error(f"Error emitting on-demand feature view lineage: {e}")

        # Emit events for feature services
        try:
            feature_services = registry.list_feature_services(
                project=project, allow_cache=allow_cache
            )
            for fs in feature_services:
                result = self.emit_feature_service_lineage(fs, feature_views, project)
                results.append(result)
        except Exception as e:
            logger.error(f"Error emitting feature service lineage: {e}")

        logger.info(
            f"Emitted {sum(results)}/{len(results)} lineage events for registry"
        )
        return results

    def emit_feature_view_lineage(
        self,
        feature_view: "FeatureView",
        project: str,
    ) -> bool:
        """
        Emit lineage for a feature view definition.

        Args:
            feature_view: The feature view
            project: Project name

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from feast.openlineage.mappers import feature_view_to_job

        try:
            namespace = self._get_namespace(project)
            job, inputs, outputs = feature_view_to_job(
                feature_view,
                namespace=namespace,
            )

            # Emit a RunEvent with OTHER state (metadata registration, not execution)
            result = self._client.emit_run_event(
                job_name=job.name,
                run_id=str(uuid.uuid4()),
                event_type=RunState.OTHER,
                inputs=inputs,
                outputs=outputs,
                job_facets=job.facets,
                namespace=namespace,
            )
            return result
        except Exception as e:
            logger.error(
                f"Error emitting feature view lineage for {feature_view.name}: {e}"
            )
            return False

    def emit_stream_feature_view_lineage(
        self,
        stream_feature_view: "StreamFeatureView",
        project: str,
    ) -> bool:
        """
        Emit lineage for a stream feature view definition.

        Args:
            stream_feature_view: The stream feature view
            project: Project name

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from feast.openlineage.mappers import feature_view_to_job

        try:
            namespace = self._get_namespace(project)
            # StreamFeatureView inherits from FeatureView
            job, inputs, outputs = feature_view_to_job(
                stream_feature_view,
                namespace=namespace,
            )

            # Emit a RunEvent with OTHER state (metadata registration, not execution)
            return self._client.emit_run_event(
                job_name=f"stream_{job.name}",
                run_id=str(uuid.uuid4()),
                event_type=RunState.OTHER,
                inputs=inputs,
                outputs=outputs,
                job_facets=job.facets,
                namespace=namespace,
            )
        except Exception as e:
            logger.error(
                f"Error emitting stream feature view lineage for {stream_feature_view.name}: {e}"
            )
            return False

    def emit_on_demand_feature_view_lineage(
        self,
        odfv: "OnDemandFeatureView",
        project: str,
    ) -> bool:
        """
        Emit lineage for an on-demand feature view definition.

        Args:
            odfv: The on-demand feature view
            project: Project name

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from feast.openlineage.facets import FeastFeatureViewFacet
        from feast.openlineage.mappers import feast_field_to_schema_field

        try:
            from openlineage.client.facet_v2 import schema_dataset

            namespace = self._get_namespace(project)

            # Build inputs from sources
            inputs = []
            for source_name, fv_proj in odfv.source_feature_view_projections.items():
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=fv_proj.name,
                    )
                )

            for source_name, req_source in odfv.source_request_sources.items():
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=f"request_source_{source_name}",
                    )
                )

            # Build output
            output_facets = {}
            if odfv.features:
                output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
                    fields=[feast_field_to_schema_field(f) for f in odfv.features]
                )

            outputs = [
                OutputDataset(
                    namespace=namespace,
                    name=odfv.name,
                    facets=output_facets,  # type: ignore[arg-type]
                )
            ]

            # Build job facets
            job_facets = {
                "feast_featureView": FeastFeatureViewFacet(
                    name=odfv.name,
                    ttl_seconds=0,
                    entities=[],
                    features=[f.name for f in odfv.features] if odfv.features else [],
                    online_enabled=True,
                    offline_enabled=True,
                    mode="ON_DEMAND",
                    description=odfv.description if odfv.description else "",
                    owner=odfv.owner if hasattr(odfv, "owner") and odfv.owner else "",
                    tags=odfv.tags if odfv.tags else {},
                )
            }

            # Emit a RunEvent with OTHER state (metadata registration, not execution)
            return self._client.emit_run_event(
                job_name=f"on_demand_feature_view_{odfv.name}",
                run_id=str(uuid.uuid4()),
                event_type=RunState.OTHER,
                inputs=inputs,
                outputs=outputs,
                job_facets=job_facets,
                namespace=namespace,
            )
        except Exception as e:
            logger.error(
                f"Error emitting on-demand feature view lineage for {odfv.name}: {e}"
            )
            return False

    def emit_feature_service_lineage(
        self,
        feature_service: "FeatureService",
        feature_views: List["FeatureView"],
        project: str,
    ) -> bool:
        """
        Emit lineage for a feature service definition.

        Args:
            feature_service: The feature service
            feature_views: List of available feature views
            project: Project name

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from feast.openlineage.mappers import feature_service_to_job

        try:
            # Find the feature views referenced by this service
            namespace = self._get_namespace(project)
            fv_names = {proj.name for proj in feature_service.feature_view_projections}
            referenced_fvs = [fv for fv in feature_views if fv.name in fv_names]

            job, inputs, outputs = feature_service_to_job(
                feature_service,
                referenced_fvs,
                namespace=namespace,
            )

            # Emit a RunEvent with OTHER state (metadata registration, not execution)
            return self._client.emit_run_event(
                job_name=job.name,
                run_id=str(uuid.uuid4()),
                event_type=RunState.OTHER,
                inputs=inputs,
                outputs=outputs,
                job_facets=job.facets,
                namespace=namespace,
            )
        except Exception as e:
            logger.error(
                f"Error emitting feature service lineage for {feature_service.name}: {e}"
            )
            return False

    def emit_materialize_start(
        self,
        feature_views: List["FeatureView"],
        start_date: Optional[datetime],
        end_date: datetime,
        project: str,
        run_id: Optional[str] = None,
    ) -> Tuple[str, bool]:
        """
        Emit a START event for a materialization run.

        Args:
            feature_views: Feature views being materialized
            start_date: Start of materialization window (None for incremental)
            end_date: End of materialization window
            project: Project name
            run_id: Optional run ID (will be generated if not provided)

        Returns:
            Tuple of (run_id, success)
        """
        if not self.is_enabled or not self._config.emit_on_materialize:
            return "", False

        from feast.openlineage.facets import FeastMaterializationFacet
        from feast.openlineage.mappers import (
            data_source_to_dataset,
            online_store_to_dataset,
        )

        run_id = run_id or str(uuid.uuid4())

        try:
            namespace = self._get_namespace(project)

            # Build inputs (data sources) - include both batch and stream sources
            inputs = []
            seen_sources = set()  # Track source names to avoid duplicates

            for fv in feature_views:
                # Add batch source
                if hasattr(fv, "batch_source") and fv.batch_source:
                    source_name = getattr(fv.batch_source, "name", None)
                    if source_name and source_name not in seen_sources:
                        seen_sources.add(source_name)
                        inputs.append(
                            data_source_to_dataset(
                                fv.batch_source,
                                namespace=namespace,
                                as_input=True,
                            )
                        )

                # Add stream source (e.g., PushSource)
                if hasattr(fv, "stream_source") and fv.stream_source:
                    source_name = getattr(fv.stream_source, "name", None)
                    if source_name and source_name not in seen_sources:
                        seen_sources.add(source_name)
                        inputs.append(
                            data_source_to_dataset(
                                fv.stream_source,
                                namespace=namespace,
                                as_input=True,
                            )
                        )

                # Add entities as inputs
                if hasattr(fv, "entities") and fv.entities:
                    for entity_name in fv.entities:
                        if entity_name and entity_name != "__dummy":
                            entity_key = f"entity_{entity_name}"
                            if entity_key not in seen_sources:
                                seen_sources.add(entity_key)
                                inputs.append(
                                    InputDataset(
                                        namespace=namespace,
                                        name=entity_key,
                                    )
                                )

            # Build outputs (online store entries)
            outputs = [
                online_store_to_dataset(
                    store_type="online_store",
                    feature_view_name=fv.name,
                    namespace=namespace,
                )
                for fv in feature_views
            ]

            # Build run facets
            run_facets = {
                "feast_materialization": FeastMaterializationFacet(
                    feature_views=[fv.name for fv in feature_views],
                    start_date=start_date.isoformat() if start_date else None,
                    end_date=end_date.isoformat() if end_date else None,
                    project=project,
                )
            }

            success = self._client.emit_run_event(
                job_name=f"materialize_{project}",
                run_id=run_id,
                event_type=RunState.START,
                inputs=inputs,
                outputs=outputs,
                run_facets=run_facets,
                namespace=namespace,
            )

            return run_id, success
        except Exception as e:
            logger.error(f"Error emitting materialize start event: {e}")
            return run_id, False

    def emit_materialize_complete(
        self,
        run_id: str,
        feature_views: List["FeatureView"],
        project: str,
        rows_written: Optional[int] = None,
    ) -> bool:
        """
        Emit a COMPLETE event for a materialization run.

        Args:
            run_id: Run ID from the start event
            feature_views: Feature views that were materialized
            project: Project name
            rows_written: Optional count of rows written

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled or not self._config.emit_on_materialize:
            return False

        from feast.openlineage.facets import FeastMaterializationFacet
        from feast.openlineage.mappers import online_store_to_dataset

        try:
            namespace = self._get_namespace(project)

            outputs = [
                online_store_to_dataset(
                    store_type="online_store",
                    feature_view_name=fv.name,
                    namespace=namespace,
                )
                for fv in feature_views
            ]

            run_facets = {
                "feast_materialization": FeastMaterializationFacet(
                    feature_views=[fv.name for fv in feature_views],
                    project=project,
                    rows_written=rows_written,
                )
            }

            return self._client.emit_run_event(
                job_name=f"materialize_{project}",
                run_id=run_id,
                event_type=RunState.COMPLETE,
                outputs=outputs,
                run_facets=run_facets,
                namespace=namespace,
            )
        except Exception as e:
            logger.error(f"Error emitting materialize complete event: {e}")
            return False

    def emit_materialize_fail(
        self,
        run_id: str,
        project: str,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Emit a FAIL event for a materialization run.

        Args:
            run_id: Run ID from the start event
            project: Project name
            error_message: Optional error message

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled or not self._config.emit_on_materialize:
            return False

        try:
            from openlineage.client.facet_v2 import error_message_run

            namespace = self._get_namespace(project)
            run_facets = {}
            if error_message:
                run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=error_message,
                    programmingLanguage="python",
                )

            return self._client.emit_run_event(
                job_name=f"materialize_{project}",
                run_id=run_id,
                event_type=RunState.FAIL,
                run_facets=run_facets,
                namespace=namespace,
            )
        except Exception as e:
            logger.error(f"Error emitting materialize fail event: {e}")
            return False

    def emit_apply(
        self,
        objects: List[Any],
        project: str,
    ) -> List[bool]:
        """
        Emit lineage for a feast apply operation.

        Creates two jobs to match Feast UI lineage model:
        1. feast_feature_views_{project}: DataSources + Entities → FeatureViews
        2. feast_feature_services_{project}: FeatureViews → FeatureServices

        This creates a lineage graph matching Feast UI:
            DataSource ──→ FeatureView ──→ FeatureService
                 ↑
              Entity

        Args:
            objects: List of Feast objects being applied
            project: Project name

        Returns:
            List of success/failure indicators
        """
        if not self.is_enabled or not self._config.emit_on_apply:
            return []

        from feast import Entity, FeatureService
        from feast.data_source import DataSource
        from feast.feature_view import FeatureView
        from feast.on_demand_feature_view import OnDemandFeatureView
        from feast.openlineage.facets import FeastProjectFacet
        from feast.openlineage.mappers import (
            data_source_to_dataset,
            entity_to_dataset,
            feast_field_to_schema_field,
        )
        from feast.stream_feature_view import StreamFeatureView

        try:
            from openlineage.client.facet_v2 import schema_dataset

            namespace = self._get_namespace(project)
            results = []

            # Categorize objects
            data_sources: List[DataSource] = []
            entities: List[Entity] = []
            feature_views: List[Union[FeatureView, OnDemandFeatureView]] = []
            on_demand_feature_views: List[OnDemandFeatureView] = []
            feature_services: List[FeatureService] = []

            for obj in objects:
                if isinstance(obj, StreamFeatureView):
                    feature_views.append(obj)
                elif isinstance(obj, OnDemandFeatureView):
                    on_demand_feature_views.append(obj)
                elif isinstance(obj, FeatureView):
                    feature_views.append(obj)
                elif isinstance(obj, FeatureService):
                    feature_services.append(obj)
                elif isinstance(obj, DataSource):
                    data_sources.append(obj)
                elif isinstance(obj, Entity):
                    if obj.name != "__dummy":
                        entities.append(obj)

            # ============================================================
            # Job 1: DataSources + Entities → FeatureViews
            # This matches: DataSource → FeatureView and Entity → FeatureView
            # ============================================================
            if feature_views or on_demand_feature_views:
                fv_inputs = []
                seen_inputs: set = set()

                # Add explicit data sources
                for ds in data_sources:
                    if ds.name and ds.name not in seen_inputs:
                        seen_inputs.add(ds.name)
                        fv_inputs.append(
                            data_source_to_dataset(
                                ds, namespace=namespace, as_input=True
                            )
                        )

                # Add entities (using direct name to match Feast UI)
                for entity in entities:
                    if entity.name not in seen_inputs:
                        seen_inputs.add(entity.name)
                        fv_inputs.append(entity_to_dataset(entity, namespace=namespace))

                # Also add data sources from feature views
                for fv in feature_views:
                    if hasattr(fv, "batch_source") and fv.batch_source:
                        source_name = getattr(fv.batch_source, "name", None)
                        if source_name and source_name not in seen_inputs:
                            seen_inputs.add(source_name)
                            fv_inputs.append(
                                data_source_to_dataset(
                                    fv.batch_source, namespace=namespace, as_input=True
                                )
                            )
                    if hasattr(fv, "stream_source") and fv.stream_source:
                        source_name = getattr(fv.stream_source, "name", None)
                        if source_name and source_name not in seen_inputs:
                            seen_inputs.add(source_name)
                            fv_inputs.append(
                                data_source_to_dataset(
                                    fv.stream_source, namespace=namespace, as_input=True
                                )
                            )

                # Build FeatureView outputs
                from openlineage.client.facet_v2 import documentation_dataset

                from feast.openlineage.facets import FeastFeatureViewFacet

                fv_outputs = []
                for fv in feature_views:
                    output_facets: Dict[str, Any] = {}

                    # Add schema with features (includes tags in description)
                    if fv.features:
                        output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
                            fields=[feast_field_to_schema_field(f) for f in fv.features]
                        )

                    # Add documentation facet with description
                    if hasattr(fv, "description") and fv.description:
                        output_facets["documentation"] = (
                            documentation_dataset.DocumentationDatasetFacet(
                                description=fv.description
                            )
                        )

                    # Add Feast-specific facet with full metadata
                    ttl_seconds = 0
                    if hasattr(fv, "ttl") and fv.ttl:
                        ttl_seconds = int(fv.ttl.total_seconds())

                    output_facets["feast_featureView"] = FeastFeatureViewFacet(
                        name=fv.name,
                        ttl_seconds=ttl_seconds,
                        entities=list(fv.entities)
                        if hasattr(fv, "entities") and fv.entities
                        else [],
                        features=[f.name for f in fv.features] if fv.features else [],
                        online_enabled=fv.online if hasattr(fv, "online") else True,
                        description=fv.description
                        if hasattr(fv, "description")
                        else "",
                        owner=fv.owner if hasattr(fv, "owner") else "",
                        tags=fv.tags if hasattr(fv, "tags") else {},
                    )

                    fv_outputs.append(
                        OutputDataset(
                            namespace=namespace,
                            name=fv.name,
                            facets=output_facets,
                        )
                    )

                for odfv in on_demand_feature_views:
                    output_facets = {}

                    # Add schema with features (includes tags in description)
                    if odfv.features:
                        output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
                            fields=[
                                feast_field_to_schema_field(f) for f in odfv.features
                            ]
                        )

                    # Add documentation facet with description
                    if hasattr(odfv, "description") and odfv.description:
                        output_facets["documentation"] = (
                            documentation_dataset.DocumentationDatasetFacet(
                                description=odfv.description
                            )
                        )

                    # Add Feast-specific facet with full metadata
                    output_facets["feast_featureView"] = FeastFeatureViewFacet(
                        name=odfv.name,
                        ttl_seconds=0,
                        entities=list(odfv.entities)
                        if hasattr(odfv, "entities") and odfv.entities
                        else [],
                        features=[f.name for f in odfv.features]
                        if odfv.features
                        else [],
                        online_enabled=True,
                        description=odfv.description
                        if hasattr(odfv, "description")
                        else "",
                        owner=odfv.owner if hasattr(odfv, "owner") else "",
                        tags=odfv.tags if hasattr(odfv, "tags") else {},
                    )

                    fv_outputs.append(
                        OutputDataset(
                            namespace=namespace,
                            name=odfv.name,
                            facets=output_facets,
                        )
                    )

                # Emit Job 1: Feature Views job
                job_facets = {
                    "feast_project": FeastProjectFacet(
                        project_name=project,
                    )
                }

                result1 = self._client.emit_run_event(
                    job_name=f"feast_feature_views_{project}",
                    run_id=str(uuid.uuid4()),
                    event_type=RunState.OTHER,
                    inputs=fv_inputs,
                    outputs=fv_outputs,
                    job_facets=job_facets,
                    namespace=namespace,
                )
                results.append(result1)

                if result1:
                    logger.info(
                        f"✓ Emitted feature views lineage for '{project}' "
                        f"({len(fv_inputs)} inputs → {len(fv_outputs)} outputs)"
                    )

            # ============================================================
            # Jobs for FeatureServices: One job per FeatureService
            # Each job shows: FeatureViews (that are part of this FS) → FeatureService
            # This matches Feast UI where links are only shown for actual membership
            # ============================================================
            for fs in feature_services:
                fs_inputs = []
                all_fs_features = []  # Collect all features for schema
                fv_names_in_fs = []  # Track feature view names

                # Only include FeatureViews that are actually part of this FeatureService
                for proj in fs.feature_view_projections:
                    fv_name = proj.name
                    if fv_name:
                        fv_names_in_fs.append(fv_name)
                        # Find the feature view to get schema
                        input_facets: Dict[str, Any] = {}

                        # Use projection features if specified, otherwise use all from FV
                        proj_features = proj.features if proj.features else []

                        for fv in feature_views + on_demand_feature_views:
                            if fv.name == fv_name:
                                # Use projection features if available, else all FV features
                                features_to_use = (
                                    proj_features
                                    if proj_features
                                    else (fv.features if fv.features else [])
                                )
                                if features_to_use:
                                    input_facets["schema"] = (
                                        schema_dataset.SchemaDatasetFacet(
                                            fields=[
                                                feast_field_to_schema_field(f)
                                                for f in features_to_use
                                            ]
                                        )
                                    )
                                    # Collect features for FS output schema
                                    all_fs_features.extend(features_to_use)
                                break

                        fs_inputs.append(
                            InputDataset(
                                namespace=namespace,
                                name=fv_name,
                                facets=input_facets,
                            )
                        )

                # Build FeatureService output with schema and metadata
                fs_output_facets: Dict[str, Any] = {}

                # Add schema with all features from constituent feature views
                if all_fs_features:
                    fs_output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
                        fields=[feast_field_to_schema_field(f) for f in all_fs_features]
                    )

                # Add documentation with feature view list
                if fv_names_in_fs:
                    from openlineage.client.facet_v2 import documentation_dataset

                    fs_output_facets["documentation"] = (
                        documentation_dataset.DocumentationDatasetFacet(
                            description=(
                                f"Feature Service '{fs.name}' aggregates features from: "
                                f"{', '.join(fv_names_in_fs)}. "
                                f"Total features: {len(all_fs_features)}."
                            )
                        )
                    )

                # Add Feast-specific facet with detailed metadata
                from feast.openlineage.facets import FeastFeatureServiceFacet

                fs_output_facets["feast_featureService"] = FeastFeatureServiceFacet(
                    name=fs.name,
                    feature_views=fv_names_in_fs,
                    feature_count=len(all_fs_features),
                    description=fs.description if fs.description else "",
                    owner=fs.owner if fs.owner else "",
                    tags=fs.tags if fs.tags else {},
                    logging_enabled=getattr(fs, "logging", None) is not None,
                )

                fs_output = OutputDataset(
                    namespace=namespace,
                    name=fs.name,
                    facets=fs_output_facets,
                )

                # Emit a job for this specific FeatureService
                job_facets = {
                    "feast_project": FeastProjectFacet(
                        project_name=project,
                    )
                }

                result = self._client.emit_run_event(
                    job_name=f"feature_service_{fs.name}",  # Prefix to avoid conflict with dataset
                    run_id=str(uuid.uuid4()),
                    event_type=RunState.OTHER,
                    inputs=fs_inputs,
                    outputs=[fs_output],
                    job_facets=job_facets,
                    namespace=namespace,
                )
                results.append(result)

            return results

        except Exception as e:
            logger.error(f"Error emitting project lineage for {project}: {e}")
            return [False]

    def close(self):
        """Close the underlying client."""
        self._client.close()
