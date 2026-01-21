"""
OpenLineage client for emitting lineage events from Feast operations.

This module provides functionality to emit standardized OpenLineage events
for Feast feature materialization, retrieval, and other data operations.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from feast.batch_feature_view import BatchFeatureView
from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.repo_config import OpenLineageConfig

_logger = logging.getLogger(__name__)


class OpenLineageClient:
    """
    Client for emitting OpenLineage events from Feast operations.
    
    This client wraps the OpenLineage Python SDK and provides Feast-specific
    functionality for creating and emitting lineage events.
    """

    def __init__(self, config: OpenLineageConfig):
        """
        Initialize the OpenLineage client.
        
        Args:
            config: OpenLineage configuration object
        """
        self.config = config
        self._client = None
        self._enabled = config.enabled  # Store enabled state locally
        
        if not self._enabled:
            _logger.debug("OpenLineage is disabled, skipping client initialization")
            return
            
        try:
            from openlineage.client import OpenLineageClient as OLClient
            from openlineage.client.transport import get_default_factory
            
            # Create transport based on configuration
            transport_factory = get_default_factory()
            transport_config = {
                "type": config.transport_type,
                **config.transport_config
            }
            
            transport = transport_factory.create(transport_config)
            self._client = OLClient(transport=transport)
            _logger.info(f"OpenLineage client initialized with transport: {config.transport_type}")
            
        except ImportError as e:
            _logger.warning(
                f"OpenLineage Python client not available: {e}. "
                "Install with: pip install openlineage-python"
            )
            self._enabled = False  # Disable locally instead of mutating config
        except Exception as e:
            _logger.error(f"Failed to initialize OpenLineage client: {e}")
            self._enabled = False  # Disable locally instead of mutating config

    def emit_materialize_start_event(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        project: str,
        run_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Emit a START event for feature materialization.
        
        Args:
            feature_view: The feature view being materialized
            start_date: Start of the materialization time range
            end_date: End of the materialization time range
            project: The Feast project name
            run_id: Optional run ID to use (generated if not provided)
            
        Returns:
            The run ID for this materialization, or None if emission failed
        """
        if not self._enabled or not self.config.emit_materialization_events:
            return None
            
        if not self._client:
            return None
            
        try:
            from openlineage.client.run import (
                RunEvent,
                RunState,
                Run,
                Job,
            )
            from openlineage.client.facet import (
                NominalTimeRunFacet,
                JobFacet,
                BaseFacet,
            )
            
            run_id = run_id or str(uuid.uuid4())
            event_time = datetime.now(timezone.utc).isoformat()
            
            # Create job
            job_name = f"feast_{project}_materialize_{feature_view.name}"
            job = Job(
                namespace=self.config.namespace,
                name=job_name,
                facets=self._create_job_facets(feature_view),
            )
            
            # Create run with facets
            run = Run(
                runId=run_id,
                facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime=start_date.isoformat(),
                        nominalEndTime=end_date.isoformat(),
                    ),
                    **self._create_run_facets(feature_view, start_date, end_date),
                },
            )
            
            # Create input and output datasets
            inputs = self._create_input_datasets(feature_view, project)
            outputs = self._create_output_datasets(feature_view, project)
            
            # Emit START event
            event = RunEvent(
                eventType=RunState.START,
                eventTime=event_time,
                run=run,
                job=job,
                producer=f"feast/{self._get_feast_version()}",
                inputs=inputs,
                outputs=outputs,
            )
            
            self._client.emit(event)
            _logger.info(f"Emitted OpenLineage START event for {job_name}, run_id={run_id}")
            return run_id
            
        except Exception as e:
            _logger.error(f"Failed to emit OpenLineage START event: {e}")
            return None

    def emit_materialize_complete_event(
        self,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        project: str,
        run_id: str,
        success: bool = True,
    ) -> None:
        """
        Emit a COMPLETE or FAIL event for feature materialization.
        
        Args:
            feature_view: The feature view being materialized
            start_date: Start of the materialization time range
            end_date: End of the materialization time range
            project: The Feast project name
            run_id: The run ID from the START event
            success: Whether the materialization succeeded
        """
        if not self._enabled or not self.config.emit_materialization_events:
            return
            
        if not self._client or not run_id:
            return
            
        try:
            from openlineage.client.run import (
                RunEvent,
                RunState,
                Run,
                Job,
            )
            from openlineage.client.facet import (
                NominalTimeRunFacet,
            )
            
            event_time = datetime.now(timezone.utc).isoformat()
            
            # Create job
            job_name = f"feast_{project}_materialize_{feature_view.name}"
            job = Job(
                namespace=self.config.namespace,
                name=job_name,
                facets=self._create_job_facets(feature_view),
            )
            
            # Create run with facets
            run = Run(
                runId=run_id,
                facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime=start_date.isoformat(),
                        nominalEndTime=end_date.isoformat(),
                    ),
                    **self._create_run_facets(feature_view, start_date, end_date),
                },
            )
            
            # Create input and output datasets
            inputs = self._create_input_datasets(feature_view, project)
            outputs = self._create_output_datasets(feature_view, project)
            
            # Emit COMPLETE or FAIL event
            event_type = RunState.COMPLETE if success else RunState.FAIL
            event = RunEvent(
                eventType=event_type,
                eventTime=event_time,
                run=run,
                job=job,
                producer=f"feast/{self._get_feast_version()}",
                inputs=inputs,
                outputs=outputs,
            )
            
            self._client.emit(event)
            _logger.info(f"Emitted OpenLineage {event_type} event for {job_name}, run_id={run_id}")
            
        except Exception as e:
            _logger.error(f"Failed to emit OpenLineage COMPLETE/FAIL event: {e}")

    def _create_job_facets(self, feature_view: FeatureView) -> Dict[str, Any]:
        """Create custom job facets for Feast feature views."""
        try:
            from openlineage.client.facet import JobFacet
            
            # Create custom Feast feature view facet
            facets = {
                "documentation": JobFacet(
                    _producer=f"feast/{self._get_feast_version()}",
                    _schemaURL="https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
                    description=f"Feast feature view: {feature_view.name}",
                ),
            }
            
            return facets
        except Exception as e:
            _logger.warning(f"Failed to create job facets: {e}")
            return {}

    def _create_run_facets(
        self, feature_view: FeatureView, start_date: datetime, end_date: datetime
    ) -> Dict[str, Any]:
        """Create custom run facets with Feast-specific metadata."""
        try:
            from openlineage.client.facet import BaseFacet
            
            # Create custom Feast facet with feature view metadata
            feast_facet = BaseFacet(
                _producer=f"feast/{self._get_feast_version()}",
                _schemaURL="https://feast.dev/openlineage/FeastFeatureViewFacet/1-0-0",
            )
            
            # Add feature view properties as additional fields
            facet_dict = {
                "featureViewName": feature_view.name,
                "features": self._extract_feature_names(feature_view),
                "entities": feature_view.entities if hasattr(feature_view, 'entities') else [],
            }
            
            # Add batch_source information if available
            if hasattr(feature_view, 'batch_source') and feature_view.batch_source:
                facet_dict["batchSourceType"] = type(feature_view.batch_source).__name__
            
            # Manually set the additional fields on the facet
            for key, value in facet_dict.items():
                setattr(feast_facet, key, value)
            
            return {"feast": feast_facet}
        except Exception as e:
            _logger.warning(f"Failed to create run facets: {e}")
            return {}

    def _create_input_datasets(
        self, feature_view: FeatureView, project: str
    ) -> List[Any]:
        """Create input dataset representations from feature view sources."""
        try:
            from openlineage.client.run import Dataset
            from openlineage.client.facet import (
                SchemaDatasetFacet,
                SchemaField,
                DataSourceDatasetFacet,
            )
            
            inputs = []
            
            # Add batch source as input if available
            if hasattr(feature_view, 'batch_source') and feature_view.batch_source:
                source = feature_view.batch_source
                dataset_name = self._get_dataset_name(source, feature_view.name, "input")
                
                # Create schema facet from feature view schema
                schema_fields = self._create_schema_fields(feature_view)
                
                dataset_facets = {}
                if schema_fields:
                    dataset_facets["schema"] = SchemaDatasetFacet(fields=schema_fields)
                
                # Add data source facet
                dataset_facets["dataSource"] = DataSourceDatasetFacet(
                    name=type(source).__name__,
                    uri=self._get_source_uri(source),
                )
                
                input_dataset = Dataset(
                    namespace=self.config.namespace,
                    name=dataset_name,
                    facets=dataset_facets,
                )
                inputs.append(input_dataset)
            
            return inputs
        except Exception as e:
            _logger.warning(f"Failed to create input datasets: {e}")
            return []

    def _create_output_datasets(
        self, feature_view: FeatureView, project: str
    ) -> List[Any]:
        """Create output dataset representations for online store."""
        try:
            from openlineage.client.run import Dataset
            from openlineage.client.facet import (
                SchemaDatasetFacet,
                SchemaField,
            )
            
            outputs = []
            
            # Create output dataset for online store
            dataset_name = f"{project}.{feature_view.name}_online"
            
            # Create schema facet from feature view schema
            schema_fields = self._create_schema_fields(feature_view)
            
            dataset_facets = {}
            if schema_fields:
                dataset_facets["schema"] = SchemaDatasetFacet(fields=schema_fields)
            
            output_dataset = Dataset(
                namespace=self.config.namespace,
                name=dataset_name,
                facets=dataset_facets,
            )
            outputs.append(output_dataset)
            
            return outputs
        except Exception as e:
            _logger.warning(f"Failed to create output datasets: {e}")
            return []

    def _get_dataset_name(
        self, source: DataSource, feature_view_name: str, dataset_type: str
    ) -> str:
        """Generate a dataset name from a data source."""
        if hasattr(source, 'name') and source.name:
            return source.name
        
        # Fallback to feature view name with type suffix
        return f"{feature_view_name}_{dataset_type}"

    def _get_source_uri(self, source: DataSource) -> str:
        """Extract URI from a data source."""
        # Try common source attributes
        if hasattr(source, 'path') and source.path:
            return source.path
        if hasattr(source, 'table') and source.table:
            return f"table://{source.table}"
        if hasattr(source, 'query') and source.query:
            return "query://inline"
        
        # Fallback
        return f"source://{type(source).__name__}"

    def _get_feast_version(self) -> str:
        """Get the current Feast version."""
        try:
            from feast.version import get_version
            return get_version()
        except Exception:
            return "unknown"

    def _extract_feature_names(self, feature_view: FeatureView) -> List[str]:
        """Extract feature names from a feature view."""
        if hasattr(feature_view, 'features') and feature_view.features:
            return [f.name for f in feature_view.features]
        return []

    def _create_schema_fields(self, feature_view: FeatureView) -> List[Any]:
        """Create schema fields from feature view features."""
        try:
            from openlineage.client.facet import SchemaField
            
            schema_fields = []
            if hasattr(feature_view, 'features') and feature_view.features:
                for feature in feature_view.features:
                    schema_fields.append(
                        SchemaField(
                            name=feature.name,
                            type=str(feature.dtype) if hasattr(feature, 'dtype') else "unknown",
                        )
                    )
            return schema_fields
        except Exception as e:
            _logger.warning(f"Failed to create schema fields: {e}")
            return []
