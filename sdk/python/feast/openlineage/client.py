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
Feast OpenLineage Client.

This module provides a wrapper around the OpenLineage client that is
specifically designed for Feast Feature Store operations.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from feast import FeatureStore

from feast.openlineage.config import OpenLineageConfig

try:
    from openlineage.client import OpenLineageClient
    from openlineage.client.event_v2 import (
        DatasetEvent,
        Job,
        JobEvent,
        Run,
        RunEvent,
        RunState,
        set_producer,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False
    OpenLineageClient = None  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)


class FeastOpenLineageClient:
    """
    OpenLineage client wrapper for Feast Feature Store.

    This client provides convenient methods for emitting OpenLineage events
    from Feast operations like materialization, feature retrieval, and
    registry changes.

    Example:
        from feast.openlineage import FeastOpenLineageClient, OpenLineageConfig

        config = OpenLineageConfig(
            transport_type="http",
            transport_url="http://localhost:5000",
        )
        client = FeastOpenLineageClient(config)

        # Emit lineage for a feature store
        client.emit_registry_lineage(feature_store.registry)
    """

    def __init__(
        self,
        config: Optional[OpenLineageConfig] = None,
        feature_store: Optional["FeatureStore"] = None,
    ):
        """
        Initialize the Feast OpenLineage client.

        Args:
            config: OpenLineage configuration. If not provided, will try to
                   load from environment variables.
            feature_store: Optional FeatureStore instance for context.
        """
        if not OPENLINEAGE_AVAILABLE:
            logger.warning(
                "OpenLineage is not installed. Lineage events will not be emitted. "
                "Install with: pip install openlineage-python"
            )
            self._client = None
            self._config = config or OpenLineageConfig(enabled=False)
            self._feature_store = feature_store
            return

        self._config = config or OpenLineageConfig.from_env()
        self._feature_store = feature_store

        if not self._config.enabled:
            logger.info("OpenLineage integration is disabled")
            self._client = None
            return

        # Set producer
        set_producer(self._config.producer)

        # Initialize the OpenLineage client
        try:
            transport_config = self._config.get_transport_config()
            self._client = OpenLineageClient(config={"transport": transport_config})
            logger.info(
                f"OpenLineage client initialized with {self._config.transport_type} transport"
            )
        except Exception as e:
            logger.error(f"Failed to initialize OpenLineage client: {e}")
            self._client = None

    @property
    def is_enabled(self) -> bool:
        """Check if the OpenLineage client is enabled and available."""
        return self._client is not None and self._config.enabled

    @property
    def config(self) -> OpenLineageConfig:
        """Get the OpenLineage configuration."""
        return self._config

    @property
    def namespace(self) -> str:
        """Get the default namespace."""
        return self._config.namespace

    def emit(self, event: Any) -> bool:
        """
        Emit an OpenLineage event.

        Args:
            event: OpenLineage event (RunEvent, DatasetEvent, or JobEvent)

        Returns:
            True if the event was emitted successfully, False otherwise
        """
        if not self.is_enabled or self._client is None:
            logger.debug("OpenLineage is disabled, skipping event emission")
            return False

        try:
            self._client.emit(event)
            return True
        except Exception as e:
            logger.error(f"Failed to emit OpenLineage event: {e}")
            return False

    def emit_run_event(
        self,
        job_name: str,
        run_id: str,
        event_type: "RunState",
        inputs: Optional[List[Any]] = None,
        outputs: Optional[List[Any]] = None,
        job_facets: Optional[Dict[str, Any]] = None,
        run_facets: Optional[Dict[str, Any]] = None,
        namespace: Optional[str] = None,
    ) -> bool:
        """
        Emit a RunEvent for a Feast operation.

        Args:
            job_name: Name of the job
            run_id: Unique run identifier (UUID)
            event_type: Type of event (START, COMPLETE, FAIL, etc.)
            inputs: List of input datasets
            outputs: List of output datasets
            job_facets: Additional job facets
            run_facets: Additional run facets
            namespace: Optional namespace for the job (defaults to client namespace)

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from datetime import datetime, timezone

        try:
            event = RunEvent(
                eventTime=datetime.now(timezone.utc).isoformat(),
                eventType=event_type,
                run=Run(runId=run_id, facets=run_facets or {}),
                job=Job(
                    namespace=namespace or self.namespace,
                    name=job_name,
                    facets=job_facets or {},
                ),
                inputs=inputs or [],
                outputs=outputs or [],
            )
            return self.emit(event)
        except Exception as e:
            logger.error(f"Failed to create RunEvent: {e}")
            return False

    def emit_dataset_event(
        self,
        dataset_name: str,
        namespace: Optional[str] = None,
        facets: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a DatasetEvent for a Feast dataset (data source, feature view).

        Args:
            dataset_name: Name of the dataset
            namespace: Optional namespace (defaults to client namespace)
            facets: Dataset facets

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from datetime import datetime, timezone

        from openlineage.client.event_v2 import StaticDataset

        try:
            event = DatasetEvent(
                eventTime=datetime.now(timezone.utc).isoformat(),
                dataset=StaticDataset(
                    namespace=namespace or self.namespace,
                    name=dataset_name,
                    facets=facets or {},
                ),
            )
            return self.emit(event)
        except Exception as e:
            logger.error(f"Failed to create DatasetEvent: {e}")
            return False

    def emit_job_event(
        self,
        job_name: str,
        inputs: Optional[List[Any]] = None,
        outputs: Optional[List[Any]] = None,
        job_facets: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a JobEvent for a Feast job definition.

        Args:
            job_name: Name of the job
            inputs: List of input datasets
            outputs: List of output datasets
            job_facets: Job facets

        Returns:
            True if successful, False otherwise
        """
        if not self.is_enabled:
            return False

        from datetime import datetime, timezone

        try:
            event = JobEvent(
                eventTime=datetime.now(timezone.utc).isoformat(),
                job=Job(
                    namespace=self.namespace,
                    name=job_name,
                    facets=job_facets or {},
                ),
                inputs=inputs or [],
                outputs=outputs or [],
            )
            return self.emit(event)
        except Exception as e:
            logger.error(f"Failed to create JobEvent: {e}")
            return False

    def close(self, timeout: float = 5.0) -> bool:
        """
        Close the OpenLineage client and flush any pending events.

        Args:
            timeout: Maximum time to wait for pending events

        Returns:
            True if closed successfully, False otherwise
        """
        if self._client is not None:
            try:
                return self._client.close(timeout)
            except Exception as e:
                logger.error(f"Error closing OpenLineage client: {e}")
                return False
        return True

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
