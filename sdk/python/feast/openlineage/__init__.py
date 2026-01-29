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
OpenLineage integration for Feast Feature Store.

This module provides **native integration** between Feast and OpenLineage for
automatic data lineage tracking. When enabled in feature_store.yaml, lineage
events are emitted automatically for:

- Feature store registry changes (apply operations)
- Feature materialization (batch and streaming) - START, COMPLETE, FAIL events

Lineage Graph Structure:
    feast apply creates a lineage graph matching Feast UI:

    DataSources + Entities → feast_feature_views_{project} → FeatureViews
    FeatureViews → feature_service_{name} → FeatureServices

    Each dataset includes:
    - Schema with feature names, types, descriptions, and tags
    - Feast-specific facets with metadata (TTL, entities, owner, etc.)

Usage:
    Simply configure OpenLineage in your feature_store.yaml:

    ```yaml
    project: my_project
    # ... other config ...

    openlineage:
      enabled: true
      transport_type: http
      transport_url: http://localhost:5000
      transport_endpoint: api/v1/lineage
      namespace: my_namespace  # Optional: defaults to project name
    ```

    Then use Feast normally - lineage events are emitted automatically!

    ```python
    from feast import FeatureStore

    fs = FeatureStore(repo_path="feature_repo")
    fs.apply([entity, feature_view, feature_service])  # Emits lineage
    fs.materialize(start, end)  # Emits START/COMPLETE/FAIL events
    ```
"""

from feast.openlineage.client import FeastOpenLineageClient
from feast.openlineage.config import OpenLineageConfig
from feast.openlineage.emitter import FeastOpenLineageEmitter
from feast.openlineage.facets import (
    FeastDataSourceFacet,
    FeastEntityFacet,
    FeastFeatureServiceFacet,
    FeastFeatureViewFacet,
    FeastMaterializationFacet,
    FeastProjectFacet,
)

__all__ = [
    # Main classes (used internally by native integration)
    "FeastOpenLineageClient",
    "FeastOpenLineageEmitter",
    "OpenLineageConfig",
    # Facets (custom Feast metadata in lineage events)
    "FeastFeatureViewFacet",
    "FeastFeatureServiceFacet",
    "FeastDataSourceFacet",
    "FeastEntityFacet",
    "FeastMaterializationFacet",
    "FeastProjectFacet",
]
