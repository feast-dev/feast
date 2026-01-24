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
Mapping utilities for converting Feast objects to OpenLineage objects.

This module provides functions to map Feast entities like FeatureViews,
FeatureServices, DataSources, and Entities to their OpenLineage equivalents.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Tuple

if TYPE_CHECKING:
    from feast import Entity, FeatureService, FeatureView
    from feast.data_source import DataSource
    from feast.field import Field

try:
    from openlineage.client.event_v2 import (
        InputDataset,
        Job,
        OutputDataset,
    )
    from openlineage.client.facet_v2 import (
        datasource_dataset,
        documentation_dataset,
        schema_dataset,
    )

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    OPENLINEAGE_AVAILABLE = False


def _check_openlineage_available():
    """Check if OpenLineage is available and raise if not."""
    if not OPENLINEAGE_AVAILABLE:
        raise ImportError(
            "OpenLineage is not installed. Please install it with: "
            "pip install openlineage-python"
        )


def feast_field_to_schema_field(
    field: "Field",
) -> "schema_dataset.SchemaDatasetFacetFields":
    """
    Convert a Feast Field to an OpenLineage schema field.

    Args:
        field: Feast Field object

    Returns:
        OpenLineage SchemaDatasetFacetFields object
    """
    _check_openlineage_available()

    # Build description with tags
    description_parts = []

    # Add description if present
    if hasattr(field, "description") and field.description:
        description_parts.append(field.description)

    # Add tags if present
    if hasattr(field, "tags") and field.tags:
        tags_str = ", ".join(f"{k}={v}" for k, v in field.tags.items())
        description_parts.append(f"[Tags: {tags_str}]")

    description = " ".join(description_parts) if description_parts else None

    return schema_dataset.SchemaDatasetFacetFields(
        name=field.name,
        type=str(field.dtype) if field.dtype else None,
        description=description,
    )


def data_source_to_dataset(
    data_source: "DataSource",
    namespace: str = "feast",
    as_input: bool = True,
) -> Any:
    """
    Convert a Feast DataSource to an OpenLineage Dataset.

    Args:
        data_source: Feast DataSource object
        namespace: OpenLineage namespace
        as_input: Whether to create an InputDataset (True) or OutputDataset (False)

    Returns:
        OpenLineage InputDataset or OutputDataset object
    """
    _check_openlineage_available()

    from feast.openlineage.facets import FeastDataSourceFacet

    # Determine source type and name
    source_type = type(data_source).__name__
    source_name = data_source.name if data_source.name else f"unnamed_{source_type}"

    # Build namespace based on source type
    dataset_namespace = _get_data_source_namespace(data_source, namespace)

    # Build facets
    facets: Dict[str, Any] = {}

    # Add datasource facet
    facets["dataSource"] = datasource_dataset.DatasourceDatasetFacet(
        name=source_name,
        uri=_get_data_source_uri(data_source),
    )

    # Add Feast-specific facet
    facets["feast_dataSource"] = FeastDataSourceFacet(
        name=source_name,
        source_type=source_type,
        timestamp_field=data_source.timestamp_field
        if hasattr(data_source, "timestamp_field")
        else None,
        created_timestamp_field=data_source.created_timestamp_column
        if hasattr(data_source, "created_timestamp_column")
        else None,
        field_mapping=data_source.field_mapping
        if hasattr(data_source, "field_mapping")
        else {},
        description=data_source.description
        if hasattr(data_source, "description")
        else "",
        tags=data_source.tags if hasattr(data_source, "tags") else {},
    )

    # Add documentation if available
    if hasattr(data_source, "description") and data_source.description:
        facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=data_source.description
        )

    if as_input:
        return InputDataset(
            namespace=dataset_namespace,
            name=source_name,
            facets=facets,
        )
    else:
        return OutputDataset(
            namespace=dataset_namespace,
            name=source_name,
            facets=facets,
        )


def _get_data_source_namespace(
    data_source: "DataSource", default_namespace: str
) -> str:
    """
    Get the OpenLineage namespace for a data source.

    Uses the same namespace as other Feast objects to ensure proper
    lineage connections in the graph.

    Args:
        data_source: Feast DataSource
        default_namespace: Default namespace to use

    Returns:
        Namespace string
    """
    # Use consistent namespace to ensure lineage graph connects properly
    return default_namespace


def _get_data_source_uri(data_source: "DataSource") -> str:
    """
    Get the URI for a data source.

    Args:
        data_source: Feast DataSource

    Returns:
        URI string representing the data source location
    """
    if hasattr(data_source, "path") and data_source.path:
        return data_source.path
    elif hasattr(data_source, "table") and data_source.table:
        return f"table://{data_source.table}"
    elif hasattr(data_source, "query") and data_source.query:
        return f"query://{hash(data_source.query)}"
    else:
        return f"feast://{data_source.name if data_source.name else 'unnamed'}"


def feature_view_to_job(
    feature_view: "FeatureView",
    namespace: str = "feast",
    include_schema: bool = True,
) -> Tuple["Job", List["InputDataset"], List["OutputDataset"]]:
    """
    Convert a Feast FeatureView to an OpenLineage Job with inputs/outputs.

    A FeatureView represents a transformation from data sources to features,
    so it maps to an OpenLineage Job with:
    - Inputs: The batch and stream sources
    - Outputs: The feature view itself (as a logical dataset)

    Args:
        feature_view: Feast FeatureView object
        namespace: OpenLineage namespace
        include_schema: Whether to include schema information

    Returns:
        Tuple of (Job, list of InputDatasets, list of OutputDatasets)
    """
    _check_openlineage_available()

    from feast.openlineage.facets import FeastFeatureViewFacet

    # Create job facets
    job_facets: Dict[str, Any] = {}

    # Add Feast-specific facet
    ttl_seconds = 0
    if feature_view.ttl:
        ttl_seconds = int(feature_view.ttl.total_seconds())

    job_facets["feast_featureView"] = FeastFeatureViewFacet(
        name=feature_view.name,
        ttl_seconds=ttl_seconds,
        entities=feature_view.entities if feature_view.entities else [],
        features=[f.name for f in feature_view.features]
        if feature_view.features
        else [],
        online_enabled=feature_view.online,
        offline_enabled=getattr(feature_view, "offline", False),
        mode=str(feature_view.mode)
        if hasattr(feature_view, "mode") and feature_view.mode
        else None,
        description=feature_view.description if feature_view.description else "",
        owner=feature_view.owner if feature_view.owner else "",
        tags=feature_view.tags if feature_view.tags else {},
    )

    # Add documentation
    if feature_view.description:
        job_facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=feature_view.description
        )

    # Create job
    job = Job(
        namespace=namespace,
        name=f"feature_view_{feature_view.name}",
        facets=job_facets,
    )

    # Create input datasets from sources
    inputs: List[InputDataset] = []

    # Add data sources as inputs
    if hasattr(feature_view, "batch_source") and feature_view.batch_source:
        inputs.append(
            data_source_to_dataset(
                feature_view.batch_source, namespace=namespace, as_input=True
            )
        )

    if hasattr(feature_view, "stream_source") and feature_view.stream_source:
        inputs.append(
            data_source_to_dataset(
                feature_view.stream_source, namespace=namespace, as_input=True
            )
        )

    # Add entities as inputs (they appear as nodes in lineage)
    if feature_view.entities:
        for entity_name in feature_view.entities:
            if entity_name and entity_name != "__dummy":
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=entity_name,
                    )
                )

    # Create output dataset (the feature view itself as a logical dataset)
    output_facets: Dict[str, Any] = {}

    if include_schema and feature_view.features:
        output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[feast_field_to_schema_field(f) for f in feature_view.features]
        )

    outputs = [
        OutputDataset(
            namespace=namespace,
            name=feature_view.name,
            facets=output_facets,
        )
    ]

    return job, inputs, outputs


def feature_service_to_job(
    feature_service: "FeatureService",
    feature_views: List["FeatureView"],
    namespace: str = "feast",
) -> Tuple["Job", List["InputDataset"], List["OutputDataset"]]:
    """
    Convert a Feast FeatureService to an OpenLineage Job with inputs/outputs.

    A FeatureService aggregates multiple feature views, so it maps to an
    OpenLineage Job with:
    - Inputs: The feature views it consumes
    - Outputs: The aggregated feature set

    Args:
        feature_service: Feast FeatureService object
        feature_views: List of FeatureView objects referenced by the service
        namespace: OpenLineage namespace

    Returns:
        Tuple of (Job, list of InputDatasets, list of OutputDatasets)
    """
    _check_openlineage_available()

    from feast.openlineage.facets import FeastFeatureServiceFacet

    # Create job facets
    job_facets: Dict[str, Any] = {}

    # Get feature view names
    fv_names = [proj.name for proj in feature_service.feature_view_projections]

    # Count total features
    total_features = sum(
        len(proj.features) if proj.features else 0
        for proj in feature_service.feature_view_projections
    )

    # Add Feast-specific facet
    job_facets["feast_featureService"] = FeastFeatureServiceFacet(
        name=feature_service.name,
        feature_views=fv_names,
        feature_count=total_features,
        description=feature_service.description if feature_service.description else "",
        owner=feature_service.owner if feature_service.owner else "",
        tags=feature_service.tags if feature_service.tags else {},
        logging_enabled=getattr(feature_service, "logging", None) is not None,
    )

    # Add documentation
    if feature_service.description:
        job_facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=feature_service.description
        )

    # Create job
    job = Job(
        namespace=namespace,
        name=f"feature_service_{feature_service.name}",
        facets=job_facets,
    )

    # Create input datasets from feature views
    inputs: List[InputDataset] = []
    all_features = []

    for fv in feature_views:
        input_facets: Dict[str, Any] = {}
        if fv.features:
            input_facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[feast_field_to_schema_field(f) for f in fv.features]
            )
            all_features.extend(fv.features)

        inputs.append(
            InputDataset(
                namespace=namespace,
                name=fv.name,
                facets=input_facets,
            )
        )

    # Create output dataset (the feature service as a logical aggregation)
    output_facets: Dict[str, Any] = {}
    if all_features:
        output_facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[feast_field_to_schema_field(f) for f in all_features]
        )

    outputs = [
        OutputDataset(
            namespace=namespace,
            name=feature_service.name,
            facets=output_facets,
        )
    ]

    return job, inputs, outputs


def entity_to_dataset(
    entity: "Entity",
    namespace: str = "feast",
) -> "InputDataset":
    """
    Convert a Feast Entity to an OpenLineage InputDataset.

    Entities define the keys for feature lookups and can be represented
    as datasets with schema information.

    Args:
        entity: Feast Entity object
        namespace: OpenLineage namespace

    Returns:
        OpenLineage InputDataset object
    """
    _check_openlineage_available()

    from feast.openlineage.facets import FeastEntityFacet

    facets: Dict[str, Any] = {}

    # Add entity facet
    facets["feast_entity"] = FeastEntityFacet(
        name=entity.name,
        join_keys=[entity.join_key] if entity.join_key else [],
        value_type=str(entity.value_type) if entity.value_type else "STRING",
        description=entity.description if entity.description else "",
        owner=entity.owner if hasattr(entity, "owner") and entity.owner else "",
        tags=entity.tags if entity.tags else {},
    )

    # Add schema for join keys
    if entity.join_key:
        facets["schema"] = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name=entity.join_key,
                    type=str(entity.value_type) if entity.value_type else "STRING",
                )
            ]
        )

    # Add documentation
    if entity.description:
        facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=entity.description
        )

    return InputDataset(
        namespace=namespace,
        name=entity.name,
        facets=facets,
    )


def online_store_to_dataset(
    store_type: str,
    feature_view_name: str,
    namespace: str = "feast",
) -> "OutputDataset":
    """
    Create an OpenLineage OutputDataset for an online store.

    Args:
        store_type: Type of online store (redis, sqlite, dynamodb, etc.)
        feature_view_name: Name of the feature view being stored
        namespace: OpenLineage namespace

    Returns:
        OpenLineage OutputDataset object
    """
    _check_openlineage_available()

    return OutputDataset(
        namespace=namespace,
        name=f"online_store_{feature_view_name}",
        facets={
            "dataSource": datasource_dataset.DatasourceDatasetFacet(
                name=f"{store_type}_online_store",
                uri=f"{store_type}://feast/{feature_view_name}",
            )
        },
    )


def offline_store_to_dataset(
    store_type: str,
    feature_view_name: str,
    namespace: str = "feast",
) -> "InputDataset":
    """
    Create an OpenLineage InputDataset for an offline store.

    Args:
        store_type: Type of offline store (file, bigquery, snowflake, etc.)
        feature_view_name: Name of the feature view being read
        namespace: OpenLineage namespace

    Returns:
        OpenLineage InputDataset object
    """
    _check_openlineage_available()

    return InputDataset(
        namespace=f"{namespace}/offline_store/{store_type}",
        name=feature_view_name,
        facets={
            "dataSource": datasource_dataset.DatasourceDatasetFacet(
                name=f"{store_type}_offline_store",
                uri=f"{store_type}://feast/{feature_view_name}",
            )
        },
    )
