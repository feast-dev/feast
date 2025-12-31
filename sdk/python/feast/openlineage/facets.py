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
Custom OpenLineage facets for Feast Feature Store.

These facets extend the standard OpenLineage facets to capture Feast-specific
metadata about feature views, feature services, data sources, and entities.
"""

from typing import Dict, List, Optional

import attr

try:
    from openlineage.client.generated.base import DatasetFacet, JobFacet, RunFacet
    from openlineage.client.utils import RedactMixin

    OPENLINEAGE_AVAILABLE = True
except ImportError:
    # Provide stub classes when OpenLineage is not installed
    OPENLINEAGE_AVAILABLE = False

    class RedactMixin:  # type: ignore[no-redef]
        pass

    @attr.define
    class JobFacet:  # type: ignore[no-redef]
        _producer: str = attr.field(default="")
        _schemaURL: str = attr.field(default="")

        def __attrs_post_init__(self):
            pass

    @attr.define
    class DatasetFacet:  # type: ignore[no-redef]
        _producer: str = attr.field(default="")
        _schemaURL: str = attr.field(default="")
        _deleted: bool = attr.field(default=None)

        def __attrs_post_init__(self):
            pass

    @attr.define
    class RunFacet:  # type: ignore[no-redef]
        _producer: str = attr.field(default="")
        _schemaURL: str = attr.field(default="")

        def __attrs_post_init__(self):
            pass


# Schema URL base for Feast facets
FEAST_FACET_SCHEMA_BASE = "https://feast.dev/spec/facets/1-0-0"


@attr.define(kw_only=True)
class FeastFeatureViewFacet(JobFacet):
    """
    Custom facet for Feast Feature View metadata.

    This facet captures Feast-specific metadata about feature views including
    TTL, entities, online/offline status, and transformation mode.

    Attributes:
        name: Feature view name
        ttl_seconds: Time-to-live in seconds (0 means no TTL)
        entities: List of entity names associated with the feature view
        features: List of feature names in the feature view
        online_enabled: Whether online retrieval is enabled
        offline_enabled: Whether offline retrieval is enabled
        mode: Transformation mode (PYTHON, PANDAS, RAY, SPARK, SQL, etc.)
        description: Human-readable description
        owner: Owner of the feature view
        tags: Key-value tags
    """

    name: str = attr.field()
    ttl_seconds: int = attr.field(default=0)
    entities: List[str] = attr.field(factory=list)
    features: List[str] = attr.field(factory=list)
    online_enabled: bool = attr.field(default=True)
    offline_enabled: bool = attr.field(default=False)
    mode: Optional[str] = attr.field(default=None)
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastFeatureViewFacet.json"


@attr.define(kw_only=True)
class FeastFeatureServiceFacet(JobFacet):
    """
    Custom facet for Feast Feature Service metadata.

    This facet captures metadata about feature services which aggregate
    multiple feature views for serving.

    Attributes:
        name: Feature service name
        feature_views: List of feature view names included in the service
        feature_count: Total number of features in the service
        description: Human-readable description
        owner: Owner of the feature service
        tags: Key-value tags
        logging_enabled: Whether feature logging is enabled
    """

    name: str = attr.field()
    feature_views: List[str] = attr.field(factory=list)
    feature_count: int = attr.field(default=0)
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)
    logging_enabled: bool = attr.field(default=False)

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastFeatureServiceFacet.json"


@attr.define(kw_only=True)
class FeastDataSourceFacet(DatasetFacet):
    """
    Custom facet for Feast Data Source metadata.

    This facet captures metadata about data sources including their type,
    configuration, and field mappings.

    Attributes:
        name: Data source name
        source_type: Type of data source (file, bigquery, snowflake, etc.)
        timestamp_field: Name of the timestamp field
        created_timestamp_field: Name of the created timestamp field
        field_mapping: Mapping from source fields to feature names
        description: Human-readable description
        tags: Key-value tags
    """

    name: str = attr.field()
    source_type: str = attr.field()
    timestamp_field: Optional[str] = attr.field(default=None)
    created_timestamp_field: Optional[str] = attr.field(default=None)
    field_mapping: Dict[str, str] = attr.field(factory=dict)
    description: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastDataSourceFacet.json"


@attr.define(kw_only=True)
class FeastEntityFacet(DatasetFacet):
    """
    Custom facet for Feast Entity metadata.

    This facet captures metadata about entities which define the keys
    for feature lookups.

    Attributes:
        name: Entity name
        join_keys: List of join key column names
        value_type: Data type of the entity
        description: Human-readable description
        owner: Owner of the entity
        tags: Key-value tags
    """

    name: str = attr.field()
    join_keys: List[str] = attr.field(factory=list)
    value_type: str = attr.field(default="STRING")
    description: str = attr.field(default="")
    owner: str = attr.field(default="")
    tags: Dict[str, str] = attr.field(factory=dict)

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastEntityFacet.json"


@attr.define(kw_only=True)
class FeastMaterializationFacet(RunFacet):
    """
    Custom facet for Feast Materialization run metadata.

    This facet captures information about feature materialization runs
    including the time range, feature views being materialized, and statistics.

    Attributes:
        feature_views: List of feature view names being materialized
        start_date: Start date of the materialization window
        end_date: End date of the materialization window
        project: Feast project name
        rows_written: Number of rows written (if available)
        online_store_type: Type of online store being written to
        offline_store_type: Type of offline store being read from
    """

    feature_views: List[str] = attr.field(factory=list)
    start_date: Optional[str] = attr.field(default=None)
    end_date: Optional[str] = attr.field(default=None)
    project: str = attr.field(default="")
    rows_written: Optional[int] = attr.field(default=None)
    online_store_type: str = attr.field(default="")
    offline_store_type: str = attr.field(default="")

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastMaterializationFacet.json"


@attr.define(kw_only=True)
class FeastRetrievalFacet(RunFacet):
    """
    Custom facet for Feast Feature Retrieval run metadata.

    This facet captures information about feature retrieval operations
    including whether it's online or historical, the feature service used,
    and retrieval statistics.

    Attributes:
        retrieval_type: Type of retrieval (online, historical)
        feature_service: Name of the feature service used (if any)
        feature_views: List of feature view names queried
        features: List of feature names retrieved
        entity_count: Number of entities queried
        full_feature_names: Whether full feature names were used
    """

    retrieval_type: str = attr.field()  # "online" or "historical"
    feature_service: Optional[str] = attr.field(default=None)
    feature_views: List[str] = attr.field(factory=list)
    features: List[str] = attr.field(factory=list)
    entity_count: Optional[int] = attr.field(default=None)
    full_feature_names: bool = attr.field(default=False)

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastRetrievalFacet.json"


@attr.define(kw_only=True)
class FeastProjectFacet(JobFacet):
    """
    Custom facet for Feast Project metadata.

    This facet captures information about the Feast project context
    for lineage events.

    Attributes:
        project_name: Name of the Feast project
        provider: Infrastructure provider (local, gcp, aws, etc.)
        online_store_type: Type of online store
        offline_store_type: Type of offline store
        registry_type: Type of registry (file, sql, etc.)
    """

    project_name: str = attr.field()
    provider: str = attr.field(default="local")
    online_store_type: str = attr.field(default="")
    offline_store_type: str = attr.field(default="")
    registry_type: str = attr.field(default="file")

    @staticmethod
    def _get_schema() -> str:
        return f"{FEAST_FACET_SCHEMA_BASE}/FeastProjectFacet.json"
