"""
Registry lineage generation for Feast objects.

This module provides functionality to generate relationship graphs between
Feast objects (entities, feature views, data sources, feature services,
saved datasets) for lineage visualization.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Set, Tuple

from feast.protos.feast.core.Registry_pb2 import Registry


def _extract_storage_identifiers(storage) -> Set[str]:
    """Extract physical location identifiers from a SavedDatasetStorage proto.

    Returns a set of non-empty strings (URIs, table names, paths) that can
    be matched against DataSource options.
    """
    ids: Set[str] = set()
    if hasattr(storage, "file_storage") and storage.HasField("file_storage"):
        if storage.file_storage.uri:
            ids.add(storage.file_storage.uri)
    if hasattr(storage, "bigquery_storage") and storage.HasField("bigquery_storage"):
        if storage.bigquery_storage.table:
            ids.add(storage.bigquery_storage.table)
    if hasattr(storage, "redshift_storage") and storage.HasField("redshift_storage"):
        if storage.redshift_storage.table:
            ids.add(storage.redshift_storage.table)
    if hasattr(storage, "snowflake_storage") and storage.HasField("snowflake_storage"):
        if storage.snowflake_storage.table:
            ids.add(storage.snowflake_storage.table)
    if hasattr(storage, "spark_storage") and storage.HasField("spark_storage"):
        if storage.spark_storage.path:
            ids.add(storage.spark_storage.path)
        if storage.spark_storage.table:
            ids.add(storage.spark_storage.table)
    if hasattr(storage, "trino_storage") and storage.HasField("trino_storage"):
        if storage.trino_storage.table:
            ids.add(storage.trino_storage.table)
    if hasattr(storage, "athena_storage") and storage.HasField("athena_storage"):
        if storage.athena_storage.table:
            ids.add(storage.athena_storage.table)
    return ids


def _extract_datasource_identifiers(data_source) -> Set[str]:
    """Extract physical location identifiers from a DataSource proto.

    Returns a set of non-empty strings (URIs, table names, paths) that can
    be compared against SavedDatasetStorage identifiers.
    """
    ids: Set[str] = set()
    opts = (
        data_source.WhichOneof("options")
        if hasattr(data_source, "WhichOneof")
        else None
    )
    if opts == "file_options" and data_source.file_options.uri:
        ids.add(data_source.file_options.uri)
    elif opts == "bigquery_options" and data_source.bigquery_options.table:
        ids.add(data_source.bigquery_options.table)
    elif opts == "redshift_options" and data_source.redshift_options.table:
        ids.add(data_source.redshift_options.table)
    elif opts == "snowflake_options" and data_source.snowflake_options.table:
        ids.add(data_source.snowflake_options.table)
    elif opts == "spark_options":
        if data_source.spark_options.path:
            ids.add(data_source.spark_options.path)
        if data_source.spark_options.table:
            ids.add(data_source.spark_options.table)
    elif opts == "trino_options" and data_source.trino_options.table:
        ids.add(data_source.trino_options.table)
    elif opts == "athena_options" and data_source.athena_options.table:
        ids.add(data_source.athena_options.table)

    # Also check batch_source if present (FeatureView's embedded source)
    if hasattr(data_source, "batch_source") and data_source.HasField("batch_source"):
        ids.update(_extract_datasource_identifiers(data_source.batch_source))

    return ids


def _build_datasource_location_index(registry: Registry) -> Dict[str, str]:
    """Build a reverse index: physical location → DataSource name.

    Scans all DataSources in the registry and maps each physical identifier
    (URI, table, path) to the DataSource's name.
    """
    location_to_name: Dict[str, str] = {}
    for ds in registry.data_sources:
        if not (hasattr(ds, "name") and ds.name):
            continue
        for loc_id in _extract_datasource_identifiers(ds):
            location_to_name[loc_id] = ds.name
    return location_to_name


class FeastObjectType(Enum):
    DATA_SOURCE = "dataSource"
    ENTITY = "entity"
    FEATURE_VIEW = "featureView"
    LABEL_VIEW = "labelView"
    FEATURE_SERVICE = "featureService"
    FEATURE = "feature"
    SAVED_DATASET = "savedDataset"


@dataclass
class EntityReference:
    type: FeastObjectType
    name: str

    def to_proto(self):
        try:
            from feast.protos.feast.registry.RegistryServer_pb2 import (
                EntityReference as EntityReferenceProto,
            )

            return EntityReferenceProto(type=self.type.value, name=self.name)
        except ImportError:
            return {"type": self.type.value, "name": self.name}


@dataclass
class EntityRelation:
    source: EntityReference
    target: EntityReference

    def to_proto(self):
        try:
            from feast.protos.feast.registry.RegistryServer_pb2 import (
                EntityRelation as EntityRelationProto,
            )

            return EntityRelationProto(
                source=self.source.to_proto(), target=self.target.to_proto()
            )
        except ImportError:
            # Fallback to dict if protobuf not generated yet
            return {"source": self.source.to_proto(), "target": self.target.to_proto()}


class RegistryLineageGenerator:
    """
    Generates lineage relationships between Feast objects.
    """

    def generate_lineage(
        self, registry: Registry
    ) -> Tuple[List[EntityRelation], List[EntityRelation]]:
        """
        Generate both direct and indirect relationships from registry objects.
        Args:
            registry: The registry protobuf containing all objects
        Returns:
            Tuple of (direct_relationships, indirect_relationships)
        """
        direct_relationships = self._parse_direct_relationships(registry)
        indirect_relationships = self._parse_indirect_relationships(
            direct_relationships, registry
        )

        return direct_relationships, indirect_relationships

    def _parse_direct_relationships(self, registry: Registry) -> List[EntityRelation]:
        """Parse direct relationships between objects."""
        relationships = []

        # FeatureService -> FeatureView/LabelView relationships
        label_view_names = {
            lv.spec.name
            for lv in registry.label_views
            if hasattr(lv, "spec") and lv.spec
        }
        for feature_service in registry.feature_services:
            if (
                hasattr(feature_service, "spec")
                and feature_service.spec
                and feature_service.spec.features
            ):
                for feature in feature_service.spec.features:
                    view_type_str = getattr(feature, "view_type", "")
                    is_label_view = (
                        view_type_str == "labelView"
                        or feature.feature_view_name in label_view_names
                    )
                    source_type = (
                        FeastObjectType.LABEL_VIEW
                        if is_label_view
                        else FeastObjectType.FEATURE_VIEW
                    )
                    rel = EntityRelation(
                        source=EntityReference(source_type, feature.feature_view_name),
                        target=EntityReference(
                            FeastObjectType.FEATURE_SERVICE,
                            feature_service.spec.name,
                        ),
                    )
                    relationships.append(rel)

        # Entity -> FeatureView and DataSource -> FeatureView relationships
        for feature_view in registry.feature_views:
            if hasattr(feature_view, "spec") and feature_view.spec:
                # Entity relationships
                if hasattr(feature_view.spec, "entities"):
                    for entity_name in feature_view.spec.entities:
                        rel = EntityRelation(
                            source=EntityReference(FeastObjectType.ENTITY, entity_name),
                            target=EntityReference(
                                FeastObjectType.FEATURE_VIEW, feature_view.spec.name
                            ),
                        )
                        relationships.append(rel)

                # Feature -> FeatureView relationships
                if hasattr(feature_view.spec, "features"):
                    for feature in feature_view.spec.features:
                        rel = EntityRelation(
                            source=EntityReference(
                                FeastObjectType.FEATURE, feature.name
                            ),
                            target=EntityReference(
                                FeastObjectType.FEATURE_VIEW, feature_view.spec.name
                            ),
                        )
                        relationships.append(rel)

                # Batch source relationship
                if (
                    hasattr(feature_view.spec, "batch_source")
                    and feature_view.spec.batch_source
                ):
                    # Try to get the data source name
                    data_source_name = None
                    if (
                        hasattr(feature_view.spec.batch_source, "name")
                        and feature_view.spec.batch_source.name
                    ):
                        data_source_name = feature_view.spec.batch_source.name
                    elif (
                        hasattr(feature_view.spec.batch_source, "table")
                        and feature_view.spec.batch_source.table
                    ):
                        # Fallback to table name for unnamed data sources
                        data_source_name = (
                            f"table:{feature_view.spec.batch_source.table}"
                        )
                    elif (
                        hasattr(feature_view.spec.batch_source, "path")
                        and feature_view.spec.batch_source.path
                    ):
                        # Fallback to path for file-based sources
                        data_source_name = f"path:{feature_view.spec.batch_source.path}"
                    else:
                        # Use a generic identifier
                        data_source_name = f"unnamed_source_{hash(str(feature_view.spec.batch_source))}"

                    if data_source_name:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.DATA_SOURCE, data_source_name
                                ),
                                target=EntityReference(
                                    FeastObjectType.FEATURE_VIEW, feature_view.spec.name
                                ),
                            )
                        )

        # OnDemand FeatureView: Feature -> OnDemandFeatureView relationships
        for odfv in registry.on_demand_feature_views:
            if hasattr(odfv, "spec") and odfv.spec:
                # Entity relationships
                if hasattr(odfv.spec, "entities"):
                    for entity_name in odfv.spec.entities:
                        rel = EntityRelation(
                            source=EntityReference(FeastObjectType.ENTITY, entity_name),
                            target=EntityReference(
                                FeastObjectType.FEATURE_VIEW, odfv.spec.name
                            ),
                        )
                        relationships.append(rel)

                # Feature -> OnDemandFeatureView relationships
                if hasattr(odfv.spec, "features"):
                    for feature in odfv.spec.features:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.FEATURE, feature.name
                                ),
                                target=EntityReference(
                                    FeastObjectType.FEATURE_VIEW, odfv.spec.name
                                ),
                            )
                        )

        # OnDemand FeatureView relationships
        for odfv in registry.on_demand_feature_views:
            if (
                hasattr(odfv, "spec")
                and odfv.spec
                and hasattr(odfv.spec, "sources")
                and odfv.spec.sources
            ):
                # Handle protobuf map structure
                if hasattr(odfv.spec.sources, "items"):
                    source_items = odfv.spec.sources.items()
                else:
                    # Fallback for different protobuf representations
                    source_items = [(k, v) for k, v in enumerate(odfv.spec.sources)]

                for source_name, source in source_items:
                    has_req = hasattr(source, "HasField") and source.HasField(
                        "request_data_source"
                    )
                    has_fvp = hasattr(source, "HasField") and source.HasField(
                        "feature_view_projection"
                    )

                    if has_req and source.request_data_source.name:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.DATA_SOURCE,
                                    source.request_data_source.name,
                                ),
                                target=EntityReference(
                                    FeastObjectType.FEATURE_VIEW, odfv.spec.name
                                ),
                            )
                        )
                    elif has_fvp and source.feature_view_projection.feature_view_name:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.FEATURE_VIEW,
                                    source.feature_view_projection.feature_view_name,
                                ),
                                target=EntityReference(
                                    FeastObjectType.FEATURE_VIEW,
                                    odfv.spec.name,
                                ),
                            )
                        )

        # Stream FeatureView relationships
        for sfv in registry.stream_feature_views:
            if hasattr(sfv, "spec") and sfv.spec:
                # Stream source
                if (
                    hasattr(sfv.spec, "stream_source")
                    and sfv.spec.stream_source
                    and hasattr(sfv.spec.stream_source, "name")
                    and sfv.spec.stream_source.name
                ):
                    relationships.append(
                        EntityRelation(
                            source=EntityReference(
                                FeastObjectType.DATA_SOURCE, sfv.spec.stream_source.name
                            ),
                            target=EntityReference(
                                FeastObjectType.FEATURE_VIEW, sfv.spec.name
                            ),
                        )
                    )

                # Batch source
                if (
                    hasattr(sfv.spec, "batch_source")
                    and sfv.spec.batch_source
                    and hasattr(sfv.spec.batch_source, "name")
                    and sfv.spec.batch_source.name
                ):
                    relationships.append(
                        EntityRelation(
                            source=EntityReference(
                                FeastObjectType.DATA_SOURCE, sfv.spec.batch_source.name
                            ),
                            target=EntityReference(
                                FeastObjectType.FEATURE_VIEW, sfv.spec.name
                            ),
                        )
                    )

        # LabelView relationships
        for label_view in registry.label_views:
            if hasattr(label_view, "spec") and label_view.spec:
                # Entity relationships
                if hasattr(label_view.spec, "entities"):
                    for entity_name in label_view.spec.entities:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.ENTITY, entity_name
                                ),
                                target=EntityReference(
                                    FeastObjectType.LABEL_VIEW, label_view.spec.name
                                ),
                            )
                        )

                # Data source relationships: LabelView uses spec.source (PushSource)
                # which contains a nested batch_source
                if hasattr(label_view.spec, "source") and label_view.spec.source:
                    source = label_view.spec.source
                    # Link to the push source itself
                    if hasattr(source, "name") and source.name:
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.DATA_SOURCE, source.name
                                ),
                                target=EntityReference(
                                    FeastObjectType.LABEL_VIEW, label_view.spec.name
                                ),
                            )
                        )
                    # Link to the nested batch source
                    if (
                        hasattr(source, "batch_source")
                        and source.batch_source
                        and hasattr(source.batch_source, "name")
                        and source.batch_source.name
                    ):
                        relationships.append(
                            EntityRelation(
                                source=EntityReference(
                                    FeastObjectType.DATA_SOURCE,
                                    source.batch_source.name,
                                ),
                                target=EntityReference(
                                    FeastObjectType.LABEL_VIEW, label_view.spec.name
                                ),
                            )
                        )
                elif (
                    hasattr(label_view.spec, "batch_source")
                    and label_view.spec.batch_source
                    and hasattr(label_view.spec.batch_source, "name")
                    and label_view.spec.batch_source.name
                ):
                    relationships.append(
                        EntityRelation(
                            source=EntityReference(
                                FeastObjectType.DATA_SOURCE,
                                label_view.spec.batch_source.name,
                            ),
                            target=EntityReference(
                                FeastObjectType.LABEL_VIEW, label_view.spec.name
                            ),
                        )
                    )

        # SavedDataset relationships
        ds_location_index = _build_datasource_location_index(registry)

        for saved_dataset in registry.saved_datasets:
            if hasattr(saved_dataset, "spec") and saved_dataset.spec:
                # FeatureService -> SavedDataset (when created via a feature service)
                if (
                    hasattr(saved_dataset.spec, "feature_service_name")
                    and saved_dataset.spec.feature_service_name
                ):
                    relationships.append(
                        EntityRelation(
                            source=EntityReference(
                                FeastObjectType.FEATURE_SERVICE,
                                saved_dataset.spec.feature_service_name,
                            ),
                            target=EntityReference(
                                FeastObjectType.SAVED_DATASET,
                                saved_dataset.spec.name,
                            ),
                        )
                    )

                # FeatureView -> SavedDataset (derived from feature refs "view:feat")
                if (
                    hasattr(saved_dataset.spec, "features")
                    and saved_dataset.spec.features
                ):
                    from feast.utils import _parse_feature_ref

                    seen_views: set = set()
                    for feat_ref in saved_dataset.spec.features:
                        try:
                            view_name, _, _ = _parse_feature_ref(feat_ref)
                        except ValueError:
                            continue
                        if view_name and view_name not in seen_views:
                            seen_views.add(view_name)
                            relationships.append(
                                EntityRelation(
                                    source=EntityReference(
                                        FeastObjectType.FEATURE_VIEW,
                                        view_name,
                                    ),
                                    target=EntityReference(
                                        FeastObjectType.SAVED_DATASET,
                                        saved_dataset.spec.name,
                                    ),
                                )
                            )

                # DataSource -> SavedDataset (matched via storage location)
                if (
                    hasattr(saved_dataset.spec, "storage")
                    and saved_dataset.spec.storage
                ):
                    storage_ids = _extract_storage_identifiers(
                        saved_dataset.spec.storage
                    )
                    matched_ds_names: set = set()
                    for loc_id in storage_ids:
                        ds_name = ds_location_index.get(loc_id)
                        if ds_name and ds_name not in matched_ds_names:
                            matched_ds_names.add(ds_name)
                            relationships.append(
                                EntityRelation(
                                    source=EntityReference(
                                        FeastObjectType.DATA_SOURCE,
                                        ds_name,
                                    ),
                                    target=EntityReference(
                                        FeastObjectType.SAVED_DATASET,
                                        saved_dataset.spec.name,
                                    ),
                                )
                            )

        return relationships

    def _parse_indirect_relationships(
        self, direct_relationships: List[EntityRelation], registry: Registry
    ) -> List[EntityRelation]:
        """Parse indirect relationships (transitive relationships through feature views)."""
        indirect_relationships = []

        # Create Entity -> FeatureService and DataSource -> FeatureService relationships
        for feature_service in registry.feature_services:
            if (
                hasattr(feature_service, "spec")
                and feature_service.spec
                and hasattr(feature_service.spec, "features")
                and feature_service.spec.features
            ):
                for feature in feature_service.spec.features:
                    if hasattr(feature, "feature_view_name"):
                        # Find all relationships that target this feature view or label view
                        related_sources = [
                            rel.source
                            for rel in direct_relationships
                            if rel.target.name == feature.feature_view_name
                            and rel.target.type
                            in (
                                FeastObjectType.FEATURE_VIEW,
                                FeastObjectType.LABEL_VIEW,
                            )
                        ]

                        # Create indirect relationships to the feature service
                        for source in related_sources:
                            indirect_relationships.append(
                                EntityRelation(
                                    source=source,
                                    target=EntityReference(
                                        FeastObjectType.FEATURE_SERVICE,
                                        feature_service.spec.name,
                                    ),
                                )
                            )

        # Create Entity -> DataSource relationships (through feature views and label views)
        # Build a map of view -> data sources
        feature_view_to_data_sources: Dict[str, List[str]] = {}
        for rel in direct_relationships:
            if rel.source.type == FeastObjectType.DATA_SOURCE and rel.target.type in (
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.LABEL_VIEW,
            ):
                if rel.target.name not in feature_view_to_data_sources:
                    feature_view_to_data_sources[rel.target.name] = []
                feature_view_to_data_sources[rel.target.name].append(rel.source.name)

        # For each Entity -> FeatureView/LabelView relationship, create Entity -> DataSource relationships
        for rel in direct_relationships:
            if rel.source.type == FeastObjectType.ENTITY and rel.target.type in (
                FeastObjectType.FEATURE_VIEW,
                FeastObjectType.LABEL_VIEW,
            ):
                if rel.target.name in feature_view_to_data_sources:
                    for data_source_name in feature_view_to_data_sources[
                        rel.target.name
                    ]:
                        indirect_relationships.append(
                            EntityRelation(
                                source=rel.source,  # The entity
                                target=EntityReference(
                                    FeastObjectType.DATA_SOURCE,
                                    data_source_name,
                                ),
                            )
                        )

        return indirect_relationships

    def get_object_relationships(
        self,
        registry: Registry,
        object_type: str,
        object_name: str,
        include_indirect: bool = False,
    ) -> List[EntityRelation]:
        """
        Get all relationships for a specific object.
        Args:
            registry: The registry protobuf
            object_type: Type of the object (dataSource, entity, featureView, featureService)
            object_name: Name of the object
            include_indirect: Whether to include indirect relationships
        Returns:
            List of relationships involving the specified object
        """
        direct_relationships, indirect_relationships = self.generate_lineage(registry)

        all_relationships = direct_relationships[:]
        if include_indirect:
            all_relationships.extend(indirect_relationships)

        # Filter relationships involving the specified object
        filtered_relationships = []
        target_type = FeastObjectType(object_type)

        for rel in all_relationships:
            if (rel.source.type == target_type and rel.source.name == object_name) or (
                rel.target.type == target_type and rel.target.name == object_name
            ):
                filtered_relationships.append(rel)

        return filtered_relationships

    def get_object_lineage_graph(
        self, registry: Registry, object_type: str, object_name: str, depth: int = 2
    ) -> Dict:
        """
        Get a complete lineage graph for an object up to specified depth.
        This can be used for more complex lineage queries and visualization.
        """
        direct_relationships, indirect_relationships = self.generate_lineage(registry)
        all_relationships = direct_relationships + indirect_relationships

        # Build adjacency graph
        graph: Dict[str, List[str]] = {}
        for rel in all_relationships:
            source_key = f"{rel.source.type.value}:{rel.source.name}"
            target_key = f"{rel.target.type.value}:{rel.target.name}"

            if source_key not in graph:
                graph[source_key] = []
            graph[source_key].append(target_key)

        # Perform BFS to get subgraph up to specified depth
        start_key = f"{object_type}:{object_name}"
        visited = set()
        result_nodes = set()
        result_edges = []

        def bfs(current_key, current_depth):
            if current_depth > depth or current_key in visited:
                return

            visited.add(current_key)
            result_nodes.add(current_key)

            if current_key in graph:
                for neighbor in graph[current_key]:
                    result_edges.append((current_key, neighbor))
                    result_nodes.add(neighbor)
                    bfs(neighbor, current_depth + 1)

        bfs(start_key, 0)

        return {"nodes": list(result_nodes), "edges": result_edges}
