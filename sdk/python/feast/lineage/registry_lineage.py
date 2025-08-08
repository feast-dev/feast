"""
Registry lineage generation for Feast objects.

This module provides functionality to generate relationship graphs between
Feast objects (entities, feature views, data sources, feature services)
for lineage visualization.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Tuple

from feast.protos.feast.core.Registry_pb2 import Registry


class FeastObjectType(Enum):
    DATA_SOURCE = "dataSource"
    ENTITY = "entity"
    FEATURE_VIEW = "featureView"
    FEATURE_SERVICE = "featureService"
    FEATURE = "feature"


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

        # FeatureService -> FeatureView relationships
        for feature_service in registry.feature_services:
            if (
                hasattr(feature_service, "spec")
                and feature_service.spec
                and feature_service.spec.features
            ):
                for feature in feature_service.spec.features:
                    rel = EntityRelation(
                        source=EntityReference(
                            FeastObjectType.FEATURE_VIEW, feature.feature_view_name
                        ),
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
                    if (
                        hasattr(source, "request_data_source")
                        and source.request_data_source
                    ):
                        if hasattr(source.request_data_source, "name"):
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
                    elif (
                        hasattr(source, "feature_view_projection")
                        and source.feature_view_projection
                    ):
                        # Find the source feature view's batch source
                        if hasattr(source.feature_view_projection, "feature_view_name"):
                            source_fv = next(
                                (
                                    fv
                                    for fv in registry.feature_views
                                    if hasattr(fv, "spec")
                                    and fv.spec
                                    and hasattr(fv.spec, "name")
                                    and fv.spec.name
                                    == source.feature_view_projection.feature_view_name
                                ),
                                None,
                            )
                            if (
                                source_fv
                                and hasattr(source_fv, "spec")
                                and source_fv.spec
                                and hasattr(source_fv.spec, "batch_source")
                                and source_fv.spec.batch_source
                                and hasattr(source_fv.spec.batch_source, "name")
                            ):
                                relationships.append(
                                    EntityRelation(
                                        source=EntityReference(
                                            FeastObjectType.DATA_SOURCE,
                                            source_fv.spec.batch_source.name,
                                        ),
                                        target=EntityReference(
                                            FeastObjectType.FEATURE_VIEW, odfv.spec.name
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
                        # Find all relationships that target this feature view
                        related_sources = [
                            rel.source
                            for rel in direct_relationships
                            if rel.target.name == feature.feature_view_name
                            and rel.target.type == FeastObjectType.FEATURE_VIEW
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

        # Create Entity -> DataSource relationships (through feature views)
        # Build a map of feature view -> data sources
        feature_view_to_data_sources: Dict[str, List[str]] = {}
        for rel in direct_relationships:
            if (
                rel.source.type == FeastObjectType.DATA_SOURCE
                and rel.target.type == FeastObjectType.FEATURE_VIEW
            ):
                if rel.target.name not in feature_view_to_data_sources:
                    feature_view_to_data_sources[rel.target.name] = []
                feature_view_to_data_sources[rel.target.name].append(rel.source.name)

        # For each Entity -> FeatureView relationship, create Entity -> DataSource relationships
        for rel in direct_relationships:
            if (
                rel.source.type == FeastObjectType.ENTITY
                and rel.target.type == FeastObjectType.FEATURE_VIEW
            ):
                # Find data sources that this feature view uses
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
