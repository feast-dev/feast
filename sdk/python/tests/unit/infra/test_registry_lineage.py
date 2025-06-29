"""Tests for registry lineage functionality."""

from feast.lineage.registry_lineage import (
    EntityReference,
    EntityRelation,
    FeastObjectType,
    RegistryLineageGenerator,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource
from feast.protos.feast.core.Entity_pb2 import Entity, EntitySpecV2
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService,
    FeatureServiceSpec,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView, FeatureViewSpec
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView,
    OnDemandFeatureViewSpec,
)
from feast.protos.feast.core.Registry_pb2 import Registry
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView,
    StreamFeatureViewSpec,
)


class TestRegistryLineage:
    def test_lineage_generator_basic(self):
        """Test basic lineage generation with simple relationships."""

        # Create registry with basic objects
        registry = Registry()

        # Create entity
        entity_spec = EntitySpecV2(name="user_id")
        entity = Entity(spec=entity_spec)
        registry.entities.append(entity)

        # Create data source
        data_source = DataSource()
        registry.data_sources.append(data_source)

        # Create feature view
        fv_spec = FeatureViewSpec(name="user_features")
        fv_spec.entities.append("user_id")
        feature_view = FeatureView(spec=fv_spec)
        registry.feature_views.append(feature_view)

        # Create feature service
        fs_spec = FeatureServiceSpec(name="user_service")
        feature_service = FeatureService(spec=fs_spec)
        registry.feature_services.append(feature_service)

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        # Should return valid results without crashing
        assert isinstance(relationships, list)
        assert isinstance(indirect_relationships, list)

    def test_object_relationships_filtering(self):
        """Test filtering relationships for a specific object."""

        # Create simple registry for testing
        registry = Registry()

        # Create a basic feature view
        fv_spec = FeatureViewSpec(name="user_features")
        feature_view = FeatureView(spec=fv_spec)
        registry.feature_views.append(feature_view)

        # Test object relationship filtering
        lineage_generator = RegistryLineageGenerator()
        relationships = lineage_generator.get_object_relationships(
            registry, "featureView", "user_features", include_indirect=False
        )

        # Should return a list (may be empty for simple test)
        assert isinstance(relationships, list)

    def test_to_proto_fallback(self):
        """Test that to_proto methods work with fallback to dict."""

        entity_ref = EntityReference(FeastObjectType.ENTITY, "test_entity")
        proto_result = entity_ref.to_proto()

        # Should return either protobuf object or dict fallback
        assert proto_result is not None
        if isinstance(proto_result, dict):
            assert proto_result["type"] == "entity"
            assert proto_result["name"] == "test_entity"

        relation = EntityRelation(
            source=EntityReference(FeastObjectType.ENTITY, "source_entity"),
            target=EntityReference(FeastObjectType.FEATURE_VIEW, "target_fv"),
        )
        relation_proto = relation.to_proto()
        assert relation_proto is not None

    def test_empty_registry(self):
        """Test lineage generation with empty registry."""
        registry = Registry()

        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        assert len(relationships) == 0
        assert len(indirect_relationships) == 0

    def test_complex_lineage_scenario(self):
        """Test complex lineage with multiple feature views and services."""
        registry = Registry()

        # Create multiple entities
        entity1_spec = EntitySpecV2(name="user_id")
        entity1 = Entity(spec=entity1_spec)

        entity2_spec = EntitySpecV2(name="product_id")
        entity2 = Entity(spec=entity2_spec)

        registry.entities.extend([entity1, entity2])

        # Create multiple data sources
        ds1 = DataSource()
        ds2 = DataSource()
        registry.data_sources.extend([ds1, ds2])

        # Create feature views
        fv1_spec = FeatureViewSpec(name="user_features")
        fv1_spec.entities.append("user_id")
        fv1 = FeatureView(spec=fv1_spec)

        fv2_spec = FeatureViewSpec(name="product_features")
        fv2_spec.entities.append("product_id")
        fv2 = FeatureView(spec=fv2_spec)

        registry.feature_views.extend([fv1, fv2])

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        # Should return valid results without crashing
        assert isinstance(relationships, list)
        assert isinstance(indirect_relationships, list)

    def test_on_demand_feature_view_lineage(self):
        """Test lineage with on-demand feature views."""
        registry = Registry()

        # Create regular feature view
        fv_spec = FeatureViewSpec(name="base_features")
        fv_spec.entities.append("user_id")
        fv = FeatureView(spec=fv_spec)
        registry.feature_views.append(fv)

        # Create on-demand feature view
        odfv_spec = OnDemandFeatureViewSpec(name="derived_features")
        odfv = OnDemandFeatureView(spec=odfv_spec)
        registry.on_demand_feature_views.append(odfv)

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        # Should handle on-demand feature views without crashing
        assert isinstance(relationships, list)
        assert isinstance(indirect_relationships, list)

    def test_stream_feature_view_lineage(self):
        """Test lineage with stream feature views."""
        registry = Registry()

        # Create stream feature view
        sfv_spec = StreamFeatureViewSpec(name="streaming_features")
        sfv_spec.entities.append("user_id")
        sfv = StreamFeatureView(spec=sfv_spec)
        registry.stream_feature_views.append(sfv)

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        # Should handle stream feature views without crashing
        assert isinstance(relationships, list)
        assert isinstance(indirect_relationships, list)

    def test_lineage_graph_generation(self):
        """Test lineage graph generation for visualization."""
        registry = Registry()

        # Create simple setup
        entity_spec = EntitySpecV2(name="user_id")
        entity = Entity(spec=entity_spec)
        registry.entities.append(entity)

        fv_spec = FeatureViewSpec(name="user_features")
        fv_spec.entities.append("user_id")
        fv = FeatureView(spec=fv_spec)
        registry.feature_views.append(fv)

        # Generate lineage graph
        lineage_generator = RegistryLineageGenerator()
        graph = lineage_generator.get_object_lineage_graph(
            registry, "featureView", "user_features", depth=2
        )

        assert "nodes" in graph
        assert "edges" in graph
        assert isinstance(graph["nodes"], list)
        assert isinstance(graph["edges"], list)

    def test_missing_object_attributes(self):
        """Test lineage generation with objects missing expected attributes."""
        registry = Registry()

        # Create feature view with minimal attributes
        fv_spec = FeatureViewSpec(name="incomplete_fv")
        fv = FeatureView(spec=fv_spec)
        registry.feature_views.append(fv)

        # Create feature service with minimal attributes
        fs_spec = FeatureServiceSpec(name="incomplete_fs")
        fs = FeatureService(spec=fs_spec)
        registry.feature_services.append(fs)

        # Should not crash and should handle gracefully
        lineage_generator = RegistryLineageGenerator()
        relationships, indirect_relationships = lineage_generator.generate_lineage(
            registry
        )

        # Should return empty or minimal relationships without crashing
        assert isinstance(relationships, list)
        assert isinstance(indirect_relationships, list)
