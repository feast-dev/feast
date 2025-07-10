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
        registry = Registry()

        # Create entities
        entity1_spec = EntitySpecV2(name="user_id")
        entity1 = Entity(spec=entity1_spec)

        entity2_spec = EntitySpecV2(name="product_id")
        entity2 = Entity(spec=entity2_spec)

        registry.entities.extend([entity1, entity2])

        # Create data source
        ds = DataSource()
        ds.name = "user_data_source"
        registry.data_sources.append(ds)

        # Create feature views
        fv1_spec = FeatureViewSpec(name="user_features")
        fv1_spec.entities.append("user_id")
        fv1_spec.batch_source.CopyFrom(ds)
        fv1 = FeatureView(spec=fv1_spec)

        fv2_spec = FeatureViewSpec(name="product_features")
        fv2_spec.entities.append("product_id")
        fv2 = FeatureView(spec=fv2_spec)

        registry.feature_views.extend([fv1, fv2])

        # Test object relationship filtering
        lineage_generator = RegistryLineageGenerator()

        # Test basic filtering (original test coverage)
        basic_relationships = lineage_generator.get_object_relationships(
            registry, "featureView", "user_features", include_indirect=False
        )
        assert isinstance(basic_relationships, list)

        # Test filtering for specific entity with detailed validation
        user_entity_relationships = lineage_generator.get_object_relationships(
            registry, "entity", "user_id", include_indirect=True
        )

        # Should include both direct (Entity -> FeatureView) and indirect (Entity -> DataSource) relationships
        assert len(user_entity_relationships) >= 2

        # Check that all relationships involve user_id
        for rel in user_entity_relationships:
            assert (
                rel.source.type == FeastObjectType.ENTITY
                and rel.source.name == "user_id"
            ) or (
                rel.target.type == FeastObjectType.ENTITY
                and rel.target.name == "user_id"
            )

        # Test filtering for different entity
        product_entity_relationships = lineage_generator.get_object_relationships(
            registry, "entity", "product_id", include_indirect=True
        )

        # Should have fewer relationships (no data source connection)
        assert len(product_entity_relationships) >= 1

        # Check that all relationships involve product_id
        for rel in product_entity_relationships:
            assert (
                rel.source.type == FeastObjectType.ENTITY
                and rel.source.name == "product_id"
            ) or (
                rel.target.type == FeastObjectType.ENTITY
                and rel.target.name == "product_id"
            )

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

    def test_entity_to_feature_view_relationships(self):
        """Test direct Entity -> FeatureView relationships."""
        registry = Registry()

        # Create entities
        entity1_spec = EntitySpecV2(name="user_id")
        entity1 = Entity(spec=entity1_spec)

        entity2_spec = EntitySpecV2(name="product_id")
        entity2 = Entity(spec=entity2_spec)

        registry.entities.extend([entity1, entity2])

        # Create feature views with entities
        fv1_spec = FeatureViewSpec(name="user_features")
        fv1_spec.entities.append("user_id")
        fv1 = FeatureView(spec=fv1_spec)

        fv2_spec = FeatureViewSpec(name="product_features")
        fv2_spec.entities.append("product_id")
        fv2 = FeatureView(spec=fv2_spec)

        # Feature view with multiple entities
        fv3_spec = FeatureViewSpec(name="user_product_features")
        fv3_spec.entities.append("user_id")
        fv3_spec.entities.append("product_id")
        fv3 = FeatureView(spec=fv3_spec)

        registry.feature_views.extend([fv1, fv2, fv3])

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        direct_relationships, indirect_relationships = (
            lineage_generator.generate_lineage(registry)
        )

        # Filter Entity -> FeatureView relationships
        entity_to_fv_relationships = [
            rel
            for rel in direct_relationships
            if rel.source.type == FeastObjectType.ENTITY
            and rel.target.type == FeastObjectType.FEATURE_VIEW
        ]

        # Should have 4 relationships:
        # user_id -> user_features
        # product_id -> product_features
        # user_id -> user_product_features
        # product_id -> user_product_features
        assert len(entity_to_fv_relationships) == 4

        # Check specific relationships
        relationship_pairs = {
            (rel.source.name, rel.target.name) for rel in entity_to_fv_relationships
        }

        expected_pairs = {
            ("user_id", "user_features"),
            ("product_id", "product_features"),
            ("user_id", "user_product_features"),
            ("product_id", "user_product_features"),
        }

        assert relationship_pairs == expected_pairs

        # Test relationship types
        for rel in entity_to_fv_relationships:
            assert rel.source.type == FeastObjectType.ENTITY
            assert rel.target.type == FeastObjectType.FEATURE_VIEW

    def test_entity_to_data_source_relationships(self):
        """Test indirect Entity -> DataSource relationships through feature views."""
        registry = Registry()

        # Create entities
        entity1_spec = EntitySpecV2(name="user_id")
        entity1 = Entity(spec=entity1_spec)

        entity2_spec = EntitySpecV2(name="product_id")
        entity2 = Entity(spec=entity2_spec)

        registry.entities.extend([entity1, entity2])

        # Create data sources
        ds1 = DataSource()
        ds1.name = "user_data_source"

        ds2 = DataSource()
        ds2.name = "product_data_source"

        ds3 = DataSource()
        ds3.name = "combined_data_source"

        registry.data_sources.extend([ds1, ds2, ds3])

        # Create feature views with entities and batch sources
        fv1_spec = FeatureViewSpec(name="user_features")
        fv1_spec.entities.append("user_id")
        fv1_spec.batch_source.CopyFrom(ds1)  # Link to user_data_source
        fv1 = FeatureView(spec=fv1_spec)

        fv2_spec = FeatureViewSpec(name="product_features")
        fv2_spec.entities.append("product_id")
        fv2_spec.batch_source.CopyFrom(ds2)  # Link to product_data_source
        fv2 = FeatureView(spec=fv2_spec)

        # Feature view with multiple entities and data source
        fv3_spec = FeatureViewSpec(name="user_product_features")
        fv3_spec.entities.append("user_id")
        fv3_spec.entities.append("product_id")
        fv3_spec.batch_source.CopyFrom(ds3)  # Link to combined_data_source
        fv3 = FeatureView(spec=fv3_spec)

        registry.feature_views.extend([fv1, fv2, fv3])

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        direct_relationships, indirect_relationships = (
            lineage_generator.generate_lineage(registry)
        )

        # Filter Entity -> DataSource relationships (should be in indirect relationships)
        entity_to_ds_relationships = [
            rel
            for rel in indirect_relationships
            if rel.source.type == FeastObjectType.ENTITY
            and rel.target.type == FeastObjectType.DATA_SOURCE
        ]

        # Should have 4 relationships:
        # user_id -> user_data_source (through user_features)
        # product_id -> product_data_source (through product_features)
        # user_id -> combined_data_source (through user_product_features)
        # product_id -> combined_data_source (through user_product_features)
        assert len(entity_to_ds_relationships) == 4

        # Check specific relationships
        relationship_pairs = {
            (rel.source.name, rel.target.name) for rel in entity_to_ds_relationships
        }

        expected_pairs = {
            ("user_id", "user_data_source"),
            ("product_id", "product_data_source"),
            ("user_id", "combined_data_source"),
            ("product_id", "combined_data_source"),
        }

        assert relationship_pairs == expected_pairs

        # Test relationship types
        for rel in entity_to_ds_relationships:
            assert rel.source.type == FeastObjectType.ENTITY
            assert rel.target.type == FeastObjectType.DATA_SOURCE

    def test_entity_relationships_with_unnamed_data_sources(self):
        """Test Entity -> DataSource relationships with unnamed data sources."""
        registry = Registry()

        entity_spec = EntitySpecV2(name="user_id")
        entity = Entity(spec=entity_spec)
        registry.entities.append(entity)

        ds_with_table = DataSource()
        ds_with_table.bigquery_options.table = "users_table"
        ds_with_table.type = DataSource.SourceType.BATCH_BIGQUERY

        ds_with_path = DataSource()
        ds_with_path.file_options.uri = "/path/to/users.parquet"
        ds_with_path.type = DataSource.SourceType.BATCH_FILE

        registry.data_sources.extend([ds_with_table, ds_with_path])

        fv1_spec = FeatureViewSpec(name="user_features_from_table")
        fv1_spec.entities.append("user_id")
        fv1_spec.batch_source.CopyFrom(ds_with_table)
        fv1 = FeatureView(spec=fv1_spec)

        fv2_spec = FeatureViewSpec(name="user_features_from_path")
        fv2_spec.entities.append("user_id")
        fv2_spec.batch_source.CopyFrom(ds_with_path)
        fv2 = FeatureView(spec=fv2_spec)

        registry.feature_views.extend([fv1, fv2])

        lineage_generator = RegistryLineageGenerator()
        direct_relationships, indirect_relationships = (
            lineage_generator.generate_lineage(registry)
        )

        # Filter Entity -> DataSource relationships
        entity_to_ds_relationships = [
            rel
            for rel in indirect_relationships
            if rel.source.type == FeastObjectType.ENTITY
            and rel.target.type == FeastObjectType.DATA_SOURCE
        ]

        # Should have 2 relationships with generated names
        assert len(entity_to_ds_relationships) == 2

        ds_names = {rel.target.name for rel in entity_to_ds_relationships}
        for name in ds_names:
            assert name.startswith("unnamed_source_"), (
                f"Expected unnamed_source_ prefix, got {name}"
            )

        # All relationships should involve the user_id entity
        for rel in entity_to_ds_relationships:
            assert rel.source.type == FeastObjectType.ENTITY
            assert rel.source.name == "user_id"
            assert rel.target.type == FeastObjectType.DATA_SOURCE

    def test_complex_entity_data_source_lineage(self):
        """Test complex scenarios with multiple entities and data sources."""
        registry = Registry()

        # Create entities
        entities = []
        for i in range(3):
            entity_spec = EntitySpecV2(name=f"entity_{i}")
            entity = Entity(spec=entity_spec)
            entities.append(entity)
        registry.entities.extend(entities)

        # Create data sources
        data_sources = []
        for i in range(3):
            ds = DataSource()
            ds.name = f"data_source_{i}"
            data_sources.append(ds)
        registry.data_sources.extend(data_sources)

        # Create feature views with various entity combinations
        # Single entity feature views
        for i in range(3):
            fv_spec = FeatureViewSpec(name=f"feature_view_{i}")
            fv_spec.entities.append(f"entity_{i}")
            fv_spec.batch_source.CopyFrom(data_sources[i])
            fv = FeatureView(spec=fv_spec)
            registry.feature_views.append(fv)

        # Multi-entity feature view
        fv_multi_spec = FeatureViewSpec(name="multi_entity_feature_view")
        fv_multi_spec.entities.extend(["entity_0", "entity_1", "entity_2"])
        fv_multi_spec.batch_source.CopyFrom(data_sources[0])  # Uses first data source
        fv_multi = FeatureView(spec=fv_multi_spec)
        registry.feature_views.append(fv_multi)

        # Generate lineage
        lineage_generator = RegistryLineageGenerator()
        direct_relationships, indirect_relationships = (
            lineage_generator.generate_lineage(registry)
        )

        # Count Entity -> FeatureView relationships
        entity_to_fv_relationships = [
            rel
            for rel in direct_relationships
            if rel.source.type == FeastObjectType.ENTITY
            and rel.target.type == FeastObjectType.FEATURE_VIEW
        ]
        # Should have 6 relationships: 3 single + 3 from multi-entity view
        assert len(entity_to_fv_relationships) == 6

        # Count Entity -> DataSource relationships
        entity_to_ds_relationships = [
            rel
            for rel in indirect_relationships
            if rel.source.type == FeastObjectType.ENTITY
            and rel.target.type == FeastObjectType.DATA_SOURCE
        ]
        # Should have 6 relationships: 3 single + 3 from multi-entity view (all to data_source_0)
        assert len(entity_to_ds_relationships) == 6

        # Verify that each entity is connected to the correct data sources
        entity_ds_connections = {}
        for rel in entity_to_ds_relationships:
            entity_name = rel.source.name
            ds_name = rel.target.name
            if entity_name not in entity_ds_connections:
                entity_ds_connections[entity_name] = set()
            entity_ds_connections[entity_name].add(ds_name)

        # Each entity should be connected to its own data source + data_source_0 (from multi-entity view)
        for i in range(3):
            entity_name = f"entity_{i}"
            expected_ds = {f"data_source_{i}", "data_source_0"}
            assert entity_ds_connections[entity_name] == expected_ds
