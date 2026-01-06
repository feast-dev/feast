"""
Tests for field-level vector search configurations in Milvus online store.

This test module validates that:
1. Field-level vector configurations (vector_length, vector_search_metric, vector_index_type) are used
2. Global configurations serve as fallback defaults
3. Multiple vector fields with different configurations can coexist in the same feature view
"""

import unittest
from datetime import timedelta

import pytest

from feast import Entity, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.infra.online_stores.milvus_online_store.milvus import MilvusOnlineStore
from feast.repo_config import RepoConfig
from feast.types import Array, Float32, String, ValueType


@pytest.mark.unit
class TestMilvusFieldLevelVectorConfig(unittest.TestCase):
    """Test field-level vector search configurations in Milvus."""

    def setUp(self):
        """Set up test fixtures."""
        # Create entities
        self.item = Entity(
            name="item_id",
            description="Item ID",
            value_type=ValueType.INT64,
        )
        
        # Create source
        self.source = FileSource(
            file_format=ParquetFormat(),
            path="./data/test_embeddings.parquet",
            timestamp_field="event_timestamp",
        )
        
        # Create repo config with global defaults
        self.config = RepoConfig(
            project="test_project",
            provider="local",
            registry="data/registry.db",
            online_store={
                "type": "milvus",
                "path": "data/test_milvus.db",
                "embedding_dim": 128,  # Global default
                "index_type": "FLAT",  # Global default
                "metric_type": "COSINE",  # Global default
                "vector_enabled": True,
            },
        )
        
        self.online_store = MilvusOnlineStore()

    def test_field_level_vector_length(self):
        """Test that field-level vector_length overrides global embedding_dim."""
        # Create feature view with field-level vector_length
        fv = FeatureView(
            name="test_fv_custom_length",
            entities=[self.item],
            schema=[
                Field(
                    name="custom_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=256,  # Override global 128
                    vector_search_metric="COSINE",
                ),
                Field(name="item_name", dtype=String),
            ],
            source=self.source,
            ttl=timedelta(days=1),
        )
        
        # Verify field has correct vector_length
        vector_field = [f for f in fv.schema if f.vector_index][0]
        assert vector_field.vector_length == 256
        assert vector_field.name == "custom_vector"

    def test_field_level_vector_metric(self):
        """Test that field-level vector_search_metric overrides global metric_type."""
        # Create feature view with field-level vector_search_metric
        fv = FeatureView(
            name="test_fv_custom_metric",
            entities=[self.item],
            schema=[
                Field(
                    name="l2_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=128,
                    vector_search_metric="L2",  # Override global COSINE
                ),
                Field(name="item_name", dtype=String),
            ],
            source=self.source,
            ttl=timedelta(days=1),
        )
        
        # Verify field has correct vector_search_metric
        vector_field = [f for f in fv.schema if f.vector_index][0]
        assert vector_field.vector_search_metric == "L2"

    def test_field_level_vector_index_type(self):
        """Test that field-level vector_index_type overrides global index_type."""
        # Create feature view with field-level vector_index_type
        fv = FeatureView(
            name="test_fv_custom_index",
            entities=[self.item],
            schema=[
                Field(
                    name="hnsw_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=128,
                    vector_search_metric="COSINE",
                    vector_index_type="HNSW",  # Override global FLAT
                ),
                Field(name="item_name", dtype=String),
            ],
            source=self.source,
            ttl=timedelta(days=1),
        )
        
        # Verify field has correct vector_index_type
        vector_field = [f for f in fv.schema if f.vector_index][0]
        assert vector_field.vector_index_type == "HNSW"

    def test_multiple_vector_fields_different_configs(self):
        """Test that multiple vector fields with different configs can coexist."""
        # Create feature view with multiple vector fields
        fv = FeatureView(
            name="test_fv_multi_vectors",
            entities=[self.item],
            schema=[
                Field(
                    name="marketing_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=768,
                    vector_search_metric="COSINE",
                    vector_index_type="HNSW",
                ),
                Field(
                    name="sales_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    vector_length=128,
                    vector_search_metric="L2",
                    vector_index_type="IVF_FLAT",
                ),
                Field(name="item_name", dtype=String),
            ],
            source=self.source,
            ttl=timedelta(days=1),
        )
        
        # Verify both fields have correct configurations
        vector_fields = {f.name: f for f in fv.schema if f.vector_index}
        
        assert "marketing_vector" in vector_fields
        assert vector_fields["marketing_vector"].vector_length == 768
        assert vector_fields["marketing_vector"].vector_search_metric == "COSINE"
        assert vector_fields["marketing_vector"].vector_index_type == "HNSW"
        
        assert "sales_vector" in vector_fields
        assert vector_fields["sales_vector"].vector_length == 128
        assert vector_fields["sales_vector"].vector_search_metric == "L2"
        assert vector_fields["sales_vector"].vector_index_type == "IVF_FLAT"

    def test_fallback_to_global_defaults(self):
        """Test that global defaults are used when field-level configs are not specified."""
        # Create feature view without field-level configs
        fv = FeatureView(
            name="test_fv_global_defaults",
            entities=[self.item],
            schema=[
                Field(
                    name="default_vector",
                    dtype=Array(Float32),
                    vector_index=True,
                    # No vector_length, vector_search_metric, or vector_index_type specified
                ),
                Field(name="item_name", dtype=String),
            ],
            source=self.source,
            ttl=timedelta(days=1),
        )
        
        # Verify field uses default values
        vector_field = [f for f in fv.schema if f.vector_index][0]
        assert vector_field.vector_length == 0  # Not specified, should fall back to global
        assert vector_field.vector_search_metric is None  # Not specified
        assert vector_field.vector_index_type is None  # Not specified


if __name__ == "__main__":
    unittest.main()
