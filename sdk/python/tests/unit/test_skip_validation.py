"""
Tests for skip_validation parameter in FeatureStore.apply() and FeatureStore.plan()
"""
from datetime import timedelta
from typing import Any, Dict

import pandas as pd

from feast import Entity, FeatureView, Field
from feast.data_source import RequestSource
from feast.feature_store import FeatureStore
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64


def test_apply_with_skip_validation(tmp_path):
    """Test that FeatureStore.apply() works with skip_validation=True"""
    
    # Create a temporary feature store
    fs = FeatureStore(
        config=f"""
project: test_skip_validation
registry: {tmp_path / "registry.db"}
provider: local
online_store:
    type: sqlite
    path: {tmp_path / "online_store.db"}
"""
    )
    
    # Create a basic feature view
    batch_source = FileSource(
        path=str(tmp_path / "data.parquet"),
        timestamp_field="event_timestamp",
    )
    
    entity = Entity(name="test_entity", join_keys=["entity_id"])
    
    fv = FeatureView(
        name="test_fv",
        entities=[entity],
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="entity_id", dtype=Int64),
        ],
        source=batch_source,
        ttl=timedelta(days=1),
    )
    
    # Apply with skip_validation=False (default)
    fs.apply([entity, fv], skip_validation=False)
    
    # Verify the feature view was applied
    feature_views = fs.list_feature_views()
    assert len(feature_views) == 1
    assert feature_views[0].name == "test_fv"
    
    # Apply again with skip_validation=True
    fv2 = FeatureView(
        name="test_fv2",
        entities=[entity],
        schema=[
            Field(name="feature2", dtype=Float32),
            Field(name="entity_id", dtype=Int64),
        ],
        source=batch_source,
        ttl=timedelta(days=1),
    )
    
    fs.apply([fv2], skip_validation=True)
    
    # Verify both feature views are present
    feature_views = fs.list_feature_views()
    assert len(feature_views) == 2
    
    fs.teardown()


def test_apply_odfv_with_skip_validation(tmp_path):
    """Test that skip_validation works for OnDemandFeatureViews to bypass _construct_random_input validation"""
    
    # Create a temporary feature store
    fs = FeatureStore(
        config=f"""
project: test_skip_odfv
registry: {tmp_path / "registry.db"}
provider: local
online_store:
    type: sqlite
    path: {tmp_path / "online_store.db"}
"""
    )
    
    # Create a basic feature view
    batch_source = FileSource(
        path=str(tmp_path / "data.parquet"),
        timestamp_field="event_timestamp",
    )
    
    entity = Entity(name="test_entity", join_keys=["entity_id"])
    
    fv = FeatureView(
        name="base_fv",
        entities=[entity],
        schema=[
            Field(name="input_feature", dtype=Int64),
            Field(name="entity_id", dtype=Int64),
        ],
        source=batch_source,
        ttl=timedelta(days=1),
    )
    
    # Create a request source
    request_source = RequestSource(
        name="request_source",
        schema=[Field(name="request_input", dtype=Int64)],
    )
    
    # Define an ODFV with transformation
    @on_demand_feature_view(
        sources=[fv, request_source],
        schema=[Field(name="output_feature", dtype=Int64)],
    )
    def test_odfv(inputs: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "output_feature": inputs["input_feature"] + inputs["request_input"]
        }
    
    # Apply with skip_validation=True to bypass infer_features() validation
    # This is the key use case mentioned in the issue
    fs.apply([entity, fv, test_odfv], skip_validation=True)
    
    # Verify the ODFV was applied
    odfvs = fs.list_on_demand_feature_views()
    assert len(odfvs) == 1
    assert odfvs[0].name == "test_odfv"
    
    fs.teardown()


def test_plan_with_skip_validation(tmp_path):
    """Test that FeatureStore.plan() works with skip_validation=True"""
    from feast.feature_store import RepoContents
    
    # Create a temporary feature store
    fs = FeatureStore(
        config=f"""
project: test_plan_skip
registry: {tmp_path / "registry.db"}
provider: local
online_store:
    type: sqlite
    path: {tmp_path / "online_store.db"}
"""
    )
    
    # Create a basic feature view
    batch_source = FileSource(
        path=str(tmp_path / "data.parquet"),
        timestamp_field="event_timestamp",
    )
    
    entity = Entity(name="test_entity", join_keys=["entity_id"])
    
    fv = FeatureView(
        name="test_fv",
        entities=[entity],
        schema=[
            Field(name="feature1", dtype=Int64),
            Field(name="entity_id", dtype=Int64),
        ],
        source=batch_source,
        ttl=timedelta(days=1),
    )
    
    # Create repo contents
    repo_contents = RepoContents(
        feature_views=[fv],
        entities=[entity],
        data_sources=[batch_source],
        on_demand_feature_views=[],
        stream_feature_views=[],
        feature_services=[],
        permissions=[],
    )
    
    # Plan with skip_validation=False (default)
    registry_diff, infra_diff, new_infra = fs.plan(repo_contents, skip_validation=False)
    
    # Verify the diff shows the feature view will be added
    assert len(registry_diff.fv_to_add) == 1
    
    # Plan with skip_validation=True
    registry_diff, infra_diff, new_infra = fs.plan(repo_contents, skip_validation=True)
    
    # Verify the diff still works correctly
    assert len(registry_diff.fv_to_add) == 1
    
    fs.teardown()
