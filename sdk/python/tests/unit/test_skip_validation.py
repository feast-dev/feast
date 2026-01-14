"""
Tests for skip_validation parameter in FeatureStore.apply() and FeatureStore.plan()

This feature allows users to skip Feature View validation when the validation system
is being overly strict. This is particularly important for:
- Feature transformations that go through validation (e.g., _construct_random_input in ODFVs)
- Cases where the type/validation system is being too restrictive

Users should be encouraged to report issues on GitHub when they need to use this flag.
"""
from datetime import timedelta
from typing import Any, Dict

import pandas as pd
import pytest

from feast import Entity, FeatureView, Field
from feast.data_source import RequestSource
from feast.feature_store import FeatureStore
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64


def test_apply_with_skip_validation_default(tmp_path):
    """Test that FeatureStore.apply() works with default skip_validation=False"""
    
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
    
    # Apply with default skip_validation (should be False)
    fs.apply([entity, fv])
    
    # Verify the feature view was applied
    feature_views = fs.list_feature_views()
    assert len(feature_views) == 1
    assert feature_views[0].name == "test_fv"
    
    fs.teardown()


def test_apply_with_skip_validation_true(tmp_path):
    """Test that FeatureStore.apply() accepts skip_validation=True parameter"""
    
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
    
    # Apply with skip_validation=True
    # This should skip the _validate_all_feature_views() call
    fs.apply([entity, fv], skip_validation=True)
    
    # Verify the feature view was applied
    feature_views = fs.list_feature_views()
    assert len(feature_views) == 1
    assert feature_views[0].name == "test_fv"
    
    fs.teardown()


def test_plan_with_skip_validation_parameter(tmp_path):
    """Test that FeatureStore.plan() accepts skip_validation parameter"""
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


def test_skip_validation_use_case_documentation():
    """
    Documentation test: This test documents the key use case for skip_validation.
    
    The skip_validation flag is particularly important for On-Demand Feature Views (ODFVs)
    that use feature transformations. During the apply() process, ODFVs call infer_features()
    which internally uses _construct_random_input() to validate the transformation.
    
    Sometimes this validation can be overly strict or fail for complex transformations.
    In such cases, users can use skip_validation=True to bypass this check.
    
    Example use case from the issue:
    - User has an ODFV with a complex transformation
    - The _construct_random_input validation fails or is too restrictive
    - User can now call: fs.apply([odfv], skip_validation=True)
    - The ODFV is registered without going through the validation
    
    Note: Users should be encouraged to report such cases on GitHub so the Feast team
    can improve the validation system.
    """
    pass  # This is a documentation test

