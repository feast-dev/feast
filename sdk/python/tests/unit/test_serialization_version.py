import os

from assertpy import assertpy

from feast import FeatureStore, RepoConfig


def test_registry_entity_serialization_version():
    r = RepoConfig(
        project="prompt_dory",
        provider="local",
        online_store="redis",
        registry=f"{os.path.dirname(__file__)}/test_project/registry.db",
    )
    fs: FeatureStore = FeatureStore(config=r)
    fvs = fs.list_feature_views()
    assertpy.assert_that(fvs[0].entity_key_serialization_version).is_equal_to(1)
