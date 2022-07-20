import tempfile

from assertpy import assertpy

from feast import RepoConfig


def test_registry_entity_serialization_version():
    with tempfile.TemporaryDirectory() as tmpdir:
        r = RepoConfig(
            project="prompt_dory",
            provider="local",
            online_store="redis",
            registry=f"{tmpdir}/registry.db",
            entity_key_serialization_version=2,
        )
        assertpy.assert_that(r.entity_key_serialization_version).is_equal_to(2)
