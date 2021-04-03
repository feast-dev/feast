import os
import time
from datetime import datetime

import pytest

from feast import FeatureStore, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig
from tests.cli_utils import CliRunner, get_example_repo


def test_online() -> None:
    """
    Test reading from the online store in local mode.
    """
    runner = CliRunner()
    with runner.local_repo(get_example_repo("example_feature_repo_1.py")) as store:
        # Write some data to two tables
        registry = store._registry
        table = registry.get_feature_view(
            project=store.config.project, name="driver_locations"
        )
        table_2 = registry.get_feature_view(
            project=store.config.project, name="driver_locations_2"
        )

        provider = store._get_provider()

        entity_key = EntityKeyProto(
            entity_names=["driver"], entity_values=[ValueProto(int64_val=1)]
        )
        provider.online_write_batch(
            project=store.config.project,
            table=table,
            data=[
                (
                    entity_key,
                    {
                        "lat": ValueProto(double_val=0.1),
                        "lon": ValueProto(string_val="1.0"),
                    },
                    datetime.utcnow(),
                    datetime.utcnow(),
                )
            ],
            progress=None,
        )

        provider.online_write_batch(
            project=store.config.project,
            table=table_2,
            data=[
                (
                    entity_key,
                    {
                        "lat": ValueProto(double_val=2.0),
                        "lon": ValueProto(string_val="2.0"),
                    },
                    datetime.utcnow(),
                    datetime.utcnow(),
                )
            ],
            progress=None,
        )

        # Retrieve two features using two keys, one valid one non-existing
        result = store.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )

        assert "driver_locations__lon" in result.to_dict()
        assert result.to_dict()["driver_locations__lon"] == ["1.0", None]
        assert result.to_dict()["driver_locations_2__lon"] == ["2.0", None]

        # invalid table reference
        with pytest.raises(ValueError):
            store.get_online_features(
                feature_refs=["driver_locations_bad:lon"], entity_rows=[{"driver": 1}],
            )

        # Create new FeatureStore object with fast cache invalidation
        cache_ttl = 1
        fs_fast_ttl = FeatureStore(
            config=RepoConfig(
                registry=RegistryConfig(
                    path=store.config.registry, cache_ttl_seconds=cache_ttl
                ),
                online_store=store.config.online_store,
                project=store.config.project,
                provider=store.config.provider,
            )
        )

        # Should download the registry and cache it permanently (or until manually refreshed)
        result = fs_fast_ttl.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations__lon"] == ["1.0", None]

        # Rename the registry.db so that it cant be used for refreshes
        os.rename(store.config.registry, store.config.registry + "_fake")

        # Wait for registry to expire
        time.sleep(cache_ttl)

        # Will try to reload registry because it has expired (it will fail because we deleted the actual registry file)
        with pytest.raises(FileNotFoundError):
            fs_fast_ttl.get_online_features(
                feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
                entity_rows=[{"driver": 1}, {"driver": 123}],
            )

        # Restore registry.db so that we can see if it actually reloads registry
        os.rename(store.config.registry + "_fake", store.config.registry)

        # Test if registry is actually reloaded and whether results return
        result = fs_fast_ttl.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations__lon"] == ["1.0", None]

        # Create a registry with infinite cache (for users that want to manually refresh the registry)
        fs_infinite_ttl = FeatureStore(
            config=RepoConfig(
                registry=RegistryConfig(
                    path=store.config.registry, cache_ttl_seconds=0
                ),
                online_store=store.config.online_store,
                project=store.config.project,
                provider=store.config.provider,
            )
        )

        # Should return results (and fill the registry cache)
        result = fs_infinite_ttl.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations__lon"] == ["1.0", None]

        # Wait a bit so that an arbitrary TTL would take effect
        time.sleep(2)

        # Rename the registry.db so that it cant be used for refreshes
        os.rename(store.config.registry, store.config.registry + "_fake")

        # TTL is infinite so this method should use registry cache
        result = fs_infinite_ttl.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations__lon"] == ["1.0", None]

        # Force registry reload (should fail because file is missing)
        with pytest.raises(FileNotFoundError):
            fs_infinite_ttl.refresh_registry()

        # Restore registry.db so that teardown works
        os.rename(store.config.registry + "_fake", store.config.registry)
