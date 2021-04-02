import os
from datetime import datetime

import pytest

from feast import FeatureStore, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from tests.cli_utils import CliRunner, get_example_repo


def test_online() -> None:
    """
    Test reading from the online store in local mode.
    """
    runner = CliRunner()
    with runner.local_repo(get_example_repo("example_feature_repo_1.py")) as store:
        # Write some data to two tables
        registry = store._get_registry()
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

        assert "driver_locations:lon" in result.to_dict()
        assert result.to_dict()["driver_locations:lon"] == ["1.0", None]
        assert result.to_dict()["driver_locations_2:lon"] == ["2.0", None]

        # invalid table reference
        with pytest.raises(ValueError):
            store.get_online_features(
                feature_refs=["driver_locations_bad:lon"], entity_rows=[{"driver": 1}],
            )

        # Create new FeatureStore object with auto_refresh_registry disabled
        fs_no_autoload = FeatureStore(config=RepoConfig(
            metadata_store=store.config.metadata_store,
            online_store=store.config.online_store,
            project=store.config.project,
            provider=store.config.provider,
            auto_refresh_registry=False,
            auto_refresh_registry_ttl_seconds=2
        ))

        # Should download the registry and cache it permanently (or until manually refreshed)
        result = fs_no_autoload.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations:lon"] == ["1.0", None]

        # Rename the metadata.db so that it cant be used for refreshes
        os.rename(store.config.metadata_store, store.config.metadata_store + "_fake")

        # Should use cached registry
        result = fs_no_autoload.get_online_features(
            feature_refs=["driver_locations:lon", "driver_locations_2:lon"],
            entity_rows=[{"driver": 1}, {"driver": 123}],
        )
        assert result.to_dict()["driver_locations:lon"] == ["1.0", None]