from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from google.cloud.bigtable.instance import Instance

from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.infra.online_stores.bigtable import (
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from tests.integration.feature_repos.universal.online_store.bigtable import (
    BigtableOnlineStoreCreator,
)


class _FeatureView:
    def __init__(self, name, entities, features):
        self.name = name
        self.entities = entities
        self.features = features


@pytest.mark.integration
def test_online_round_trip_with_configured_app_profile():
    creator = BigtableOnlineStoreCreator(
        "test_bigtable_app_profile", app_profile_id="test-app-profile"
    )
    online_store_config = creator.create_online_store()
    try:
        repo_config = RepoConfig(
            registry="file://test_registry/registry.db",
            project="test_bigtable_app_profile",
            provider="gcp",
            online_store=BigtableOnlineStoreConfig(**online_store_config),
            offline_store=FileOfflineStoreConfig(),
            entity_key_serialization_version=2,
        )
        feature_view = _FeatureView(
            name="driver_stats", entities=["driver"], features=[object()]
        )
        store = BigtableOnlineStore()

        # Admin path: no app profile involved (matches design doc).
        store.update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )
        now = datetime.now(timezone.utc)

        # Spy on the real `Instance.table()` (the emulator has no concept of app
        # profiles, so it won't reject or otherwise reveal a wrong/missing one —
        # this is the only way to prove our fork actually threads the configured
        # profile into the real bigtable client, not just that the round trip works).
        with patch.object(
            Instance, "table", autospec=True, side_effect=Instance.table
        ) as mock_table:
            store.online_write_batch(
                config=repo_config,
                table=feature_view,
                data=[
                    (entity_key, {"conv_rate": ValueProto(float_val=0.5)}, now, None)
                ],
                progress=None,
            )

            result = store.online_read(
                config=repo_config, table=feature_view, entity_keys=[entity_key]
            )

        assert len(mock_table.call_args_list) == 2, "expected one table() call for write, one for read"
        for call in mock_table.call_args_list:
            assert call.kwargs["app_profile_id"] == "test-app-profile"

        assert len(result) == 1
        event_ts, values = result[0]
        assert values["conv_rate"].float_val == pytest.approx(0.5)
    finally:
        creator.teardown()
