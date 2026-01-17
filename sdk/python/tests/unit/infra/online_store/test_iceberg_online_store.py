import types
from datetime import datetime

import pytest

pyiceberg = pytest.importorskip("pyiceberg")
pyarrow = pytest.importorskip("pyarrow")

from pyiceberg.transforms import IdentityTransform

from feast.infra.online_stores.contrib.iceberg_online_store.iceberg import (
    IcebergOnlineStore,
    IcebergOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto


def test_iceberg_online_store_config_storage_options_isolated():
    config1 = IcebergOnlineStoreConfig()
    config2 = IcebergOnlineStoreConfig()

    config1.storage_options["k"] = "v"

    assert "k" not in config2.storage_options


def test_iceberg_online_store_partition_spec_entity_hash_identity_transform():
    store = IcebergOnlineStore()
    config = IcebergOnlineStoreConfig(partition_strategy="entity_hash")

    spec = store._build_partition_spec(config)

    assert len(spec.fields) == 1
    assert isinstance(spec.fields[0].transform, IdentityTransform)


def test_iceberg_online_read_applies_selected_fields_projection(monkeypatch):
    store = IcebergOnlineStore()

    online_config = IcebergOnlineStoreConfig(
        catalog_type="sql",
        catalog_name="test_catalog",
        uri="sqlite:///dummy.db",
        warehouse="warehouse",
        namespace="online",
        partition_strategy="entity_hash",
        partition_count=256,
    )

    repo_config = types.SimpleNamespace(
        online_store=online_config,
        project="test_project",
        entity_key_serialization_version=3,
    )

    feature_view = types.SimpleNamespace(
        name="driver_stats",
        features=[
            types.SimpleNamespace(name="conv_rate"),
            types.SimpleNamespace(name="acc_rate"),
        ],
    )

    class DummyScan:
        def __init__(self, selected_fields):
            self.selected_fields = selected_fields

        def to_arrow(self):
            return pyarrow.Table.from_pydict({c: [] for c in self.selected_fields})

    class DummyIcebergTable:
        def __init__(self):
            self.scan_kwargs = None

        def scan(self, **kwargs):
            self.scan_kwargs = kwargs
            return DummyScan(kwargs.get("selected_fields", ("*",)))

    dummy_table = DummyIcebergTable()
    dummy_catalog = types.SimpleNamespace(load_table=lambda identifier: dummy_table)

    monkeypatch.setattr(store, "_get_cached_catalog", lambda cfg: dummy_catalog)
    monkeypatch.setattr(store, "_get_table_identifier", lambda cfg, project, tbl: "online.test")

    entity_hashes = iter([1, 2])
    monkeypatch.setattr(store, "_hash_entity_key", lambda *args, **kwargs: next(entity_hashes))

    monkeypatch.setattr(
        store,
        "_convert_arrow_to_feast",
        lambda *args, **kwargs: [(None, None), (None, None)],
    )

    store.online_read(
        config=repo_config,
        table=feature_view,
        entity_keys=[EntityKeyProto(), EntityKeyProto()],
        requested_features=["conv_rate"],
    )

    assert dummy_table.scan_kwargs is not None
    assert dummy_table.scan_kwargs["row_filter"] == "entity_hash IN (1,2)"
    assert dummy_table.scan_kwargs["selected_fields"] == (
        "entity_key",
        "entity_hash",
        "event_ts",
        "created_ts",
        "conv_rate",
    )


def test_deterministic_tie_breaking_with_equal_event_timestamps():
    """Test that created_ts is used as tiebreaker when event_ts values are equal."""
    store = IcebergOnlineStore()

    repo_config = types.SimpleNamespace(
        entity_key_serialization_version=3,
    )

    # Create Arrow table with two rows having same event_ts but different created_ts
    # The row with the later created_ts should win
    entity_key_hex = "abc123"
    event_ts = datetime(2026, 1, 16, 12, 0, 0)

    arrow_table = pyarrow.Table.from_pydict({
        "entity_key": [entity_key_hex, entity_key_hex],
        "entity_hash": [1, 1],
        "event_ts": [event_ts, event_ts],  # Same event_ts
        "created_ts": [
            datetime(2026, 1, 16, 11, 0, 0),  # Earlier created_ts
            datetime(2026, 1, 16, 11, 30, 0),  # Later created_ts (should win)
        ],
        "feature1": [100, 200],  # Different values
    })

    # Mock entity key
    entity_key_proto = EntityKeyProto()

    # Mock serialize_entity_key to return our test hex
    from unittest.mock import patch
    with patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.serialize_entity_key") as mock_serialize:
        mock_serialize.return_value = bytes.fromhex(entity_key_hex)

        result = store._convert_arrow_to_feast(
            arrow_table,
            entity_keys=[entity_key_proto],
            requested_features=["feature1"],
            config=repo_config,
        )

    # Should return the row with later created_ts (value=200)
    assert len(result) == 1
    event_ts_result, features_result = result[0]
    assert event_ts_result == event_ts
    assert features_result is not None
    assert "feature1" in features_result
    # The later created_ts row should win
    assert features_result["feature1"].int64_val == 200


def test_deterministic_tie_breaking_prefers_later_event_ts():
    """Test that later event_ts is preferred over earlier event_ts."""
    store = IcebergOnlineStore()

    repo_config = types.SimpleNamespace(
        entity_key_serialization_version=3,
    )

    entity_key_hex = "abc123"

    arrow_table = pyarrow.Table.from_pydict({
        "entity_key": [entity_key_hex, entity_key_hex],
        "entity_hash": [1, 1],
        "event_ts": [
            datetime(2026, 1, 16, 11, 0, 0),  # Earlier event_ts
            datetime(2026, 1, 16, 12, 0, 0),  # Later event_ts (should win)
        ],
        "created_ts": [
            datetime(2026, 1, 16, 10, 0, 0),
            datetime(2026, 1, 16, 10, 0, 0),
        ],
        "feature1": [100, 200],
    })

    entity_key_proto = EntityKeyProto()

    from unittest.mock import patch
    with patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.serialize_entity_key") as mock_serialize:
        mock_serialize.return_value = bytes.fromhex(entity_key_hex)

        result = store._convert_arrow_to_feast(
            arrow_table,
            entity_keys=[entity_key_proto],
            requested_features=["feature1"],
            config=repo_config,
        )

    assert len(result) == 1
    event_ts_result, features_result = result[0]
    assert event_ts_result == datetime(2026, 1, 16, 12, 0, 0)
    assert features_result["feature1"].int64_val == 200


def test_partition_count_default_is_32():
    """Test that default partition_count is 32 to avoid small file problem."""
    config = IcebergOnlineStoreConfig()
    assert config.partition_count == 32


def test_append_only_warning_shown_once():
    """Test that append-only warning is only logged once per instance."""
    from unittest.mock import MagicMock, patch

    store = IcebergOnlineStore()

    # Mock logger
    mock_logger = MagicMock()

    online_config = IcebergOnlineStoreConfig(
        catalog_type="sql",
        catalog_name="test",
        uri="sqlite:///test.db",
    )

    repo_config = types.SimpleNamespace(
        online_store=online_config,
        project="test",
        entity_key_serialization_version=3,
    )

    feature_view = types.SimpleNamespace(
        name="test_fv",
        features=[types.SimpleNamespace(name="f1", dtype=types.SimpleNamespace(to_value_type=lambda: 3))],
    )

    # Mock dependencies
    with patch.multiple(
        store,
        _get_cached_catalog=MagicMock(return_value=MagicMock()),
        _get_or_create_online_table=MagicMock(return_value=MagicMock(append=MagicMock())),
        _convert_feast_to_arrow=MagicMock(return_value=pyarrow.Table.from_pydict({"col": [1]})),
    ), patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.logger", mock_logger):

        # First write - should warn
        store.online_write_batch(repo_config, feature_view, [], None)
        assert mock_logger.warning.call_count == 1

        # Second write - should NOT warn again
        store.online_write_batch(repo_config, feature_view, [], None)
        assert mock_logger.warning.call_count == 1  # Still 1, not 2


def test_vectorized_deduplication_performance():
    """Test that vectorized deduplication handles large datasets efficiently."""
    import time
    from unittest.mock import patch

    store = IcebergOnlineStore()

    repo_config = types.SimpleNamespace(
        entity_key_serialization_version=3,
    )

    # Create a large dataset with duplicates to test performance
    # 10,000 entities, each with 10 versions (100,000 rows total)
    num_entities = 10_000
    versions_per_entity = 10
    total_rows = num_entities * versions_per_entity

    entity_keys_list = []
    entity_hashes_list = []
    event_ts_list = []
    created_ts_list = []
    feature_values = []

    base_time = datetime(2026, 1, 1, 0, 0, 0)

    for entity_id in range(num_entities):
        # Create proper hex string matching what mock_serialize returns
        entity_key_hex = f"{entity_id:08x}"
        for version in range(versions_per_entity):
            entity_keys_list.append(entity_key_hex)
            entity_hashes_list.append(entity_id % 256)
            # Later versions have later timestamps
            event_ts_list.append(
                datetime(2026, 1, 1 + version, 0, 0, 0)
            )
            created_ts_list.append(
                datetime(2026, 1, 1 + version, 0, version, 0)
            )
            # Latest version has the highest value
            feature_values.append(version * 100)

    arrow_table = pyarrow.Table.from_pydict({
        "entity_key": entity_keys_list,
        "entity_hash": entity_hashes_list,
        "event_ts": event_ts_list,
        "created_ts": created_ts_list,
        "feature1": feature_values,
    })

    # Create entity key protos for a subset of entities
    test_entity_count = 1000
    entity_key_protos = [EntityKeyProto() for _ in range(test_entity_count)]

    # Mock serialize_entity_key to return test entity keys
    def mock_serialize(entity_key_proto, entity_key_serialization_version):
        idx = entity_key_protos.index(entity_key_proto)
        # Create a proper hex string (just the hex digits, no prefix)
        hex_str = f"{idx:08x}"
        return bytes.fromhex(hex_str)

    with patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.serialize_entity_key", side_effect=mock_serialize):
        start_time = time.time()
        result = store._convert_arrow_to_feast(
            arrow_table,
            entity_keys=entity_key_protos,
            requested_features=["feature1"],
            config=repo_config,
        )
        elapsed_time = time.time() - start_time

    # Verify correctness: should return latest version for each entity
    assert len(result) == test_entity_count

    # Check a sample of results
    for i in range(min(10, test_entity_count)):
        event_ts_result, features_result = result[i]
        # Latest version should be selected (version 9 out of 0-9)
        assert features_result is not None
        assert "feature1" in features_result
        # Latest version has value = 9 * 100 = 900
        assert features_result["feature1"].int64_val == 900

    # Performance assertion: should complete in reasonable time
    # With vectorized operations, 100K rows should process in < 1 second
    # Old implementation would take 10-30 seconds for 1M rows
    assert elapsed_time < 2.0, f"Deduplication took {elapsed_time:.2f}s, expected < 2.0s"

    print(f"\nVectorized deduplication performance: {elapsed_time:.3f}s for {total_rows:,} rows")
    print(f"Throughput: {total_rows / elapsed_time:,.0f} rows/sec")


def test_vectorized_deduplication_correctness_multiple_entities():
    """Test that vectorized deduplication correctly handles multiple entities with duplicates."""
    from unittest.mock import patch

    store = IcebergOnlineStore()

    repo_config = types.SimpleNamespace(
        entity_key_serialization_version=3,
    )

    # Create test data with 3 entities, each having 3 versions
    arrow_table = pyarrow.Table.from_pydict({
        "entity_key": [
            "entity_a", "entity_a", "entity_a",  # 3 versions of entity_a
            "entity_b", "entity_b", "entity_b",  # 3 versions of entity_b
            "entity_c", "entity_c", "entity_c",  # 3 versions of entity_c
        ],
        "entity_hash": [1, 1, 1, 2, 2, 2, 3, 3, 3],
        "event_ts": [
            datetime(2026, 1, 1), datetime(2026, 1, 2), datetime(2026, 1, 3),  # entity_a
            datetime(2026, 1, 1), datetime(2026, 1, 2), datetime(2026, 1, 3),  # entity_b
            datetime(2026, 1, 1), datetime(2026, 1, 2), datetime(2026, 1, 3),  # entity_c
        ],
        "created_ts": [
            datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 2, 0, 0), datetime(2026, 1, 3, 0, 0),
            datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 2, 0, 0), datetime(2026, 1, 3, 0, 0),
            datetime(2026, 1, 1, 0, 0), datetime(2026, 1, 2, 0, 0), datetime(2026, 1, 3, 0, 0),
        ],
        "feature1": [10, 20, 30, 40, 50, 60, 70, 80, 90],  # Latest should be 30, 60, 90
    })

    entity_key_protos = [EntityKeyProto(), EntityKeyProto(), EntityKeyProto()]

    def mock_serialize(entity_key_proto, entity_key_serialization_version):
        idx = entity_key_protos.index(entity_key_proto)
        return bytes.fromhex(f"entity_{chr(ord('a') + idx)}")

    with patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.serialize_entity_key", side_effect=mock_serialize):
        result = store._convert_arrow_to_feast(
            arrow_table,
            entity_keys=entity_key_protos,
            requested_features=["feature1"],
            config=repo_config,
        )

    # Verify all entities got their latest values
    assert len(result) == 3

    # entity_a should have value 30 (latest)
    assert result[0][1] is not None
    assert result[0][1]["feature1"].int64_val == 30
    assert result[0][0] == datetime(2026, 1, 3)

    # entity_b should have value 60 (latest)
    assert result[1][1] is not None
    assert result[1][1]["feature1"].int64_val == 60
    assert result[1][0] == datetime(2026, 1, 3)

    # entity_c should have value 90 (latest)
    assert result[2][1] is not None
    assert result[2][1]["feature1"].int64_val == 90
    assert result[2][0] == datetime(2026, 1, 3)


def test_vectorized_deduplication_preserves_entity_order():
    """Test that results are returned in the same order as entity_keys input."""
    from unittest.mock import patch

    store = IcebergOnlineStore()

    repo_config = types.SimpleNamespace(
        entity_key_serialization_version=3,
    )

    # Create data with entities in non-alphabetical order in the table
    arrow_table = pyarrow.Table.from_pydict({
        "entity_key": ["entity_c", "entity_a", "entity_b"],
        "entity_hash": [3, 1, 2],
        "event_ts": [
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
        ],
        "created_ts": [
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
        ],
        "feature1": [300, 100, 200],
    })

    # Request entities in specific order: b, a, c
    entity_key_protos = [EntityKeyProto(), EntityKeyProto(), EntityKeyProto()]
    entity_order = ['b', 'a', 'c']

    def mock_serialize(entity_key_proto, entity_key_serialization_version):
        idx = entity_key_protos.index(entity_key_proto)
        entity_char = entity_order[idx]
        return bytes.fromhex(f"entity_{entity_char}")

    with patch("feast.infra.online_stores.contrib.iceberg_online_store.iceberg.serialize_entity_key", side_effect=mock_serialize):
        result = store._convert_arrow_to_feast(
            arrow_table,
            entity_keys=entity_key_protos,
            requested_features=["feature1"],
            config=repo_config,
        )

    # Verify order matches entity_keys order (b, a, c), not table order (c, a, b)
    assert len(result) == 3
    assert result[0][1]["feature1"].int64_val == 200  # entity_b
    assert result[1][1]["feature1"].int64_val == 100  # entity_a
    assert result[2][1]["feature1"].int64_val == 300  # entity_c

