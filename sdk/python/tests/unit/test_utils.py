"""
Tests for feast.utils module.

These unit tests cover the _convert_rows_to_protobuf function which is critical
for online feature retrieval performance. The function converts raw database
rows to protobuf format for the serving response.
"""

from datetime import datetime, timezone

from feast.protos.feast.serving.ServingService_pb2 import FieldStatus
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.utils import _convert_rows_to_protobuf


class TestConvertRowsToProtobuf:
    """Tests for _convert_rows_to_protobuf function."""

    def test_basic_conversion(self):
        """Test basic conversion with single feature and entity."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        value = ValueProto(float_val=1.5)

        read_rows = [(timestamp, {"feature_1": value})]
        requested_features = ["feature_1"]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        assert len(result) == 1
        ts_vector, status_vector, value_vector = result[0]
        assert len(ts_vector) == 1
        assert ts_vector[0].seconds == int(timestamp.timestamp())
        assert value_vector[0] == value

    def test_multiple_features_same_timestamp(self):
        """Test that multiple features share the same pre-computed timestamp.

        This verifies the optimization: timestamps are computed once per entity,
        not once per feature per entity.
        """
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        value1 = ValueProto(float_val=1.0)
        value2 = ValueProto(float_val=2.0)

        read_rows = [(timestamp, {"feature_1": value1, "feature_2": value2})]
        requested_features = ["feature_1", "feature_2"]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        assert len(result) == 2
        ts1 = result[0][0][0]
        ts2 = result[1][0][0]
        assert ts1.seconds == ts2.seconds
        assert ts1.seconds == int(timestamp.timestamp())

    def test_multiple_entities(self):
        """Test conversion with multiple entities having different timestamps."""
        ts1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

        read_rows = [
            (ts1, {"feature_1": ValueProto(float_val=1.0)}),
            (ts2, {"feature_1": ValueProto(float_val=2.0)}),
        ]
        requested_features = ["feature_1"]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        assert len(result) == 1
        ts_vector, status_vector, value_vector = result[0]
        assert len(ts_vector) == 2
        assert ts_vector[0].seconds == int(ts1.timestamp())
        assert ts_vector[1].seconds == int(ts2.timestamp())

    def test_null_timestamp_handling(self):
        """Test that null timestamps produce empty Timestamp proto."""
        read_rows = [
            (None, {"feature_1": ValueProto(float_val=1.0)}),
            (
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                {"feature_1": ValueProto(float_val=2.0)},
            ),
        ]
        requested_features = ["feature_1"]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        ts_vector = result[0][0]
        assert ts_vector[0].seconds == 0  # Null timestamp -> empty proto
        assert ts_vector[1].seconds != 0  # Valid timestamp

    def test_missing_feature_data(self):
        """Test handling of missing feature data (None row)."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        read_rows = [
            (timestamp, {"feature_1": ValueProto(float_val=1.0)}),
            (timestamp, None),  # No feature data for this entity
        ]
        requested_features = ["feature_1"]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        ts_vector, status_vector, value_vector = result[0]
        assert len(ts_vector) == 2
        assert status_vector[0] == FieldStatus.PRESENT
        assert status_vector[1] == FieldStatus.NOT_FOUND

    def test_feature_not_in_row(self):
        """Test handling when requested feature is not in the row's data."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        read_rows = [
            (timestamp, {"feature_1": ValueProto(float_val=1.0)}),
        ]
        requested_features = ["feature_1", "feature_2"]  # feature_2 not in data

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        assert len(result) == 2
        # feature_1 is present
        assert result[0][1][0] == FieldStatus.PRESENT
        # feature_2 is not found
        assert result[1][1][0] == FieldStatus.NOT_FOUND

    def test_empty_inputs(self):
        """Test handling of empty inputs."""
        # Empty rows
        result = _convert_rows_to_protobuf(["feature_1"], [])
        assert len(result) == 1
        assert len(result[0][0]) == 0  # Empty ts_vector

        # Empty features
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _convert_rows_to_protobuf([], [(timestamp, {"f": ValueProto()})])
        assert len(result) == 0

    def test_large_scale_correctness(self):
        """Test correctness with large number of features and entities.

        This test verifies that the optimized implementation produces correct
        results at scale (50 features x 500 entities = 25,000 data points).
        """
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        num_entities = 500
        num_features = 50

        feature_data = {
            f"feature_{i}": ValueProto(float_val=float(i)) for i in range(num_features)
        }
        read_rows = [(timestamp, feature_data.copy()) for _ in range(num_entities)]
        requested_features = [f"feature_{i}" for i in range(num_features)]

        result = _convert_rows_to_protobuf(requested_features, read_rows)

        # Verify structure
        assert len(result) == num_features
        for feature_idx, (ts_vector, status_vector, value_vector) in enumerate(result):
            assert len(ts_vector) == num_entities
            assert len(status_vector) == num_entities
            assert len(value_vector) == num_entities

            # Verify all timestamps are the same (pre-computed once)
            expected_ts = int(timestamp.timestamp())
            for ts in ts_vector:
                assert ts.seconds == expected_ts

            # Verify all statuses are PRESENT
            for status in status_vector:
                assert status == FieldStatus.PRESENT
