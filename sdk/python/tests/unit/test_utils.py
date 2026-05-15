"""
Tests for feast.utils module.

These unit tests cover the _populate_response_from_feature_data function
which converts raw online_read rows into protobuf FeatureVectors and
populates the GetOnlineFeaturesResponse.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.utils import _populate_response_from_feature_data


def _make_table(name="test_fv"):
    """Create a minimal mock FeatureView for testing."""
    table = MagicMock()
    table.projection.name_to_use.return_value = name
    table.projection.name_alias = None
    table.projection.name = name
    return table


class TestPopulateResponseFromFeatureData:
    """Tests for _populate_response_from_feature_data function."""

    def test_basic_single_feature(self):
        """Test basic conversion with single feature and single entity."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        value = ValueProto(float_val=1.5)

        read_rows = [(timestamp, {"feature_1": value})]
        indexes = ([0],)
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=1,
        )

        assert len(response.results) == 1
        assert response.results[0].values[0] == value
        assert response.results[0].statuses[0] == FieldStatus.PRESENT
        assert response.results[0].event_timestamps[0].seconds == int(
            timestamp.timestamp()
        )
        assert list(response.metadata.feature_names.val) == ["feature_1"]

    def test_multiple_features_same_entity(self):
        """Test multiple features from the same row."""
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        v1 = ValueProto(float_val=1.0)
        v2 = ValueProto(float_val=2.0)

        read_rows = [(timestamp, {"feature_1": v1, "feature_2": v2})]
        indexes = ([0],)
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1", "feature_2"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=1,
        )

        assert len(response.results) == 2
        assert response.results[0].values[0] == v1
        assert response.results[1].values[0] == v2
        ts1 = response.results[0].event_timestamps[0].seconds
        ts2 = response.results[1].event_timestamps[0].seconds
        assert ts1 == ts2 == int(timestamp.timestamp())

    def test_multiple_entities_deduplication(self):
        """Test that duplicate entity rows are correctly mapped via indexes."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        val = ValueProto(float_val=42.0)

        read_rows = [(ts, {"feature_1": val})]
        indexes = ([0, 1, 2],)  # One unique row maps to 3 output positions
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=3,
        )

        assert len(response.results[0].values) == 3
        for i in range(3):
            assert response.results[0].values[i] == val
            assert response.results[0].statuses[i] == FieldStatus.PRESENT

    def test_null_timestamp_handling(self):
        """Test that null timestamps produce empty Timestamp proto."""
        read_rows = [
            (None, {"feature_1": ValueProto(float_val=1.0)}),
            (
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                {"feature_1": ValueProto(float_val=2.0)},
            ),
        ]
        indexes = ([0],), ([1],)
        indexes = ([0], [1])
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=2,
        )

        ts_list = response.results[0].event_timestamps
        assert ts_list[0].seconds == 0  # Null timestamp -> empty proto
        assert ts_list[1].seconds != 0  # Valid timestamp

    def test_missing_feature_data(self):
        """Test handling of missing feature data (None row)."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        read_rows = [
            (ts, {"feature_1": ValueProto(float_val=1.0)}),
            (ts, None),
        ]
        indexes = ([0], [1])
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=2,
        )

        assert response.results[0].statuses[0] == FieldStatus.PRESENT
        assert response.results[0].statuses[1] == FieldStatus.NOT_FOUND

    def test_feature_not_in_row(self):
        """Test handling when requested feature is not in the row's data."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        read_rows = [(ts, {"feature_1": ValueProto(float_val=1.0)})]
        indexes = ([0],)
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1", "feature_2"],
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=1,
        )

        assert len(response.results) == 2
        assert response.results[0].statuses[0] == FieldStatus.PRESENT
        assert response.results[1].statuses[0] == FieldStatus.NOT_FOUND

    def test_empty_inputs(self):
        """Test handling of empty inputs."""
        response = GetOnlineFeaturesResponse(results=[])
        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=[],
            indexes=(),
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=0,
        )
        assert len(response.results) == 1
        assert len(response.results[0].values) == 0

        response2 = GetOnlineFeaturesResponse(results=[])
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        _populate_response_from_feature_data(
            requested_features=[],
            read_rows=[(ts, {"f": ValueProto()})],
            indexes=([0],),
            online_features_response=response2,
            full_feature_names=False,
            table=_make_table(),
            output_len=1,
        )
        assert len(response2.results) == 0

    def test_full_feature_names(self):
        """Test that full_feature_names prefixes feature names with table name."""
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        read_rows = [(ts, {"feature_1": ValueProto(float_val=1.0)})]
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=["feature_1"],
            read_rows=read_rows,
            indexes=([0],),
            online_features_response=response,
            full_feature_names=True,
            table=_make_table("my_fv"),
            output_len=1,
        )

        assert list(response.metadata.feature_names.val) == ["my_fv__feature_1"]

    def test_large_scale_correctness(self):
        """Test correctness with large number of features and entities.

        This test verifies that the fused implementation produces correct
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
        indexes = tuple([i] for i in range(num_entities))
        response = GetOnlineFeaturesResponse(results=[])

        _populate_response_from_feature_data(
            requested_features=requested_features,
            read_rows=read_rows,
            indexes=indexes,
            online_features_response=response,
            full_feature_names=False,
            table=_make_table(),
            output_len=num_entities,
        )

        assert len(response.results) == num_features
        expected_ts = int(timestamp.timestamp())
        for feature_idx in range(num_features):
            fv = response.results[feature_idx]
            assert len(fv.values) == num_entities
            assert len(fv.statuses) == num_entities
            assert len(fv.event_timestamps) == num_entities

            for ts in fv.event_timestamps:
                assert ts.seconds == expected_ts
            for status in fv.statuses:
                assert status == FieldStatus.PRESENT
