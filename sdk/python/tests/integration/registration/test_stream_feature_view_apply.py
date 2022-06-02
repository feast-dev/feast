from datetime import timedelta

import pytest

from feast import Entity, Field, FileSource
from feast.aggregation import Aggregation
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.stream_feature_view import stream_feature_view
from feast.types import Float32


@pytest.mark.integration
def test_apply_stream_feature_view(environment) -> None:
    """
    Test apply of StreamFeatureView.
    """
    fs = environment.feature_store

    # Create Feature Views
    entity = Entity(name="driver_entity", join_keys=["test_key"])

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="test_path", timestamp_field="event_timestamp"),
        watermark=timedelta(days=1),
    )

    @stream_feature_view(
        entities=[entity],
        ttl=timedelta(days=30),
        owner="test@example.com",
        online=True,
        schema=[Field(name="dummy_field", dtype=Float32)],
        description="desc",
        aggregations=[
            Aggregation(
                column="dummy_field", function="max", time_window=timedelta(days=1),
            ),
            Aggregation(
                column="dummy_field2", function="count", time_window=timedelta(days=24),
            ),
        ],
        timestamp_field="event_timestamp",
        mode="spark",
        source=stream_source,
        tags={},
    )
    def simple_sfv(df):
        return df

    fs.apply([entity, simple_sfv])
    stream_feature_views = fs.list_stream_feature_views()
    assert len(stream_feature_views) == 1
    assert stream_feature_views[0] == simple_sfv

    entities = fs.list_entities()
    assert len(entities) == 1
    assert entities[0] == entity

    features = fs.get_online_features(
        features=["simple_sfv:dummy_field",], entity_rows=[{"test_key": 1001}],
    ).to_dict(include_event_timestamps=True)

    assert "test_key" in features
    assert features["test_key"] == [1001]
    assert "dummy_field" in features
    assert features["dummy_field"] == [None]
