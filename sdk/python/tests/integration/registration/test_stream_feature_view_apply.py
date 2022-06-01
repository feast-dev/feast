import pytest
from datetime import timedelta

from feast.types import Float32
from feast import StreamFeatureView, FileSource, Entity, Field
from feast.data_source import KafkaSource
from feast.data_format import AvroFormat
from feast.aggregation import Aggregation

@pytest.mark.integration
def test_read_pre_applied(environment) -> None:
    """
    Test apply of StreamFeatureView.
    """
    fs = environment.feature_store

    # Create Feature Views
    entity = Entity(name="driver_entity", join_keys=["test_key"])

    def simple_udf(x: int):
        return x + 3

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(
            path="test_path",
            timestamp_field="event_timestamp"
        )
    )

    sfv = StreamFeatureView(
        name="test kafka stream feature view",
        entities=[entity],
        ttl=timedelta(days=30),
        owner="test@example.com",
        online=True,
        schema=[Field(name="dummy_field", dtype=Float32)],
        description="desc",
        aggregations=[
            Aggregation(
                column="dummy_field",
                function="max",
                time_window=timedelta(days=1),
            ),
            Aggregation(
                column="dummy_field2",
                function="count",
                time_window=timedelta(days=24),
            )
        ],
        timestamp_field="event_timestamp",
        mode="spark",
        source=stream_source,
        udf=simple_udf,
        tags={},
    )
    fs.apply([entity, sfv])
    stream_feature_views = fs.list_stream_feature_views()
    assert len(stream_feature_views) == 1
    assert stream_feature_views[0] == sfv

    entities = fs.list_entities()
    assert len(entities) == 1
    assert entities[0] == entity