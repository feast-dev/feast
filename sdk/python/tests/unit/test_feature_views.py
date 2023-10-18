from datetime import timedelta

import pytest

from feast.aggregation import Aggregation
from feast.batch_feature_view import BatchFeatureView
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource, PushSource
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.types.Value_pb2 import ValueType
from feast.stream_feature_view import StreamFeatureView, stream_feature_view
from feast.types import Float32


def test_create_feature_view_with_conflicting_entities():
    user1 = Entity(name="user1", join_keys=["user_id"])
    user2 = Entity(name="user2", join_keys=["user_id"])
    batch_source = FileSource(path="some path")

    with pytest.raises(ValueError):
        _ = FeatureView(
            name="test",
            entities=[user1, user2],
            ttl=timedelta(days=30),
            source=batch_source,
        )


def test_create_batch_feature_view():
    batch_source = FileSource(path="some path")
    BatchFeatureView(
        name="test batch feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=batch_source,
    )

    with pytest.raises(TypeError):
        BatchFeatureView(
            name="test batch feature view", entities=[], ttl=timedelta(days=30)
        )

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    with pytest.raises(ValueError):
        BatchFeatureView(
            name="test batch feature view",
            entities=[],
            ttl=timedelta(days=30),
            source=stream_source,
        )


def test_create_stream_feature_view():
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    StreamFeatureView(
        name="test kafka stream feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=stream_source,
        aggregations=[],
    )

    push_source = PushSource(
        name="push source", batch_source=FileSource(path="some path")
    )
    StreamFeatureView(
        name="test push source feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=push_source,
        aggregations=[],
    )

    with pytest.raises(TypeError):
        StreamFeatureView(
            name="test batch feature view",
            entities=[],
            ttl=timedelta(days=30),
            aggregations=[],
        )

    with pytest.raises(ValueError):
        StreamFeatureView(
            name="test batch feature view",
            entities=[],
            ttl=timedelta(days=30),
            source=FileSource(path="some path"),
            aggregations=[],
        )


def simple_udf(x: int):
    return x + 3


def test_stream_feature_view_serialization():
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
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
            )
        ],
        timestamp_field="event_timestamp",
        mode="spark",
        source=stream_source,
        udf=simple_udf,
        tags={},
    )

    sfv_proto = sfv.to_proto()

    new_sfv = StreamFeatureView.from_proto(sfv_proto=sfv_proto)
    assert new_sfv == sfv


def test_stream_feature_view_udfs():
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
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
                column="dummy_field",
                function="max",
                time_window=timedelta(days=1),
            )
        ],
        timestamp_field="event_timestamp",
        source=stream_source,
    )
    def pandas_udf(pandas_df):
        import pandas as pd

        assert type(pandas_df) == pd.DataFrame
        df = pandas_df.transform(lambda x: x + 10, axis=1)
        return df

    import pandas as pd

    df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
    sfv = pandas_udf
    sfv_proto = sfv.to_proto()
    new_sfv = StreamFeatureView.from_proto(sfv_proto)
    new_df = new_sfv.udf(df)

    expected_df = pd.DataFrame({"A": [11, 12, 13], "B": [20, 30, 40]})

    assert new_df.equals(expected_df)


def test_stream_feature_view_initialization_with_optional_fields_omitted():
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )

    sfv = StreamFeatureView(
        name="test kafka stream feature view",
        entities=[entity],
        schema=[],
        description="desc",
        timestamp_field="event_timestamp",
        source=stream_source,
        tags={},
    )
    sfv_proto = sfv.to_proto()

    new_sfv = StreamFeatureView.from_proto(sfv_proto=sfv_proto)
    assert new_sfv == sfv


def test_hash():
    file_source = FileSource(name="my-file-source", path="test.parquet")
    feature_view_1 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    feature_view_2 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
        source=file_source,
    )
    feature_view_3 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
    )
    feature_view_4 = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[Field(name="feature1", dtype=Float32)],
        source=file_source,
        description="test",
    )

    s1 = {feature_view_1, feature_view_2}
    assert len(s1) == 1

    s2 = {feature_view_1, feature_view_3}
    assert len(s2) == 2

    s3 = {feature_view_3, feature_view_4}
    assert len(s3) == 2

    s4 = {feature_view_1, feature_view_2, feature_view_3, feature_view_4}
    assert len(s4) == 3


# TODO(felixwang9817): Add tests for proto conversion.
# TODO(felixwang9817): Add tests for field mapping logic.


def test_field_types():
    with pytest.raises(TypeError):
        Field(name="name", dtype=ValueType.INT32)
