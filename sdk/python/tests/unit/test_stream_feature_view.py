import copy
from datetime import datetime, timedelta

import pytest

from feast import utils
from feast.aggregation import Aggregation
from feast.batch_feature_view import BatchFeatureView
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource, PushSource
from feast.entity import Entity
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.stream_feature_view import StreamFeatureView, stream_feature_view
from feast.types import Float32


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
    assert (
        sfv_proto.spec.feature_transformation.user_defined_function.name == "simple_udf"
    )


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


def test_stream_feature_view_proto_type():
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    sfv = StreamFeatureView(
        name="test stream featureview proto class",
        entities=[],
        ttl=timedelta(days=30),
        source=stream_source,
        aggregations=[],
    )
    assert sfv.proto_class is StreamFeatureViewProto


def test_stream_feature_view_copy():
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    sfv = StreamFeatureView(
        name="test stream featureview proto class",
        entities=[],
        ttl=timedelta(days=30),
        source=stream_source,
        aggregations=[],
    )
    assert sfv == copy.copy(sfv)


def test_update_materialization_intervals():
    entity = Entity(name="driver_entity", join_keys=["test_key"])
    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )

    # Create a stream feature view that is already present in the SQL registry
    stored_stream_feature_view = StreamFeatureView(
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
    current_time = datetime.utcnow()
    start_date = utils.make_tzaware(current_time - timedelta(days=1))
    end_date = utils.make_tzaware(current_time)
    stored_stream_feature_view.materialization_intervals.append((start_date, end_date))

    # Update the stream feature view i.e. here it's simply the name
    updated_stream_feature_view = StreamFeatureView(
        name="test kafka stream feature view updated",
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

    updated_stream_feature_view.update_materialization_intervals(
        stored_stream_feature_view.materialization_intervals
    )

    assert (
        updated_stream_feature_view.materialization_intervals is not None
        and len(stored_stream_feature_view.materialization_intervals) == 1
    )
    assert (
        updated_stream_feature_view.materialization_intervals[0][0]
        == stored_stream_feature_view.materialization_intervals[0][0]
    )
    assert (
        updated_stream_feature_view.materialization_intervals[0][1]
        == stored_stream_feature_view.materialization_intervals[0][1]
    )
