from datetime import timedelta

import pytest

from feast import PushSource
from feast.batch_feature_view import BatchFeatureView
from feast.data_format import AvroFormat
from feast.data_source import KafkaSource
from feast.infra.offline_stores.file_source import FileSource
from feast.stream_feature_view import StreamFeatureView


def test_create_batch_feature_view():
    batch_source = FileSource(path="some path")
    BatchFeatureView(
        name="test batch feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=batch_source,
    )

    with pytest.raises(ValueError):
        BatchFeatureView(
            name="test batch feature view", entities=[], ttl=timedelta(days=30)
        )

    stream_source = KafkaSource(
        name="kafka",
        timestamp_field="",
        bootstrap_servers="",
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
        timestamp_field="",
        bootstrap_servers="",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=FileSource(path="some path"),
    )
    StreamFeatureView(
        name="test kafka stream feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=stream_source,
    )

    push_source = PushSource(
        name="push source", batch_source=FileSource(path="some path")
    )
    StreamFeatureView(
        name="test push source feature view",
        entities=[],
        ttl=timedelta(days=30),
        source=push_source,
    )

    with pytest.raises(ValueError):
        StreamFeatureView(
            name="test batch feature view", entities=[], ttl=timedelta(days=30)
        )

    with pytest.raises(ValueError):
        StreamFeatureView(
            name="test batch feature view",
            entities=[],
            ttl=timedelta(days=30),
            source=FileSource(path="some path"),
        )
