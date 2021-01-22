from feast import (
    Client,
    Entity,
    Feature,
    FeatureTable,
    FileSource,
    KafkaSource,
    ValueType,
)
from feast.data_format import AvroFormat, ParquetFormat
from feast.pyspark.abc import SparkJobStatus
from feast.wait import wait_retry_backoff


def create_schema(kafka_broker, topic_name, feature_table_name, avro_schema):
    entity = Entity(name="key", description="Key", value_type=ValueType.INT64)
    feature_table = FeatureTable(
        name=feature_table_name,
        entities=["key"],
        features=[Feature("num", ValueType.INT64), Feature("set", ValueType.STRING)],
        batch_source=FileSource(
            event_timestamp_column="event_timestamp",
            file_format=ParquetFormat(),
            file_url="/dev/null",
        ),
        stream_source=KafkaSource(
            event_timestamp_column="event_timestamp",
            bootstrap_servers=kafka_broker,
            message_format=AvroFormat(avro_schema),
            topic=topic_name,
        ),
    )
    return entity, feature_table


def start_job(feast_client: Client, feature_table: FeatureTable, pytestconfig):
    if pytestconfig.getoption("scheduled_streaming_job"):
        return

    job = feast_client.start_stream_to_online_ingestion(feature_table)
    wait_retry_backoff(
        lambda: (None, job.get_status() == SparkJobStatus.IN_PROGRESS), 240
    )
    return job


def stop_job(job, feast_client: Client, feature_table: FeatureTable):
    if job:
        job.cancel()
    else:
        feast_client.delete_feature_table(feature_table.name)
