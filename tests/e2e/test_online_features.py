import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Union

import avro.schema
import numpy as np
import pandas as pd
import pytest
import pytz
from avro.io import BinaryEncoder, DatumWriter
from google.cloud import bigquery
from kafka.admin import KafkaAdminClient
from kafka.producer import KafkaProducer

from feast import (
    BigQuerySource,
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


def generate_data():
    df = pd.DataFrame(columns=["s2id", "unique_drivers", "event_timestamp"])
    df["s2id"] = np.random.choice(999999, size=100, replace=False)
    df["unique_drivers"] = np.random.randint(0, 1000, 100)
    df["event_timestamp"] = pd.to_datetime(
        np.random.randint(int(time.time()), int(time.time()) + 3600, 100), unit="s"
    )
    df["date"] = df["event_timestamp"].dt.date

    return df


def test_offline_ingestion(
    feast_client: Client, batch_source: Union[BigQuerySource, FileSource]
):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)

    feature_table = FeatureTable(
        name="drivers",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=batch_source,
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    original = generate_data()
    feast_client.ingest(feature_table, original)  # write to batch (offline) storage

    verify_data_ingested(feast_client, feature_table, original)


@pytest.mark.env("gcloud")
def test_offline_ingestion_from_bq_view(pytestconfig, bq_dataset, feast_client: Client):
    original = generate_data()
    bq_project = pytestconfig.getoption("bq_project")

    bq_client = bigquery.Client(project=bq_project)
    source_ref = bigquery.TableReference(
        bigquery.DatasetReference(bq_project, bq_dataset),
        f"ingestion_source_{datetime.now():%Y%m%d%H%M%s}",
    )
    bq_client.load_table_from_dataframe(generate_data(), source_ref).result()

    view_ref = bigquery.TableReference(
        bigquery.DatasetReference(bq_project, bq_dataset),
        f"ingestion_view_{datetime.now():%Y%m%d%H%M%s}",
    )
    view = bigquery.Table(view_ref)
    view.view_query = f"select * from `{source_ref.project}.{source_ref.dataset_id}.{source_ref.table_id}`"
    bq_client.create_table(view)

    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64)
    feature_table = FeatureTable(
        name="bq_ingestion",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=BigQuerySource(
            event_timestamp_column="event_timestamp",
            table_ref=f"{view_ref.project}:{view_ref.dataset_id}.{view_ref.table_id}",
        ),
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)
    verify_data_ingested(feast_client, feature_table, original)


def test_streaming_ingestion(
    feast_client: Client, local_staging_path: str, kafka_server
):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)
    kafka_broker = f"{kafka_server[0]}:{kafka_server[1]}"
    topic_name = f"avro-{uuid.uuid4()}"

    feature_table = FeatureTable(
        name="drivers_stream",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=FileSource(
            event_timestamp_column="event_timestamp",
            created_timestamp_column="event_timestamp",
            file_format=ParquetFormat(),
            file_url=os.path.join(local_staging_path, "batch-storage"),
        ),
        stream_source=KafkaSource(
            "event_timestamp",
            "event_timestamp",
            kafka_broker,
            AvroFormat(avro_schema()),
            topic=topic_name,
        ),
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    job = feast_client.start_stream_to_online_ingestion(feature_table)

    wait_retry_backoff(
        lambda: (None, job.get_status() == SparkJobStatus.IN_PROGRESS), 60
    )

    wait_retry_backoff(
        lambda: (None, check_consumer_exist(kafka_broker, topic_name)), 60
    )

    try:
        original = generate_data()[["s2id", "unique_drivers", "event_timestamp"]]
        for record in original.to_dict("records"):
            record["event_timestamp"] = (
                record["event_timestamp"].to_pydatetime().replace(tzinfo=pytz.utc)
            )

            send_avro_record_to_kafka(
                topic_name,
                record,
                bootstrap_servers=kafka_broker,
                avro_schema_json=avro_schema(),
            )

        def get_online_features():
            features = feast_client.get_online_features(
                ["drivers_stream:unique_drivers"],
                entity_rows=[{"s2id": s2_id} for s2_id in original["s2id"].tolist()],
            ).to_dict()
            df = pd.DataFrame.from_dict(features)
            return df, not df["drivers_stream:unique_drivers"].isna().any()

        ingested = wait_retry_backoff(get_online_features, 60)
    finally:
        job.cancel()

    pd.testing.assert_frame_equal(
        ingested[["s2id", "drivers_stream:unique_drivers"]],
        original[["s2id", "unique_drivers"]].rename(
            columns={"unique_drivers": "drivers_stream:unique_drivers"}
        ),
    )


def verify_data_ingested(
    feast_client: Client, feature_table: FeatureTable, original: pd.DataFrame
):
    job = feast_client.start_offline_to_online_ingestion(
        feature_table, datetime.today(), datetime.today() + timedelta(days=1)
    )

    wait_retry_backoff(lambda: (None, job.get_status() == SparkJobStatus.COMPLETED), 60)

    features = feast_client.get_online_features(
        [f"{feature_table.name}:unique_drivers"],
        entity_rows=[{"s2id": s2_id} for s2_id in original["s2id"].tolist()],
    ).to_dict()

    ingested = pd.DataFrame.from_dict(features)
    pd.testing.assert_frame_equal(
        ingested[["s2id", f"{feature_table.name}:unique_drivers"]],
        original[["s2id", "unique_drivers"]].rename(
            columns={"unique_drivers": f"{feature_table.name}:unique_drivers"}
        ),
    )


def avro_schema():
    return json.dumps(
        {
            "type": "record",
            "name": "TestMessage",
            "fields": [
                {"name": "s2id", "type": "long"},
                {"name": "unique_drivers", "type": "long"},
                {
                    "name": "event_timestamp",
                    "type": {"type": "long", "logicalType": "timestamp-micros"},
                },
            ],
        }
    )


def send_avro_record_to_kafka(topic, value, bootstrap_servers, avro_schema_json):
    value_schema = avro.schema.parse(avro_schema_json)

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    writer = DatumWriter(value_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)

    writer.write(value, encoder)

    try:
        producer.send(topic=topic, value=bytes_writer.getvalue())
    except Exception as e:
        print(
            f"Exception while producing record value - {value} to topic - {topic}: {e}"
        )
    else:
        print(f"Successfully producing record value - {value} to topic - {topic}")

    producer.flush()


def check_consumer_exist(bootstrap_servers, topic_name):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer_groups = admin.describe_consumer_groups(
        group_ids=[group_id for group_id, _ in admin.list_consumer_groups()]
    )
    subscriptions = {
        subscription
        for group in consumer_groups
        for member in group.members
        if not isinstance(member.member_metadata, bytes)
        for subscription in member.member_metadata.subscription
    }
    return topic_name in subscriptions
