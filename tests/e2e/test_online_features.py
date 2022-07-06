import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Union

import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery

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
from tests.e2e.utils.kafka import check_consumer_exist, ingest_and_retrieve


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
    entity = Entity(
        name="s2id",
        description="S2id",
        value_type=ValueType.INT64,
    )

    feature_table = FeatureTable(
        name="drivers",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=batch_source,
    )

    feast_client.apply(entity)
    feast_client.apply(feature_table)

    original = generate_data()
    feast_client.ingest(feature_table, original)  # write to batch (offline) storage

    ingest_and_verify(feast_client, feature_table, original)


@pytest.mark.env("gcloud")
def test_offline_ingestion_from_bq_view(pytestconfig, bq_dataset, feast_client: Client):
    original = generate_data()
    bq_project = pytestconfig.getoption("bq_project")

    bq_client = bigquery.Client(project=bq_project)
    source_ref = bigquery.TableReference(
        bigquery.DatasetReference(bq_project, bq_dataset),
        f"ingestion_source_{datetime.now():%Y%m%d%H%M%s}",
    )
    bq_client.load_table_from_dataframe(original, source_ref).result()

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

    feast_client.apply(entity)
    feast_client.apply(feature_table)

    ingest_and_verify(feast_client, feature_table, original)


def test_streaming_ingestion(
    feast_client: Client, local_staging_path: str, kafka_server, pytestconfig
):
    entity = Entity(
        name="s2id",
        description="S2id",
        value_type=ValueType.INT64,
    )
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
            event_timestamp_column="event_timestamp",
            bootstrap_servers=kafka_broker,
            message_format=AvroFormat(avro_schema()),
            topic=topic_name,
        ),
    )

    feast_client.apply(entity)
    feast_client.apply(feature_table)

    if not pytestconfig.getoption("scheduled_streaming_job"):
        job = feast_client.start_stream_to_online_ingestion(feature_table)
        assert job.get_feature_table() == feature_table.name
        wait_retry_backoff(
            lambda: (None, job.get_status() == SparkJobStatus.IN_PROGRESS), 180
        )
    else:
        job = None

    wait_retry_backoff(
        lambda: (None, check_consumer_exist(kafka_broker, topic_name)), 300
    )

    test_data = generate_data()[["s2id", "unique_drivers", "event_timestamp"]]

    try:
        ingested = ingest_and_retrieve(
            feast_client,
            test_data,
            avro_schema_json=avro_schema(),
            topic_name=topic_name,
            kafka_broker=kafka_broker,
            entity_rows=[{"s2id": s2_id} for s2_id in test_data["s2id"].tolist()],
            feature_names=["drivers_stream:unique_drivers"],
        )
    finally:
        if job:
            job.cancel()
        else:
            feast_client.delete_feature_table(feature_table.name)

    pd.testing.assert_frame_equal(
        ingested[["s2id", "drivers_stream:unique_drivers"]],
        test_data[["s2id", "unique_drivers"]].rename(
            columns={"unique_drivers": "drivers_stream:unique_drivers"}
        ),
    )


def ingest_and_verify(
    feast_client: Client, feature_table: FeatureTable, original: pd.DataFrame
):
    job = feast_client.start_offline_to_online_ingestion(
        feature_table,
        original.event_timestamp.min().to_pydatetime(),
        original.event_timestamp.max().to_pydatetime() + timedelta(seconds=1),
    )
    assert job.get_feature_table() == feature_table.name

    wait_retry_backoff(
        lambda: (None, job.get_status() == SparkJobStatus.COMPLETED), 180
    )

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


def test_list_jobs_long_table_name(
    feast_client: Client, batch_source: Union[BigQuerySource, FileSource]
):
    entity = Entity(
        name="s2id",
        description="S2id",
        value_type=ValueType.INT64,
    )

    feature_table = FeatureTable(
        name="just1a2featuretable3with4a5really6really7really8really9really10really11really12long13name",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=batch_source,
    )

    feast_client.apply(entity)
    feast_client.apply(feature_table)

    data_sample = generate_data()
    feast_client.ingest(feature_table, data_sample)

    job = feast_client.start_offline_to_online_ingestion(
        feature_table,
        data_sample.event_timestamp.min().to_pydatetime(),
        data_sample.event_timestamp.max().to_pydatetime() + timedelta(seconds=1),
    )

    wait_retry_backoff(
        lambda: (None, job.get_status() == SparkJobStatus.COMPLETED), 180
    )
    all_job_ids = [
        job.get_id()
        for job in feast_client.list_jobs(
            include_terminated=True, table_name=feature_table.name
        )
    ]
    assert job.get_id() in all_job_ids


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
