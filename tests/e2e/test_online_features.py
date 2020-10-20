import io
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import avro.schema
import numpy as np
import pandas as pd
import pyspark
import pytest
import pytz
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer

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


def generate_data():
    df = pd.DataFrame(columns=["s2id", "unique_drivers", "event_timestamp"])
    df["s2id"] = np.random.choice(999999, size=100, replace=False)
    df["unique_drivers"] = np.random.randint(0, 1000, 100)
    df["event_timestamp"] = pd.to_datetime(
        np.random.randint(int(time.time()), int(time.time()) + 3600, 100), unit="s"
    )
    df["date"] = df["event_timestamp"].dt.date

    return df


@pytest.fixture(scope="session")
def feast_version():
    return "0.8-SNAPSHOT"


@pytest.fixture(scope="session")
def ingestion_job_jar(pytestconfig, feast_version):
    default_path = (
        Path(__file__).parent.parent.parent
        / "spark"
        / "ingestion"
        / "target"
        / f"feast-ingestion-spark-{feast_version}.jar"
    )

    return pytestconfig.getoption("ingestion_jar") or f"file://{default_path}"


@pytest.fixture(scope="session")
def feast_client(pytestconfig, ingestion_job_jar):
    redis_host, redis_port = pytestconfig.getoption("redis_url").split(":")

    if pytestconfig.getoption("env") == "local":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="standalone",
            spark_standalone_master="local",
            spark_home=os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__),
            spark_ingestion_jar=ingestion_job_jar,
            redis_host=redis_host,
            redis_port=redis_port,
        )

    if pytestconfig.getoption("env") == "gcloud":
        return Client(
            core_url=pytestconfig.getoption("core_url"),
            serving_url=pytestconfig.getoption("serving_url"),
            spark_launcher="dataproc",
            dataproc_cluster_name=pytestconfig.getoption("dataproc_cluster_name"),
            dataproc_project=pytestconfig.getoption("dataproc_project"),
            dataproc_region=pytestconfig.getoption("dataproc_region"),
            dataproc_staging_location=os.path.join(
                pytestconfig.getoption("staging_path"), "dataproc"
            ),
            spark_ingestion_jar=ingestion_job_jar,
        )


@pytest.fixture(scope="function")
def staging_path(pytestconfig, tmp_path):
    if pytestconfig.getoption("env") == "local":
        return f"file://{tmp_path}"

    staging_path = pytestconfig.getoption("staging_path")
    return os.path.join(staging_path, str(uuid.uuid4()))


def test_offline_ingestion(feast_client: Client, staging_path: str):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)

    feature_table = FeatureTable(
        name="drivers",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=FileSource(
            "event_timestamp",
            "event_timestamp",
            ParquetFormat(),
            os.path.join(staging_path, "batch-storage"),
        ),
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    original = generate_data()
    feast_client.ingest(feature_table, original)  # write to batch (offline) storage

    job = feast_client.start_offline_to_online_ingestion(
        feature_table, datetime.today(), datetime.today() + timedelta(days=1)
    )

    wait_retry_backoff(lambda: (None, job.get_status() == SparkJobStatus.COMPLETED), 60)

    features = feast_client.get_online_features(
        ["drivers:unique_drivers"],
        entity_rows=[{"s2id": s2_id} for s2_id in original["s2id"].tolist()],
    ).to_dict()

    ingested = pd.DataFrame.from_dict(features)
    pd.testing.assert_frame_equal(
        ingested[["s2id", "drivers:unique_drivers"]],
        original[["s2id", "unique_drivers"]].rename(
            columns={"unique_drivers": "drivers:unique_drivers"}
        ),
    )


def test_streaming_ingestion(feast_client: Client, staging_path: str, pytestconfig):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)

    feature_table = FeatureTable(
        name="drivers_stream",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=FileSource(
            "event_timestamp",
            "event_timestamp",
            ParquetFormat(),
            os.path.join(staging_path, "batch-storage"),
        ),
        stream_source=KafkaSource(
            "event_timestamp",
            "event_timestamp",
            pytestconfig.getoption("kafka_brokers"),
            AvroFormat(avro_schema()),
            topic="avro",
        ),
    )

    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    job = feast_client.start_stream_to_online_ingestion(feature_table)

    wait_retry_backoff(
        lambda: (None, job.get_status() == SparkJobStatus.IN_PROGRESS), 60
    )

    try:
        original = generate_data()[["s2id", "unique_drivers", "event_timestamp"]]
        for record in original.to_dict("records"):
            record["event_timestamp"] = (
                record["event_timestamp"].to_pydatetime().replace(tzinfo=pytz.utc)
            )

            send_avro_record_to_kafka(
                "avro",
                record,
                bootstrap_servers=pytestconfig.getoption("kafka_brokers"),
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

    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "request.timeout.ms": "1000",
    }

    producer = Producer(producer_config)

    writer = DatumWriter(value_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)

    writer.write(value, encoder)

    try:
        producer.produce(topic=topic, value=bytes_writer.getvalue())
    except Exception as e:
        print(
            f"Exception while producing record value - {value} to topic - {topic}: {e}"
        )
    else:
        print(f"Successfully producing record value - {value} to topic - {topic}")

    producer.flush()
