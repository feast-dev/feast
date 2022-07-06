import io
from typing import Any, Dict, List, Optional

import avro.schema
import pandas as pd
import pytz
from avro.io import BinaryEncoder, DatumWriter
from kafka import KafkaAdminClient, KafkaProducer

from feast import Client
from feast.wait import wait_retry_backoff


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
        group_ids=[
            group_id
            for group_id, _ in admin.list_consumer_groups()
            if group_id.startswith("spark-kafka-source")
        ]
    )
    subscriptions = {
        subscription
        for group in consumer_groups
        for member in group.members
        if not isinstance(member.member_metadata, bytes)
        for subscription in member.member_metadata.subscription
    }
    return topic_name in subscriptions


def ingest_and_retrieve(
    feast_client: Client,
    df: pd.DataFrame,
    topic_name: str,
    kafka_broker: str,
    avro_schema_json: str,
    entity_rows: List[Dict[str, Any]],
    feature_names: List[Any],
    expected_ingested_count: Optional[int] = None,
):
    expected_ingested_count = expected_ingested_count or df.shape[0]

    for record in df.to_dict("records"):
        record["event_timestamp"] = (
            record["event_timestamp"].to_pydatetime().replace(tzinfo=pytz.utc)
        )

        send_avro_record_to_kafka(
            topic_name,
            record,
            bootstrap_servers=kafka_broker,
            avro_schema_json=avro_schema_json,
        )

    def get_online_features():
        features = feast_client.get_online_features(
            feature_names, entity_rows=entity_rows,
        ).to_dict()
        out_df = pd.DataFrame.from_dict(features)
        return out_df, out_df[feature_names].count().min() >= expected_ingested_count

    ingested = wait_retry_backoff(get_online_features, 180)
    return ingested
