import json
from time import sleep

import pandas as pd
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "-m",
    "--mode",
    default="setup",
    choices=["setup", "teardown"],
    help="Whether to setup or teardown a Kafka topic with driver stats events",
)

args = parser.parse_args()


def create_stream(topic_name, servers=["localhost:9092"]):
    topic_name = topic_name
    producer = KafkaProducer(bootstrap_servers=servers)

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.create_topics([topic])
        print(f"Topic {topic_name} created")
    except Exception as e:
        print(str(e))
        pass

    # Sort values to support tumbling_epoch for input building
    df = pd.read_parquet("feature_repo/data/driver_stats.parquet").sort_values(
        by="event_timestamp"
    )
    print("Emitting events")
    for row in df[["driver_id", "event_timestamp", "created", "miles_driven",]].to_dict(
        "records"
    ):
        # Make event one more year recent to simulate fresher data
        row["event_timestamp"] = (
            row["event_timestamp"] + pd.Timedelta(weeks=52)
        ).strftime("%Y-%m-%d %H:%M:%S")
        row["created"] = row["created"].strftime("%Y-%m-%d %H:%M:%S")
        producer.send(topic_name, json.dumps(row).encode())
        sleep(0.1)


def teardown_stream(topic_name, servers=["localhost:9092"]):
    try:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        print(admin.delete_topics([topic_name]))
        print(f"Topic {topic_name} deleted")
    except Exception as e:
        print(str(e))
        pass


if __name__ == "__main__":
    parsed_args = vars(args)
    mode = parsed_args["mode"]
    if mode == "setup":
        create_stream("drivers")
    else:
        teardown_stream("drivers")
