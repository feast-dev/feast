import argparse

from feast.types.FeatureRow_pb2 import FeatureRow
from kafka import KafkaConsumer


def log_feature_row_messages(bootstrap_servers, topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)
    for record in consumer:
        feature_row = FeatureRow()
        feature_row.ParseFromString(record.value)
        print(feature_row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()
    log_feature_row_messages(args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    main()
