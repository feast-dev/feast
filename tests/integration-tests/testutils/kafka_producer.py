import argparse

import numpy as np
import pandas as pd
from feast.types.FeatureRow_pb2 import FeatureRow
from feast.types.Feature_pb2 import Feature
from feast.types.Value_pb2 import Value
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer

from testutils.spec import get_entity_name, get_feature_infos


def produce_feature_rows(
    entity_name, feature_infos, feature_values_filepath, bootstrap_servers, topic
):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    feature_values = pd.read_csv(
        feature_values_filepath,
        names=["id", "event_timestamp"] + [f["name"] for f in feature_infos],
        dtype=dict(
            [("id", np.string_)] + [(f["name"], f["dtype"]) for f in feature_infos]
        ),
        parse_dates=["event_timestamp"],
    )

    for i, row in feature_values.iterrows():
        feature_row = FeatureRow()
        feature_row.entityKey = row["id"]
        feature_row.entityName = entity_name

        timestamp = Timestamp()
        timestamp.FromJsonString(row["event_timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ"))
        feature_row.eventTimestamp.CopyFrom(timestamp)

        for info in feature_infos:
            feature = Feature()
            feature.id = info["id"]
            feature_value = Value()
            feature_name = info["name"]
            if info["dtype"] is "Int64":
                feature_value.int64Val = row[feature_name]
            elif info["dtype"] is "Int32":
                feature_value.int32Val = row[feature_name]
            elif info["dtype"] is np.float64:
                feature_value.doubleVal = row[feature_name]
            else:
                raise RuntimeError(
                    f"Unsupported dtype: {info['dtype']}\n"
                    "Supported valueType: INT32, INT64, FLOAT, DOUBLE\n"
                    "Please update your feature specs in testdata/feature_specs folder"
                )
            feature.value.CopyFrom(feature_value)
            feature_row.features.extend([feature])

        producer.send(topic, feature_row.SerializeToString())
        producer.flush()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap_servers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--entity_spec_file", required=True)
    parser.add_argument("--feature_spec_files", required=True)
    parser.add_argument("--feature_values_file", required=True)
    args = parser.parse_args()

    entity_name = get_entity_name(args.entity_spec_file)
    feature_infos = get_feature_infos(args.feature_spec_files)

    produce_feature_rows(
        entity_name,
        feature_infos,
        args.feature_values_file,
        args.bootstrap_servers,
        args.topic,
    )


if __name__ == "__main__":
    main()
