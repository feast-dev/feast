import argparse
import warnings

import numpy as np
import pandas as pd
from feast.sdk.client import Client
from feast.sdk.resources.feature_set import FeatureSet
from feast.sdk.utils import bq_util

# Ignore BigQuery warning when not using service account for cleaner log
from testutils.spec import get_entity_name, get_feature_infos

warnings.simplefilter("ignore")


def validate_warehouse(
    entity_name, feature_infos, feature_values_filepath, dataset, project=None
):
    print("\n[Validating warehouse data]")

    expected = pd.read_csv(
        feature_values_filepath,
        names=["id", "event_timestamp"] + [f["name"] for f in feature_infos],
        dtype=dict(
            [("id", np.string_)] + [(f["name"], f["dtype"]) for f in feature_infos]
        ),
        parse_dates=["event_timestamp"],
    )

    dtypes = {"event_timestamp": "datetime64[ns]"}
    for f in feature_infos:
        dtypes[f["name"]] = f["dtype"]

    # TODO: Retrieve actual values via Feast Core rather than directly from BigQuery
    #       Need to change Python SDK so can retrieve values via Feast Core while
    #       "ensuring correct value types"
    actual = (
        bq_util.query_to_dataframe(
            f"SELECT {','.join(expected.columns)} FROM `{dataset}.{entity_name}_view`",
            project=project,
        )
        .sort_values(["id", "event_timestamp"])
        .reset_index(drop=True)
        .astype(dtypes)
    )[expected.columns]

    pd.testing.assert_frame_equal(expected, actual)

    print("OK")


def validate_serving(entity_name, feature_infos, feature_values_filepath, feast_client):
    print("\n[Validating serving data]")

    expected = pd.read_csv(
        feature_values_filepath,
        names=[entity_name] + [f["id"] for f in feature_infos],
        dtype=dict(
            [(entity_name, np.string_)] + [(f["id"], f["dtype"]) for f in feature_infos]
        ),
    )
    actual = (
        feast_client.get_serving_data(
            FeatureSet(entity=entity_name, features=[f["id"] for f in feature_infos]),
            entity_keys=expected[entity_name].values,
        )
        .sort_values(entity_name)
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(expected, actual)
    print("OK")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity_spec_file", required=True)
    parser.add_argument("--feature_spec_files", required=True)
    parser.add_argument("--expected-warehouse-values-file")
    parser.add_argument("--expected-serving-values-file")
    parser.add_argument("--bigquery-dataset-for-warehouse")
    parser.add_argument("--feast-serving-url")
    parser.add_argument("--project")
    args = parser.parse_args()

    feast_client = Client(serving_url=args.feast_serving_url)
    entity_name = get_entity_name(args.entity_spec_file)
    feature_infos = get_feature_infos(args.feature_spec_files)

    if args.expected_warehouse_values_file:
        if not args.bigquery_dataset_for_warehouse:
            raise RuntimeError("Missing option --bigquery-dataset-for-warehouse")
        validate_warehouse(
            entity_name,
            feature_infos,
            args.expected_warehouse_values_file,
            args.bigquery_dataset_for_warehouse,
            args.project,
        )
    else:
        print("\n[Skipping validation of Feast Warehouse data]")
        print("because --expected-warehouse-values-file is not set")

    if args.expected_serving_values_file:
        validate_serving(
            entity_name, feature_infos, args.expected_serving_values_file, feast_client
        )
    else:
        print("\n[Skipping validation of Feast Serving data]")
        print("because --expected-serving-values-file is not set")


if __name__ == "__main__":
    main()
