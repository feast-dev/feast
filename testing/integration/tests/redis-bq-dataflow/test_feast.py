import os
import subprocess
from subprocess import PIPE
from time import sleep

import feast.sdk.utils.gs_utils as utils
import numpy as np
import pandas as pd
import pytest
import yaml
from feast.sdk.client import Client
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_set import FeatureSet
from feast.sdk.utils.bq_util import TableDownloader
from google.cloud import storage


@pytest.fixture
def client():
    return Client(verbose=True)


# Init the system by registering relevant resources.
def _register_resources(client, entities_fldr, features_fldr):
    resources = []
    for ent_file in os.listdir(entities_fldr):
        resources.append(Entity.from_yaml(os.path.join(entities_fldr, ent_file)))
    for feat_file in os.listdir(features_fldr):
        resources.append(Feature.from_yaml(os.path.join(features_fldr, feat_file)))
    client.apply(resources)


# Run an import job given an import spec.
def _run_job_and_wait_for_completion(job_yaml):
    out = subprocess.run(
        "feast jobs run {}".format(job_yaml).split(" "), check=True, stdout=PIPE
    )
    job_id = out.stdout.decode("utf-8").split(" ")[-1]
    job_status, job_complete = "UNKNOWN", False
    terminal_states = ["COMPLETED", "ABORTED", "ERROR", "UNKNOWN"]
    while not job_complete:
        out = subprocess.run(
            "feast get job {}".format(job_id).split(" "), check=True, stdout=PIPE
        )
        job_details = yaml.load(out.stdout.decode("utf-8").replace("\t", " "))
        job_status = job_details["Status"]
        print("Job id {} currently {}".format(job_id, job_status))
        if job_status in terminal_states:
            break
        sleep(10)
    return job_status


def _stage_data(local, remote):
    split = utils.split_gs_path(remote)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(split[0])
    blob = bucket.blob(split[1])

    blob.upload_from_filename(local)


# Get the BQ data and get only columns to compare, then sort by id and timestamp
def _get_data_from_bq_and_sort(table_name, bucket_name):
    got = TableDownloader().download_table_as_df(
        table_name, "gs://{}/test-cases/extract.csv".format(bucket_name)
    )
    return (
        got.drop(["created_timestamp", "job_id"], axis=1)
        .sort_values(["id", "event_timestamp"])
        .reset_index(drop=True)
    )


class TestFeastIntegration:
    def test_end_to_end(self, client):
        project_id = os.environ.get("PROJECT_ID")
        bucket_name = os.environ.get("BUCKET_NAME")

        features = [
            "feature_double_redis",
            "feature_float_redis",
            "feature_int32_redis",
            "feature_int64_redis",
        ]
        expected = self.get_expected_data(features)

        self.run_batch_import(bucket_name, client)
        self.validate_warehouse_data(bucket_name, project_id, expected)
        self.validate_serving_data(client, features, expected)

    @staticmethod
    def get_expected_data(features):
        expected = pd.read_csv(
            "data/test_data.csv",
            header=None,
            names=["id", "event_timestamp"] + features,
        )
        expected = expected.sort_values(["id", "event_timestamp"]).reset_index(
            drop=True
        )
        expected["event_timestamp"] = pd.to_datetime(
            expected["event_timestamp"]
        ).dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        return expected

    @staticmethod
    def run_batch_import(bucket_name, client):
        _stage_data(
            "data/test_data.csv", "gs://{}/test-cases/test_data.csv".format(bucket_name)
        )
        _register_resources(client, "data/entity", "data/feature")
        job_status = _run_job_and_wait_for_completion("data/import/import_csv.yaml")
        assert job_status == "COMPLETED"

    @staticmethod
    def validate_serving_data(client, features, expected):
        features_type_mapping = {
            "myentity": np.string_,
            "myentity.feature_double_redis": np.float64,
            "myentity.feature_float_redis": np.float64,
            "myentity.feature_int32_redis": np.int64,
            "myentity.feature_int64_redis": np.int64,
        }

        feature_set = FeatureSet(
            entity="myentity", features=["myentity." + f for f in features]
        )
        actual_latest = client.get_serving_data(
            feature_set, entity_keys=[str(id) for id in list(expected.id.unique())]
        ).astype(features_type_mapping)
        actual_latest = actual_latest.sort_values(["myentity"])
        expected["event_timestamp"] = pd.to_datetime(expected["event_timestamp"])
        expected_latest = expected.loc[
            expected.groupby("id").event_timestamp.idxmax(), :
        ]
        expected_latest.columns = ["myentity", "timestamp"] + [
            "myentity." + f for f in features
        ]
        expected_latest = (
            expected_latest[actual_latest.columns]
            .sort_values(["myentity"])
            .reset_index(drop=True)
        ).astype(features_type_mapping)

        assert (
            pd.testing.assert_frame_equal(
                expected_latest, actual_latest, check_less_precise=True, check_like=True
            )
            is None
        )

    @staticmethod
    def validate_warehouse_data(bucket_name, project_id, expected):
        features_type_mapping = {
            "id": np.string_,
            "event_timestamp": np.string_,
            "feature_double_redis": np.float64,
            "feature_float_redis": np.float64,
            "feature_int32_redis": np.int64,
            "feature_int64_redis": np.int64,
        }

        actual = TableDownloader().download_table_as_df(
            project_id + ".feast_it.myentity",
            "gs://{}/test-cases/extract.csv".format(bucket_name),
        )
        actual = actual.drop(["created_timestamp", "job_id"], axis=1).sort_values(
            ["id", "event_timestamp"]
        )
        actual = (
            actual.drop_duplicates()
            .reset_index(drop=True)
            .astype(features_type_mapping)
        )
        expected = expected.astype(features_type_mapping)

        assert (
            pd.testing.assert_frame_equal(
                expected, actual, check_less_precise=True, check_like=True
            )
            is None
        )
