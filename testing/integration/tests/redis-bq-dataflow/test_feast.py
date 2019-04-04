import os
import subprocess
from subprocess import PIPE
from time import sleep

import fastavro
import feast.sdk.utils.gs_utils as utils
import numpy as np
import pandas as pd
import pytest
import yaml
from feast.sdk.client import Client
from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.resources.feature_set import FeatureSet
from feast.sdk.utils import bq_util
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


class TestFeastIntegration:
    def test_end_to_end(self, client):
        project_id = os.environ.get("PROJECT_ID")
        bucket_name = os.environ.get("BUCKET_NAME")
        testdata_path = os.path.abspath(os.path.join(__file__, "..", "testdata"))

        with open(os.path.join(testdata_path, "myentity.avro"), "rb") as myentity_file:
            avro_reader = fastavro.reader(myentity_file)
            expected = (
                pd.DataFrame.from_records(avro_reader)
                .drop(columns=["created_timestamp"])
                .sort_values(["id", "event_timestamp"]).reset_index(drop=True)
            )

        # self.run_batch_import(bucket_name, client)
        # self.validate_warehouse_data(project_id, expected)
        self.validate_serving_data(client, expected)

    @staticmethod
    def run_batch_import(bucket_name, client):
        _stage_data(
            "testdata/myentity.csv",
            "gs://{}/test-cases/myentity.csv".format(bucket_name),
        )
        _register_resources(client, "testdata/entity", "testdata/feature")
        job_status = _run_job_and_wait_for_completion("testdata/import/import_csv.yaml")
        assert job_status == "COMPLETED"

    @staticmethod
    def validate_serving_data(client, expected):
        features_type_mapping = {
            "myentity": np.string_,
            "myentity.feature_double_redis": np.float64,
            "myentity.feature_float_redis": np.float64,
            "myentity.feature_int32_redis": np.int64,
            "myentity.feature_int64_redis": np.int64,
        }

        feature_set = FeatureSet(
            entity="myentity", features=[f for f in features_type_mapping.keys() if "." in f]
        )
        actual = client.get_serving_data(
            feature_set, entity_keys=[str(id) for id in list(expected.id.unique())]
        ).astype(features_type_mapping)
        actual = actual.sort_values(["myentity"]).reset_index(drop=True)
        actual = actual.astype({"myentity": np.object})
        expected = expected

        print(expected.head().values, actual.head(10).values)
        print(actual.dtypes)
        print(actual.shape)
        print(expected.shape)
        # assert expected.equals(actual)

    @staticmethod
    def validate_warehouse_data(project_id, expected):
        # TODO: change the dataset back
        actual = (
            bq_util.query_to_dataframe(
                f"SELECT * FROM `dheryanto.myentity_view`", project=project_id
            )
            .drop(columns=["created_timestamp"])
            .sort_values(["id", "event_timestamp"]).reset_index(drop=True)
        )
        assert expected.equals(actual)
