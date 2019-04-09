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
        if job_details["Status"] != job_status:
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
                .sort_values(["id", "event_timestamp"])
                .reset_index(drop=True)
            )

        self.run_batch_import(bucket_name, client)
        self.validate_warehouse_data(project_id, expected)
        self.validate_serving_data(client, expected)

    @staticmethod
    def run_batch_import(bucket_name, client):
        _stage_data(
            "testdata/myentity.avro",
            "gs://{}/test-cases/myentity.avro".format(bucket_name),
        )
        _register_resources(client, "testdata/entity", "testdata/feature")
        job_status = _run_job_and_wait_for_completion("testdata/import/import_csv.yaml")
        assert job_status == "COMPLETED"

    @staticmethod
    def validate_serving_data(client, expected):
        features = {
            "myentity.feature_double_redis",
            "myentity.feature_float_redis",
            "myentity.feature_int32_redis",
            "myentity.feature_int64_redis",
        }

        feature_set = FeatureSet(entity="myentity", features=[f for f in features])
        entity_keys = [str(id) for id in list(expected.id.unique())]
        actual = client.get_serving_data(feature_set, entity_keys=entity_keys)
        actual = actual.sort_values(["myentity"]).reset_index(drop=True)

        # Serving store returns only latest feature values so we update expected values
        # to only have the feature values with latest event timestamp
        expected = expected.loc[expected.groupby("id")["event_timestamp"].idxmax()]
        # Ensure the columns in expected are the same as those in actual
        # For example, in actual there is not event_timestamp column
        expected = expected.rename(columns={"id": "myentity"})
        expected = expected[
            [c.replace("myentity.", "") for c in actual.columns]
        ].reset_index(drop=True)

        np.testing.assert_array_equal(expected, actual)

    @staticmethod
    def validate_warehouse_data(project_id, expected):
        actual = (
            bq_util.query_to_dataframe(
                f"SELECT * FROM `feast_it.myentity_view`", project=project_id
            )
            # created_timestamp is not relevant for validating correctness of import
            # and retrieval of feature values
            .drop(columns=["created_timestamp"])
            .sort_values(["id", "event_timestamp"])
            .reset_index(drop=True)
        )
        assert expected.equals(actual)
