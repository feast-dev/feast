import pytest
import subprocess
from subprocess import PIPE
import yaml
import os
from time import sleep
import pandas as pd
from google.cloud import storage

import feast.sdk.utils.gs_utils as utils

from feast.sdk.resources.entity import Entity
from feast.sdk.resources.feature import Feature
from feast.sdk.client import Client
from feast.sdk.resources.feature_set import FeatureSet, FileType
from feast.sdk.utils.bq_util import TableDownloader

@pytest.fixture
def client():
    return Client(verbose=True)

'''Init the system by registering relevant resources.
'''
def register_resources(client, entities_fldr, features_fldr):
    resources = []
    for ent_file in os.listdir(entities_fldr):
        resources.append(Entity.from_yaml(os.path.join(entities_fldr, ent_file)))
    for feat_file in os.listdir(features_fldr):
        resources.append(Feature.from_yaml(os.path.join(features_fldr, feat_file)))
    client.apply(resources)

'''Run an import job given an import spec.
'''
def run_job_and_wait_for_completion(job_yaml):
    out = subprocess.run("feast jobs run {}".format(job_yaml).split(" "),
        check=True, stdout=PIPE)
    job_id = out.stdout.decode('utf-8').split(' ')[-1]
    job_complete = False
    terminal_states = ["COMPLETED", "ABORTED", "ERROR", "UNKNOWN"]
    while not job_complete:
        out = subprocess.run("feast get job {}".format(job_id).split(" "),
            check=True, stdout=PIPE)
        job_details = yaml.load(out.stdout.decode('utf-8').replace('\t', ' '))
        job_status = job_details['Status']
        print("Job id {} currently {}".format(job_id, job_status))
        if job_status in terminal_states:
            break
        sleep(10)
    return job_status

'''Stage data to a remote location
'''
def stage_data(local, remote):
    split = utils.split_gs_path(remote)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(split[0])
    blob = bucket.blob(split[1])

    blob.upload_from_filename(local)


class TestFeastIntegration:
    def test_end_to_end(self, client):
        project_id = os.environ.get("PROJECT_ID")
        bucket_name = os.environ.get("BUCKET_NAME")

        stage_data("data/test_data.csv", "gs://{}/test-cases/test_data.csv".format(bucket_name))
        register_resources(client, "data/entity", "data/feature")
        result = run_job_and_wait_for_completion("data/import/import_csv.yaml")
        
        # Ensure that the job is able to reach completion
        assert result == "COMPLETED"

        # Check data in bq
        features = ["feature_double_redis", "feature_float_redis", 
            "feature_int32_redis", "feature_int64_redis"]
        wanted = pd.read_csv("data/test_data.csv", 
            header=None, 
            names=["id", "event_timestamp"] + features)
        wanted = wanted \
            .sort_values(["id", "event_timestamp"]) \
            .reset_index(drop=True)
        wanted['event_timestamp'] = pd.to_datetime(wanted['event_timestamp'])\
            .dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        got = TableDownloader().download_table_as_df(
            project_id + ".feast_it.myentity",
            "gs://{}/test-cases/extract.csv".format(bucket_name))
        got = got.drop("created_timestamp", axis=1) \
            .sort_values(["id", "event_timestamp"]) \
            .reset_index(drop=True)
        assert pd.testing.assert_frame_equal(got, wanted[got.columns], check_less_precise=True) is None

        # Check data in redis
        feature_set = FeatureSet(entity="myentity", 
            features=["myentity." + f for f in features])
        actual_latest = client.get_serving_data(feature_set, entity_keys=[str(id) for id in list(wanted.id.unique())])
        actual_latest = actual_latest.sort_values(["id"])
        wanted['event_timestamp'] = pd.to_datetime(wanted['event_timestamp'])
        wanted_latest = wanted.loc[wanted.groupby('id').event_timestamp.idxmax(),:]
        wanted_latest.columns = ["myentity", "timestamp"] + ["myentity." + f for f in features]
        wanted_latest = wanted_latest[actual_latest.columns] \
            .sort_values(["id"]) \
            .reset_index(drop=True)
        wanted_latest["myentity"] = wanted_latest["myentity"].astype(str)
        
        assert pd.testing.assert_frame_equal(actual_latest, wanted_latest, check_less_precise=True) is None

        
