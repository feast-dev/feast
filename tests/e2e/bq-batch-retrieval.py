import math
import os
import random
import time
import uuid
from datetime import datetime
from datetime import timedelta
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pytest
import pytz
from feast.client import Client
from feast.core.CoreService_pb2 import ListStoresRequest
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet
from feast.type_map import ValueType
from google.cloud import storage, bigquery
from google.protobuf.duration_pb2 import Duration
from pandavro import to_avro

pd.set_option("display.max_columns", None)

PROJECT_NAME = "batch_" + uuid.uuid4().hex.upper()[0:6]


@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope="module")
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture(scope="module")
def gcs_path(pytestconfig):
    return pytestconfig.getoption("gcs_path")


@pytest.fixture(scope="module")
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url)
    client.create_project(PROJECT_NAME)
    client.set_project(PROJECT_NAME)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.mark.first
@pytest.mark.direct_runner
@pytest.mark.dataflow_runner
@pytest.mark.run(order=1)
def test_batch_apply_all_featuresets(client):
    client.set_project(PROJECT_NAME)

    file_fs1 = FeatureSet(
        "file_feature_set",
        features=[Feature("feature_value1", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(file_fs1)

    gcs_fs1 = FeatureSet(
        "gcs_feature_set",
        features=[Feature("feature_value2", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(gcs_fs1)

    proc_time_fs = FeatureSet(
        "processing_time",
        features=[Feature("feature_value3", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(proc_time_fs)

    add_cols_fs = FeatureSet(
        "additional_columns",
        features=[Feature("feature_value4", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(add_cols_fs)

    historical_fs = FeatureSet(
        "historical",
        features=[Feature("feature_value5", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(historical_fs)

    fs1 = FeatureSet(
        "feature_set_1",
        features=[Feature("feature_value6", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )

    fs2 = FeatureSet(
        "feature_set_2",
        features=[Feature("other_feature_value7", ValueType.INT64)],
        entities=[Entity("other_entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(fs1)
    client.apply(fs2)

    no_max_age_fs = FeatureSet(
        "no_max_age",
        features=[Feature("feature_value8", ValueType.INT64)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=0),
    )
    client.apply(no_max_age_fs)


@pytest.mark.direct_runner
@pytest.mark.dataflow_runner
@pytest.mark.run(order=10)
def test_batch_get_batch_features_with_file(client):
    file_fs1 = client.get_feature_set(name="file_feature_set")

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_1_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value1": [f"{i}" for i in range(N_ROWS)],
        }
    )
    client.ingest(file_fs1, features_1_df, timeout=480)

    # Rename column (datetime -> event_timestamp)
    features_1_df = features_1_df.rename(columns={"datetime": "event_timestamp"})

    to_avro(
        df=features_1_df[["event_timestamp", "entity_id"]],
        file_path_or_buffer="file_feature_set.avro",
    )

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows="file://file_feature_set.avro",
        feature_refs=[f"{PROJECT_NAME}/feature_value1"],
    )

    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["entity_id"].to_list() == [
        int(i) for i in output["feature_value1"].to_list()
    ]


@pytest.mark.direct_runner
@pytest.mark.dataflow_runner
@pytest.mark.run(order=11)
def test_batch_get_batch_features_with_gs_path(client, gcs_path):
    gcs_fs1 = client.get_feature_set(name="gcs_feature_set")

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_1_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value2": [f"{i}" for i in range(N_ROWS)],
        }
    )
    client.ingest(gcs_fs1, features_1_df, timeout=360)

    # Rename column (datetime -> event_timestamp)
    features_1_df = features_1_df.rename(columns={"datetime": "event_timestamp"})

    # Output file to local
    file_name = "gcs_feature_set.avro"
    to_avro(
        df=features_1_df[["event_timestamp", "entity_id"]],
        file_path_or_buffer=file_name,
    )

    uri = urlparse(gcs_path)
    bucket = uri.hostname
    ts = int(time.time())
    remote_path = str(uri.path).strip("/") + f"{ts}/{file_name}"

    # Upload file to gcs
    storage_client = storage.Client(project=None)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(remote_path)
    blob.upload_from_filename(file_name)

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=f"{gcs_path}{ts}/*", feature_refs=[f"{PROJECT_NAME}/feature_value2"]
    )

    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["entity_id"].to_list() == [
        int(i) for i in output["feature_value2"].to_list()
    ]


@pytest.mark.direct_runner
@pytest.mark.run(order=12)
def test_batch_order_by_creation_time(client):
    proc_time_fs = client.get_feature_set(name="processing_time")

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    N_ROWS = 10
    incorrect_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value3": ["WRONG"] * N_ROWS,
        }
    )
    correct_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value3": ["CORRECT"] * N_ROWS,
        }
    )
    client.ingest(proc_time_fs, incorrect_df)
    time.sleep(15)
    client.ingest(proc_time_fs, correct_df)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=incorrect_df[["datetime", "entity_id"]],
        feature_refs=[f"{PROJECT_NAME}/feature_value3"],
    )
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["feature_value3"].to_list() == ["CORRECT"] * N_ROWS


@pytest.mark.direct_runner
@pytest.mark.run(order=13)
def test_batch_additional_columns_in_entity_table(client):
    add_cols_fs = client.get_feature_set(name="additional_columns")

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value4": ["abc"] * N_ROWS,
        }
    )
    client.ingest(add_cols_fs, features_df)

    entity_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "additional_string_col": ["hello im extra"] * N_ROWS,
            "additional_float_col": [random.random() for i in range(N_ROWS)],
        }
    )

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=entity_df, feature_refs=[f"{PROJECT_NAME}/feature_value4"]
    )
    output = feature_retrieval_job.to_dataframe().sort_values(by=["entity_id"])
    print(output.head(10))

    assert np.allclose(
        output["additional_float_col"], entity_df["additional_float_col"]
    )
    assert (
        output["additional_string_col"].to_list()
        == entity_df["additional_string_col"].to_list()
    )
    assert output["feature_value4"].to_list() == features_df["feature_value4"].to_list()


@pytest.mark.direct_runner
@pytest.mark.run(order=14)
def test_batch_point_in_time_correctness_join(client):
    historical_fs = client.get_feature_set(name="historical")

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    N_EXAMPLES = 10
    historical_df = pd.DataFrame(
        {
            "datetime": [
                time_offset - timedelta(seconds=50),
                time_offset - timedelta(seconds=30),
                time_offset - timedelta(seconds=10),
            ]
            * N_EXAMPLES,
            "entity_id": [i for i in range(N_EXAMPLES) for _ in range(3)],
            "feature_value5": ["WRONG", "WRONG", "CORRECT"] * N_EXAMPLES,
        }
    )
    entity_df = pd.DataFrame(
        {
            "datetime": [time_offset - timedelta(seconds=10)] * N_EXAMPLES,
            "entity_id": [i for i in range(N_EXAMPLES)],
        }
    )

    client.ingest(historical_fs, historical_df)

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=entity_df, feature_refs=[f"{PROJECT_NAME}/feature_value5"]
    )
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["feature_value5"].to_list() == ["CORRECT"] * N_EXAMPLES


@pytest.mark.direct_runner
@pytest.mark.run(order=15)
def test_batch_multiple_featureset_joins(client):
    fs1 = client.get_feature_set(name="feature_set_1")
    fs2 = client.get_feature_set(name="feature_set_2")

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_1_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value6": [f"{i}" for i in range(N_ROWS)],
        }
    )
    client.ingest(fs1, features_1_df)

    features_2_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "other_entity_id": [i for i in range(N_ROWS)],
            "other_feature_value7": [i for i in range(N_ROWS)],
        }
    )
    client.ingest(fs2, features_2_df)

    entity_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "other_entity_id": [N_ROWS - 1 - i for i in range(N_ROWS)],
        }
    )

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=entity_df,
        feature_refs=[
            f"{PROJECT_NAME}/feature_value6",
            f"{PROJECT_NAME}/other_feature_value7",
        ],
    )
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["entity_id"].to_list() == [
        int(i) for i in output["feature_value6"].to_list()
    ]
    assert (
        output["other_entity_id"].to_list() == output["other_feature_value7"].to_list()
    )


@pytest.mark.direct_runner
@pytest.mark.run(order=16)
def test_batch_no_max_age(client):
    no_max_age_fs = client.get_feature_set(name="no_max_age")

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    N_ROWS = 10
    features_8_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value8": [i for i in range(N_ROWS)],
        }
    )
    client.ingest(no_max_age_fs, features_8_df)

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=features_8_df[["datetime", "entity_id"]],
        feature_refs=[f"{PROJECT_NAME}/feature_value8"],
    )

    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["entity_id"].to_list() == output["feature_value8"].to_list()


@pytest.fixture(scope="module", autouse=True)
def infra_teardown(pytestconfig, core_url, serving_url):
    client = Client(core_url=core_url, serving_url=serving_url)
    client.set_project(PROJECT_NAME)

    marker = pytestconfig.getoption("-m")
    yield marker
    if marker == "dataflow_runner":
        ingest_jobs = client.list_ingest_jobs()
        ingest_jobs = [
            client.list_ingest_jobs(job.id)[0].external_id
            for job in ingest_jobs
            if job.status == IngestionJobStatus.RUNNING
        ]

        cwd = os.getcwd()
        with open(f"{cwd}/ingesting_jobs.txt", "w+") as output:
            for job in ingest_jobs:
                output.write("%s\n" % job)
    else:
        print("Cleaning up not required")


@pytest.fixture(scope="module")
def update_featureset_dataframe():
    n_rows = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    return pd.DataFrame(
        {
            "datetime": [time_offset] * n_rows,
            "entity_id": [i for i in range(n_rows)],
            "update_feature1": ["a" for i in range(n_rows)],
            "update_feature2": [i + 2 for i in range(n_rows)],
            "update_feature3": [i for i in range(n_rows)],
            "update_feature4": ["b" for i in range(n_rows)],
        }
    )


@pytest.mark.direct_runner
@pytest.mark.run(order=20)
def test_update_featureset_apply_featureset_and_ingest_first_subset(
    client, update_featureset_dataframe
):
    subset_columns = ["datetime", "entity_id", "update_feature1", "update_feature2"]
    subset_df = update_featureset_dataframe.iloc[:5][subset_columns]
    update_fs = FeatureSet(
        "update_fs",
        entities=[Entity(name="entity_id", dtype=ValueType.INT64)],
        max_age=Duration(seconds=432000),
    )
    update_fs.infer_fields_from_df(subset_df)
    client.apply(update_fs)

    client.ingest(feature_set=update_fs, source=subset_df)

    time.sleep(15)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=update_featureset_dataframe[["datetime", "entity_id"]].iloc[:5],
        feature_refs=[
            f"{PROJECT_NAME}/update_feature1",
            f"{PROJECT_NAME}/update_feature2",
        ],
    )

    output = feature_retrieval_job.to_dataframe().sort_values(by=["entity_id"])
    print(output.head())

    assert output["update_feature1"].to_list() == subset_df["update_feature1"].to_list()
    assert output["update_feature2"].to_list() == subset_df["update_feature2"].to_list()


@pytest.mark.direct_runner
@pytest.mark.timeout(600)
@pytest.mark.run(order=21)
def test_update_featureset_update_featureset_and_ingest_second_subset(
    client, update_featureset_dataframe
):
    subset_columns = [
        "datetime",
        "entity_id",
        "update_feature1",
        "update_feature3",
        "update_feature4",
    ]
    subset_df = update_featureset_dataframe.iloc[5:][subset_columns]
    update_fs = FeatureSet(
        "update_fs",
        entities=[Entity(name="entity_id", dtype=ValueType.INT64)],
        max_age=Duration(seconds=432000),
    )
    update_fs.infer_fields_from_df(subset_df)
    client.apply(update_fs)

    # We keep retrying this ingestion until all values make it into the buffer.
    # This is a necessary step because bigquery streaming caches table schemas
    # and as a result, rows may be lost.
    while True:
        ingestion_id = client.ingest(feature_set=update_fs, source=subset_df)
        time.sleep(15)  # wait for rows to get written to bq
        rows_ingested = get_rows_ingested(client, update_fs, ingestion_id)
        if rows_ingested == len(subset_df):
            print(f"Number of rows successfully ingested: {rows_ingested}. Continuing.")
            break
        print(
            f"Number of rows successfully ingested: {rows_ingested}. Retrying ingestion."
        )
        time.sleep(30)

    feature_retrieval_job = client.get_batch_features(
        entity_rows=update_featureset_dataframe[["datetime", "entity_id"]].iloc[5:],
        feature_refs=[
            f"{PROJECT_NAME}/update_feature1",
            f"{PROJECT_NAME}/update_feature3",
            f"{PROJECT_NAME}/update_feature4",
        ],
    )

    output = feature_retrieval_job.to_dataframe().sort_values(by=["entity_id"])
    print(output.head())

    assert output["update_feature1"].to_list() == subset_df["update_feature1"].to_list()
    assert output["update_feature3"].to_list() == subset_df["update_feature3"].to_list()
    assert output["update_feature4"].to_list() == subset_df["update_feature4"].to_list()


@pytest.mark.direct_runner
@pytest.mark.run(order=22)
def test_update_featureset_retrieve_all_fields(client, update_featureset_dataframe):
    with pytest.raises(Exception):
        feature_retrieval_job = client.get_batch_features(
            entity_rows=update_featureset_dataframe[["datetime", "entity_id"]],
            feature_refs=[
                f"{PROJECT_NAME}/update_feature1",
                f"{PROJECT_NAME}/update_feature2",
                f"{PROJECT_NAME}/update_feature3",
                f"{PROJECT_NAME}/update_feature4",
            ],
        )
        feature_retrieval_job.result()


@pytest.mark.direct_runner
@pytest.mark.run(order=23)
def test_update_featureset_retrieve_valid_fields(client, update_featureset_dataframe):
    feature_retrieval_job = client.get_batch_features(
        entity_rows=update_featureset_dataframe[["datetime", "entity_id"]],
        feature_refs=[
            f"{PROJECT_NAME}/update_feature1",
            f"{PROJECT_NAME}/update_feature3",
            f"{PROJECT_NAME}/update_feature4",
        ],
    )
    output = feature_retrieval_job.to_dataframe().sort_values(by=["entity_id"])
    print(output.head(10))
    assert (
        output["update_feature1"].to_list()
        == update_featureset_dataframe["update_feature1"].to_list()
    )
    # we have to convert to float because the column contains np.NaN
    assert [math.isnan(i) for i in output["update_feature3"].to_list()[:5]] == [
        True
    ] * 5
    assert output["update_feature3"].to_list()[5:] == [
        float(i) for i in update_featureset_dataframe["update_feature3"].to_list()[5:]
    ]
    assert (
        output["update_feature4"].to_list()
        == [None] * 5 + update_featureset_dataframe["update_feature4"].to_list()[5:]
    )


def get_rows_ingested(
    client: Client, feature_set: FeatureSet, ingestion_id: str
) -> int:
    response = client._core_service_stub.ListStores(
        ListStoresRequest(filter=ListStoresRequest.Filter(name="historical"))
    )
    bq_config = response.store[0].bigquery_config
    project = bq_config.project_id
    dataset = bq_config.dataset_id
    table = f"{PROJECT_NAME}_{feature_set.name}"

    bq_client = bigquery.Client(project=project)
    rows = bq_client.query(
        f'SELECT COUNT(*) as count FROM `{project}.{dataset}.{table}` WHERE ingestion_id = "{ingestion_id}"'
    ).result()

    for row in rows:
        return row["count"]
