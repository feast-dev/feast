import math
import os
import random
import tempfile
import time
import uuid
from copy import copy
from datetime import datetime, timedelta

import grpc
import numpy as np
import pandas as pd
import pytest
import pytz
from google.protobuf.duration_pb2 import Duration

from feast.client import Client
from feast.config import Config
from feast.constants import CONFIG_AUTH_PROVIDER
from feast.contrib.job_controller.client import Client as JCClient
from feast.core import CoreService_pb2
from feast.core.CoreService_pb2 import ApplyFeatureSetResponse, GetFeatureSetResponse
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet, FeatureSetRef
from feast.grpc.auth import get_auth_metadata_plugin
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.source import KafkaSource
from feast.type_map import ValueType
from feast.types.Value_pb2 import Int64List
from feast.types.Value_pb2 import Value as Value
from feast.wait import wait_retry_backoff

FLOAT_TOLERANCE = 0.00001
PROJECT_NAME = "basic_" + uuid.uuid4().hex.upper()[0:6]
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
AUTH_PROVIDER = "google"


def basic_dataframe(entities, features, ingest_time, n_size, null_features=[]):
    """
    Generate a basic feast-ingestable dataframe for testing.
    Entity value incrementally increase from 1 to n_size
    Features values are randomlly generated floats.
    entities - names of entities
    features - names of the features
    ingest_time - ingestion timestamp
    n_size - no. of rows in the generated dataframe.
    null_features - names of features that contain null values
    Returns the generated dataframe
    """
    df_dict = {
        "datetime": [ingest_time.replace(tzinfo=pytz.utc) for _ in range(n_size)],
    }
    for entity_name in entities:
        df_dict[entity_name] = list(range(1, n_size + 1))
    for feature_name in features:
        df_dict[feature_name] = [np.random.rand() for _ in range(n_size)]
    for null_feature_name in null_features:
        df_dict[null_feature_name] = [None for _ in range(n_size)]
    return pd.DataFrame(df_dict)


def check_online_response(feature_ref, ingest_df, response):
    """
    Check the feature value and status in the given online serving response.
    feature_refs - string feature ref used to access feature in response
    ingest_df - dataframe of ingested values
    response - response to extract retrieved feature value and metadata
    Returns True if given response has expected feature value and metadata, otherwise False.
    """
    feature_ref_splits = feature_ref.split(":")
    if len(feature_ref_splits) == 1:
        feature_name = feature_ref
    else:
        _, feature_name = feature_ref_splits

    returned_status = response.field_values[0].statuses[feature_ref]
    if ingest_df.loc[0, feature_name] is None:
        return returned_status == GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE
    else:
        sent_value = float(ingest_df.iloc[0][feature_name])
        returned_value = float(response.field_values[0].fields[feature_ref].float_val)
        return (
            math.isclose(sent_value, returned_value, abs_tol=FLOAT_TOLERANCE)
            and returned_status == GetOnlineFeaturesResponse.FieldStatus.PRESENT
        )


@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope="module")
def jobcontroller_url(pytestconfig):
    return pytestconfig.getoption("jobcontroller_url")


@pytest.fixture(scope="module")
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture(scope="module")
def enable_auth(pytestconfig):
    return True if pytestconfig.getoption("enable_auth").lower() == "true" else False


@pytest.fixture(scope="module")
def kafka_brokers(pytestconfig):
    return pytestconfig.getoption("kafka_brokers")


@pytest.fixture(scope="module")
def client(core_url, serving_url, allow_dirty, enable_auth):
    # Get client for core and serving
    # if enable_auth is True, Google Id token will be
    # passed in the metadata for authentication.
    client = Client(
        core_url=core_url,
        serving_url=serving_url,
        enable_auth=enable_auth,
        auth_provider=AUTH_PROVIDER,
    )
    client.create_project(PROJECT_NAME)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.fixture(scope="module")
def jobcontroller_client(jobcontroller_url):
    client = JCClient(jobcontroller_url=jobcontroller_url)
    return client


@pytest.fixture(scope="module")
def ingest_time():
    return datetime.utcnow()


@pytest.fixture(scope="module")
def cust_trans_df(ingest_time):
    return basic_dataframe(
        entities=["customer_id"],
        features=["daily_transactions", "total_transactions"],
        null_features=["null_values"],
        ingest_time=ingest_time,
        n_size=5,
    )


@pytest.fixture(scope="module")
def driver_df(ingest_time):
    return basic_dataframe(
        entities=["driver_id"],
        features=["rating", "cost"],
        ingest_time=ingest_time,
        n_size=5,
    )


def test_version_returns_results(client):
    version_info = client.version()
    assert not version_info["core"] == "not configured"
    assert not version_info["serving"] == "not configured"


def test_list_feature_sets_when_auth_enabled_should_raise(enable_auth):
    if enable_auth:
        client = Client(core_url=core_url, serving_url=serving_url, enable_auth=False)
        with pytest.raises(ConnectionError):
            client.list_feature_sets()


@pytest.mark.timeout(45)
@pytest.mark.run(order=10)
def test_basic_register_feature_set_success(client):
    # Register feature set without project
    cust_trans_fs_expected = FeatureSet.from_yaml(
        f"{DIR_PATH}/basic/cust_trans_fs.yaml"
    )
    driver_fs_expected = FeatureSet.from_yaml(f"{DIR_PATH}/basic/driver_fs.yaml")
    client.apply(cust_trans_fs_expected)
    client.apply(driver_fs_expected)
    cust_trans_fs_actual = client.get_feature_set("customer_transactions")
    assert cust_trans_fs_actual == cust_trans_fs_expected
    driver_fs_actual = client.get_feature_set("driver")
    assert driver_fs_actual == driver_fs_expected

    # Register feature set with project
    cust_trans_fs_expected = FeatureSet.from_yaml(
        f"{DIR_PATH}/basic/cust_trans_fs.yaml"
    )
    client.set_project(PROJECT_NAME)
    client.apply(cust_trans_fs_expected)
    cust_trans_fs_actual = client.get_feature_set(
        "customer_transactions", project=PROJECT_NAME
    )
    assert cust_trans_fs_actual == cust_trans_fs_expected

    # Register feature set with labels
    driver_unlabelled_fs = FeatureSet(
        "driver_unlabelled",
        features=[Feature("rating", ValueType.FLOAT), Feature("cost", ValueType.FLOAT)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    driver_labeled_fs_expected = FeatureSet(
        "driver_labeled",
        features=[Feature("rating", ValueType.FLOAT), Feature("cost", ValueType.FLOAT)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
        labels={"key1": "val1"},
    )
    client.set_project(PROJECT_NAME)
    client.apply(driver_unlabelled_fs)
    client.apply(driver_labeled_fs_expected)
    driver_fs_actual = client.list_feature_sets(
        project=PROJECT_NAME, labels={"key1": "val1"}
    )[0]
    assert driver_fs_actual == driver_labeled_fs_expected

    # reset client's project for other tests
    client.set_project()


@pytest.mark.timeout(300)
@pytest.mark.run(order=11)
def test_basic_ingest_success(client, cust_trans_df, driver_df):
    cust_trans_fs = client.get_feature_set(name="customer_transactions")
    driver_fs = client.get_feature_set(name="driver")

    # Ingest customer transaction data
    client.ingest(cust_trans_fs, cust_trans_df)
    client.ingest(driver_fs, driver_df)
    time.sleep(5)


@pytest.mark.timeout(90)
@pytest.mark.run(order=12)
def test_basic_retrieve_online_success(client, cust_trans_df):
    feature_refs = ["daily_transactions", "total_transactions", "null_values"]

    # Poll serving for feature values until the correct values are returned
    def try_get_features():
        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=cust_trans_df.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        is_ok = all(
            [
                check_online_response(ref, cust_trans_df, response)
                for ref in feature_refs
            ]
        )
        return response, is_ok

    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )


@pytest.mark.timeout(90)
@pytest.mark.run(order=13)
def test_basic_retrieve_online_multiple_featureset(client, cust_trans_df, driver_df):
    # Test retrieve with different variations of the string feature refs
    # ie feature set inference for feature refs without specified feature set
    feature_ref_df_mapping = [
        ("customer_transactions:daily_transactions", cust_trans_df),
        ("driver:rating", driver_df),
        ("total_transactions", cust_trans_df),
    ]

    # Poll serving for feature values until the correct values are returned
    def try_get_features():
        feature_refs = [mapping[0] for mapping in feature_ref_df_mapping]
        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=cust_trans_df.iloc[0]["customer_id"]
                        ),
                        "driver_id": Value(int64_val=driver_df.iloc[0]["driver_id"]),
                    }
                )
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        is_ok = all(
            [
                check_online_response(ref, df, response)
                for ref, df in feature_ref_df_mapping
            ]
        )
        return response, is_ok

    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )


@pytest.fixture(scope="module")
def nonlist_entity_dataframe():
    # Dataframe setup for feature retrieval with entity provided not in list format
    N_ROWS = 2
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    customer_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "customer_id2": [i for i in range(N_ROWS)],
            "customer2_rating": [i for i in range(N_ROWS)],
            "customer2_cost": [float(i) + 0.5 for i in range(N_ROWS)],
            "customer2_past_transactions_int": [[i, i + 2] for i in range(N_ROWS)],
            "customer2_past_transactions_double": [
                [float(i) + 0.5, float(i) + 2] for i in range(N_ROWS)
            ],
            "customer2_past_transactions_float": [
                [float(i) + 0.5, float(i) + 2] for i in range(N_ROWS)
            ],
            "customer2_past_transactions_string": [
                ["first_" + str(i), "second_" + str(i)] for i in range(N_ROWS)
            ],
            "customer2_past_transactions_bool": [[True, False] for _ in range(N_ROWS)],
        }
    )
    return customer_df


@pytest.fixture(scope="module")
def list_entity_dataframe():
    # Dataframe setup for feature retrieval with entity provided in list format
    N_ROWS = 2
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    customer_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "district_ids": [
                [np.int64(i), np.int64(i + 1), np.int64(i + 2)] for i in range(N_ROWS)
            ],
            "district_rating": [i for i in range(N_ROWS)],
            "district_cost": [float(i) + 0.5 for i in range(N_ROWS)],
            "district_past_transactions_int": [[i, i + 2] for i in range(N_ROWS)],
            "district_past_transactions_double": [
                [float(i) + 0.5, float(i) + 2] for i in range(N_ROWS)
            ],
            "district_past_transactions_float": [
                [float(i) + 0.5, float(i) + 2] for i in range(N_ROWS)
            ],
            "district_past_transactions_string": [
                ["first_" + str(i), "second_" + str(i)] for i in range(N_ROWS)
            ],
            "district_past_transactions_bool": [[True, False] for _ in range(N_ROWS)],
        }
    )
    return customer_df


@pytest.mark.timeout(600)
@pytest.mark.run(order=14)
def test_basic_retrieve_online_entity_nonlistform(
    client, nonlist_entity_dataframe, list_entity_dataframe
):
    # Case 1: Feature retrieval with multiple entities retrieval check
    customer_fs = FeatureSet(
        name="customer2",
        features=[
            Feature(name="customer2_rating", dtype=ValueType.INT64),
            Feature(name="customer2_cost", dtype=ValueType.FLOAT),
            Feature(name="customer2_past_transactions_int", dtype=ValueType.INT64_LIST),
            Feature(
                name="customer2_past_transactions_double", dtype=ValueType.DOUBLE_LIST
            ),
            Feature(
                name="customer2_past_transactions_float", dtype=ValueType.FLOAT_LIST
            ),
            Feature(
                name="customer2_past_transactions_string", dtype=ValueType.STRING_LIST
            ),
            Feature(name="customer2_past_transactions_bool", dtype=ValueType.BOOL_LIST),
        ],
        entities=[Entity("customer_id2", ValueType.INT64)],
        max_age=Duration(seconds=3600),
    )

    client.set_project(PROJECT_NAME)
    client.apply(customer_fs)

    customer_fs = client.get_feature_set(name="customer2")
    client.ingest(customer_fs, nonlist_entity_dataframe, timeout=600)
    time.sleep(15)

    online_request_entity = [{"customer_id2": 0}, {"customer_id2": 1}]
    online_request_features = [
        "customer2_rating",
        "customer2_cost",
        "customer2_past_transactions_int",
        "customer2_past_transactions_double",
        "customer2_past_transactions_float",
        "customer2_past_transactions_string",
        "customer2_past_transactions_bool",
    ]
    online_request_entity2 = [
        {"customer_id2": Value(int64_val=0)},
        {"customer_id2": Value(int64_val=1)},
    ]

    def try_get_features1():
        response = client.get_online_features(
            entity_rows=online_request_entity, feature_refs=online_request_features
        )
        is_ok = check_online_response(
            "customer2_rating", nonlist_entity_dataframe, response
        )
        return response, is_ok

    def try_get_features2():
        response = client.get_online_features(
            entity_rows=online_request_entity2, feature_refs=online_request_features
        )
        is_ok = check_online_response(
            "customer2_rating", nonlist_entity_dataframe, response
        )
        return response, is_ok

    online_features_actual1 = wait_retry_backoff(
        retry_fn=try_get_features1,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_actual2 = wait_retry_backoff(
        retry_fn=try_get_features2,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_expected = {
        "customer_id2": [0, 1],
        "customer2_rating": [0, 1],
        "customer2_cost": [0.5, 1.5],
        "customer2_past_transactions_int": [[0, 2], [1, 3]],
        "customer2_past_transactions_double": [[0.5, 2.0], [1.5, 3.0]],
        "customer2_past_transactions_float": [[0.5, 2.0], [1.5, 3.0]],
        "customer2_past_transactions_string": [
            ["first_0", "second_0"],
            ["first_1", "second_1"],
        ],
        "customer2_past_transactions_bool": [[True, False], [True, False]],
    }

    assert online_features_actual1.to_dict() == online_features_expected
    assert online_features_actual2.to_dict() == online_features_expected

    # Case 2: Feature retrieval with multiple entities retrieval check with mixed types
    with pytest.raises(TypeError) as excinfo:
        online_request_entity2 = [{"customer_id": 0}, {"customer_id": "error_pls"}]
        online_features_actual2 = client.get_online_features(
            entity_rows=online_request_entity2, feature_refs=online_request_features
        )

    assert (
        "Input entity customer_id has mixed types, ValueType.STRING and ValueType.INT64. That is not allowed."
        in str(excinfo.value)
    )


@pytest.mark.timeout(600)
@pytest.mark.run(order=15)
def test_basic_retrieve_online_entity_listform(client, list_entity_dataframe):
    # Case 1: Features retrieval with entity in list format check
    district_fs = FeatureSet(
        name="district",
        features=[
            Feature(name="district_rating", dtype=ValueType.INT64),
            Feature(name="district_cost", dtype=ValueType.FLOAT),
            Feature(name="district_past_transactions_int", dtype=ValueType.INT64_LIST),
            Feature(
                name="district_past_transactions_double", dtype=ValueType.DOUBLE_LIST
            ),
            Feature(
                name="district_past_transactions_float", dtype=ValueType.FLOAT_LIST
            ),
            Feature(
                name="district_past_transactions_string", dtype=ValueType.STRING_LIST
            ),
            Feature(name="district_past_transactions_bool", dtype=ValueType.BOOL_LIST),
        ],
        entities=[Entity("district_ids", dtype=ValueType.INT64_LIST)],
        max_age=Duration(seconds=3600),
    )

    client.set_project(PROJECT_NAME)
    client.apply(district_fs)

    district_fs = client.get_feature_set(name="district")
    client.ingest(district_fs, list_entity_dataframe, timeout=600)
    time.sleep(15)

    online_request_entity = [{"district_ids": [np.int64(1), np.int64(2), np.int64(3)]}]
    online_request_features = [
        "district_rating",
        "district_cost",
        "district_past_transactions_int",
        "district_past_transactions_double",
        "district_past_transactions_float",
        "district_past_transactions_string",
        "district_past_transactions_bool",
    ]
    online_request_entity2 = [
        {"district_ids": Value(int64_list_val=Int64List(val=[1, 2, 3]))}
    ]

    def try_get_features1():
        response = client.get_online_features(
            entity_rows=online_request_entity, feature_refs=online_request_features
        )
        is_ok = check_online_response(
            "district_rating", list_entity_dataframe, response
        )
        return response, is_ok

    def try_get_features2():
        response = client.get_online_features(
            entity_rows=online_request_entity2, feature_refs=online_request_features
        )
        is_ok = check_online_response(
            "district_rating", list_entity_dataframe, response
        )
        return response, is_ok

    online_features_actual = wait_retry_backoff(
        retry_fn=try_get_features1,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_actual2 = wait_retry_backoff(
        retry_fn=try_get_features2,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_expected = {
        "district_ids": [[np.int64(1), np.int64(2), np.int64(3)]],
        "district_rating": [1],
        "district_cost": [1.5],
        "district_past_transactions_int": [[1, 3]],
        "district_past_transactions_double": [[1.5, 3.0]],
        "district_past_transactions_float": [[1.5, 3.0]],
        "district_past_transactions_string": [["first_1", "second_1"]],
        "district_past_transactions_bool": [[True, False]],
    }

    assert online_features_actual.to_dict() == online_features_expected
    assert online_features_actual2.to_dict() == online_features_expected

    # Case 2: Features retrieval with entity in list format check with mixed types
    with pytest.raises(ValueError) as excinfo:
        online_request_entity2 = [{"district_ids": [np.int64(1), np.int64(2), True]}]
        online_features_actual2 = client.get_online_features(
            entity_rows=online_request_entity2, feature_refs=online_request_features
        )

    assert (
        "List value type for field district_ids is inconsistent. ValueType.INT64 different from ValueType.BOOL."
        in str(excinfo.value)
    )


@pytest.mark.timeout(600)
@pytest.mark.run(order=16)
def test_basic_ingest_retrieval_fs(client):
    # Set to another project to test ingestion based on current project context
    client.set_project(PROJECT_NAME + "_NS1")
    driver_fs = FeatureSet(
        name="driver_fs",
        features=[
            Feature(name="driver_fs_rating", dtype=ValueType.FLOAT),
            Feature(name="driver_fs_cost", dtype=ValueType.FLOAT),
        ],
        entities=[Entity("driver_fs_id", ValueType.INT64)],
        max_age=Duration(seconds=3600),
    )
    client.apply(driver_fs)

    N_ROWS = 2
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    driver_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "driver_fs_id": [i for i in range(N_ROWS)],
            "driver_fs_rating": [float(i) for i in range(N_ROWS)],
            "driver_fs_cost": [float(i) + 0.5 for i in range(N_ROWS)],
        }
    )
    client.ingest(driver_fs, driver_df, timeout=600)
    time.sleep(15)

    online_request_entity = [{"driver_fs_id": 0}, {"driver_fs_id": 1}]
    online_request_features = ["driver_fs_rating", "driver_fs_cost"]

    def try_get_features():
        response = client.get_online_features(
            entity_rows=online_request_entity, feature_refs=online_request_features
        )
        is_ok = check_online_response("driver_fs_rating", driver_df, response)
        return response, is_ok

    online_features_actual = wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_expected = {
        "driver_fs_id": [0, 1],
        "driver_fs_rating": [0.0, 1.0],
        "driver_fs_cost": [0.5, 1.5],
    }

    assert online_features_actual.to_dict() == online_features_expected


@pytest.mark.timeout(600)
@pytest.mark.run(order=17)
def test_basic_ingest_retrieval_str(client):
    # Set to another project to test ingestion based on current project context
    client.set_project(PROJECT_NAME + "_NS1")
    customer_fs = FeatureSet(
        name="cust_fs",
        features=[
            Feature(name="cust_rating", dtype=ValueType.INT64),
            Feature(name="cust_cost", dtype=ValueType.FLOAT),
        ],
        entities=[Entity("cust_id", ValueType.INT64)],
        max_age=Duration(seconds=3600),
    )
    client.apply(customer_fs)

    N_ROWS = 2
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    cust_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "cust_id": [i for i in range(N_ROWS)],
            "cust_rating": [i for i in range(N_ROWS)],
            "cust_cost": [float(i) + 0.5 for i in range(N_ROWS)],
        }
    )
    client.ingest("cust_fs", cust_df, timeout=600)
    time.sleep(15)

    online_request_entity = [{"cust_id": 0}, {"cust_id": 1}]
    online_request_features = ["cust_rating", "cust_cost"]

    def try_get_features():
        response = client.get_online_features(
            entity_rows=online_request_entity, feature_refs=online_request_features
        )
        is_ok = check_online_response("cust_rating", cust_df, response)
        return response, is_ok

    online_features_actual = wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    online_features_expected = {
        "cust_id": [0, 1],
        "cust_rating": [0, 1],
        "cust_cost": [0.5, 1.5],
    }

    assert online_features_actual.to_dict() == online_features_expected


@pytest.mark.timeout(600)
@pytest.mark.run(order=18)
def test_basic_retrieve_feature_row_missing_fields(client, cust_trans_df):
    feature_refs = ["daily_transactions", "total_transactions", "null_values"]

    # apply cust_trans_fs and ingest dataframe
    client.set_project(PROJECT_NAME + "_basic_retrieve_missing_fields")
    old_cust_trans_fs = FeatureSet.from_yaml(f"{DIR_PATH}/basic/cust_trans_fs.yaml")
    client.apply(old_cust_trans_fs)
    client.ingest(old_cust_trans_fs, cust_trans_df)

    # update cust_trans_fs with one additional feature.
    # feature rows ingested before the feature set update will be missing a field.
    new_cust_trans_fs = client.get_feature_set(name="customer_transactions")
    new_cust_trans_fs.add(Feature("n_trips", ValueType.INT64))
    client.apply(new_cust_trans_fs)
    # sleep to ensure feature set update is propagated
    time.sleep(15)

    # attempt to retrieve features from feature rows with missing fields
    def try_get_features():
        response = client.get_online_features(
            entity_rows=[
                {"customer_id": np.int64(cust_trans_df.iloc[0]["customer_id"])}
            ],
            feature_refs=feature_refs + ["n_trips"],
        )  # type: GetOnlineFeaturesResponse
        # check if the ingested fields can be correctly retrieved.
        is_ok = all(
            [
                check_online_response(ref, cust_trans_df, response)
                for ref in feature_refs
            ]
        )
        # should return null_value status for missing field n_trips
        is_missing_ok = (
            response.field_values[0].statuses["n_trips"]
            == GetOnlineFeaturesResponse.FieldStatus.NULL_VALUE
        )
        return response, is_ok and is_missing_ok

    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )


@pytest.mark.timeout(600)
@pytest.mark.run(order=19)
def test_basic_retrieve_feature_row_extra_fields(client, cust_trans_df):
    feature_refs = ["daily_transactions", "total_transactions"]
    # apply cust_trans_fs and ingest dataframe
    client.set_project(PROJECT_NAME + "_basic_retrieve_missing_fields")
    old_cust_trans_fs = FeatureSet.from_yaml(f"{DIR_PATH}/basic/cust_trans_fs.yaml")
    client.apply(old_cust_trans_fs)
    client.ingest(old_cust_trans_fs, cust_trans_df)

    # update cust_trans_fs with the null_values feature dropped.
    # feature rows ingested before the feature set update will have an extra field.
    new_cust_trans_fs = client.get_feature_set(name="customer_transactions")
    new_cust_trans_fs.drop("null_values")
    client.apply(new_cust_trans_fs)
    # sleep to ensure feature set update is propagated
    time.sleep(15)

    # attempt to retrieve features from feature rows with extra fields
    def try_get_features():
        response = client.get_online_features(
            entity_rows=[
                {"customer_id": np.int64(cust_trans_df.iloc[0]["customer_id"])}
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        # check if the non dropped fields can be correctly retrieved.
        is_ok = all(
            [
                check_online_response(ref, cust_trans_df, response)
                for ref in feature_refs
            ]
        )
        return response, is_ok

    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )


@pytest.fixture(scope="module")
def all_types_dataframe():
    return pd.DataFrame(
        {
            "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(3)],
            "user_id": [1001, 1002, 1003],
            "int32_feature": [np.int32(1), np.int32(2), np.int32(3)],
            "int64_feature": [np.int64(1), np.int64(2), np.int64(3)],
            "float_feature": [np.float(0.1), np.float(0.2), np.float(0.3)],
            "double_feature": [np.float64(0.1), np.float64(0.2), np.float64(0.3)],
            "string_feature": ["one", "two", "three"],
            "bytes_feature": [b"one", b"two", b"three"],
            "bool_feature": [True, False, False],
            "int32_list_feature": [
                np.array([1, 2, 3, 4], dtype=np.int32),
                np.array([1, 2, 3, 4], dtype=np.int32),
                np.array([1, 2, 3, 4], dtype=np.int32),
            ],
            "int64_list_feature": [
                np.array([1, 2, 3, 4], dtype=np.int64),
                np.array([1, 2, 3, 4], dtype=np.int64),
                np.array([1, 2, 3, 4], dtype=np.int64),
            ],
            "float_list_feature": [
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float32),
            ],
            "double_list_feature": [
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
                np.array([1.1, 1.2, 1.3, 1.4], dtype=np.float64),
            ],
            "string_list_feature": [
                np.array(["one", "two", "three"]),
                np.array(["one", "two", "three"]),
                np.array(["one", "two", "three"]),
            ],
            "bytes_list_feature": [
                np.array([b"one", b"two", b"three"]),
                np.array([b"one", b"two", b"three"]),
                np.array([b"one", b"two", b"three"]),
            ],
            # "bool_list_feature": [
            #     np.array([True, False, True]),
            #     np.array([True, False, True]),
            #     np.array([True, False, True]),
            # ],
            # TODO: https://github.com/feast-dev/feast/issues/341
        }
    )


@pytest.mark.timeout(45)
@pytest.mark.run(order=20)
def test_all_types_register_feature_set_success(client):
    client.set_project(PROJECT_NAME)

    all_types_fs_expected = FeatureSet(
        name="all_types",
        entities=[Entity(name="user_id", dtype=ValueType.INT64)],
        features=[
            Feature(name="float_feature", dtype=ValueType.FLOAT),
            Feature(name="int64_feature", dtype=ValueType.INT64),
            Feature(name="int32_feature", dtype=ValueType.INT32),
            Feature(name="string_feature", dtype=ValueType.STRING),
            Feature(name="bytes_feature", dtype=ValueType.BYTES),
            Feature(name="bool_feature", dtype=ValueType.BOOL),
            Feature(name="double_feature", dtype=ValueType.DOUBLE),
            Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
            Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
            Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
            Feature(name="string_list_feature", dtype=ValueType.STRING_LIST),
            Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
        ],
        max_age=Duration(seconds=3600),
    )

    # Register feature set
    client.apply(all_types_fs_expected)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(15)

    all_types_fs_actual = client.get_feature_set(name="all_types")

    assert all_types_fs_actual == all_types_fs_expected

    if all_types_fs_actual is None:
        raise Exception(
            "Client cannot retrieve 'all_types_fs' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )


@pytest.mark.timeout(300)
@pytest.mark.run(order=21)
def test_all_types_ingest_success(client, all_types_dataframe):
    # Get all_types feature set
    all_types_fs = client.get_feature_set(name="all_types")

    # Ingest user embedding data
    client.ingest(all_types_fs, all_types_dataframe)


@pytest.mark.timeout(90)
@pytest.mark.run(order=22)
def test_all_types_retrieve_online_success(client, all_types_dataframe):
    # Poll serving for feature values until the correct values are returned_float_list
    feature_refs = [
        "float_feature",
        "int64_feature",
        "int32_feature",
        "double_feature",
        "string_feature",
        "bool_feature",
        "bytes_feature",
        "float_list_feature",
        "int64_list_feature",
        "int32_list_feature",
        "string_list_feature",
        "bytes_list_feature",
        "double_list_feature",
    ]

    def try_get_features():
        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "user_id": Value(
                            int64_val=all_types_dataframe.iloc[0]["user_id"]
                        )
                    }
                )
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        is_ok = check_online_response("float_feature", all_types_dataframe, response)
        return response, is_ok

    response = wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    # check returned values
    returned_float_list = (
        response.field_values[0].fields["float_list_feature"].float_list_val.val
    )
    sent_float_list = all_types_dataframe.iloc[0]["float_list_feature"]
    assert math.isclose(
        returned_float_list[0], sent_float_list[0], abs_tol=FLOAT_TOLERANCE
    )
    # check returned metadata
    assert (
        response.field_values[0].statuses["float_list_feature"]
        == GetOnlineFeaturesResponse.FieldStatus.PRESENT
    )


@pytest.mark.timeout(300)
@pytest.mark.run(order=35)
def test_all_types_ingest_jobs(jobcontroller_client, client, all_types_dataframe):
    # list ingestion jobs given featureset
    client.set_project(PROJECT_NAME)

    all_types_fs = client.get_feature_set(name="all_types")
    ingest_jobs = jobcontroller_client.list_ingest_jobs(
        feature_set_ref=FeatureSetRef.from_feature_set(all_types_fs)
    )
    # filter ingestion jobs to only those that are running
    ingest_jobs = [
        job for job in ingest_jobs if job.status == IngestionJobStatus.RUNNING
    ]
    assert len(ingest_jobs) >= 1

    ingest_job = ingest_jobs[0]
    # restart ingestion ingest_job
    # restart means stop current job
    # (replacement will be automatically spawned)
    jobcontroller_client.restart_ingest_job(ingest_job)
    # wait for replacement to be created
    time.sleep(15)  # should be more than polling_interval

    # id without timestamp part
    # that remains the same between jobs
    shared_id = "-".join(ingest_job.id.split("-")[:-1])
    ingest_jobs = jobcontroller_client.list_ingest_jobs(
        feature_set_ref=FeatureSetRef.from_feature_set(all_types_fs)
    )
    replacement_jobs = [
        job
        for job in ingest_jobs
        if job.status == IngestionJobStatus.RUNNING
        and job.id.startswith(shared_id)
        and job.id != ingest_job.id
    ]

    assert len(replacement_jobs) >= 1
    replacement_job = replacement_jobs[0]

    replacement_job.wait(IngestionJobStatus.RUNNING)
    assert replacement_job.status == IngestionJobStatus.RUNNING

    # stop ingestion ingest_job
    jobcontroller_client.stop_ingest_job(replacement_job)
    replacement_job.wait(IngestionJobStatus.ABORTED)
    assert replacement_job.status == IngestionJobStatus.ABORTED


@pytest.fixture(scope="module")
def large_volume_dataframe():
    ROW_COUNT = 100000
    offset = random.randint(1000000, 10000000)  # ensure a unique key space
    customer_data = pd.DataFrame(
        {
            "datetime": [
                datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(ROW_COUNT)
            ],
            "customer_id": [offset + inc for inc in range(ROW_COUNT)],
            "daily_transactions_large": [np.random.rand() for _ in range(ROW_COUNT)],
            "total_transactions_large": [256 for _ in range(ROW_COUNT)],
        }
    )
    return customer_data


@pytest.mark.timeout(45)
@pytest.mark.run(order=40)
def test_large_volume_register_feature_set_success(client):
    cust_trans_fs_expected = FeatureSet.from_yaml(
        f"{DIR_PATH}/large_volume/cust_trans_large_fs.yaml"
    )

    # Register feature set
    client.apply(cust_trans_fs_expected)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(10)
    cust_trans_fs_actual = client.get_feature_set(name="customer_transactions_large")

    assert cust_trans_fs_actual == cust_trans_fs_expected

    if cust_trans_fs_actual is None:
        raise Exception(
            "Client cannot retrieve 'customer_transactions' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )


@pytest.mark.timeout(300)
@pytest.mark.run(order=41)
def test_large_volume_ingest_success(client, large_volume_dataframe):
    # Get large volume feature set
    cust_trans_fs = client.get_feature_set(name="customer_transactions_large")

    # Ingest customer transaction data
    client.ingest(cust_trans_fs, large_volume_dataframe)


@pytest.mark.timeout(90)
@pytest.mark.run(order=42)
def test_large_volume_retrieve_online_success(client, large_volume_dataframe):
    # Poll serving for feature values until the correct values are returned
    feature_refs = [
        "daily_transactions_large",
        "total_transactions_large",
    ]
    while True:
        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=large_volume_dataframe.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        is_ok = all(
            [
                check_online_response(ref, large_volume_dataframe, response)
                for ref in feature_refs
            ]
        )
        return None, is_ok


@pytest.fixture(scope="module")
def all_types_parquet_file():
    COUNT = 20000

    df = pd.DataFrame(
        {
            "datetime": [datetime.utcnow() for _ in range(COUNT)],
            "customer_id": [np.int32(random.randint(0, 10000)) for _ in range(COUNT)],
            "int32_feature_parquet": [
                np.int32(random.randint(0, 10000)) for _ in range(COUNT)
            ],
            "int64_feature_parquet": [
                np.int64(random.randint(0, 10000)) for _ in range(COUNT)
            ],
            "float_feature_parquet": [np.float(random.random()) for _ in range(COUNT)],
            "double_feature_parquet": [
                np.float64(random.random()) for _ in range(COUNT)
            ],
            "string_feature_parquet": [
                "one" + str(random.random()) for _ in range(COUNT)
            ],
            "bytes_feature_parquet": [b"one" for _ in range(COUNT)],
            "int32_list_feature_parquet": [
                np.array([1, 2, 3, random.randint(0, 10000)], dtype=np.int32)
                for _ in range(COUNT)
            ],
            "int64_list_feature_parquet": [
                np.array([1, random.randint(0, 10000), 3, 4], dtype=np.int64)
                for _ in range(COUNT)
            ],
            "float_list_feature_parquet": [
                np.array([1.1, 1.2, 1.3, random.random()], dtype=np.float32)
                for _ in range(COUNT)
            ],
            "double_list_feature_parquet": [
                np.array([1.1, 1.2, 1.3, random.random()], dtype=np.float64)
                for _ in range(COUNT)
            ],
            "string_list_feature_parquet": [
                np.array(["one", "two" + str(random.random()), "three"])
                for _ in range(COUNT)
            ],
            "bytes_list_feature_parquet": [
                np.array([b"one", b"two", b"three"]) for _ in range(COUNT)
            ],
        }
    )

    # TODO: Boolean list is not being tested.
    #  https://github.com/feast-dev/feast/issues/341

    file_path = os.path.join(tempfile.mkdtemp(), "all_types.parquet")
    df.to_parquet(file_path, allow_truncated_timestamps=True)
    return file_path


@pytest.mark.timeout(300)
@pytest.mark.run(order=50)
def test_all_types_parquet_register_feature_set_success(client):
    # Load feature set from file
    all_types_parquet_expected = FeatureSet.from_yaml(
        f"{DIR_PATH}/all_types_parquet/all_types_parquet.yaml"
    )

    # Register feature set
    client.apply(all_types_parquet_expected)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(30)

    all_types_parquet_actual = client.get_feature_set(name="all_types_parquet")

    assert all_types_parquet_actual == all_types_parquet_expected

    if all_types_parquet_actual is None:
        raise Exception(
            "Client cannot retrieve 'customer_transactions' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )


@pytest.mark.timeout(600)
@pytest.mark.run(order=51)
def test_all_types_infer_register_ingest_file_success(client, all_types_parquet_file):
    # Get feature set
    all_types_fs = client.get_feature_set(name="all_types_parquet")

    # Ingest user embedding data
    client.ingest(feature_set=all_types_fs, source=all_types_parquet_file)


@pytest.mark.timeout(200)
@pytest.mark.run(order=60)
def test_list_entities_and_features(client):
    customer_entity = Entity("customer_id", ValueType.INT64)
    driver_entity = Entity("driver_id", ValueType.INT64)

    customer_feature_rating = Feature(
        name="rating", dtype=ValueType.FLOAT, labels={"key1": "val1"}
    )
    customer_feature_cost = Feature(name="cost", dtype=ValueType.FLOAT)
    driver_feature_rating = Feature(name="rating", dtype=ValueType.FLOAT)
    driver_feature_cost = Feature(
        name="cost", dtype=ValueType.FLOAT, labels={"key1": "val1"}
    )

    filter_by_project_entity_labels_expected = dict(
        [("customer:rating", customer_feature_rating)]
    )

    filter_by_project_entity_expected = dict(
        [("driver:cost", driver_feature_cost), ("driver:rating", driver_feature_rating)]
    )

    filter_by_project_labels_expected = dict(
        [
            ("customer:rating", customer_feature_rating),
            ("driver:cost", driver_feature_cost),
        ]
    )

    customer_fs = FeatureSet(
        "customer",
        features=[customer_feature_rating, customer_feature_cost],
        entities=[customer_entity],
        max_age=Duration(seconds=100),
    )

    driver_fs = FeatureSet(
        "driver",
        features=[driver_feature_rating, driver_feature_cost],
        entities=[driver_entity],
        max_age=Duration(seconds=100),
    )

    client.set_project(PROJECT_NAME)
    client.apply(customer_fs)
    client.apply(driver_fs)

    # Test for listing of features
    # Case 1: Filter by: project, entities and labels
    filter_by_project_entity_labels_actual = client.list_features_by_ref(
        project=PROJECT_NAME, entities=["customer_id"], labels={"key1": "val1"}
    )

    # Case 2: Filter by: project, entities
    filter_by_project_entity_actual = client.list_features_by_ref(
        project=PROJECT_NAME, entities=["driver_id"]
    )

    # Case 3: Filter by: project, labels
    filter_by_project_labels_actual = client.list_features_by_ref(
        project=PROJECT_NAME, labels={"key1": "val1"}
    )

    assert set(filter_by_project_entity_labels_expected) == set(
        filter_by_project_entity_labels_actual
    )
    assert set(filter_by_project_entity_expected) == set(
        filter_by_project_entity_actual
    )
    assert set(filter_by_project_labels_expected) == set(
        filter_by_project_labels_actual
    )


@pytest.mark.timeout(500)
@pytest.mark.run(order=70)
def test_sources_deduplicate_ingest_jobs(client, jobcontroller_client, kafka_brokers):
    shared_source = KafkaSource(kafka_brokers, "dup_shared")
    dup_source_fs_1 = FeatureSet(
        name="duplicate_source_fs_1",
        features=[Feature("fs1", ValueType.FLOAT), Feature("fs2", ValueType.FLOAT)],
        entities=[Entity("e2", ValueType.INT64)],
        source=shared_source,
    )
    dup_source_fs_2 = copy(dup_source_fs_1)
    dup_source_fs_2.name = "duplicate_source_fs_2"

    def is_same_jobs():
        fs_1_jobs = jobcontroller_client.list_ingest_jobs(
            feature_set_ref=FeatureSetRef(
                name=dup_source_fs_1.name, project=dup_source_fs_1.project
            )
        )
        fs_2_jobs = jobcontroller_client.list_ingest_jobs(
            feature_set_ref=FeatureSetRef(
                name=dup_source_fs_2.name, project=dup_source_fs_2.project
            )
        )
        same = True
        if not (len(fs_1_jobs) > 0 and len(fs_1_jobs) == len(fs_2_jobs)):
            same = False
        for fs_1_job in fs_1_jobs:
            for fs_2_job in fs_2_jobs:
                if (
                    not fs_1_job.source.to_proto() == fs_2_job.source.to_proto()
                    and fs_1_job.source.to_proto() == shared_source.to_proto()
                ):
                    same = False
                if fs_1_job.id != fs_2_job.id:
                    same = False
        return same

    def is_different_jobs():
        fs_1_jobs = jobcontroller_client.list_ingest_jobs(
            feature_set_ref=FeatureSetRef(
                name=dup_source_fs_1.name, project=dup_source_fs_1.project
            )
        )
        fs_2_jobs = jobcontroller_client.list_ingest_jobs(
            feature_set_ref=FeatureSetRef(
                name=dup_source_fs_2.name, project=dup_source_fs_2.project
            )
        )
        different = True
        if not (len(fs_1_jobs) > 0 and len(fs_2_jobs) > 0):
            different = False
        for fs_1_job in fs_1_jobs:
            if fs_1_job.source.to_proto() == alt_source.to_proto():
                different = False
        for fs_2_job in fs_2_jobs:
            if fs_2_job.source.to_proto() == shared_source.to_proto():
                different = False
        for fs_1_job in fs_1_jobs:
            for fs_2_job in fs_2_jobs:
                if fs_1_job.id == fs_2_job.id:
                    different = False
        return different

    # register multiple feature sets with the same source
    # only one ingest job should spawned due to test ingest job deduplication
    client.apply(dup_source_fs_1)
    client.apply(dup_source_fs_2)

    while not is_same_jobs():
        time.sleep(1)

    # update feature sets with different sources, should have different jobs
    alt_source = KafkaSource(kafka_brokers, "alt_source")
    dup_source_fs_2.source = alt_source
    client.apply(dup_source_fs_2)

    while not is_different_jobs():
        time.sleep(1)

    # update feature sets with same source again, should have the same job
    dup_source_fs_2.source = shared_source
    client.apply(dup_source_fs_2)

    while not is_same_jobs():
        time.sleep(1)


@pytest.mark.run(order=30)
def test_sink_writes_only_recent_rows(client):
    client.set_project("default")

    feature_refs = ["driver:rating", "driver:cost"]

    later_df = basic_dataframe(
        entities=["driver_id"],
        features=["rating", "cost"],
        ingest_time=datetime.utcnow(),
        n_size=5,
    )

    earlier_df = basic_dataframe(
        entities=["driver_id"],
        features=["rating", "cost"],
        ingest_time=datetime.utcnow() - timedelta(minutes=5),
        n_size=5,
    )

    def try_get_features():
        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"driver_id": Value(int64_val=later_df.iloc[0]["driver_id"])}
                )
            ],
            feature_refs=feature_refs,
        )  # type: GetOnlineFeaturesResponse
        is_ok = all(
            [check_online_response(ref, later_df, response) for ref in feature_refs]
        )
        return response, is_ok

    # test compaction within batch
    client.ingest("driver", pd.concat([earlier_df, later_df]))
    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )

    # test read before write
    client.ingest("driver", earlier_df)
    time.sleep(10)
    wait_retry_backoff(
        retry_fn=try_get_features,
        timeout_secs=90,
        timeout_msg="Timed out trying to get online feature values",
    )


# TODO: rewrite these using python SDK once the labels are implemented there
class TestsBasedOnGrpc:
    GRPC_CONNECTION_TIMEOUT = 3
    LABEL_KEY = "my"
    LABEL_VALUE = "label"

    @pytest.fixture(scope="module")
    def core_service_stub(self, core_url):
        if core_url.endswith(":443"):
            core_channel = grpc.secure_channel(core_url, grpc.ssl_channel_credentials())
        else:
            core_channel = grpc.insecure_channel(core_url)

        try:
            grpc.channel_ready_future(core_channel).result(
                timeout=self.GRPC_CONNECTION_TIMEOUT
            )
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Core gRPC server {core_url} "
            )
        core_service_stub = CoreServiceStub(core_channel)
        return core_service_stub

    @pytest.fixture(scope="module")
    def auth_meta_data(self, enable_auth):
        if not enable_auth:
            return None
        else:
            metadata = {CONFIG_AUTH_PROVIDER: AUTH_PROVIDER}
            metadata_plugin = get_auth_metadata_plugin(config=Config(metadata))
            return metadata_plugin.get_signed_meta()

    def apply_feature_set(self, core_service_stub, feature_set_proto, auth_meta_data):
        try:
            apply_fs_response = core_service_stub.ApplyFeatureSet(
                CoreService_pb2.ApplyFeatureSetRequest(feature_set=feature_set_proto),
                timeout=self.GRPC_CONNECTION_TIMEOUT,
                metadata=auth_meta_data,
            )  # type: ApplyFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return apply_fs_response.feature_set

    def get_feature_set(self, core_service_stub, name, project, auth_meta_data):
        try:
            get_feature_set_response = core_service_stub.GetFeatureSet(
                CoreService_pb2.GetFeatureSetRequest(
                    project=project, name=name.strip(),
                ),
                metadata=auth_meta_data,
            )  # type: GetFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return get_feature_set_response.feature_set

    @pytest.mark.timeout(45)
    @pytest.mark.run(order=51)
    def test_register_feature_set_with_labels(self, core_service_stub, auth_meta_data):
        feature_set_name = "test_feature_set_labels"
        feature_set_proto = FeatureSet(
            name=feature_set_name,
            project=PROJECT_NAME,
            labels={self.LABEL_KEY: self.LABEL_VALUE},
        ).to_proto()
        self.apply_feature_set(core_service_stub, feature_set_proto, auth_meta_data)

        retrieved_feature_set = self.get_feature_set(
            core_service_stub, feature_set_name, PROJECT_NAME, auth_meta_data
        )

        assert self.LABEL_KEY in retrieved_feature_set.spec.labels
        assert retrieved_feature_set.spec.labels[self.LABEL_KEY] == self.LABEL_VALUE

    @pytest.mark.timeout(45)
    @pytest.mark.run(order=52)
    def test_register_feature_with_labels(self, core_service_stub, auth_meta_data):
        feature_set_name = "test_feature_labels"
        feature_set_proto = FeatureSet(
            name=feature_set_name,
            project=PROJECT_NAME,
            features=[
                Feature(
                    name="rating",
                    dtype=ValueType.INT64,
                    labels={self.LABEL_KEY: self.LABEL_VALUE},
                )
            ],
        ).to_proto()
        self.apply_feature_set(core_service_stub, feature_set_proto, auth_meta_data)

        retrieved_feature_set = self.get_feature_set(
            core_service_stub, feature_set_name, PROJECT_NAME, auth_meta_data
        )
        retrieved_feature = retrieved_feature_set.spec.features[0]

        assert self.LABEL_KEY in retrieved_feature.labels
        assert retrieved_feature.labels[self.LABEL_KEY] == self.LABEL_VALUE
