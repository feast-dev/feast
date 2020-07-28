import math
import random
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import pytz
from google.protobuf.duration_pb2 import Duration

from feast.client import Client
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.type_map import ValueType
from feast.types.Value_pb2 import Value as Value
from feast.wait import wait_retry_backoff

_GRPC_CONNECTION_TIMEOUT_APPLY_KEY = 1200

_GRPC_CONNECTION_TIMEOUT_DEFAULT = 20

FLOAT_TOLERANCE = 0.00001
PROJECT_NAME = "basic_" + uuid.uuid4().hex.upper()[0:6]


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


def wait_for(fn, timeout: timedelta, sleep=5):
    until = datetime.now() + timeout
    last_exc = BaseException()

    while datetime.now() <= until:
        try:
            fn()
        except Exception as exc:
            last_exc = exc
        else:
            return
        time.sleep(sleep)

    raise last_exc


@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope="module")
def batch_serving_url(pytestconfig):
    return pytestconfig.getoption("batch_serving_url")


@pytest.fixture(scope="module")
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture(scope="module")
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(
        core_url=core_url,
        serving_url=serving_url,
        grpc_connection_timeout_default=_GRPC_CONNECTION_TIMEOUT_DEFAULT,
        grpc_connection_timeout_apply_key=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY,
        batch_feature_request_wait_time_seconds=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY,
    )
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


@pytest.fixture(scope="module")
def batch_client(core_url, batch_serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=batch_serving_url)
    client.set_project(PROJECT_NAME)
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


@pytest.mark.timeout(600)
@pytest.mark.run(order=21)
def test_all_types_ingest_success(client, all_types_dataframe):
    # Get all_types feature set
    all_types_fs = client.get_feature_set(name="all_types")

    # Ingest user embedding data
    client.ingest(
        all_types_fs, all_types_dataframe, timeout=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY
    )


@pytest.mark.timeout(1200)
@pytest.mark.run(order=22)
def test_all_types_retrieve_online_success(client, all_types_dataframe):
    # Poll serving for feature values until the correct values are returned
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
        timeout_secs=1200,
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


@pytest.mark.timeout(1200)
@pytest.mark.run(order=23)
def test_all_types_retrieve_batch_success(client, batch_client, all_types_dataframe):
    def check():
        feature_retrieval_job = batch_client.get_historical_features(
            entity_rows=all_types_dataframe[["datetime", "user_id"]],
            feature_refs=["string_feature"],
            project=PROJECT_NAME,
        )

        output = feature_retrieval_job.to_dataframe()
        print(output.head())

        assert output["user_id"].to_list() == all_types_dataframe["user_id"].to_list()
        assert (
            output["string_feature"].to_list()
            == all_types_dataframe["string_feature"].to_list()
        )

    wait_for(check, timedelta(minutes=10))


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
@pytest.mark.run(order=30)
def test_large_volume_register_feature_set_success(client):
    cust_trans_fs_expected = FeatureSet.from_yaml(
        "redis/large_volume/cust_trans_large_fs.yaml"
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
@pytest.mark.run(order=31)
def test_large_volume_ingest_success(client, large_volume_dataframe):
    # Get large volume feature set
    cust_trans_fs = client.get_feature_set(name="customer_transactions_large")

    # Ingest customer transaction data
    client.ingest(
        cust_trans_fs,
        large_volume_dataframe,
        timeout=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY,
    )


@pytest.mark.timeout(300)
@pytest.mark.run(order=32)
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
