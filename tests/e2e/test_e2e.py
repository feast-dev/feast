import math
import random
import time
from feast.entity import Entity
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.types.Value_pb2 import Value as Value
from feast.client import Client
from feast.feature_set import FeatureSet
from feast.type_map import ValueType
from google.protobuf.duration_pb2 import Duration
import pytest
from datetime import datetime, timedelta
import pytz

import pandas as pd
import numpy as np

from feast.feature import Feature

FLOAT_TOLERANCE = 0.00001


@pytest.fixture()
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture()
def online_serving_url(pytestconfig):
    return pytestconfig.getoption("online_serving_url")


@pytest.fixture()
def batch_serving_url(pytestconfig):
    return pytestconfig.getoption("batch_serving_url")


@pytest.fixture()
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture
def online_client(core_url, online_serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=online_serving_url)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.fixture
def batch_client(core_url, batch_serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=batch_serving_url)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.mark.timeout(300)
def test_basic(online_client):
    client = online_client

    cust_trans_fs = client.get_feature_set(name="customer_transactions", version=1)

    # TODO: Fix source handling in Feast Core to support true idempotent
    #  applies. In this case, applying a feature set without a source will
    #  create a new feature set every time.

    if cust_trans_fs is None:
        # Load feature set from file
        cust_trans_fs = FeatureSet.from_yaml("feature_sets/cust_trans_fs.yaml")

        # Register feature set
        client.apply(cust_trans_fs)

        # Feast Core needs some time to fully commit the FeatureSet applied
        # when there is no existing job yet for the Featureset
        time.sleep(15)
        cust_trans_fs = client.get_feature_set(name="customer_transactions", version=1)

        if cust_trans_fs is None:
            raise Exception(
                "Client cannot retrieve 'customer_transactions' FeatureSet "
                "after registration. Either Feast Core does not save the "
                "FeatureSet correctly or the client needs to wait longer for FeatureSet "
                "to be committed."
            )

    offset = random.randint(1000, 100000)  # ensure a unique key space is used
    customer_data = pd.DataFrame(
        {
            "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(5)],
            "customer_id": [offset + inc for inc in range(5)],
            "daily_transactions": [np.random.rand() for _ in range(5)],
            "total_transactions": [512 for _ in range(5)],
        }
    )

    # Ingest customer transaction data
    cust_trans_fs.ingest(dataframe=customer_data)

    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=customer_data.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_ids=[
                "customer_transactions:1:daily_transactions",
                "customer_transactions:1:total_transactions",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields["customer_transactions:1:daily_transactions"]
                .float_val
        )
        sent_daily_transactions = float(customer_data.iloc[0]["daily_transactions"])

        if math.isclose(
                sent_daily_transactions,
                returned_daily_transactions,
                abs_tol=FLOAT_TOLERANCE,
        ):
            break


@pytest.mark.timeout(300)
def test_all_types(online_client):
    client = online_client
    all_types_fs = client.get_feature_set(name="all_types", version=1)

    if all_types_fs is None:
        # Register new feature set if it doesnt exist
        all_types_fs = FeatureSet(
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
                Feature(name="float_list_feature", dtype=ValueType.FLOAT_LIST),
                Feature(name="int64_list_feature", dtype=ValueType.INT64_LIST),
                Feature(name="int32_list_feature", dtype=ValueType.INT32_LIST),
                Feature(name="string_list_feature", dtype=ValueType.STRING_LIST),
                Feature(name="bytes_list_feature", dtype=ValueType.BYTES_LIST),
                Feature(name="bool_list_feature", dtype=ValueType.BOOL_LIST),
                Feature(name="double_list_feature", dtype=ValueType.DOUBLE_LIST),
            ],
            max_age=Duration(seconds=3600),
        )

        # Register feature set
        client.apply(all_types_fs)

        # Feast Core needs some time to fully commit the FeatureSet applied
        # when there is no existing job yet for the Featureset
        time.sleep(10)
        all_types_fs = client.get_feature_set(name="all_types", version=1)

        if all_types_fs is None:
            raise Exception(
                "Client cannot retrieve 'all_types_fs' FeatureSet "
                "after registration. Either Feast Core does not save the "
                "FeatureSet correctly or the client needs to wait longer for FeatureSet "
                "to be committed."
            )

    all_types_df = pd.DataFrame(
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
            "bool_list_feature": [
                np.array([True, False, True]),
                np.array([True, False, True]),
                np.array([True, False, True]),
            ],
        }
    )

    # Ingest user embedding data
    all_types_fs.ingest(dataframe=all_types_df)
    time.sleep(3)

    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"user_id": Value(int64_val=all_types_df.iloc[0]["user_id"])}
                )
            ],
            feature_ids=[
                "all_types:1:float_feature",
                "all_types:1:int64_feature",
                "all_types:1:int32_feature",
                "all_types:1:string_feature",
                "all_types:1:bytes_feature",
                "all_types:1:bool_feature",
                "all_types:1:double_feature",
                "all_types:1:float_list_feature",
                "all_types:1:int64_list_feature",
                "all_types:1:int32_list_feature",
                "all_types:1:string_list_feature",
                "all_types:1:bytes_list_feature",
                "all_types:1:bool_list_feature",
                "all_types:1:double_list_feature",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_float_list = (
            response.field_values[0]
                .fields["all_types:1:float_list_feature"]
                .float_list_val.val
        )

        sent_float_list = all_types_df.iloc[0]["float_list_feature"]

        # TODO: Add tests for each value and type
        if math.isclose(
                returned_float_list[0], sent_float_list[0], abs_tol=FLOAT_TOLERANCE
        ):
            break

        # Wait for values to appear in Serving
        time.sleep(1)


@pytest.mark.timeout(1200)
def test_large_volume(online_client, batch_client):
    ROW_COUNT = 50000
    client = online_client

    cust_trans_fs = client.get_feature_set(
        name="customer_transactions_large", version=1
    )
    if cust_trans_fs is None:
        # Load feature set from file
        cust_trans_fs = FeatureSet.from_yaml("feature_sets/cust_trans_large_fs.yaml")

        # Register feature set
        client.apply(cust_trans_fs)

        # Feast Core needs some time to fully commit the FeatureSet applied
        # when there is no existing job yet for the Featureset
        time.sleep(10)
        cust_trans_fs = client.get_feature_set(
            name="customer_transactions_large", version=1
        )

        if cust_trans_fs is None:
            raise Exception(
                "Client cannot retrieve 'customer_transactions' FeatureSet "
                "after registration. Either Feast Core does not save the "
                "FeatureSet correctly or the client needs to wait longer for FeatureSet "
                "to be committed."
            )

    offset = random.randint(1000000, 10000000)  # ensure a unique key space
    customer_data = pd.DataFrame(
        {
            "datetime": [
                datetime.utcnow().replace(tzinfo=pytz.utc) for _ in range(ROW_COUNT)
            ],
            "customer_id": [offset + inc for inc in range(ROW_COUNT)],
            "daily_transactions": [np.random.rand() for _ in range(ROW_COUNT)],
            "total_transactions": [256 for _ in range(ROW_COUNT)],
        }
    )

    # Ingest customer transaction data
    cust_trans_fs.ingest(dataframe=customer_data)

    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=customer_data.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_ids=[
                "customer_transactions_large:1:daily_transactions",
                "customer_transactions_large:1:total_transactions",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields["customer_transactions_large:1:daily_transactions"]
                .float_val
        )
        sent_daily_transactions = float(customer_data.iloc[0]["daily_transactions"])

        if math.isclose(
                sent_daily_transactions,
                returned_daily_transactions,
                abs_tol=FLOAT_TOLERANCE,
        ):
            break

    # Test if values in wh store are correct
    feature_retrieval_job = batch_client.get_batch_features(
        entity_rows=customer_data[['datetime', 'customer_id']],
        feature_ids=[
            "customer_transactions_large:1:daily_transactions",
            "customer_transactions_large:1:total_transactions",
        ],
    )

    batch_df = feature_retrieval_job.to_dataframe()
    batch_df = batch_df[['event_timestamp',
                         'customer_id',
                         'customer_transactions_large_v1_daily_transactions',
                         'customer_transactions_large_v1_total_transactions']]
    batch_df.columns = ['datetime',
                        'customer_id',
                        'daily_transactions',
                        'total_transactions']
    pd.testing.assert_frame_equal(batch_df, customer_data, check_less_precise=True)


@pytest.mark.timeout(1200)
def test_batch_multiple_feature_sets(batch_client):
    client = batch_client
    merchant_sales_fs = client.get_feature_set(
        name="merchant_sales", version=1
    )
    loc_sales_fs = client.get_feature_set(
        name="location", version=1
    )
    if merchant_sales_fs is None or loc_sales_fs is None:
        # Load feature set from file
        merchant_sales_fs = FeatureSet.from_yaml("feature_sets/mer_sales_fs.yaml")

        # Register feature set
        client.apply(merchant_sales_fs)

        # Feast Core needs some time to fully commit the FeatureSet applied
        # when there is no existing job yet for the Featureset
        time.sleep(10)
        merchant_sales_fs = client.get_feature_set(
            name="merchant_sales", version=1
        )

        if merchant_sales_fs is None:
            raise Exception(
                "Client cannot retrieve 'merchant_sales' FeatureSet "
                "after registration. Either Feast Core does not save the "
                "FeatureSet correctly or the client needs to wait longer for FeatureSet "
                "to be committed."
            )

        loc_sales_fs = FeatureSet.from_yaml("feature_sets/loc_sales_fs.yaml")

        # Register feature set
        client.apply(loc_sales_fs)

        # Feast Core needs some time to fully commit the FeatureSet applied
        # when there is no existing job yet for the Featureset
        time.sleep(10)
        loc_sales_fs = client.get_feature_set(
            name="location", version=1
        )

        if loc_sales_fs is None:
            raise Exception(
                "Client cannot retrieve 'location' FeatureSet "
                "after registration. Either Feast Core does not save the "
                "FeatureSet correctly or the client needs to wait longer for FeatureSet "
                "to be committed."
            )

    N_MERCHANTS = 600
    N_TIMESTAMPS = 7
    N_LOCATIONS = 10

    offset = random.randint(1000000, 10000000)  # ensure a unique key space
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(days=N_TIMESTAMPS)

    merchants = [(offset + inc, random.randint(0, N_LOCATIONS)) for inc in range(N_MERCHANTS)]
    datetimes = [time_offset + timedelta(days=1 * i) for i in range(N_TIMESTAMPS)]
    merchant_data = pd.DataFrame(
        {
            "datetime": [ts for ts in datetimes for i in range(N_MERCHANTS)],
            "merchant_id": [m[0] for m in merchants] * N_TIMESTAMPS,
            "location_id": [m[1] for m in merchants] * N_TIMESTAMPS,
            "daily_sales": [random.randint(6000, 10000) for _ in range(N_MERCHANTS * N_TIMESTAMPS)],
            "total_revenue": [random.randint(100000, 200000) for _ in range(N_MERCHANTS * N_TIMESTAMPS)],
        }
    )
    loc_data = pd.DataFrame(
        {
            "datetime": [time_offset] * N_LOCATIONS,
            "location_id": [i for i in range(N_LOCATIONS)],
            "total_revenue": [random.randint(100000, 500000) for _ in range(N_LOCATIONS)],
        }
    )

    merchant_sales_fs.ingest(dataframe=merchant_data[["datetime", "merchant_id", "daily_sales", "total_revenue"]])
    loc_sales_fs.ingest(dataframe=loc_data)

    expected = merchant_data.merge(loc_data[["datetime", "location_id", "total_revenue"]], on=["location_id"])

    feature_retrieval_job = batch_client.get_batch_features(
        entity_rows=merchant_data[["datetime", "merchant_id", "location_id"]],
        feature_ids=[
            "merchant_sales:1:daily_sales",
            "merchant_sales:1:total_revenue",
            "location:1:total_revenue"
        ],
    )
    actual = feature_retrieval_job.to_dataframe()
