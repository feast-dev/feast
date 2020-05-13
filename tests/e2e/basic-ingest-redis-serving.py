import pytest
import math
import random
import time
import grpc
from feast.entity import Entity
from feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core import CoreService_pb2
from feast.types.Value_pb2 import Value as Value
from feast.client import Client
from feast.feature_set import FeatureSet, FeatureSetRef
from feast.type_map import ValueType
from google.protobuf.duration_pb2 import Duration
from datetime import datetime
import pytz

import pandas as pd
import numpy as np
import tempfile
import os
from feast.feature import Feature
import uuid

FLOAT_TOLERANCE = 0.00001
PROJECT_NAME = 'basic_' + uuid.uuid4().hex.upper()[0:6]


@pytest.fixture(scope='module')
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope='module')
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope='module')
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption(
        "allow_dirty").lower() == "true" else False


@pytest.fixture(scope='module')
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


@pytest.fixture(scope='module')
def basic_dataframe():
    offset = random.randint(1000, 100000)  # ensure a unique key space is used
    return pd.DataFrame(
        {
            "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                         range(5)],
            "customer_id": [offset + inc for inc in range(5)],
            "daily_transactions": [np.random.rand() for _ in range(5)],
            "total_transactions": [512 for _ in range(5)],
        }
    )


@pytest.mark.timeout(45)
@pytest.mark.run(order=10)
def test_basic_register_feature_set_success(client):
    # Load feature set from file
    cust_trans_fs_expected = FeatureSet.from_yaml("basic/cust_trans_fs.yaml")

    client.set_project(PROJECT_NAME)

    # Register feature set
    client.apply(cust_trans_fs_expected)

    cust_trans_fs_actual = client.get_feature_set(name="customer_transactions")

    assert cust_trans_fs_actual == cust_trans_fs_expected

    if cust_trans_fs_actual is None:
        raise Exception(
            "Client cannot retrieve 'customer_transactions' FeatureSet "
            "after registration. Either Feast Core does not save the "
            "FeatureSet correctly or the client needs to wait longer for FeatureSet "
            "to be committed."
        )


@pytest.mark.timeout(300)
@pytest.mark.run(order=11)
def test_basic_ingest_success(client, basic_dataframe):
    client.set_project(PROJECT_NAME)

    cust_trans_fs = client.get_feature_set(name="customer_transactions")

    # Ingest customer transaction data
    client.ingest(cust_trans_fs, basic_dataframe)
    time.sleep(5)


@pytest.mark.timeout(45)
@pytest.mark.run(order=12)
def test_basic_retrieve_online_success(client, basic_dataframe):
    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        client.set_project(PROJECT_NAME)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=basic_dataframe.iloc[0]["customer_id"]
                        )
                    }
                )
            ],
            feature_refs=[
                "daily_transactions",
                "total_transactions",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields[PROJECT_NAME + "/daily_transactions"]
                .float_val
        )
        sent_daily_transactions = float(
            basic_dataframe.iloc[0]["daily_transactions"])

        if math.isclose(
                sent_daily_transactions,
                returned_daily_transactions,
                abs_tol=FLOAT_TOLERANCE,
        ):
            break


@pytest.mark.timeout(300)
@pytest.mark.run(order=19)
def test_basic_ingest_jobs(client, basic_dataframe):
    # list ingestion jobs given featureset
    cust_trans_fs = client.get_feature_set(name="customer_transactions")
    ingest_jobs = client.list_ingest_jobs(
        feature_set_ref=FeatureSetRef.from_feature_set(cust_trans_fs))
    # filter ingestion jobs to only those that are running
    ingest_jobs = [job for job in ingest_jobs if job.status == IngestionJobStatus.RUNNING]
    assert len(ingest_jobs) >= 1

    for ingest_job in ingest_jobs:
        # restart ingestion ingest_job
        client.restart_ingest_job(ingest_job)
        ingest_job.wait(IngestionJobStatus.RUNNING)
        assert ingest_job.status == IngestionJobStatus.RUNNING

        # stop ingestion ingest_job
        client.stop_ingest_job(ingest_job)
        ingest_job.wait(IngestionJobStatus.ABORTED)
        assert ingest_job.status == IngestionJobStatus.ABORTED


@pytest.fixture(scope='module')
def all_types_dataframe():
    return pd.DataFrame(
        {
            "datetime": [datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                         range(3)],
            "user_id": [1001, 1002, 1003],
            "int32_feature": [np.int32(1), np.int32(2), np.int32(3)],
            "int64_feature": [np.int64(1), np.int64(2), np.int64(3)],
            "float_feature": [np.float(0.1), np.float(0.2), np.float(0.3)],
            "double_feature": [np.float64(0.1), np.float64(0.2),
                               np.float64(0.3)],
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
            # TODO: https://github.com/gojek/feast/issues/341
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
            Feature(name="string_list_feature",
                    dtype=ValueType.STRING_LIST),
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


@pytest.mark.timeout(45)
@pytest.mark.run(order=22)
def test_all_types_retrieve_online_success(client, all_types_dataframe):
    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={"user_id": Value(
                        int64_val=all_types_dataframe.iloc[0]["user_id"])}
                )
            ],
            feature_refs=[
                "float_feature",
                "int64_feature",
                "int32_feature",
                "string_feature",
                "bytes_feature",
                "bool_feature",
                "double_feature",
                "float_list_feature",
                "int64_list_feature",
                "int32_list_feature",
                "string_list_feature",
                "bytes_list_feature",
                "double_list_feature",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_float_list = (
            response.field_values[0]
                .fields[PROJECT_NAME + "/float_list_feature"]
                .float_list_val.val
        )

        sent_float_list = all_types_dataframe.iloc[0]["float_list_feature"]

        if math.isclose(
                returned_float_list[0], sent_float_list[0], abs_tol=FLOAT_TOLERANCE
        ):
            break


@pytest.mark.timeout(300)
@pytest.mark.run(order=29)
def test_all_types_ingest_jobs(client, all_types_dataframe):
    # list ingestion jobs given featureset
    all_types_fs = client.get_feature_set(name="all_types")
    ingest_jobs = client.list_ingest_jobs(
        feature_set_ref=FeatureSetRef.from_feature_set(all_types_fs))
    # filter ingestion jobs to only those that are running
    ingest_jobs = [job for job in ingest_jobs if job.status == IngestionJobStatus.RUNNING]
    assert len(ingest_jobs) >= 1

    for ingest_job in ingest_jobs:
        # restart ingestion ingest_job
        client.restart_ingest_job(ingest_job)
        ingest_job.wait(IngestionJobStatus.RUNNING)
        assert ingest_job.status == IngestionJobStatus.RUNNING

        # stop ingestion ingest_job
        client.stop_ingest_job(ingest_job)
        ingest_job.wait(IngestionJobStatus.ABORTED)
        assert ingest_job.status == IngestionJobStatus.ABORTED


@pytest.fixture(scope='module')
def large_volume_dataframe():
    ROW_COUNT = 100000
    offset = random.randint(1000000, 10000000)  # ensure a unique key space
    customer_data = pd.DataFrame(
        {
            "datetime": [
                datetime.utcnow().replace(tzinfo=pytz.utc) for _ in
                range(ROW_COUNT)
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
        "large_volume/cust_trans_large_fs.yaml")

    # Register feature set
    client.apply(cust_trans_fs_expected)

    # Feast Core needs some time to fully commit the FeatureSet applied
    # when there is no existing job yet for the Featureset
    time.sleep(10)
    cust_trans_fs_actual = client.get_feature_set(
        name="customer_transactions_large")

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
    client.ingest(cust_trans_fs, large_volume_dataframe)


@pytest.mark.timeout(45)
@pytest.mark.run(order=32)
def test_large_volume_retrieve_online_success(client, large_volume_dataframe):
    # Poll serving for feature values until the correct values are returned
    while True:
        time.sleep(1)

        response = client.get_online_features(
            entity_rows=[
                GetOnlineFeaturesRequest.EntityRow(
                    fields={
                        "customer_id": Value(
                            int64_val=large_volume_dataframe.iloc[0][
                                "customer_id"]
                        )
                    }
                )
            ],
            feature_refs=[
                "daily_transactions_large",
                "total_transactions_large",
            ],
        )  # type: GetOnlineFeaturesResponse

        if response is None:
            continue

        returned_daily_transactions = float(
            response.field_values[0]
                .fields[PROJECT_NAME + "/daily_transactions_large"]
                .float_val
        )
        sent_daily_transactions = float(
            large_volume_dataframe.iloc[0]["daily_transactions_large"])

        if math.isclose(
                sent_daily_transactions,
                returned_daily_transactions,
                abs_tol=FLOAT_TOLERANCE,
        ):
            break


@pytest.fixture(scope='module')
def all_types_parquet_file():
    COUNT = 20000

    df = pd.DataFrame(
        {
            "datetime": [datetime.utcnow() for _ in range(COUNT)],
            "customer_id": [np.int32(random.randint(0, 10000)) for _ in
                            range(COUNT)],
            "int32_feature_parquet": [np.int32(random.randint(0, 10000)) for _ in
                                      range(COUNT)],
            "int64_feature_parquet": [np.int64(random.randint(0, 10000)) for _ in
                                      range(COUNT)],
            "float_feature_parquet": [np.float(random.random()) for _ in range(COUNT)],
            "double_feature_parquet": [np.float64(random.random()) for _ in
                                       range(COUNT)],
            "string_feature_parquet": ["one" + str(random.random()) for _ in
                                       range(COUNT)],
            "bytes_feature_parquet": [b"one" for _ in range(COUNT)],
            "int32_list_feature_parquet": [
                np.array([1, 2, 3, random.randint(0, 10000)], dtype=np.int32)
                for _
                in range(COUNT)
            ],
            "int64_list_feature_parquet": [
                np.array([1, random.randint(0, 10000), 3, 4], dtype=np.int64)
                for _
                in range(COUNT)
            ],
            "float_list_feature_parquet": [
                np.array([1.1, 1.2, 1.3, random.random()], dtype=np.float32) for
                _
                in range(COUNT)
            ],
            "double_list_feature_parquet": [
                np.array([1.1, 1.2, 1.3, random.random()], dtype=np.float64) for
                _
                in range(COUNT)
            ],
            "string_list_feature_parquet": [
                np.array(["one", "two" + str(random.random()), "three"]) for _
                in
                range(COUNT)
            ],
            "bytes_list_feature_parquet": [
                np.array([b"one", b"two", b"three"]) for _ in range(COUNT)
            ],
        }
    )

    # TODO: Boolean list is not being tested.
    #  https://github.com/gojek/feast/issues/341

    file_path = os.path.join(tempfile.mkdtemp(), 'all_types.parquet')
    df.to_parquet(file_path, allow_truncated_timestamps=True)
    return file_path


@pytest.mark.timeout(300)
@pytest.mark.run(order=40)
def test_all_types_parquet_register_feature_set_success(client):
    # Load feature set from file
    all_types_parquet_expected = FeatureSet.from_yaml(
        "all_types_parquet/all_types_parquet.yaml")

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
@pytest.mark.run(order=41)
def test_all_types_infer_register_ingest_file_success(client,
                                                      all_types_parquet_file):
    # Get feature set
    all_types_fs = client.get_feature_set(name="all_types_parquet")

    # Ingest user embedding data
    client.ingest(feature_set=all_types_fs, source=all_types_parquet_file)


# TODO: rewrite these using python SDK once the labels are implemented there
class TestsBasedOnGrpc:
    GRPC_CONNECTION_TIMEOUT = 3
    LABEL_KEY = "my"
    LABEL_VALUE = "label"

    @pytest.fixture(scope="module")
    def core_service_stub(self, core_url):
        if core_url.endswith(":443"):
            core_channel = grpc.secure_channel(
                core_url, grpc.ssl_channel_credentials()
            )
        else:
            core_channel = grpc.insecure_channel(core_url)

        try:
            grpc.channel_ready_future(core_channel).result(timeout=self.GRPC_CONNECTION_TIMEOUT)
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                f"Connection timed out while attempting to connect to Feast "
                f"Core gRPC server {core_url} "
            )
        core_service_stub = CoreServiceStub(core_channel)
        return core_service_stub

    def apply_feature_set(self, core_service_stub, feature_set_proto):
        try:
            apply_fs_response = core_service_stub.ApplyFeatureSet(
                CoreService_pb2.ApplyFeatureSetRequest(feature_set=feature_set_proto),
                timeout=self.GRPC_CONNECTION_TIMEOUT,
            )  # type: ApplyFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return apply_fs_response.feature_set

    def get_feature_set(self, core_service_stub, name, project):
        try:
            get_feature_set_response = core_service_stub.GetFeatureSet(
                CoreService_pb2.GetFeatureSetRequest(
                    project=project, name=name.strip(),
                )
            )  # type: GetFeatureSetResponse
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())
        return get_feature_set_response.feature_set

    @pytest.mark.timeout(45)
    @pytest.mark.run(order=51)
    def test_register_feature_set_with_labels(self, core_service_stub):
        feature_set_name = "test_feature_set_labels"
        feature_set_proto = FeatureSet(feature_set_name, PROJECT_NAME).to_proto()
        feature_set_proto.spec.labels[self.LABEL_KEY] = self.LABEL_VALUE
        self.apply_feature_set(core_service_stub, feature_set_proto)

        retrieved_feature_set = self.get_feature_set(core_service_stub, feature_set_name, PROJECT_NAME)

        assert self.LABEL_KEY in retrieved_feature_set.spec.labels
        assert retrieved_feature_set.spec.labels[self.LABEL_KEY] == self.LABEL_VALUE

    @pytest.mark.timeout(45)
    @pytest.mark.run(order=52)
    def test_register_feature_with_labels(self, core_service_stub):
        feature_set_name = "test_feature_labels"
        feature_set_proto = FeatureSet(feature_set_name, PROJECT_NAME, features=[Feature("rating", ValueType.INT64)]) \
            .to_proto()
        feature_set_proto.spec.features[0].labels[self.LABEL_KEY] = self.LABEL_VALUE
        self.apply_feature_set(core_service_stub, feature_set_proto)

        retrieved_feature_set = self.get_feature_set(core_service_stub, feature_set_name, PROJECT_NAME)
        retrieved_feature = retrieved_feature_set.spec.features[0]

        assert self.LABEL_KEY in retrieved_feature.labels
        assert retrieved_feature.labels[self.LABEL_KEY] == self.LABEL_VALUE
