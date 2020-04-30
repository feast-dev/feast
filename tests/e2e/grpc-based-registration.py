import pytest
import grpc
import uuid

import feast.core.CoreService_pb2
from feast import Feature
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.feature_set import FeatureSet
from feast import ValueType

PROJECT_NAME = 'batch_' + uuid.uuid4().hex.upper()[0:6]
LAST_VERSION = 0
GRPC_CONNECTION_TIMEOUT = 3
LABEL_KEY = "my"
LABEL_VALUE = "label"


@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def core_service_stub(core_url):
    if core_url.endswith(":443"):
        core_channel = grpc.secure_channel(
            core_url, grpc.ssl_channel_credentials()
        )
    else:
        core_channel = grpc.insecure_channel(core_url)

    try:
        grpc.channel_ready_future(core_channel).result(timeout=GRPC_CONNECTION_TIMEOUT)
    except grpc.FutureTimeoutError:
        raise ConnectionError(
            f"Connection timed out while attempting to connect to Feast "
            f"Core gRPC server {core_url} "
        )
    core_service_stub = CoreServiceStub(core_channel)
    return core_service_stub


def apply_feature_set(core_service_stub, feature_set_proto):
    try:
        apply_fs_response = core_service_stub.ApplyFeatureSet(
            feast.core.CoreService_pb2.ApplyFeatureSetRequest(feature_set=feature_set_proto),
            timeout=GRPC_CONNECTION_TIMEOUT,
        )  # type: ApplyFeatureSetResponse
    except grpc.RpcError as e:
        raise grpc.RpcError(e.details())
    return apply_fs_response.feature_set


def get_feature_set(core_service_stub, name, project):
    try:
        get_feature_set_response = core_service_stub.GetFeatureSet(
            feast.core.CoreService_pb2.GetFeatureSetRequest(
                project=project, name=name.strip(), version=LAST_VERSION
            )
        )  # type: GetFeatureSetResponse
    except grpc.RpcError as e:
        raise grpc.RpcError(e.details())
    return get_feature_set_response.feature_set


@pytest.mark.timeout(45)
def test_feature_set_labels(core_service_stub):
    feature_set_name = "test_feature_set_labels"
    feature_set_proto = FeatureSet(feature_set_name, PROJECT_NAME).to_proto()
    feature_set_proto.spec.labels[LABEL_KEY] = LABEL_VALUE
    apply_feature_set(core_service_stub, feature_set_proto)

    retrieved_feature_set = get_feature_set(core_service_stub, feature_set_name, PROJECT_NAME)

    assert LABEL_KEY in retrieved_feature_set.spec.labels
    assert retrieved_feature_set.spec.labels[LABEL_KEY] == LABEL_VALUE


def test_feature_labels(core_service_stub):
    feature_set_name = "test_feature_labels"
    feature_set_proto = FeatureSet(feature_set_name, PROJECT_NAME, features=[Feature("rating", ValueType.INT64)])\
        .to_proto()
    feature_set_proto.spec.features[0].labels[LABEL_KEY] = LABEL_VALUE
    apply_feature_set(core_service_stub, feature_set_proto)

    retrieved_feature_set = get_feature_set(core_service_stub, feature_set_name, PROJECT_NAME)
    retrieved_feature = retrieved_feature_set.spec.features[0]

    assert LABEL_KEY in retrieved_feature.labels
    assert retrieved_feature.labels[LABEL_KEY] == LABEL_VALUE
