import socket
import threading
from contextlib import closing
from typing import List

import grpc
import pytest

from feast import FeatureService, ValueType
from feast.embedded_go.online_features_service import EmbeddedOnlineFeatureServer
from feast.feast_object import FeastObject
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.type_map import python_values_to_proto_values
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    AVAILABLE_OFFLINE_STORES,
    AVAILABLE_ONLINE_STORES,
    REDIS_CONFIG,
    construct_test_environment,
    construct_universal_feature_views,
    construct_universal_test_data,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)

LOCAL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(online_store=REDIS_CONFIG, go_feature_retrieval=True),
]


@pytest.fixture(
    params=[
        c
        for c in LOCAL_REPO_CONFIGS
        if c.offline_store_creator in AVAILABLE_OFFLINE_STORES
        and c.online_store in AVAILABLE_ONLINE_STORES
    ]
)
def local_environment(request):
    e = construct_test_environment(request.param)

    def cleanup():
        e.feature_store.teardown()

    request.addfinalizer(cleanup)
    return e


@pytest.fixture
def initialized_registry(local_environment):
    fs = local_environment.feature_store

    entities, datasets, data_sources = construct_universal_test_data(local_environment)
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        name="driver_features", features=[feature_views.driver]
    )
    feast_objects: List[FeastObject] = [feature_service]
    feast_objects.extend(feature_views.values())
    feast_objects.extend([driver(), customer(), location()])

    fs.apply(feast_objects)
    fs.materialize(local_environment.start_date, local_environment.end_date)


@pytest.fixture
def grpc_server_port(local_environment, initialized_registry):
    fs = local_environment.feature_store

    embedded = EmbeddedOnlineFeatureServer(
        repo_path=str(fs.repo_path.absolute()), repo_config=fs.config, feature_store=fs,
    )
    port = free_port()

    t = threading.Thread(target=embedded.start_grpc_server, args=("127.0.0.1", port))
    t.start()

    wait_retry_backoff(
        lambda: (None, check_port_open("127.0.0.1", port)), timeout_secs=15
    )

    yield port
    embedded.stop_grpc_server()


@pytest.fixture
def grpc_client(grpc_server_port):
    ch = grpc.insecure_channel(f"localhost:{grpc_server_port}")
    yield ServingServiceStub(ch)


@pytest.mark.integration
@pytest.mark.universal
def test_go_grpc_server(grpc_client):
    resp: GetOnlineFeaturesResponse = grpc_client.GetOnlineFeatures(
        GetOnlineFeaturesRequest(
            feature_service="driver_features",
            entities={
                "driver_id": RepeatedValue(
                    val=python_values_to_proto_values(
                        [5001, 5002], feature_type=ValueType.INT64
                    )
                )
            },
            full_feature_names=True,
        )
    )
    assert list(resp.metadata.feature_names.val) == [
        "driver_id",
        "driver_stats__conv_rate",
        "driver_stats__acc_rate",
        "driver_stats__avg_daily_trips",
    ]
    for vector in resp.results:
        assert all([s == FieldStatus.PRESENT for s in vector.statuses])


def free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def check_port_open(host, port) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0
