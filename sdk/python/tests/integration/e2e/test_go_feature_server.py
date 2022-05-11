import socket
import threading
import time
from contextlib import closing
from datetime import datetime
from typing import List

import grpc
import pandas as pd
import pytest
import pytz

from feast import FeatureService, ValueType
from feast.embedded_go.lib.embedded import LoggingOptions
from feast.embedded_go.online_features_service import EmbeddedOnlineFeatureServer
from feast.feast_object import FeastObject
from feast.feature_logging import LoggingConfig
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
LOCAL_REPO_CONFIGS = [
    c
    for c in LOCAL_REPO_CONFIGS
    if c.offline_store_creator in AVAILABLE_OFFLINE_STORES
    and c.online_store in AVAILABLE_ONLINE_STORES
]

NANOSECOND = 1
MILLISECOND = 1000_000 * NANOSECOND
SECOND = 1000 * MILLISECOND


@pytest.fixture(
    params=LOCAL_REPO_CONFIGS,
    ids=[str(c) for c in LOCAL_REPO_CONFIGS],
    scope="session",
)
def local_environment(request):
    e = construct_test_environment(request.param, fixture_request=request)

    def cleanup():
        e.feature_store.teardown()

    request.addfinalizer(cleanup)
    return e


@pytest.fixture(scope="session")
def test_data(local_environment):
    return construct_universal_test_data(local_environment)


@pytest.fixture(scope="session")
def initialized_registry(local_environment, test_data):
    fs = local_environment.feature_store

    _, _, data_sources = test_data
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        name="driver_features",
        features=[feature_views.driver],
        logging_config=LoggingConfig(
            destination=local_environment.data_source_creator.create_logged_features_destination(),
            sample_rate=1.0,
        ),
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

    t = threading.Thread(
        target=embedded.start_grpc_server,
        args=("127.0.0.1", port),
        kwargs=dict(
            enable_logging=True,
            logging_options=LoggingOptions(
                ChannelCapacity=100,
                WriteInterval=100 * MILLISECOND,
                FlushInterval=1 * SECOND,
                EmitTimeout=10 * MILLISECOND,
            ),
        ),
    )
    t.start()

    wait_retry_backoff(
        lambda: (None, check_port_open("127.0.0.1", port)), timeout_secs=15
    )

    yield port
    embedded.stop_grpc_server()
    # wait for graceful stop
    time.sleep(2)


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


@pytest.mark.integration
@pytest.mark.universal
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_feature_logging(grpc_client, local_environment, test_data, full_feature_names):
    fs = local_environment.feature_store
    feature_service = fs.get_feature_service("driver_features")
    log_start_date = datetime.now().astimezone(pytz.UTC)
    driver_ids = list(range(5001, 5011))

    for driver_id in driver_ids:
        # send each driver id in separate request
        grpc_client.GetOnlineFeatures(
            GetOnlineFeaturesRequest(
                feature_service="driver_features",
                entities={
                    "driver_id": RepeatedValue(
                        val=python_values_to_proto_values(
                            [driver_id], feature_type=ValueType.INT64
                        )
                    )
                },
                full_feature_names=full_feature_names,
            )
        )
        # with some pause
        time.sleep(0.1)

    _, datasets, _ = test_data
    latest_rows = get_latest_rows(datasets.driver_df, "driver_id", driver_ids)
    features = [
        feature.name
        for proj in feature_service.feature_view_projections
        for feature in proj.features
    ]
    expected_logs = generate_expected_logs(
        latest_rows, "driver_stats", features, ["driver_id"], "event_timestamp"
    )

    def retrieve():
        retrieval_job = fs._get_provider().retrieve_feature_service_logs(
            feature_service=feature_service,
            start_date=log_start_date,
            end_date=datetime.now().astimezone(pytz.UTC),
            config=fs.config,
            registry=fs._registry,
        )
        try:
            df = retrieval_job.to_df()
        except Exception:
            # Table or directory was not created yet
            return None, False

        return df, df.shape[0] == len(driver_ids)

    persisted_logs = wait_retry_backoff(
        retrieve, timeout_secs=60, timeout_msg="Logs retrieval failed"
    )

    persisted_logs = persisted_logs.sort_values(by="driver_id").reset_index(drop=True)
    persisted_logs = persisted_logs[expected_logs.columns]
    pd.testing.assert_frame_equal(expected_logs, persisted_logs, check_dtype=False)


def free_port():
    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


def check_port_open(host, port) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def get_latest_rows(df, join_key, entity_values):
    rows = df[df[join_key].isin(entity_values)]
    return rows.loc[rows.groupby(join_key)["event_timestamp"].idxmax()]


def generate_expected_logs(
    df, feature_view_name, features, join_keys, timestamp_column
):
    logs = pd.DataFrame()
    for join_key in join_keys:
        logs[join_key] = df[join_key]

    for feature in features:
        logs[f"{feature_view_name}__{feature}"] = df[feature]
        logs[f"{feature_view_name}__{feature}__timestamp"] = df[timestamp_column]
        logs[f"{feature_view_name}__{feature}__status"] = FieldStatus.PRESENT

    return logs.sort_values(by=join_keys).reset_index(drop=True)
