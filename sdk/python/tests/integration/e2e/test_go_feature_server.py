import threading
import time
from datetime import datetime
from typing import List

import grpc
import pandas as pd
import pytest
import pytz
import requests

from feast.embedded_go.online_features_service import EmbeddedOnlineFeatureServer
from feast.feast_object import FeastObject
from feast.feature_logging import LoggingConfig
from feast.feature_service import FeatureService
from feast.infra.feature_servers.base_config import FeatureLoggingConfig
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.protos.feast.types.Value_pb2 import RepeatedValue
from feast.type_map import python_values_to_proto_values
from feast.value_type import ValueType
from feast.wait import wait_retry_backoff
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)
from tests.utils.http_server import check_port_open, free_port
from tests.utils.test_log_creator import generate_expected_logs, get_latest_rows


@pytest.mark.integration
@pytest.mark.goserver
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
@pytest.mark.goserver
def test_go_http_server(http_server_port):
    response = requests.post(
        f"http://localhost:{http_server_port}/get-online-features",
        json={
            "feature_service": "driver_features",
            "entities": {"driver_id": [5001, 5002]},
            "full_feature_names": True,
        },
    )
    assert response.status_code == 200, response.text
    response = response.json()
    assert set(response.keys()) == {"metadata", "results"}
    metadata = response["metadata"]
    results = response["results"]
    assert response["metadata"] == {
        "feature_names": [
            "driver_id",
            "driver_stats__conv_rate",
            "driver_stats__acc_rate",
            "driver_stats__avg_daily_trips",
        ]
    }, metadata
    assert len(results) == 4, results
    assert all(
        set(result.keys()) == {"event_timestamps", "statuses", "values"}
        for result in results
    ), results
    assert all(
        result["statuses"] == ["PRESENT", "PRESENT"] for result in results
    ), results
    assert results[0]["values"] == [5001, 5002], results
    for result in results[1:]:
        assert len(result["values"]) == 2, result
        assert all(value is not None for value in result["values"]), result


@pytest.mark.integration
@pytest.mark.goserver
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_feature_logging(
    grpc_client, environment, universal_data_sources, full_feature_names
):
    fs = environment.feature_store
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

    _, datasets, _ = universal_data_sources
    latest_rows = get_latest_rows(datasets.driver_df, "driver_id", driver_ids)
    feature_view = fs.get_feature_view("driver_stats")
    features = [
        feature.name
        for proj in feature_service.feature_view_projections
        for feature in proj.features
    ]
    expected_logs = generate_expected_logs(
        latest_rows, feature_view, features, ["driver_id"], "event_timestamp"
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


"""
Start go feature server either on http or grpc based on the repo configuration for testing.
"""


def _server_port(environment, server_type: str):
    if not environment.test_repo_config.go_feature_serving:
        pytest.skip("Only for Go path")

    fs = environment.feature_store

    embedded = EmbeddedOnlineFeatureServer(
        repo_path=str(fs.repo_path.absolute()),
        repo_config=fs.config,
        feature_store=fs,
    )
    port = free_port()
    if server_type == "grpc":
        target = embedded.start_grpc_server
    elif server_type == "http":
        target = embedded.start_http_server
    else:
        raise ValueError("Server Type must be either 'http' or 'grpc'")

    t = threading.Thread(
        target=target,
        args=("127.0.0.1", port),
        kwargs=dict(
            enable_logging=True,
            logging_options=FeatureLoggingConfig(
                enabled=True,
                queue_capacity=100,
                write_to_disk_interval_secs=1,
                flush_interval_secs=1,
                emit_timeout_micro_secs=10000,
            ),
        ),
    )
    t.start()

    wait_retry_backoff(
        lambda: (None, check_port_open("127.0.0.1", port)), timeout_secs=15
    )

    yield port
    if server_type == "grpc":
        embedded.stop_grpc_server()
    else:
        embedded.stop_http_server()

    # wait for graceful stop
    time.sleep(5)


# Go test fixtures


@pytest.fixture
def initialized_registry(environment, universal_data_sources):
    fs = environment.feature_store

    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    feature_service = FeatureService(
        name="driver_features",
        features=[feature_views.driver],
        logging_config=LoggingConfig(
            destination=environment.data_source_creator.create_logged_features_destination(),
            sample_rate=1.0,
        ),
    )
    feast_objects: List[FeastObject] = [feature_service]
    feast_objects.extend(feature_views.values())
    feast_objects.extend([driver(), customer(), location()])

    fs.apply(feast_objects)
    fs.materialize(environment.start_date, environment.end_date)


@pytest.fixture
def grpc_server_port(environment, initialized_registry):
    yield from _server_port(environment, "grpc")


@pytest.fixture
def http_server_port(environment, initialized_registry):
    yield from _server_port(environment, "http")


@pytest.fixture
def grpc_client(grpc_server_port):
    ch = grpc.insecure_channel(f"localhost:{grpc_server_port}")
    yield ServingServiceStub(ch)
