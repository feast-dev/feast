import os
import tempfile
from datetime import datetime, timedelta

import assertpy
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from feast import FeatureStore, FeatureView, FileSource
from feast.errors import FeatureViewNotFoundException
from feast.feature_logging import FeatureServiceLoggingSource
from feast.infra.offline_stores.remote import (
    RemoteOfflineStore,
    RemoteOfflineStoreConfig,
)
from feast.offline_server import OfflineServer, _init_auth_manager
from feast.repo_config import RepoConfig
from feast.torch_wrapper import get_torch
from tests.utils.cli_repo_creator import CliRunner

PROJECT_NAME = "test_remote_offline"


@pytest.fixture
def empty_offline_server(environment):
    store = environment.feature_store

    location = "grpc+tcp://localhost:0"
    _init_auth_manager(store=store)
    return OfflineServer(store=store, location=location)


@pytest.fixture
def arrow_client(empty_offline_server):
    return flight.FlightClient(f"grpc://localhost:{empty_offline_server.port}")


def test_offline_server_is_alive(environment, empty_offline_server, arrow_client):
    server = empty_offline_server
    client = arrow_client

    assertpy.assert_that(server).is_not_none
    assertpy.assert_that(server.port).is_not_equal_to(0)

    actions = list(client.list_actions())
    flights = list(client.list_flights())

    assertpy.assert_that(actions).is_equal_to(
        [
            (
                "offline_write_batch",
                "Writes the specified arrow table to the data source underlying the specified feature view.",
            ),
            (
                "write_logged_features",
                "Writes logged features to a specified destination in the offline store.",
            ),
            (
                "persist",
                "Synchronously executes the underlying query and persists the result in the same offline store at the "
                "specified destination.",
            ),
        ]
    )
    assertpy.assert_that(flights).is_empty()


def default_store(temp_dir):
    runner = CliRunner()
    result = runner.run(["init", PROJECT_NAME], cwd=temp_dir)
    repo_path = os.path.join(temp_dir, PROJECT_NAME, "feature_repo")
    assert result.returncode == 0

    result = runner.run(["--chdir", repo_path, "apply"], cwd=temp_dir)
    assert result.returncode == 0

    fs = FeatureStore(repo_path=repo_path)
    return fs


def remote_feature_store(offline_server):
    offline_config = RemoteOfflineStoreConfig(
        type="remote", host="0.0.0.0", port=offline_server.port
    )

    registry_path = os.path.join(
        str(offline_server.store.repo_path),
        offline_server.store.config.registry.path,
    )
    store = FeatureStore(
        config=RepoConfig(
            project=PROJECT_NAME,
            registry=registry_path,
            provider="local",
            offline_store=offline_config,
            entity_key_serialization_version=3,
            # repo_config =
        )
    )
    return store


def test_remote_offline_store_apis():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = default_store(str(temp_dir))
        location = "grpc+tcp://localhost:0"

        _init_auth_manager(store=store)
        server = OfflineServer(store=store, location=location)

        assertpy.assert_that(server).is_not_none
        assertpy.assert_that(server.port).is_not_equal_to(0)

        fs = remote_feature_store(server)

        _test_get_historical_features_returns_data(fs)
        _test_get_historical_features_to_tensor(fs)
        _test_get_historical_features_returns_nan(fs)
        _test_get_historical_features_to_tensor_with_nan(fs)
        _test_offline_write_batch(str(temp_dir), fs)
        _test_write_logged_features(str(temp_dir), fs)
        _test_pull_latest_from_table_or_query(str(temp_dir), fs)
        _test_pull_all_from_table_or_query(str(temp_dir), fs)


def test_remote_offline_store_exception_handling():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = default_store(str(temp_dir))
        location = "grpc+tcp://localhost:0"

        _init_auth_manager(store=store)
        server = OfflineServer(store=store, location=location)

        assertpy.assert_that(server).is_not_none
        assertpy.assert_that(server.port).is_not_equal_to(0)

        fs = remote_feature_store(server)
        data_file = os.path.join(
            temp_dir, fs.project, "feature_repo/data/driver_stats.parquet"
        )
        data_df = pd.read_parquet(data_file)

        with pytest.raises(
            FeatureViewNotFoundException,
            match="Feature view test does not exist in project test_remote_offline",
        ):
            RemoteOfflineStore.offline_write_batch(
                fs.config,
                FeatureView(name="test", source=FileSource(path="test")),
                pa.Table.from_pandas(data_df),
                progress=None,
            )


def _test_get_historical_features_returns_data(fs: FeatureStore):
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            "label_driver_reported_satisfaction": [1, 5, 3],
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )

    features = [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
    ]

    training_df = fs.get_historical_features(entity_df, features).to_df()

    assertpy.assert_that(training_df).is_not_none()
    assertpy.assert_that(len(training_df)).is_equal_to(3)

    for index, driver_id in enumerate(entity_df["driver_id"]):
        assertpy.assert_that(training_df["driver_id"][index]).is_equal_to(driver_id)
        for feature in features:
            column_id = feature.split(":")[1]
            value = training_df[column_id][index]
            assertpy.assert_that(value).is_not_nan()


def _test_get_historical_features_to_tensor(fs: FeatureStore):
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1001, 1002, 1003],
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            "label_driver_reported_satisfaction": [1, 5, 3],
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )

    features = [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
    ]

    job = fs.get_historical_features(entity_df, features)
    tensor_data = job.to_tensor()

    assertpy.assert_that(tensor_data).is_not_none()
    assertpy.assert_that(tensor_data["driver_id"].shape[0]).is_equal_to(3)
    torch = get_torch()
    for key, values in tensor_data.items():
        if isinstance(values, torch.Tensor):
            assertpy.assert_that(values.shape[0]).is_equal_to(3)
            for val in values:
                val_float = val.item()
                assertpy.assert_that(val_float).is_instance_of((float, int))
                assertpy.assert_that(val_float).is_not_nan()


def _test_get_historical_features_returns_nan(fs: FeatureStore):
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [1, 2, 3],
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 8, 12, 10),
                datetime(2021, 4, 12, 16, 40, 26),
            ],
            "label_driver_reported_satisfaction": [1, 5, 3],
            "val_to_add": [1, 2, 3],
            "val_to_add_2": [10, 20, 30],
        }
    )

    features = [
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
        "transformed_conv_rate:conv_rate_plus_val1",
        "transformed_conv_rate:conv_rate_plus_val2",
    ]

    training_df = fs.get_historical_features(entity_df, features).to_df()

    assertpy.assert_that(training_df).is_not_none()
    assertpy.assert_that(len(training_df)).is_equal_to(3)

    for index, driver_id in enumerate(entity_df["driver_id"]):
        assertpy.assert_that(training_df["driver_id"][index]).is_equal_to(driver_id)
        for feature in features:
            column_id = feature.split(":")[1]
            value = training_df[column_id][index]
            assertpy.assert_that(value).is_nan()


def _test_get_historical_features_to_tensor_with_nan(fs: FeatureStore):
    entity_df = pd.DataFrame.from_dict(
        {
            "driver_id": [9991, 9992],  # IDs with no matching features
            "event_timestamp": [
                datetime(2021, 4, 12, 10, 59, 42),
                datetime(2021, 4, 12, 10, 59, 42),
            ],
        }
    )
    features = ["driver_hourly_stats:conv_rate"]
    job = fs.get_historical_features(entity_df, features)
    tensor_data = job.to_tensor()
    assert "conv_rate" in tensor_data
    values = tensor_data["conv_rate"]
    # conv_rate is a float feature, missing values should be NaN
    torch = get_torch()
    for val in values:
        assert isinstance(val, torch.Tensor) or torch.is_tensor(val)
        assertpy.assert_that(torch.isnan(val).item()).is_true()


def _test_offline_write_batch(temp_dir, fs: FeatureStore):
    data_file = os.path.join(
        temp_dir, fs.project, "feature_repo/data/driver_stats.parquet"
    )
    data_df = pd.read_parquet(data_file)
    feature_view = fs.get_feature_view("driver_hourly_stats")

    RemoteOfflineStore.offline_write_batch(
        fs.config, feature_view, pa.Table.from_pandas(data_df), progress=None
    )


def _test_write_logged_features(temp_dir, fs: FeatureStore):
    data_file = os.path.join(
        temp_dir, fs.project, "feature_repo/data/driver_stats.parquet"
    )
    data_df = pd.read_parquet(data_file)
    feature_service = fs.get_feature_service("driver_activity_v1")

    RemoteOfflineStore.write_logged_features(
        config=fs.config,
        data=pa.Table.from_pandas(data_df),
        source=FeatureServiceLoggingSource(feature_service, fs.config.project),
        logging_config=feature_service.logging_config,
        registry=fs.registry,
    )


def _test_pull_latest_from_table_or_query(temp_dir, fs: FeatureStore):
    data_source = fs.get_data_source("driver_hourly_stats_source")

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    RemoteOfflineStore.pull_latest_from_table_or_query(
        config=fs.config,
        data_source=data_source,
        join_key_columns=[],
        feature_name_columns=[],
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        start_date=start_date,
        end_date=end_date,
    ).to_df()


def _test_pull_all_from_table_or_query(temp_dir, fs: FeatureStore):
    data_source = fs.get_data_source("driver_hourly_stats_source")

    end_date = datetime.now().replace(microsecond=0, second=0, minute=0)
    start_date = end_date - timedelta(days=15)
    RemoteOfflineStore.pull_all_from_table_or_query(
        config=fs.config,
        data_source=data_source,
        join_key_columns=[],
        feature_name_columns=[],
        timestamp_field="event_timestamp",
        start_date=start_date,
        end_date=end_date,
    ).to_df()
