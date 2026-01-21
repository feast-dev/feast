import logging
from typing import Dict, Optional

import pandas as pd
import pytest
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse import (
    ClickhouseOfflineStoreConfig,
    df_to_clickhouse_table,
)
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
)
from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig
from feast.infra.utils.clickhouse.connection_utils import get_client
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

logger = logging.getLogger(__name__)

CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_OFFLINE_DB = "default"
CLICKHOUSE_ONLINE_DB = "default_online"


@pytest.fixture(scope="session")
def clickhouse_container():
    container = ClickHouseContainer(
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        dbname=CLICKHOUSE_OFFLINE_DB,
    )
    container.start()

    log_string_to_wait_for = "Logging errors to"
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=30,
        interval=10,
    )
    logger.info("Waited for %s seconds until clickhouse container was up", waited)

    yield container
    container.stop()


class ClickhouseDataSourceCreator(DataSourceCreator):
    def create_logged_features_destination(self) -> LoggingDestination:
        return None  # type: ignore

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(
            project_name,
        )

        self.project_name = project_name
        self.container = fixture_request.getfixturevalue("clickhouse_container")
        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.clickhouse_offline_store.tests' "
                "must be include into pytest plugins"
            )

        self.offline_store_config = ClickhouseOfflineStoreConfig(
            type="clickhouse",
            host="localhost",
            port=self.container.get_exposed_port(8123),
            database=CLICKHOUSE_OFFLINE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        destination_name = self.get_prefixed_table_name(destination_name)

        if self.offline_store_config:
            if timestamp_field is None:
                timestamp_field = "ts"
            df_to_clickhouse_table(
                self.offline_store_config, df, destination_name, timestamp_field
            )
        return ClickhouseSource(
            name=destination_name,
            query=f"SELECT * FROM {destination_name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_offline_store_config(self) -> ClickhouseOfflineStoreConfig:
        assert self.offline_store_config
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_saved_dataset_destination(self):
        pass

    def teardown(self):
        pass


def test_get_client_with_additional_params(clickhouse_container):
    """
    Test that get_client works with a real ClickHouse container and properly passes
    additional settings like send_receive_timeout.
    """
    # Create config with custom send_receive_timeout
    config = ClickhouseConfig(
        host=clickhouse_container.get_container_host_ip(),
        port=clickhouse_container.get_exposed_port(8123),
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_OFFLINE_DB,
        additional_client_args={"send_receive_timeout": 60},
    )

    # Get client and verify it works
    client = get_client(config)

    # Verify client is connected and functional by running a simple query
    result = client.query("SELECT 1 AS test_value")
    assert result.result_rows == [(1,)]

    # Verify the send_receive_timeout was applied
    assert client.timeout._read == 60
