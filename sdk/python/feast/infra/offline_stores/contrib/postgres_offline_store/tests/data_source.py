import logging
from typing import Dict, Literal, Optional

import pandas as pd
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStoreConfig,
    PostgreSQLSource,
)
from feast.infra.utils.postgres.connection_utils import df_to_postgres_table
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

logger = logging.getLogger(__name__)

POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"
POSTGRES_DB = "test"


class PostgreSQLOnlineStoreConfig(PostgreSQLConfig):
    type: Literal["postgres"] = "postgres"


@pytest.fixture(scope="session")
def postgres_container():
    container = (
        DockerContainer("postgres:latest")
        .with_exposed_ports(5432)
        .with_env("POSTGRES_USER", POSTGRES_USER)
        .with_env("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env("POSTGRES_DB", POSTGRES_DB)
    )

    container.start()

    log_string_to_wait_for = "database system is ready to accept connections"
    waited = wait_for_logs(
        container=container,
        predicate=log_string_to_wait_for,
        timeout=30,
        interval=10,
    )
    logger.info("Waited for %s seconds until postgres container was up", waited)

    yield container
    container.stop()


class PostgreSQLDataSourceCreator(DataSourceCreator, OnlineStoreCreator):
    def create_logged_features_destination(self) -> LoggingDestination:
        return None  # type: ignore

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(
            project_name,
        )

        self.project_name = project_name
        self.container = fixture_request.getfixturevalue("postgres_container")
        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.postgres_offline_store.tests' "
                "must be include into pytest plugins"
            )

        self.offline_store_config = PostgreSQLOfflineStoreConfig(
            type="postgres",
            host="localhost",
            port=self.container.get_exposed_port(5432),
            database=self.container.env["POSTGRES_DB"],
            db_schema="public",
            user=self.container.env["POSTGRES_USER"],
            password=self.container.env["POSTGRES_PASSWORD"],
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
            df_to_postgres_table(self.offline_store_config, df, destination_name)
        return PostgreSQLSource(
            name=destination_name,
            query=f"SELECT * FROM {destination_name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_offline_store_config(self) -> PostgreSQLOfflineStoreConfig:
        assert self.offline_store_config
        return self.offline_store_config

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_online_store(self) -> PostgreSQLOnlineStoreConfig:  # type: ignore
        assert self.container
        return PostgreSQLOnlineStoreConfig(
            type="postgres",
            host="localhost",
            port=self.container.get_exposed_port(5432),
            database=POSTGRES_DB,
            db_schema="feature_store",
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def create_saved_dataset_destination(self):
        # FIXME: ...
        return None

    def teardown(self):
        pass
