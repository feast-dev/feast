from typing import Dict, List, Optional

import pandas as pd
import pytest
from sqlalchemy import create_engine
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.mysql import MySqlContainer

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.mariadb_offline_store.mariadb import (
    MariaDBOfflineStoreConfig,
    _df_to_create_mariadb_table,
)
from feast.infra.offline_stores.contrib.mariadb_offline_store.mariadb_source import (
    MariaDBSource,
)
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

MARIADB_USER = "root"
MARIADB_PASSWORD = ""


@pytest.fixture(scope="session")
def mariadb_container():
    container = MySqlContainer(
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        image="mariadb:latest",
    )
    container.start()
    log_string_to_wait_for = "Service Broker manager has started"
    wait_for_logs(container=container, predicate=log_string_to_wait_for, timeout=30)

    yield container
    container.stop()


class MariaDBDataSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(project_name)
        self.tables_created: List[str] = []
        self.container = fixture_request.getfixturevalue("mariadb_container")

        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.mariadb_offline_store.tests' "
                "must be include into pytest plugins"
            )

    def create_offline_store_config(self) -> MariaDBOfflineStoreConfig:
        return MariaDBOfflineStoreConfig(
            connection_string=self.container.get_connection_url(),
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        # Make sure the field mapping is correct and convert the datetime datasources.
        if timestamp_field in df:
            df[timestamp_field] = pd.to_datetime(df[timestamp_field], utc=True).fillna(
                pd.Timestamp.now()
            )
        if created_timestamp_column in df:
            df[created_timestamp_column] = pd.to_datetime(
                df[created_timestamp_column], utc=True
            ).fillna(pd.Timestamp.now())

        connection_string = self.create_offline_store_config().connection_string
        engine = create_engine(connection_string)
        destination_name = self.get_prefixed_table_name(destination_name)
        # Create table
        engine.execute(_df_to_create_mariadb_table(df, destination_name))  # type: ignore

        # Upload dataframe to mariadb table
        df.to_sql(destination_name, engine, index=False, if_exists="append")

        self.tables.append(destination_name)
        return MariaDBSource(
            name="ci_mariadb_source",
            connection_str=connection_string,
            table_ref=destination_name,
            event_timestamp_column=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        raise NotImplementedError

    def get_prefixed_table_name(self, destination_name: str) -> str:
        return f"{self.project_name}_{destination_name}"

    def teardown(self):
        raise NotImplementedError
