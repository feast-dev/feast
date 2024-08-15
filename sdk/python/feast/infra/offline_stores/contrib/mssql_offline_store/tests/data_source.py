from typing import Dict, List, Optional

import ibis
import pandas as pd
import pytest
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.mssql import SqlServerContainer

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

MSSQL_USER = "SA"
MSSQL_PASSWORD = "yourStrong(!)Password"


@pytest.fixture(scope="session")
def mssql_container():
    container = SqlServerContainer(
        username=MSSQL_USER,
        password=MSSQL_PASSWORD,
        image="mcr.microsoft.com/azure-sql-edge:1.0.6",
    )
    container.start()
    log_string_to_wait_for = "Service Broker manager has started"
    wait_for_logs(container=container, predicate=log_string_to_wait_for, timeout=30)

    yield container
    container.stop()


class MsSqlDataSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(project_name)
        self.tables_created: List[str] = []
        self.container = fixture_request.getfixturevalue("mssql_container")

        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.mssql_offline_store.tests' "
                "must be include into pytest plugins"
            )

    def create_offline_store_config(self) -> MsSqlServerOfflineStoreConfig:
        connection_string = self.container.get_connection_url()
        connection_string += "?driver=FreeTDS"
        return MsSqlServerOfflineStoreConfig(
            connection_string=connection_string,
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = None,
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

        con = ibis.mssql.connect(
            user=self.container.username,
            password=self.container.password,
            host=self.container.get_container_host_ip(),
            port=self.container.get_exposed_port(self.container.port),
            database=self.container.dbname,
            driver="FreeTDS",
        )

        destination_name = self.get_prefixed_table_name(destination_name)

        con.create_table(
            name=destination_name,
            schema=ibis.Schema.from_pandas(df.dtypes.to_dict().items()),
        )
        con.insert(table_name=destination_name, obj=df)

        self.tables.append(destination_name)
        return MsSqlServerSource(
            name="ci_mssql_source",
            connection_str=connection_string,
            table_ref=destination_name,
            event_timestamp_column=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        raise NotImplementedError

    def create_logged_features_destination(self) -> LoggingDestination:
        raise NotImplementedError

    def get_prefixed_table_name(self, destination_name: str) -> str:
        return f"{self.project_name}_{destination_name}"

    def teardown(self):
        pass
        # raise NotImplementedError
