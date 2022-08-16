from typing import Dict, List

import pandas as pd
import pyarrow as pa
import pytest
from sqlalchemy import create_engine
from testcontainers.core.container import DockerContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pa_to_mssql_type
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

MSSQL_USER = "SA"
MSSQL_PASSWORD = "yourStrong(!)Password"


# This is the sql container to use if your machine doesn't support the official msql docker container.
@pytest.fixture(scope="session")
def mssql_container():
    container = (
        DockerContainer("mcr.microsoft.com/azure-sql-edge:1.0.6")
        .with_exposed_ports("1433")
        .with_env("ACCEPT_EULA", "1")
        .with_env("MSSQL_USER", MSSQL_USER)
        .with_env("MSSQL_SA_PASSWORD", MSSQL_PASSWORD)
    )
    container.start()
    log_string_to_wait_for = "Service Broker manager has started"
    wait_for_logs(container=container, predicate=log_string_to_wait_for, timeout=30)

    yield container
    container.stop()


def _df_to_create_table_sql(df: pd.DataFrame, table_name: str) -> str:
    pa_table = pa.Table.from_pandas(df)

    columns = [f""""{f.name}" {pa_to_mssql_type(f.type)}""" for f in pa_table.schema]
    return f"""
        CREATE TABLE "{table_name}" (
            {", ".join(columns)}
        );
        """




class MsSqlDataSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(project_name)
        self.tables_created: List[str] = []
        self.container = SqlServerContainer(user=MSSQL_USER, password=MSSQL_PASSWORD)
        #self.container = fixture_request.getfixturevalue("mssql_container")
        self.container.start()
        #self.exposed_port = self.container.get_exposed_port("1433")
        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.mssql_offline_store.tests' "
                "must be include into pytest plugins"
            )

    def create_offline_store_config(self) -> MsSqlServerOfflineStoreConfig:
        return MsSqlServerOfflineStoreConfig(
            connection_string=self.container.get_connection_url(),
            # connection_string=(
            #     f"mssql+pyodbc://{MSSQL_USER}:{MSSQL_PASSWORD}@0.0.0.0:1433/master?"
            #     "driver=ODBC+Driver+17+for+SQL+Server"
            # )
        )

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
        **kwargs,
    ) -> DataSource:
        # if timestamp_field in df:
        #     df[timestamp_field] = pd.to_datetime(df[timestamp_field], utc=True).fillna(pd.Timestamp.now()) #.dt.tz_localize(None)
        #     # Make sure the field mapping is correct and convert the datetime datasources.
        # if created_timestamp_column in df:
        #     df[created_timestamp_column] = pd.to_datetime(df[created_timestamp_column], utc=True).fillna(pd.Timestamp.now()) #.dt.tz_localize(None)
        
        connection_string = self.create_offline_store_config().connection_string
        engine = create_engine(connection_string)
        # Create table
        
        destination_name = self.get_prefixed_table_name(destination_name)
        #_df_to_create_table_sql(df, destination_name)
        engine.execute(_df_to_create_table_sql(df, destination_name))
        # Upload dataframe to azure table
        df.to_sql(destination_name, engine, index=False, if_exists='append')
        #, dtype={timestamp_field: DATETIME2(), created_timestamp_column: DATETIME2()}
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
        pass

    def get_prefixed_table_name(self, destination_name: str) -> str:
        return f"{self.project_name}_{destination_name}"

    def teardown(self):
        pass
