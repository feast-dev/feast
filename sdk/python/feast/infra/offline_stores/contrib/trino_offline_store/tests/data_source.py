import pathlib
import uuid
from typing import Dict, List, Optional

import pandas as pd
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload import (
    upload_pandas_dataframe_to_trino,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    SavedDatasetTrinoStorage,
    TrinoSource,
)
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


@pytest.fixture(scope="session")
def trino_container():
    current_file = pathlib.Path(__file__).parent.resolve()
    catalog_dir = current_file.parent.joinpath("catalog")
    container = (
        DockerContainer("trinodb/trino:376")
        .with_volume_mapping(catalog_dir, "/etc/catalog/")
        .with_exposed_ports("8080")
    )

    container.start()

    log_string_to_wait_for = "SERVER STARTED"
    wait_for_logs(container=container, predicate=log_string_to_wait_for, timeout=30)

    yield container

    container.stop()


class TrinoSourceCreator(DataSourceCreator):
    tables: List[str] = []

    def __init__(
        self, project_name: str, fixture_request: pytest.FixtureRequest, **kwargs
    ):
        super().__init__(project_name)
        self.tables_created: List[str] = []
        self.container = fixture_request.getfixturevalue("trino_container")
        if not self.container:
            raise RuntimeError(
                "In order to use this data source "
                "'feast.infra.offline_stores.contrib.trino_offline_store.tests' "
                "must be include into pytest plugins"
            )
        self.exposed_port = self.container.get_exposed_port("8080")
        self.container_host = self.container.get_container_host_ip()
        self.client = Trino(
            user="user",
            catalog="memory",
            host=self.container_host,
            port=self.exposed_port,
            source="trino-python-client",
            http_scheme="http",
            verify=False,
            extra_credential=None,
            auth=None,
        )

    def teardown(self):
        pass

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        destination_name = self.get_prefixed_table_name(destination_name)
        self.client.execute_query(
            f"CREATE SCHEMA IF NOT EXISTS memory.{self.project_name}"
        )
        self.client.execute_query(f"DROP TABLE IF EXISTS {destination_name}")

        self.tables.append(destination_name)

        upload_pandas_dataframe_to_trino(
            client=self.client,
            df=df,
            table=destination_name,
            connector_args={"type": "memory"},
        )

        return TrinoSource(
            name="ci_trino_offline_store",
            table=destination_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            query=f"SELECT * FROM {destination_name}",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def create_saved_dataset_destination(self) -> SavedDatasetTrinoStorage:
        table = self.get_prefixed_table_name(
            f"persisted_ds_{str(uuid.uuid4()).replace('-', '_')}"
        )
        self.tables.append(table)

        return SavedDatasetTrinoStorage(table=table)

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"memory.{self.project_name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return TrinoOfflineStoreConfig(
            host=self.container_host,
            port=self.exposed_port,
            catalog="memory",
            dataset=self.project_name,
            connector={"type": "memory"},
            user="test",
            auth=None,
        )
