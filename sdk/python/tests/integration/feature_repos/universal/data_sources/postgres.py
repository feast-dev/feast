import logging
from typing import Dict, Optional

import pandas as pd
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStoreConfig,
    PostgreSQLSource,
)
from feast.infra.utils.postgres.connection_utils import df_to_postgres_table
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)

logger = logging.getLogger(__name__)


class PostgresSourceCreatorSingleton:
    postgres_user = "test"
    postgres_password = "test"
    postgres_db = "test"

    running = False

    project_name = None
    container = None
    provided_container = None

    offline_store_config = None

    @classmethod
    def initialize(cls, project_name: str, *args, **kwargs):
        cls.project_name = project_name

        if "offline_container" not in kwargs or not kwargs.get(
            "offline_container", None
        ):
            # If we don't get an offline container provided, we try to create it on the fly.
            # the problem here is that each test creates its own container, which basically
            # browns out developer laptops.
            cls.container = (
                DockerContainer("postgres:latest")
                .with_exposed_ports(5432)
                .with_env("POSTGRES_USER", cls.postgres_user)
                .with_env("POSTGRES_PASSWORD", cls.postgres_password)
                .with_env("POSTGRES_DB", cls.postgres_db)
            )

            cls.container.start()
            cls.provided_container = False
            log_string_to_wait_for = "database system is ready to accept connections"
            waited = wait_for_logs(
                container=cls.container,
                predicate=log_string_to_wait_for,
                timeout=30,
                interval=10,
            )
            logger.info("Waited for %s seconds until postgres container was up", waited)
            cls.running = True
        else:
            cls.provided_container = True
            cls.container = kwargs["offline_container"]

        cls.offline_store_config = PostgreSQLOfflineStoreConfig(
            type="postgres",
            host="localhost",
            port=cls.container.get_exposed_port(5432),
            database=cls.container.env["POSTGRES_DB"],
            db_schema="public",
            user=cls.container.env["POSTGRES_USER"],
            password=cls.container.env["POSTGRES_PASSWORD"],
        )

    @classmethod
    def create_data_source(
        cls,
        df: pd.DataFrame,
        destination_name: str,
        suffix: Optional[str] = None,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:

        destination_name = cls.get_prefixed_table_name(destination_name)

        if cls.offline_store_config:
            df_to_postgres_table(cls.offline_store_config, df, destination_name)

        return PostgreSQLSource(
            name=destination_name,
            query=f"SELECT * FROM {destination_name}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    @classmethod
    def create_offline_store_config(cls) -> PostgreSQLOfflineStoreConfig:
        assert cls.offline_store_config
        return cls.offline_store_config

    @classmethod
    def get_prefixed_table_name(cls, suffix: str) -> str:
        return f"{cls.project_name}_{suffix}"

    @classmethod
    def create_online_store(cls) -> Dict[str, str]:
        assert cls.container
        return {
            "type": "postgres",
            "host": "localhost",
            "port": cls.container.get_exposed_port(5432),
            "database": cls.postgres_db,
            "db_schema": "feature_store",
            "user": cls.postgres_user,
            "password": cls.postgres_password,
        }

    @classmethod
    def create_saved_dataset_destination(cls):
        # FIXME: ...
        return None

    @classmethod
    def teardown(cls):
        if not cls.provided_container and cls.running:
            cls.container.stop()
            cls.running = False
            cls.container = None
            cls.project = None


class PostgreSQLDataSourceCreator(DataSourceCreator, OnlineStoreCreator):

    postgres_user = "test"
    postgres_password = "test"
    postgres_db = "test"

    running = False

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        PostgresSourceCreatorSingleton.initialize(project_name, args, kwargs)

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        suffix: Optional[str] = None,
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:

        return PostgresSourceCreatorSingleton.create_data_source(
            df,
            destination_name,
            suffix,
            timestamp_field,
            created_timestamp_column,
            field_mapping,
        )

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return PostgresSourceCreatorSingleton.create_offline_store_config()

    def get_prefixed_table_name(self, suffix: str) -> str:
        return PostgresSourceCreatorSingleton.get_prefixed_table_name(suffix)

    def create_online_store(self) -> Dict[str, str]:
        return PostgresSourceCreatorSingleton.create_online_store()

    def create_saved_dataset_destination(self):
        # FIXME: ...
        return None

    def teardown(self):
        PostgresSourceCreatorSingleton.teardown()
