# Copyright 2024 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Test data source utilities for SQLAlchemy offline store integration tests.

This module provides utilities for creating test data sources that can be used
with the SQLAlchemy offline store in integration tests.
"""

from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine

from feast.data_source import DataSource
from feast.infra.offline_stores.contrib.sqlalchemy_offline_store.sqlalchemy_source import (
    SQLAlchemySource,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class SQLAlchemyDataSourceCreator(DataSourceCreator):
    """
    DataSourceCreator for SQLAlchemy-based data sources.

    This creator supports any SQLAlchemy-compatible database for testing.
    By default, it uses SQLite for in-memory testing, but can be configured
    to use any database by providing a connection string.
    """

    tables: List[str] = []

    def __init__(
        self,
        project_name: str,
        connection_string: str = "sqlite:///:memory:",
        *args,
        **kwargs,
    ):
        """
        Initialize the SQLAlchemy data source creator.

        Args:
            project_name: Name of the Feast project
            connection_string: SQLAlchemy connection string
                Default is SQLite in-memory database
        """
        super().__init__(project_name, *args, **kwargs)
        self.connection_string = connection_string
        self.project_name = project_name
        self.engine = create_engine(connection_string)

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        timestamp_field: str = "ts",
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
    ) -> DataSource:
        """
        Create a SQLAlchemy data source from a DataFrame.

        Args:
            df: DataFrame containing the data
            destination_name: Name for the table/source
            timestamp_field: Name of the timestamp column
            created_timestamp_column: Optional created timestamp column
            field_mapping: Optional field mapping

        Returns:
            A SQLAlchemySource data source
        """
        table_name = f"test_{self.project_name}_{destination_name}"

        # Upload DataFrame to the database
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        self.tables.append(table_name)

        return SQLAlchemySource(
            name=destination_name,
            table=table_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {},
        )

    def create_saved_dataset_destination(self) -> str:
        """Create a destination for saved datasets."""
        table_name = f"saved_dataset_{self.project_name}"
        return table_name

    def get_prefixed_table_name(self, destination_name: str) -> str:
        """Get the prefixed table name for a destination."""
        return f"test_{self.project_name}_{destination_name}"

    def teardown(self):
        """Clean up test tables."""
        with self.engine.connect() as conn:
            for table in self.tables:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
        self.tables = []
