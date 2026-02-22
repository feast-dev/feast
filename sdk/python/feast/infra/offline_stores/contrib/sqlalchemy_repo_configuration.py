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
Example configuration for SQLAlchemy offline store.

This module provides example configurations for using the SQLAlchemy offline store
with various database backends.

Example YAML configurations:

1. PostgreSQL:
   ```yaml
   project: my_feast_project
   registry: data/registry.db
   provider: local
   offline_store:
     type: sqlalchemy
     connection_string: postgresql+psycopg://user:password@localhost:5432/feast_db
     db_schema: public
     sqlalchemy_config_kwargs:
       echo: false
       pool_pre_ping: true
   online_store:
     type: sqlite
     path: data/online.db
   ```

2. MySQL:
   ```yaml
   project: my_feast_project
   registry: data/registry.db
   provider: local
   offline_store:
     type: sqlalchemy
     connection_string: mysql+pymysql://user:password@localhost:3306/feast_db
     sqlalchemy_config_kwargs:
       echo: false
       pool_pre_ping: true
   online_store:
     type: sqlite
     path: data/online.db
   ```

3. SQLite (for testing):
   ```yaml
   project: my_feast_project
   registry: data/registry.db
   provider: local
   offline_store:
     type: sqlalchemy
     connection_string: sqlite:///data/offline.db
   online_store:
     type: sqlite
     path: data/online.db
   ```

4. MS SQL Server:
   ```yaml
   project: my_feast_project
   registry: data/registry.db
   provider: local
   offline_store:
     type: sqlalchemy
     connection_string: mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server
     sqlalchemy_config_kwargs:
       echo: false
       pool_pre_ping: true
   online_store:
     type: sqlite
     path: data/online.db
   ```

Example Python usage:

    from datetime import datetime, timedelta
    from feast import Entity, FeatureView, Field, FeatureStore
    from feast.types import Float32, Int64
    from feast.infra.offline_stores.contrib.sqlalchemy_offline_store import SQLAlchemySource

    # Define an entity
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )

    # Define a data source using SQLAlchemy
    driver_stats_source = SQLAlchemySource(
        name="driver_stats_source",
        table="driver_stats",  # Table in the database
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )

    # Or use a query
    driver_stats_source = SQLAlchemySource(
        name="driver_stats_source",
        query="SELECT * FROM driver_stats WHERE active = true",
        timestamp_field="event_timestamp",
    )

    # Define a feature view
    driver_stats_fv = FeatureView(
        name="driver_stats",
        entities=[driver],
        ttl=timedelta(days=1),
        schema=[
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int64),
        ],
        source=driver_stats_source,
    )

    # Use with FeatureStore
    store = FeatureStore(repo_path=".")
    store.apply([driver, driver_stats_fv])
"""

from feast.infra.offline_stores.contrib.sqlalchemy_offline_store.tests.data_source import (
    SQLAlchemyDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    # SQLite configuration (in-memory for testing)
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=SQLAlchemyDataSourceCreator,
    ),
]
