import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import sqlglot

from feast.entity import Entity
from feast.feature_view import FeatureView, FeatureViewProjection, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStore,
    PostgreSQLOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
def test_pull_latest_from_table_with_nested_timestamp_or_query(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_offline_store_config(),
    )

    test_data_source = PostgreSQLSource(
        name="test_nested_batch_source",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="nested_timestamp",
        field_mapping={
            "event_header.event_published_datetime_utc": "nested_timestamp",
        },
    )

    # Define the parameters for the method
    join_key_columns = ["key1", "key2"]
    feature_name_columns = ["feature1", "feature2"]
    timestamp_field = "event_header.event_published_datetime_utc"
    created_timestamp_column = "created_timestamp"
    start_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2021, 1, 2, tzinfo=timezone.utc)

    # Call the method
    retrieval_job = PostgreSQLOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    expected_query = """SELECT
                b."key1", b."key2", b."feature1", b."feature2", b."event_header.event_published_datetime_utc", b."created_timestamp"
                
            FROM (
                SELECT a."key1", a."key2", a."feature1", a."feature2", a."event_header.event_published_datetime_utc", a."created_timestamp",
                ROW_NUMBER() OVER(PARTITION BY a."key1", a."key2" ORDER BY a."event_header.event_published_datetime_utc" DESC, a."created_timestamp" DESC) AS _feast_row
                FROM offline_store_database_name.offline_store_table_name a
                WHERE a."event_header.event_published_datetime_utc" BETWEEN '2021-01-01 00:00:00+00:00'::timestamptz AND '2021-01-02 00:00:00+00:00'::timestamptz
            ) b
            WHERE _feast_row = 1"""  # noqa: W293

    logger.debug("Expected query:\n%s", expected_query)

    assert isinstance(retrieval_job, RetrievalJob)
    assert actual_query == expected_query


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
def test_pull_latest_from_table_without_nested_timestamp_or_query(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_offline_store_config(),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    # Define the parameters for the method
    join_key_columns = ["key1", "key2"]
    feature_name_columns = ["feature1", "feature2"]
    timestamp_field = "event_published_datetime_utc"
    created_timestamp_column = "created_timestamp"
    start_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2021, 1, 2, tzinfo=timezone.utc)

    # Call the method
    retrieval_job = PostgreSQLOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    expected_query = """SELECT
                b."key1", b."key2", b."feature1", b."feature2", b."event_published_datetime_utc", b."created_timestamp"
                
            FROM (
                SELECT a."key1", a."key2", a."feature1", a."feature2", a."event_published_datetime_utc", a."created_timestamp",
                ROW_NUMBER() OVER(PARTITION BY a."key1", a."key2" ORDER BY a."event_published_datetime_utc" DESC, a."created_timestamp" DESC) AS _feast_row
                FROM offline_store_database_name.offline_store_table_name a
                WHERE a."event_published_datetime_utc" BETWEEN '2021-01-01 00:00:00+00:00'::timestamptz AND '2021-01-02 00:00:00+00:00'::timestamptz
            ) b
            WHERE _feast_row = 1"""  # noqa: W293

    logger.debug("Expected query:\n%s", expected_query)

    assert isinstance(retrieval_job, RetrievalJob)
    assert actual_query == expected_query


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
def test_pull_all_from_table_or_query(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_offline_store_config(),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    # Define the parameters for the method
    join_key_columns = ["key1", "key2"]
    feature_name_columns = ["feature1", "feature2"]
    timestamp_field = "event_published_datetime_utc"
    start_date = datetime(2021, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2021, 1, 2, tzinfo=timezone.utc)

    # Call the method
    retrieval_job = PostgreSQLOfflineStore.pull_all_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        start_date=start_date,
        end_date=end_date,
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    expected_query = """SELECT key1, key2, feature1, feature2, event_published_datetime_utc
            FROM offline_store_database_name.offline_store_table_name AS paftoq_alias
            WHERE "event_published_datetime_utc" BETWEEN '2021-01-01 00:00:00+00:00'::timestamptz AND '2021-01-02 00:00:00+00:00'::timestamptz"""  # noqa: W293

    logger.debug("Expected query:\n%s", expected_query)

    assert isinstance(retrieval_job, RetrievalJob)
    assert actual_query == expected_query


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.df_to_postgres_table"
)
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.get_query_schema"
)
def test_get_historical_features_entity_select_modes(
    mock_get_query_schema, mock_df_to_postgres_table, mock_get_conn
):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    # Mock the query schema to return a simple schema
    mock_get_query_schema.return_value = {
        "event_timestamp": pd.Timestamp,
        "driver_id": pd.Int64Dtype(),
    }

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_offline_store_config(),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    test_feature_view = FeatureView(
        name="test_feature_view",
        entities=_mock_entity(),
        schema=[
            Field(name="feature1", dtype=Float32),
        ],
        source=test_data_source,
    )

    mock_registry = MagicMock()
    mock_registry.get_feature_view.return_value = test_feature_view

    # Create a DataFrame with the required event_timestamp column
    entity_df = pd.DataFrame(
        {"event_timestamp": [datetime(2021, 1, 1)], "driver_id": [1]}
    )

    retrieval_job = PostgreSQLOfflineStore.get_historical_features(
        config=test_repo_config,
        feature_views=[test_feature_view],
        feature_refs=["test_feature_view:feature1"],
        entity_df=entity_df,
        registry=mock_registry,
        project="test_project",
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    # Check that the query starts with WITH and contains the expected comment block
    assert actual_query.startswith("""WITH

/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/""")

    sqlglot.parse(actual_query)
    assert True


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.df_to_postgres_table"
)
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.get_query_schema"
)
def test_get_historical_features_entity_select_modes_embed_query(
    mock_get_query_schema, mock_df_to_postgres_table, mock_get_conn
):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    # Mock the query schema to return a simple schema
    mock_get_query_schema.return_value = {
        "event_timestamp": pd.Timestamp,
        "driver_id": pd.Int64Dtype(),
    }

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=PostgreSQLOfflineStoreConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="test_db",
            db_schema="public",
            user="test_user",
            password="test_password",
            entity_select_mode="embed_query",
        ),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    test_feature_view = FeatureView(
        name="test_feature_view",
        entities=_mock_entity(),
        schema=[
            Field(name="feature1", dtype=Float32),
        ],
        source=test_data_source,
    )

    mock_registry = MagicMock()
    mock_registry.get_feature_view.return_value = test_feature_view

    # Use a SQL query string instead of DataFrame for embed_query mode
    entity_df = """
    SELECT
        event_timestamp,
        driver_id
    FROM (
        VALUES
            ('2021-01-01'::timestamp, 1)
    ) AS t(event_timestamp, driver_id)
    """

    retrieval_job = PostgreSQLOfflineStore.get_historical_features(
        config=test_repo_config,
        feature_views=[test_feature_view],
        feature_refs=["test_feature_view:feature1"],
        entity_df=entity_df,
        registry=mock_registry,
        project="test_project",
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    # Check that the query starts with WITH and contains the expected comment block
    assert actual_query.startswith("""WITH

    entity_query AS (""")

    # Verify the SQL is valid by parsing it
    sqlglot.parse(actual_query)  # This will raise ParseError if SQL is invalid
    assert True  # If we get here, the SQL is valid


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.df_to_postgres_table"
)
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.get_query_schema"
)
def test_get_historical_features_entity_select_modes_embed_query(
    mock_get_query_schema, mock_df_to_postgres_table, mock_get_conn
):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    # Mock the query schema to return a simple schema
    mock_get_query_schema.return_value = {
        "event_timestamp": pd.Timestamp,
        "driver_id": pd.Int64Dtype(),
    }

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=PostgreSQLOfflineStoreConfig(
            type="postgres",
            host="localhost",
            port=5432,
            database="test_db",
            db_schema="public",
            user="test_user",
            password="test_password",
            entity_select_mode="embed_query",
        ),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    test_feature_view = FeatureView(
        name="test_feature_view",
        entities=_mock_entity(),
        schema=[
            Field(name="feature1", dtype=Float32),
        ],
        source=test_data_source,
    )

    mock_registry = MagicMock()
    mock_registry.get_feature_view.return_value = test_feature_view

    # Use a DataFrame even though embed_query mode is used
    entity_df = pd.DataFrame(
        {"event_timestamp": [datetime(2021, 1, 1)], "driver_id": [1]}
    )

    retrieval_job = PostgreSQLOfflineStore.get_historical_features(
        config=test_repo_config,
        feature_views=[test_feature_view],
        feature_refs=["test_feature_view:feature1"],
        entity_df=entity_df,
        registry=mock_registry,
        project="test_project",
    )

    actual_query = retrieval_job.to_sql().strip()
    logger.debug("Actual query:\n%s", actual_query)

    # Check that the query starts with WITH and contains the expected comment block
    assert actual_query.startswith("""WITH

/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/""")

    sqlglot.parse(actual_query)
    assert True


@patch("feast.infra.offline_stores.contrib.postgres_offline_store.postgres._get_conn")
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.df_to_postgres_table"
)
@patch(
    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.get_query_schema"
)
@patch("feast.on_demand_feature_view.OnDemandFeatureView.get_requested_odfvs")
def test_get_historical_features_no_feature_view(
    mock_get_requested_odfvs,
    mock_get_query_schema,
    mock_df_to_postgres_table,
    mock_get_conn,
):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn

    # Create a mock OnDemandFeatureView
    mock_odfv = MagicMock(spec=OnDemandFeatureView)
    mock_odfv.name = "test_odfv"
    mock_odfv.features = [Field(name="feature1", dtype=Float32)]
    mock_odfv.projection = FeatureViewProjection(
        name="test_odfv",
        name_alias="test_odfv",
        features=[Field(name="feature1", dtype=Float32)],
        desired_features=[],
    )
    mock_get_requested_odfvs.return_value = [mock_odfv]

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=_mock_offline_store_config(),
    )

    test_data_source = PostgreSQLSource(
        name="test_batch_source",
        description="test_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
    )

    test_feature_view = FeatureView(
        name="test_feature_view",
        entities=_mock_entity(),
        schema=[
            Field(name="feature1", dtype=Float32),
        ],
        source=test_data_source,
    )

    mock_registry = MagicMock()
    mock_registry.get_on_demand_feature_view.return_value = test_feature_view
    mock_registry.list_on_demand_feature_views.return_value = [mock_odfv]

    entity_df = pd.DataFrame(
        {"event_timestamp": [datetime(2021, 1, 1)], "driver_id": [1]}
    )

    retrieval_job = PostgreSQLOfflineStore.get_historical_features(
        config=test_repo_config,
        feature_views=[],
        feature_refs=["test_odfv:feature1"],
        entity_df=entity_df,
        registry=mock_registry,
        project="test_project",
    )

    sqlglot.parse(retrieval_job.to_sql().strip(), dialect="postgres")
    assert True


def _mock_offline_store_config():
    return PostgreSQLOfflineStoreConfig(
        type="postgres",
        host="localhost",
        port=5432,
        database="test_db",
        db_schema="public",
        user="test_user",
        password="test_password",
    )


def _mock_entity():
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            description="Driver ID",
            value_type=ValueType.INT64,
        )
    ]
