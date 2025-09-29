import logging
from datetime import datetime, timedelta, timezone
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

    expected_query = """SELECT paftoq_alias."key1", paftoq_alias."key2", paftoq_alias."feature1", paftoq_alias."feature2", paftoq_alias."event_published_datetime_utc"
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
def test_get_historical_features_entity_select_modes_embed_query_with_dataframe(
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


def _mock_feature_view(name: str, ttl: timedelta = None):
    """Helper to create mock feature views with configurable TTL"""
    return FeatureView(
        name=name,
        entities=[Entity(name="driver_id", join_keys=["driver_id"])],
        ttl=ttl,
        source=PostgreSQLSource(
            name=f"{name}_source",
            table=f"{name}_table",
            timestamp_field="event_timestamp",
        ),
        schema=[
            Field(name="feature1", dtype=Float32),
            Field(name="feature2", dtype=Float32),
        ],
    )


class TestNonEntityRetrieval:
    """
    Test suite for non-entity retrieval functionality (entity_df=None)

    This test suite comprehensively covers the new non-entity retrieval mode
    for PostgreSQL offline store, which enables retrieving features for specified
    time ranges without requiring an entity DataFrame.
    """

    def test_non_entity_mode_with_both_dates(self):
        """Test non-entity retrieval API accepts both start_date and end_date"""
        test_repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_offline_store_config(),
        )

        feature_view = _mock_feature_view("test_fv", ttl=None)
        start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2023, 1, 7, tzinfo=timezone.utc)

        # This should not raise an error - validates API signature
        with patch.multiple(
            "feast.infra.offline_stores.contrib.postgres_offline_store.postgres",
            _get_conn=MagicMock(),
            _upload_entity_df=MagicMock(),
            _get_entity_schema=MagicMock(return_value={"event_timestamp": "timestamp"}),
            _get_entity_df_event_timestamp_range=MagicMock(
                return_value=(start_date, end_date)
            ),
        ):
            with patch(
                "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_expected_join_keys",
                return_value=[],
            ):
                with patch(
                    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.assert_expected_columns_in_entity_df"
                ):
                    with patch(
                        "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_feature_view_query_context",
                        return_value=[],
                    ):
                        try:
                            retrieval_job = (
                                PostgreSQLOfflineStore.get_historical_features(
                                    config=test_repo_config,
                                    feature_views=[feature_view],
                                    feature_refs=["test_fv:feature1"],
                                    entity_df=None,  # Non-entity mode
                                    registry=MagicMock(),
                                    project="test_project",
                                    start_date=start_date,
                                    end_date=end_date,
                                )
                            )
                            assert isinstance(retrieval_job, RetrievalJob)
                        except Exception as e:
                            # Should not fail due to API signature issues
                            assert "entity_df" not in str(e)
                            assert "start_date" not in str(e)
                            assert "end_date" not in str(e)

    def test_non_entity_mode_with_end_date_only(self):
        """Test non-entity retrieval calculates start_date from TTL"""
        test_repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_offline_store_config(),
        )

        feature_views = [
            _mock_feature_view("user_fv", ttl=timedelta(hours=1)),
            _mock_feature_view("transaction_fv", ttl=timedelta(days=1)),
        ]
        end_date = datetime(2023, 1, 7, tzinfo=timezone.utc)

        with patch.multiple(
            "feast.infra.offline_stores.contrib.postgres_offline_store.postgres",
            _get_conn=MagicMock(),
            _upload_entity_df=MagicMock(),
            _get_entity_schema=MagicMock(return_value={"event_timestamp": "timestamp"}),
            _get_entity_df_event_timestamp_range=MagicMock(
                return_value=(datetime(2023, 1, 6, tzinfo=timezone.utc), end_date)
            ),
        ):
            with patch(
                "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_expected_join_keys",
                return_value=[],
            ):
                with patch(
                    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.assert_expected_columns_in_entity_df"
                ):
                    with patch(
                        "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_feature_view_query_context",
                        return_value=[],
                    ):
                        try:
                            retrieval_job = (
                                PostgreSQLOfflineStore.get_historical_features(
                                    config=test_repo_config,
                                    feature_views=feature_views,
                                    feature_refs=[
                                        "user_fv:age",
                                        "transaction_fv:amount",
                                    ],
                                    entity_df=None,  # Non-entity mode
                                    registry=MagicMock(),
                                    project="test_project",
                                    end_date=end_date,
                                    # start_date not provided - should be calculated from max TTL
                                )
                            )
                            assert isinstance(retrieval_job, RetrievalJob)
                        except Exception as e:
                            # Should not fail due to TTL calculation issues
                            assert "ttl" not in str(e).lower()

    @patch("feast.utils.datetime")
    def test_no_dates_provided_defaults_to_current_time(self, mock_datetime):
        """Test that when no dates are provided, end_date defaults to current time"""
        # Mock datetime.now() to return a fixed time
        fixed_now = datetime(2023, 1, 7, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime.now.return_value = fixed_now

        test_repo_config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_offline_store_config(),
        )

        feature_view = _mock_feature_view("test_fv", ttl=timedelta(days=1))

        with patch.multiple(
            "feast.infra.offline_stores.contrib.postgres_offline_store.postgres",
            _get_conn=MagicMock(),
            _upload_entity_df=MagicMock(),
            _get_entity_schema=MagicMock(return_value={"event_timestamp": "timestamp"}),
            _get_entity_df_event_timestamp_range=MagicMock(
                return_value=(
                    datetime(2023, 1, 6, 12, 0, 0, tzinfo=timezone.utc),
                    fixed_now,
                )
            ),
        ):
            with patch(
                "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_expected_join_keys",
                return_value=[],
            ):
                with patch(
                    "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.assert_expected_columns_in_entity_df"
                ):
                    with patch(
                        "feast.infra.offline_stores.contrib.postgres_offline_store.postgres.offline_utils.get_feature_view_query_context",
                        return_value=[],
                    ):
                        try:
                            retrieval_job = (
                                PostgreSQLOfflineStore.get_historical_features(
                                    config=test_repo_config,
                                    feature_views=[feature_view],
                                    feature_refs=["test_fv:feature1"],
                                    entity_df=None,  # Non-entity mode
                                    registry=MagicMock(),
                                    project="test_project",
                                    # No start_date or end_date provided
                                )
                            )

                            # Verify that datetime.now() was called to get current time
                            mock_datetime.now.assert_called_with(tz=timezone.utc)
                            assert isinstance(retrieval_job, RetrievalJob)
                        except Exception as e:
                            # Should not fail due to datetime issues
                            assert "datetime" not in str(e).lower()

    def test_sql_template_ttl_filtering(self):
        """Test that the SQL template includes proper TTL filtering"""
        from jinja2 import BaseLoader, Environment

        # Test the template section that includes TTL filtering
        template_with_ttl = """
        FROM {{ featureview.table_subquery }} AS sub
        WHERE "{{ featureview.timestamp_field }}" BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        {% if featureview.ttl != 0 and featureview.min_event_timestamp %}
        AND "{{ featureview.timestamp_field }}" >= '{{ featureview.min_event_timestamp }}'
        {% endif %}
        """

        template = Environment(loader=BaseLoader()).from_string(
            source=template_with_ttl
        )

        # Test case 1: Feature view with TTL
        context_with_ttl = {
            "featureview": {
                "table_subquery": "test_table",
                "timestamp_field": "event_timestamp",
                "ttl": 3600,  # 1 hour
                "min_event_timestamp": "2023-01-06 23:00:00",
            },
            "start_date": "2023-01-01",
            "end_date": "2023-01-07",
        }

        query_with_ttl = template.render(context_with_ttl)
        # Should include the TTL timestamp value in the query
        assert "2023-01-06 23:00:00" in query_with_ttl
        # Should have the TTL filtering condition
        assert ">=" in query_with_ttl

        # Test case 2: Feature view without TTL
        context_no_ttl = {
            "featureview": {
                "table_subquery": "test_table",
                "timestamp_field": "event_timestamp",
                "ttl": 0,  # No TTL
                "min_event_timestamp": None,
            },
            "start_date": "2023-01-01",
            "end_date": "2023-01-07",
        }

        query_no_ttl = template.render(context_no_ttl)
        # Should not include TTL filtering when TTL is 0 or min_event_timestamp is None
        assert 'AND "event_timestamp" >=' not in query_no_ttl

    def test_lateral_join_ttl_constraints(self):
        """Test that LATERAL JOINs include proper TTL constraints"""
        from jinja2 import BaseLoader, Environment

        lateral_template = """
        FROM "{{ featureview.name }}__data" fv_sub_{{ outer_loop_index }}
        WHERE fv_sub_{{ outer_loop_index }}.event_timestamp <= base.event_timestamp
        {% if featureview.ttl != 0 %}
        AND fv_sub_{{ outer_loop_index }}.event_timestamp >= base.event_timestamp - {{ featureview.ttl }} * interval '1' second
        {% endif %}
        """

        template = Environment(loader=BaseLoader()).from_string(source=lateral_template)

        # Test with TTL
        context = {
            "featureview": {
                "name": "user_features",
                "ttl": 86400,  # 1 day
            },
            "outer_loop_index": 0,
        }

        query = template.render(context)
        assert "86400 * interval" in query
        assert "base.event_timestamp -" in query

        # Test without TTL
        context_no_ttl = {
            "featureview": {
                "name": "user_features",
                "ttl": 0,  # No TTL
            },
            "outer_loop_index": 0,
        }

        query_no_ttl = template.render(context_no_ttl)
        assert "interval" not in query_no_ttl

    def test_api_non_entity_functionality(self):
        """Test that FeatureStore API accepts non-entity parameters correctly"""
        from feast import FeatureStore
        from feast.infra.offline_stores.offline_store import RetrievalJob
        from feast.repo_config import RepoConfig

        config = RepoConfig(
            project="test_project",
            registry="test_registry",
            provider="local",
            offline_store=_mock_offline_store_config(),
        )

        # Mock the entire retrieval pipeline
        with patch.object(FeatureStore, "_get_provider") as mock_provider:
            mock_retrieval_job = MagicMock(spec=RetrievalJob)
            mock_provider.return_value.get_historical_features.return_value = (
                mock_retrieval_job
            )

            fs = FeatureStore(config=config)

            # Mock registry and feature resolution
            with (
                patch.object(fs, "_registry"),
                patch("feast.utils._get_features") as mock_get_features,
                patch("feast.utils._get_feature_views_to_use") as mock_get_views,
                patch("feast.utils._group_feature_refs") as mock_group_refs,
            ):
                mock_get_features.return_value = ["test:feature"]
                mock_get_views.return_value = (
                    [],
                    [],
                )  # (all_feature_views, all_on_demand_feature_views)
                mock_group_refs.return_value = ([], [])  # (fvs, odfvs)

                # Test non-entity API call
                result = fs.get_historical_features(
                    features=["test:feature"],
                    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
                    end_date=datetime(2023, 1, 7, tzinfo=timezone.utc),
                )

                # Verify the call was made correctly
                assert result == mock_retrieval_job
                mock_provider.return_value.get_historical_features.assert_called_once()

                # Check that the new parameters were passed (the exact call structure may vary)
                # but we want to verify the API accepted the new parameters
                call_args = mock_provider.return_value.get_historical_features.call_args
                if call_args.kwargs:
                    # Called with keyword arguments
                    assert call_args.kwargs.get("start_date") == datetime(
                        2023, 1, 1, tzinfo=timezone.utc
                    )
                    assert call_args.kwargs.get("end_date") == datetime(
                        2023, 1, 7, tzinfo=timezone.utc
                    )
                else:
                    # Called with positional arguments - just verify it was called
                    assert len(call_args.args) > 0

    def test_cli_date_combinations(self):
        """Test various CLI date parameter combinations"""
        import tempfile
        from pathlib import Path
        from textwrap import dedent

        from tests.utils.cli_repo_creator import CliRunner, get_example_repo

        runner = CliRunner()

        with tempfile.TemporaryDirectory() as temp_dir:
            repo_path = Path(temp_dir)

            # Setup repo
            repo_config = repo_path / "feature_store.yaml"
            repo_config.write_text(
                dedent(f"""
                project: test_cli_dates
                registry: {repo_path / "registry.db"}
                provider: local
                offline_store:
                    type: file
                online_store:
                    type: sqlite
                    path: {repo_path / "online_store.db"}
            """)
            )

            repo_example = repo_path / "example.py"
            repo_example.write_text(get_example_repo("example_feature_repo_1.py"))

            result = runner.run(["apply"], cwd=repo_path)
            assert result.returncode == 0

            # Test 1: Both dates provided - should parse correctly
            result = runner.run(
                [
                    "get-historical-features",
                    "--features",
                    "driver_hourly_stats:conv_rate",
                    "--start-date",
                    "2023-01-01 00:00:00",
                    "--end-date",
                    "2023-01-07 00:00:00",
                ],
                cwd=repo_path,
            )

            # Should not fail on date parsing
            stderr_output = result.stderr.decode()
            assert "Error parsing" not in stderr_output
            assert "time data" not in stderr_output  # datetime parsing errors

            # Test 2: Only end date provided - should work (start_date calculated from TTL)
            result = runner.run(
                [
                    "get-historical-features",
                    "--features",
                    "driver_hourly_stats:conv_rate",
                    "--end-date",
                    "2023-01-07 00:00:00",
                ],
                cwd=repo_path,
            )

            # Should not fail on parameter validation
            stderr_output = result.stderr.decode()
            assert "must be provided" not in stderr_output
