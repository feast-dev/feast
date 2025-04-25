from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStore,
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.repo_config import RepoConfig
from feast.types import Float32, ValueType


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_pull_latest_from_table_with_nested_timestamp_or_query(mock_get_spark_session):
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )

    test_data_source = SparkSource(
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
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)

    # Call the method
    retrieval_job = SparkOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    expected_query = """SELECT
                    key1, key2, feature1, feature2, nested_timestamp, created_timestamp
                    
                FROM (
                    SELECT key1, key2, feature1, feature2, event_header.event_published_datetime_utc AS nested_timestamp, created_timestamp,
                    ROW_NUMBER() OVER(PARTITION BY key1, key2 ORDER BY event_header.event_published_datetime_utc DESC, created_timestamp DESC) AS feast_row_
                    FROM `offline_store_database_name`.`offline_store_table_name` t1
                    WHERE event_header.event_published_datetime_utc BETWEEN TIMESTAMP('2021-01-01 00:00:00.000000') AND TIMESTAMP('2021-01-02 00:00:00.000000')
                ) t2
                WHERE feast_row_ = 1"""  # noqa: W293

    assert isinstance(retrieval_job, RetrievalJob)
    assert retrieval_job.query.strip() == expected_query.strip()


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_pull_latest_from_table_with_nested_timestamp_or_query_and_date_partition_column_set(
    mock_get_spark_session,
):
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )

    test_data_source = SparkSource(
        name="test_nested_batch_source",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="nested_timestamp",
        field_mapping={
            "event_header.event_published_datetime_utc": "nested_timestamp",
        },
        date_partition_column="effective_date",
    )

    # Define the parameters for the method
    join_key_columns = ["key1", "key2"]
    feature_name_columns = ["feature1", "feature2"]
    timestamp_field = "event_header.event_published_datetime_utc"
    created_timestamp_column = "created_timestamp"
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)

    # Call the method
    retrieval_job = SparkOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    expected_query = """SELECT
                    key1, key2, feature1, feature2, nested_timestamp, created_timestamp
                    
                FROM (
                    SELECT key1, key2, feature1, feature2, event_header.event_published_datetime_utc AS nested_timestamp, created_timestamp,
                    ROW_NUMBER() OVER(PARTITION BY key1, key2 ORDER BY event_header.event_published_datetime_utc DESC, created_timestamp DESC) AS feast_row_
                    FROM `offline_store_database_name`.`offline_store_table_name` t1
                    WHERE event_header.event_published_datetime_utc BETWEEN TIMESTAMP('2021-01-01 00:00:00.000000') AND TIMESTAMP('2021-01-02 00:00:00.000000') AND effective_date >= '2021-01-01' AND effective_date <= '2021-01-02' 
                ) t2
                WHERE feast_row_ = 1"""  # noqa: W293, W291

    assert isinstance(retrieval_job, RetrievalJob)
    assert retrieval_job.query.strip() == expected_query.strip()


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_pull_latest_from_table_without_nested_timestamp_or_query(
    mock_get_spark_session,
):
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )

    test_data_source = SparkSource(
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
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)

    # Call the method
    retrieval_job = SparkOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    expected_query = """SELECT
                    key1, key2, feature1, feature2, event_published_datetime_utc, created_timestamp
                    
                FROM (
                    SELECT key1, key2, feature1, feature2, event_published_datetime_utc, created_timestamp,
                    ROW_NUMBER() OVER(PARTITION BY key1, key2 ORDER BY event_published_datetime_utc DESC, created_timestamp DESC) AS feast_row_
                    FROM `offline_store_database_name`.`offline_store_table_name` t1
                    WHERE event_published_datetime_utc BETWEEN TIMESTAMP('2021-01-01 00:00:00.000000') AND TIMESTAMP('2021-01-02 00:00:00.000000')
                ) t2
                WHERE feast_row_ = 1"""  # noqa: W293

    assert isinstance(retrieval_job, RetrievalJob)
    assert retrieval_job.query.strip() == expected_query.strip()


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_pull_latest_from_table_without_nested_timestamp_or_query_and_date_partition_column_set(
    mock_get_spark_session,
):
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )

    test_data_source = SparkSource(
        name="test_batch_source",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name",
        timestamp_field="event_published_datetime_utc",
        date_partition_column="effective_date",
    )

    # Define the parameters for the method
    join_key_columns = ["key1", "key2"]
    feature_name_columns = ["feature1", "feature2"]
    timestamp_field = "event_published_datetime_utc"
    created_timestamp_column = "created_timestamp"
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2021, 1, 2)

    # Call the method
    retrieval_job = SparkOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=join_key_columns,
        feature_name_columns=feature_name_columns,
        timestamp_field=timestamp_field,
        created_timestamp_column=created_timestamp_column,
        start_date=start_date,
        end_date=end_date,
    )

    expected_query = """SELECT
                    key1, key2, feature1, feature2, event_published_datetime_utc, created_timestamp
                    
                FROM (
                    SELECT key1, key2, feature1, feature2, event_published_datetime_utc, created_timestamp,
                    ROW_NUMBER() OVER(PARTITION BY key1, key2 ORDER BY event_published_datetime_utc DESC, created_timestamp DESC) AS feast_row_
                    FROM `offline_store_database_name`.`offline_store_table_name` t1
                    WHERE event_published_datetime_utc BETWEEN TIMESTAMP('2021-01-01 00:00:00.000000') AND TIMESTAMP('2021-01-02 00:00:00.000000') AND effective_date >= '2021-01-01' AND effective_date <= '2021-01-02' 
                ) t2
                WHERE feast_row_ = 1"""  # noqa: W293, W291

    assert isinstance(retrieval_job, RetrievalJob)
    assert retrieval_job.query.strip() == expected_query.strip()


@patch(
    "feast.infra.offline_stores.contrib.spark_offline_store.spark.get_spark_session_or_start_new_with_repoconfig"
)
def test_get_historical_features(mock_get_spark_session):
    mock_spark_session = MagicMock()
    mock_get_spark_session.return_value = mock_spark_session

    test_repo_config = RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )

    test_data_source1 = SparkSource(
        name="test_nested_batch_source1",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name1",
        timestamp_field="nested_timestamp",
        field_mapping={
            "event_header.event_published_datetime_utc": "nested_timestamp",
        },
        date_partition_column="effective_date",
        date_partition_column_format="%Y%m%d",
    )

    test_data_source2 = SparkSource(
        name="test_nested_batch_source2",
        description="test_nested_batch_source",
        table="offline_store_database_name.offline_store_table_name2",
        timestamp_field="nested_timestamp",
        field_mapping={
            "event_header.event_published_datetime_utc": "nested_timestamp",
        },
        date_partition_column="effective_date",
    )

    test_feature_view1 = FeatureView(
        name="test_feature_view1",
        entities=_mock_entity(),
        schema=[
            Field(name="feature1", dtype=Float32),
        ],
        source=test_data_source1,
    )

    test_feature_view2 = FeatureView(
        name="test_feature_view2",
        entities=_mock_entity(),
        schema=[
            Field(name="feature2", dtype=Float32),
        ],
        source=test_data_source2,
    )

    # Create a DataFrame with the required event_timestamp column
    entity_df = pd.DataFrame(
        {"event_timestamp": [datetime(2021, 1, 1)], "driver_id": [1]}
    )

    mock_registry = MagicMock()
    retrieval_job = SparkOfflineStore.get_historical_features(
        config=test_repo_config,
        feature_views=[test_feature_view2, test_feature_view1],
        feature_refs=["test_feature_view2:feature2", "test_feature_view1:feature1"],
        entity_df=entity_df,
        registry=mock_registry,
        project="test_project",
    )
    query = retrieval_job.query.strip()

    assert "effective_date <= '2021-01-01'" in query
    assert "effective_date <= '20210101'" in query


def _mock_entity():
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            description="Driver ID",
            value_type=ValueType.INT64,
        )
    ]
