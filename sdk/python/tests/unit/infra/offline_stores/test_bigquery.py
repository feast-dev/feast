from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow
import pytest

from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStore,
    BigQueryOfflineStoreConfig,
    BigQueryRetrievalJob,
)
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig


@pytest.fixture
def pandas_dataframe():
    return pd.DataFrame(
        data={
            "key": [1, 2, 3],
            "value": ["a", None, "c"],
        }
    )


@pytest.fixture
def big_query_result(pandas_dataframe):
    class BigQueryResult:
        def to_dataframe(self, **kwargs):
            return pandas_dataframe

        def to_arrow(self, **kwargs):
            return pyarrow.Table.from_pandas(pandas_dataframe)

        def exception(self, timeout=None):
            return None

    return BigQueryResult()


class TestBigQueryRetrievalJob:
    query = "SELECT * FROM bigquery"
    client = Mock()
    retrieval_job = BigQueryRetrievalJob(
        query=query,
        client=client,
        config=RepoConfig(
            registry="gs://ml-test/repo/registry.db",
            project="test",
            provider="gcp",
            online_store=SqliteOnlineStoreConfig(type="sqlite"),
            offline_store=BigQueryOfflineStoreConfig(type="bigquery", dataset="feast"),
        ),
        full_feature_names=True,
        on_demand_feature_views=[],
    )

    def test_to_sql(self):
        assert self.retrieval_job.to_sql() == self.query

    def test_to_df(self, big_query_result, pandas_dataframe):
        self.client.query.return_value = big_query_result
        actual = self.retrieval_job.to_df()
        pd.testing.assert_frame_equal(actual, pandas_dataframe)

    def test_to_df_timeout(self, big_query_result):
        self.client.query.return_value = big_query_result
        with patch.object(self.retrieval_job, "_execute_query"):
            self.retrieval_job.to_df(timeout=30)
            self.retrieval_job._execute_query.assert_called_once_with(
                query=self.query, timeout=30
            )

    def test_to_arrow(self, big_query_result, pandas_dataframe):
        self.client.query.return_value = big_query_result
        actual = self.retrieval_job.to_arrow()
        pd.testing.assert_frame_equal(actual.to_pandas(), pandas_dataframe)

    def test_to_arrow_timeout(self, big_query_result):
        self.client.query.return_value = big_query_result
        with patch.object(self.retrieval_job, "_execute_query"):
            self.retrieval_job.to_arrow(timeout=30)
            self.retrieval_job._execute_query.assert_called_once_with(
                query=self.query, timeout=30
            )


@patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
def test_pull_latest_from_table_or_query_partition_pruning(mock_get_bigquery_client):
    mock_get_bigquery_client.return_value = Mock()
    test_repo_config = RepoConfig(
        registry="gs://ml-test/repo/registry.db",
        project="test",
        provider="gcp",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=BigQueryOfflineStoreConfig(type="bigquery", dataset="feast"),
    )
    test_data_source = BigQuerySource(
        table="project:dataset.table",
        timestamp_field="event_timestamp",
        date_partition_column="partition_date",
    )
    retrieval_job = BigQueryOfflineStore.pull_latest_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["feature1"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2021, 1, 2, tzinfo=timezone.utc),
    )
    actual_query = retrieval_job.to_sql()
    assert (
        "event_timestamp BETWEEN TIMESTAMP('2021-01-01T00:00:00+00:00') AND TIMESTAMP('2021-01-02T00:00:00+00:00')"
        in actual_query
    )
    assert "partition_date >= '2021-01-01'" in actual_query
    assert "partition_date <= '2021-01-02'" in actual_query


@patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
def test_pull_all_from_table_or_query_partition_pruning(mock_get_bigquery_client):
    mock_get_bigquery_client.return_value = Mock()
    test_repo_config = RepoConfig(
        registry="gs://ml-test/repo/registry.db",
        project="test",
        provider="gcp",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=BigQueryOfflineStoreConfig(type="bigquery", dataset="feast"),
    )
    test_data_source = BigQuerySource(
        table="project:dataset.table",
        timestamp_field="event_timestamp",
        date_partition_column="partition_date",
    )
    retrieval_job = BigQueryOfflineStore.pull_all_from_table_or_query(
        config=test_repo_config,
        data_source=test_data_source,
        join_key_columns=["driver_id"],
        feature_name_columns=["feature1"],
        timestamp_field="event_timestamp",
        start_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2021, 1, 2, tzinfo=timezone.utc),
    )
    actual_query = retrieval_job.to_sql()
    assert (
        "event_timestamp BETWEEN TIMESTAMP('2021-01-01T00:00:00+00:00') AND TIMESTAMP('2021-01-02T00:00:00+00:00')"
        in actual_query
    )
    assert "partition_date >= '2021-01-01'" in actual_query
    assert "partition_date <= '2021-01-02'" in actual_query


class TestBigQuerySourceGetTableQueryString:
    def test_table_only(self):
        source = BigQuerySource(
            name="test",
            table="project.dataset.table",
            timestamp_field="ts",
        )
        assert source.get_table_query_string() == "`project.dataset.table`"

    def test_query_only(self):
        source = BigQuerySource(
            name="test",
            query="SELECT * FROM `project.dataset.table` WHERE active = TRUE",
            timestamp_field="ts",
        )
        assert (
            source.get_table_query_string()
            == "(SELECT * FROM `project.dataset.table` WHERE active = TRUE)"
        )

    def test_both_table_and_query_prefers_query(self):
        """When both table and query are set, query takes priority for reads."""
        query = (
            "SELECT * FROM `project.dataset.table`"
            " QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_id, event_time) = 1"
        )
        source = BigQuerySource(
            name="test",
            table="project.dataset.table",
            query=query,
            timestamp_field="ts",
        )
        result = source.get_table_query_string()
        assert result.startswith("(")
        assert "QUALIFY" in result
        assert result != "`project.dataset.table`"

    def test_table_property_unaffected_by_query_priority(self):
        """The .table property is still accessible for write paths."""
        source = BigQuerySource(
            name="test",
            table="project.dataset.write_target",
            query="SELECT * FROM `project.dataset.write_target` WHERE deduped",
            timestamp_field="ts",
        )
        assert source.table == "project.dataset.write_target"
