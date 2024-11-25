from unittest.mock import Mock, patch

import pandas as pd
import pyarrow
import pytest

from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStoreConfig,
    BigQueryRetrievalJob,
)
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


class TestBigQueryRetrievalJobWithBillingProject:
    query = "SELECT * FROM bigquery"
    client = Mock()

    def test_project_id_exists(self):
        with pytest.raises(ValueError):
            retrieval_job = BigQueryRetrievalJob(
                query=self.query,
                client=self.client,
                config=RepoConfig(
                    registry="gs://ml-test/repo/registry.db",
                    project="test",
                    provider="gcp",
                    online_store=SqliteOnlineStoreConfig(type="sqlite"),
                    offline_store=BigQueryOfflineStoreConfig(
                        type="bigquery",
                        dataset="feast",
                        billing_project_id="test-billing-project",
                    ),
                ),
                full_feature_names=True,
                on_demand_feature_views=[],
            )
            retrieval_job.to_df()
