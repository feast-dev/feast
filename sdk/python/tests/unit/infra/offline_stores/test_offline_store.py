from typing import List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow
import pytest

from feast.infra.offline_stores.contrib.athena_offline_store.athena import (
    AthenaOfflineStoreConfig,
    AthenaRetrievalJob,
)
from feast.infra.offline_stores.contrib.mssql_offline_store.mssql import (
    MsSqlServerOfflineStoreConfig,
    MsSqlServerRetrievalJob,
)
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStoreConfig,
    PostgreSQLRetrievalJob,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStoreConfig,
    SparkRetrievalJob,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    TrinoRetrievalJob,
)
from feast.infra.offline_stores.file import FileRetrievalJob
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.infra.offline_stores.redshift import (
    RedshiftOfflineStoreConfig,
    RedshiftRetrievalJob,
)
from feast.infra.offline_stores.snowflake import (
    SnowflakeOfflineStoreConfig,
    SnowflakeRetrievalJob,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.saved_dataset import SavedDatasetStorage


class MockRetrievalJob(RetrievalJob):
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Synchronously executes the underlying query and returns the result as a pandas dataframe.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_df` should be used.
        """
        pass

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Synchronously executes the underlying query and returns the result as an arrow table.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_arrow` should be used.
        """
        pass

    @property
    def full_feature_names(self) -> bool:
        """Returns True if full feature names should be applied to the results of the query."""
        pass

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        """Returns a list containing all the on demand feature views to be handled."""
        pass

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        """
        Synchronously executes the underlying query and persists the result in the same offline store
        at the specified destination.

        Args:
            storage: The saved dataset storage object specifying where the result should be persisted.
            allow_overwrite: If True, a pre-existing location (e.g. table or file) can be overwritten.
                Currently not all individual offline store implementations make use of this parameter.
        """
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Returns metadata about the retrieval job."""
        pass


# Since RetreivalJob are not really tested for subclasses we add some tests here.
@pytest.fixture(
    params=[
        MockRetrievalJob,
        FileRetrievalJob,
        RedshiftRetrievalJob,
        SnowflakeRetrievalJob,
        AthenaRetrievalJob,
        MsSqlServerRetrievalJob,
        PostgreSQLRetrievalJob,
        SparkRetrievalJob,
        TrinoRetrievalJob,
    ]
)
def retrieval_job(request, environment):
    if request.param is FileRetrievalJob:
        return FileRetrievalJob(lambda: 1, full_feature_names=False)
    elif request.param is RedshiftRetrievalJob:
        offline_store_config = RedshiftOfflineStoreConfig(
            cluster_id="feast-integration-tests",
            region="us-west-2",
            user="admin",
            database="feast",
            s3_staging_location="s3://feast-integration-tests/redshift/tests/ingestion",
            iam_role="arn:aws:iam::402087665549:role/redshift_s3_access_role",
        )
        environment.test_repo_config.offline_store = offline_store_config
        return RedshiftRetrievalJob(
            query="query",
            redshift_client="",
            s3_resource="",
            config=environment.test_repo_config,
            full_feature_names=False,
        )
    elif request.param is SnowflakeRetrievalJob:
        offline_store_config = SnowflakeOfflineStoreConfig(
            type="snowflake.offline",
            account="snow",
            user="snow",
            password="snow",
            role="snow",
            warehouse="snow",
            database="FEAST",
            schema="OFFLINE",
            storage_integration_name="FEAST_S3",
            blob_export_location="s3://feast-snowflake-offload/export",
        )
        environment.test_repo_config.offline_store = offline_store_config
        environment.test_repo_config.project = "project"
        return SnowflakeRetrievalJob(
            query="query",
            snowflake_conn=MagicMock(),
            config=environment.test_repo_config,
            full_feature_names=False,
        )
    elif request.param is AthenaRetrievalJob:
        offline_store_config = AthenaOfflineStoreConfig(
            data_source="athena",
            region="athena",
            database="athena",
            workgroup="athena",
            s3_staging_location="athena",
        )

        environment.test_repo_config.offline_store = offline_store_config
        return AthenaRetrievalJob(
            query="query",
            athena_client="client",
            s3_resource="",
            config=environment.test_repo_config.offline_store,
            full_feature_names=False,
        )
    elif request.param is MsSqlServerRetrievalJob:

        return MsSqlServerRetrievalJob(
            query="query",
            engine=MagicMock(),
            config=MsSqlServerOfflineStoreConfig(
                connection_string="str"
            ),  # TODO: this does not match the RetrievalJob pattern. Suppose to be RepoConfig
            full_feature_names=False,
        )
    elif request.param is PostgreSQLRetrievalJob:
        offline_store_config = PostgreSQLOfflineStoreConfig(
            host="str",
            database="str",
            user="str",
            password="str",
        )
        environment.test_repo_config.offline_store = offline_store_config
        return PostgreSQLRetrievalJob(
            query="query",
            config=environment.test_repo_config.offline_store,
            full_feature_names=False,
        )
    elif request.param is SparkRetrievalJob:
        offline_store_config = SparkOfflineStoreConfig()
        environment.test_repo_config.offline_store = offline_store_config
        return SparkRetrievalJob(
            spark_session=MagicMock(),
            query="str",
            full_feature_names=False,
            config=environment.test_repo_config,
        )
    elif request.param is TrinoRetrievalJob:
        offline_store_config = SparkOfflineStoreConfig()
        environment.test_repo_config.offline_store = offline_store_config
        return TrinoRetrievalJob(
            query="str",
            client=MagicMock(),
            config=environment.test_repo_config,
            full_feature_names=False,
        )
    else:
        return request.param()


def test_to_sql():
    assert MockRetrievalJob().to_sql() is None


@pytest.mark.parametrize("timeout", (None, 30))
def test_to_df_timeout(retrieval_job, timeout: Optional[int]):
    with patch.object(retrieval_job, "_to_df_internal") as mock_to_df_internal:
        retrieval_job.to_df(timeout=timeout)
        mock_to_df_internal.assert_called_once_with(timeout=timeout)


@pytest.mark.parametrize("timeout", (None, 30))
def test_to_arrow_timeout(retrieval_job, timeout: Optional[int]):
    with patch.object(retrieval_job, "_to_arrow_internal") as mock_to_arrow_internal:
        retrieval_job.to_arrow(timeout=timeout)
        mock_to_arrow_internal.assert_called_once_with(timeout=timeout)
