from typing import List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow
import pytest

from feast.infra.offline_stores.contrib.athena_offline_store.athena import (
    AthenaOfflineStoreConfig,
    AthenaRetrievalJob,
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
from feast.infra.offline_stores.dask import DaskRetrievalJob
from feast.infra.offline_stores.offline_store import RetrievalJob, RetrievalMetadata
from feast.infra.offline_stores.redshift import (
    RedshiftOfflineStoreConfig,
    RedshiftRetrievalJob,
)
from feast.infra.offline_stores.remote import (
    RemoteOfflineStoreConfig,
    RemoteRetrievalJob,
)
from feast.infra.offline_stores.snowflake import (
    SnowflakeOfflineStoreConfig,
    SnowflakeRetrievalJob,
)
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.saved_dataset import SavedDatasetStorage


class MockRetrievalJob(RetrievalJob):
    def to_sql(self) -> str:
        return ""

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Synchronously executes the underlying query and returns the result as a pandas dataframe.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_df` should be used.
        """
        return pd.DataFrame()

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Synchronously executes the underlying query and returns the result as an arrow table.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_arrow` should be used.
        """
        return pyarrow.Table()

    @property
    def full_feature_names(self) -> bool:  # type: ignore
        """Returns True if full feature names should be applied to the results of the query."""
        return False

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:  # type: ignore
        """Returns a list containing all the on demand feature views to be handled."""
        return []

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
        raise NotImplementedError


# Since RetreivalJob are not really tested for subclasses we add some tests here.
@pytest.fixture(
    params=[
        MockRetrievalJob,
        DaskRetrievalJob,
        RedshiftRetrievalJob,
        SnowflakeRetrievalJob,
        AthenaRetrievalJob,
        PostgreSQLRetrievalJob,
        SparkRetrievalJob,
        TrinoRetrievalJob,
        RemoteRetrievalJob,
    ]
)
def retrieval_job(request, environment):
    if request.param is DaskRetrievalJob:
        return DaskRetrievalJob(lambda: 1, full_feature_names=False)
    elif request.param is RedshiftRetrievalJob:
        offline_store_config = RedshiftOfflineStoreConfig(
            cluster_id="feast-int-bucket",
            region="us-west-2",
            user="admin",
            database="feast",
            s3_staging_location="s3://feast-int-bucket/redshift/tests/ingestion",
            iam_role="arn:aws:iam::585132637328:role/service-role/AmazonRedshift-CommandsAccessRole-20240403T092631",
            workgroup="",
        )
        config = environment.config.model_copy(
            update={"offline_config": offline_store_config}
        )
        return RedshiftRetrievalJob(
            query="query",
            redshift_client="",
            s3_resource="",
            config=config,
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
        config = environment.config.model_copy(
            update={"offline_config": offline_store_config}
        )
        environment.project = "project"
        return SnowflakeRetrievalJob(
            query="query",
            snowflake_conn=MagicMock(),
            config=config,
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

        return AthenaRetrievalJob(
            query="query",
            athena_client="client",
            s3_resource="",
            config=environment.config,
            full_feature_names=False,
        )
    elif request.param is PostgreSQLRetrievalJob:
        offline_store_config = PostgreSQLOfflineStoreConfig(
            host="str",
            database="str",
            user="str",
            password="str",
        )
        return PostgreSQLRetrievalJob(
            query="query",
            config=environment.config,
            full_feature_names=False,
        )
    elif request.param is SparkRetrievalJob:
        offline_store_config = SparkOfflineStoreConfig()
        return SparkRetrievalJob(
            spark_session=MagicMock(),
            query="str",
            full_feature_names=False,
            config=environment.config,
        )
    elif request.param is TrinoRetrievalJob:
        offline_store_config = SparkOfflineStoreConfig()
        return TrinoRetrievalJob(
            query="str",
            client=MagicMock(),
            config=environment.config,
            full_feature_names=False,
        )
    elif request.param is RemoteRetrievalJob:
        offline_store_config = RemoteOfflineStoreConfig(
            type="remote",
            host="localhost",
            port=0,
        )
        environment.config._offline_store = offline_store_config

        entity_df = pd.DataFrame.from_dict(
            {
                "id": [1],
                "event_timestamp": ["datetime"],
                "val_to_add": [1],
            }
        )

        return RemoteRetrievalJob(
            client=MagicMock(),
            api_parameters={
                "str": "str",
            },
            api="api",
            table=pyarrow.Table.from_pandas(entity_df),
            entity_df=entity_df,
            metadata=RetrievalMetadata(
                features=["1", "2", "3", "4"],
                keys=["1", "2", "3", "4"],
            ),
        )
    else:
        return request.param()


def test_to_sql():
    assert MockRetrievalJob().to_sql() == ""


@pytest.mark.parametrize("timeout", (None, 30))
def test_to_df_timeout(retrieval_job, timeout: Optional[int]):
    with patch.object(retrieval_job, "_to_arrow_internal") as mock_to_df_internal:
        retrieval_job.to_df(timeout=timeout)
        mock_to_df_internal.assert_called_once_with(timeout=timeout)


@pytest.mark.parametrize("timeout", (None, 30))
def test_to_arrow_timeout(retrieval_job, timeout: Optional[int]):
    with patch.object(retrieval_job, "_to_arrow_internal") as mock_to_arrow_internal:
        retrieval_job.to_arrow(timeout=timeout)
        mock_to_arrow_internal.assert_called_once_with(timeout=timeout)
