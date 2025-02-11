import re
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
import pytest
from pytest_mock import MockFixture

from feast import FeatureView, Field, FileSource
from feast.infra.offline_stores.snowflake import (
    SnowflakeOfflineStoreConfig,
    SnowflakeRetrievalJob,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.types import Array, String


@pytest.fixture(params=["s3", "s3gov"])
def retrieval_job(request):
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
        blob_export_location=f"{request.param}://feast-snowflake-offload/export",
    )
    retrieval_job = SnowflakeRetrievalJob(
        query="SELECT * FROM snowflake",
        snowflake_conn=MagicMock(),
        config=RepoConfig(
            registry="s3://ml-test/repo/registry.db",
            project="test",
            provider="snowflake.offline",
            online_store=SqliteOnlineStoreConfig(type="sqlite"),
            offline_store=offline_store_config,
            entity_key_serialization_version=2,
        ),
        full_feature_names=True,
        on_demand_feature_views=[],
    )
    return retrieval_job


def test_to_remote_storage(retrieval_job):
    stored_files = ["just a path", "maybe another"]
    with (
        patch.object(
            retrieval_job, "to_snowflake", return_value=None
        ) as mock_to_snowflake,
        patch.object(
            retrieval_job, "_get_file_names_from_copy_into", return_value=stored_files
        ) as mock_get_file_names_from_copy,
    ):
        assert retrieval_job.to_remote_storage() == stored_files, (
            "should return the list of files"
        )
        mock_to_snowflake.assert_called_once()
        mock_get_file_names_from_copy.assert_called_once_with(ANY, ANY)
        native_path = mock_get_file_names_from_copy.call_args[0][1]
        assert re.match("^s3://.*", native_path), "path should be s3://*"


def test_snowflake_to_df_internal(
    retrieval_job: SnowflakeRetrievalJob, mocker: MockFixture
):
    mock_execute = mocker.patch(
        "feast.infra.offline_stores.snowflake.execute_snowflake_statement"
    )
    mock_execute.return_value.fetch_pandas_all.return_value = pd.DataFrame.from_dict(
        {"feature1": ['["1", "2", "3"]', None, "[]"]}  # For Valid, Null, and Empty
    )

    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="feature1", dtype=Array(String)),
        ],
        source=FileSource(path="dummy.path"),  # Dummy value
    )
    retrieval_job._feature_views = [feature_view]
    retrieval_job._to_df_internal()
