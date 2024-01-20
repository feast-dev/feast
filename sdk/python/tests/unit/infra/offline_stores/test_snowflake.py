import re
from unittest.mock import ANY, MagicMock, patch

import pytest

from feast.infra.offline_stores.snowflake import (
    SnowflakeOfflineStoreConfig,
    SnowflakeRetrievalJob,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig


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
        ),
        full_feature_names=True,
        on_demand_feature_views=[],
    )
    return retrieval_job


def test_to_remote_storage(retrieval_job):
    stored_files = ["just a path", "maybe another"]
    with patch.object(
        retrieval_job, "to_snowflake", return_value=None
    ) as mock_to_snowflake, patch.object(
        retrieval_job, "_get_file_names_from_copy_into", return_value=stored_files
    ) as mock_get_file_names_from_copy:
        assert (
            retrieval_job.to_remote_storage() == stored_files
        ), "should return the list of files"
        mock_to_snowflake.assert_called_once()
        mock_get_file_names_from_copy.assert_called_once_with(ANY, ANY)
        native_path = mock_get_file_names_from_copy.call_args[0][1]
        assert re.match("^s3://.*", native_path), "path should be s3://*"
