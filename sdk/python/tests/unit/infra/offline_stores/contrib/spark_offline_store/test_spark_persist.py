"""
Unit tests for SparkRetrievalJob.persist() and SavedDatasetSparkStorage.from_data_source().

Covers the fix for https://github.com/feast-dev/feast/issues/6261 where:
1. SavedDatasetStorage.from_data_source() did not support SparkSource
2. SavedDatasetSparkStorage lacked a from_data_source() method
3. SparkRetrievalJob.persist() only supported table-based storage, not path-based
"""

from unittest.mock import MagicMock

import pytest

from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStoreConfig,
    SparkRetrievalJob,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SavedDatasetSparkStorage,
    SparkSource,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.table_format import IcebergFormat

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def repo_config():
    return RepoConfig(
        registry="file:///tmp/registry.db",
        project="test",
        provider="local",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=SparkOfflineStoreConfig(type="spark"),
    )


@pytest.fixture()
def table_spark_source():
    return SparkSource(
        name="my_table",
        table="db.my_table",
        timestamp_field="event_timestamp",
    )


@pytest.fixture()
def path_spark_source():
    return SparkSource(
        name="my_path_source",
        path="s3a://bucket/data/features/",
        file_format="parquet",
        timestamp_field="event_timestamp",
    )


def _make_spark_retrieval_job(repo_config, remote_warehouse=True):
    """Build a SparkRetrievalJob with a mocked SparkSession."""
    mock_spark = MagicMock()

    if remote_warehouse:
        mock_spark.conf.get.side_effect = lambda key: {
            "hive.metastore.uris": "thrift://metastore:9083",
        }.get(key, None)
    else:

        def _local_conf_get(key):
            if key == "hive.metastore.uris":
                raise Exception("not set")
            if key == "spark.sql.warehouse.dir":
                return "file:///tmp/spark-warehouse"
            return None

        mock_spark.conf.get.side_effect = _local_conf_get

    return SparkRetrievalJob(
        spark_session=mock_spark,
        query="SELECT 1",
        full_feature_names=False,
        config=repo_config,
    )


# ---------------------------------------------------------------------------
# Group 1: SavedDatasetSparkStorage.from_data_source()
# ---------------------------------------------------------------------------


class TestSavedDatasetSparkStorageFromDataSource:
    def test_from_data_source_with_table_source(self, table_spark_source):
        storage = SavedDatasetSparkStorage.from_data_source(table_spark_source)

        assert isinstance(storage, SavedDatasetSparkStorage)
        assert storage.spark_options.table == "db.my_table"
        assert storage.spark_options.query is None
        assert storage.spark_options.path is None

    def test_from_data_source_with_path_source(self, path_spark_source):
        storage = SavedDatasetSparkStorage.from_data_source(path_spark_source)

        assert isinstance(storage, SavedDatasetSparkStorage)
        assert storage.spark_options.path == "s3a://bucket/data/features/"
        assert storage.spark_options.file_format == "parquet"
        assert storage.spark_options.table is None
        assert storage.spark_options.query is None

    def test_from_data_source_rejects_non_spark_source(self):
        file_source = FileSource(
            path="/tmp/data.parquet",
            timestamp_field="event_timestamp",
        )
        with pytest.raises(AssertionError):
            SavedDatasetSparkStorage.from_data_source(file_source)


# ---------------------------------------------------------------------------
# Group 2: SavedDatasetStorage.from_data_source() dispatch
# ---------------------------------------------------------------------------


class TestSavedDatasetStorageDispatch:
    def test_from_data_source_resolves_spark(self, table_spark_source):
        storage = SavedDatasetStorage.from_data_source(table_spark_source)

        assert isinstance(storage, SavedDatasetSparkStorage)
        assert storage.spark_options.table == "db.my_table"

    def test_from_data_source_resolves_path_spark(self, path_spark_source):
        storage = SavedDatasetStorage.from_data_source(path_spark_source)

        assert isinstance(storage, SavedDatasetSparkStorage)
        assert storage.spark_options.path == "s3a://bucket/data/features/"
        assert storage.spark_options.file_format == "parquet"

    def test_roundtrip_table_source(self, table_spark_source):
        storage = SavedDatasetStorage.from_data_source(table_spark_source)
        roundtripped = storage.to_data_source()

        assert isinstance(roundtripped, SparkSource)
        assert roundtripped.table == table_spark_source.table
        assert roundtripped.query == table_spark_source.query
        assert roundtripped.path == table_spark_source.path

    def test_roundtrip_path_source(self):
        source = SparkSource(
            name="my_path_source",
            table="fallback_name",
            timestamp_field="event_timestamp",
        )
        storage = SavedDatasetStorage.from_data_source(source)
        roundtripped = storage.to_data_source()

        assert isinstance(roundtripped, SparkSource)
        assert roundtripped.table == source.table


# ---------------------------------------------------------------------------
# Group 3: SparkRetrievalJob.persist()
# ---------------------------------------------------------------------------


class TestSparkRetrievalJobPersist:
    def test_persist_with_table_saves_as_table(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(table="output_table")

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.saveAsTable.assert_called_once_with("output_table")

    def test_persist_with_table_and_format(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(table="output_table", file_format="parquet")

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.format.assert_called_once_with("parquet")
        mock_df.write.format.return_value.saveAsTable.assert_called_once_with(
            "output_table"
        )

    def test_persist_with_path_writes_to_path(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(
            path="s3a://bucket/output/", file_format="parquet"
        )

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.format.assert_called_once_with("parquet")
        mock_df.write.format.return_value.mode.assert_called_once_with("error")
        mock_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "s3a://bucket/output/"
        )

    def test_persist_with_path_defaults_to_parquet(self, repo_config):
        """When path is set with table_format but no file_format, persist defaults to parquet."""
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(
            path="s3a://bucket/output/",
            file_format=None,
            table_format=IcebergFormat(catalog="test_catalog"),
        )

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.format.assert_called_once_with("parquet")

    def test_persist_with_path_allow_overwrite(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(
            path="s3a://bucket/output/", file_format="parquet"
        )

        job.persist(storage, allow_overwrite=True)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")

    def test_persist_with_path_custom_format(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(
            path="s3a://bucket/output/", file_format="avro"
        )

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.write.format.assert_called_once_with("avro")
        mock_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "s3a://bucket/output/"
        )

    def test_persist_raises_without_table_or_path(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=True)
        storage = SavedDatasetSparkStorage(query="SELECT * FROM t")

        with pytest.raises(
            ValueError, match="either 'table' or 'path' must be specified"
        ):
            job.persist(storage)

    def test_persist_local_warehouse_creates_temp_view(self, repo_config):
        job = _make_spark_retrieval_job(repo_config, remote_warehouse=False)
        storage = SavedDatasetSparkStorage(table="output_table")

        job.persist(storage)

        mock_df = job.spark_session.sql.return_value
        mock_df.createOrReplaceTempView.assert_called_once_with("output_table")
