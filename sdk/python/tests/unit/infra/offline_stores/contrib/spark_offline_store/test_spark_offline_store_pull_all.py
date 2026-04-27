"""
Unit tests for SparkOfflineStore.pull_all_from_table_or_query SQL generation.

Covers the bug where feature_name_columns=[] (signalling "read all source
columns" for BatchFeatureView UDF transformations) caused a bare
  SELECT user_id, event_timestamp FROM source
instead of SELECT *, silently dropping all columns the UDF needs.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast.infra.offline_stores.contrib.spark_offline_store.spark import (  # noqa: E402
    SparkOfflineStore,
    SparkOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (  # noqa: E402
    SparkSource,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

START = datetime(2023, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Fixed table name returned by the mocked get_table_query_string
_TABLE_EXPR = "`raw_reviews`"


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
def spark_source():
    return SparkSource(
        name="raw_reviews",
        path="s3a://bucket/processed/reviews/",
        file_format="parquet",
        timestamp_field="event_timestamp",
    )


def _run_pull_all(repo_config, spark_source, feature_name_columns):
    """
    Call pull_all_from_table_or_query with a mocked SparkSession and mocked
    data-source table resolution, then return the SQL query string.

    Two things are patched so no real Spark/S3 access occurs:
      1. get_spark_session_or_start_new_with_repoconfig → MagicMock session
      2. spark_source.get_table_query_string → fixed table expression
         (avoids SparkSource.validate / _load_dataframe_from_path hitting S3)
    """
    mock_spark = MagicMock()

    with (
        patch(
            "feast.infra.offline_stores.contrib.spark_offline_store.spark"
            ".get_spark_session_or_start_new_with_repoconfig",
            return_value=mock_spark,
        ),
        patch.object(
            spark_source,
            "get_table_query_string",
            return_value=_TABLE_EXPR,
        ),
    ):
        job = SparkOfflineStore.pull_all_from_table_or_query(
            config=repo_config,
            data_source=spark_source,
            join_key_columns=["user_id"],
            feature_name_columns=feature_name_columns,
            timestamp_field="event_timestamp",
            created_timestamp_column=None,
            start_date=START,
            end_date=END,
        )

    return job.query.strip()


def test_pull_all_with_empty_feature_cols_generates_select_star(
    repo_config, spark_source
):
    """
    feature_name_columns=[] must produce SELECT * so UDF-based
    BatchFeatureViews receive all raw source columns for aggregation.
    """
    sql = _run_pull_all(repo_config, spark_source, feature_name_columns=[])

    assert sql.startswith("SELECT *"), (
        "Expected 'SELECT *' when feature_name_columns=[], "
        f"got: {sql[:120]!r}\n\n"
        "BatchFeatureView UDFs need all raw source columns to compute "
        "aggregations — projecting only join key + timestamp silently "
        "drops rating, text, helpful_vote, etc."
    )
    assert "user_id" not in sql.split("FROM")[0], (
        "SELECT * must not also explicitly list join key columns"
    )


def test_pull_all_with_feature_cols_generates_explicit_projection(
    repo_config, spark_source
):
    """
    When feature_name_columns is non-empty (normal FeatureView path),
    the query must project only the requested columns — not SELECT *.
    """
    sql = _run_pull_all(
        repo_config,
        spark_source,
        feature_name_columns=["avg_rating", "review_count"],
    )

    assert "SELECT *" not in sql, (
        "Non-empty feature_name_columns must produce explicit SELECT projection, not SELECT *"
    )
    assert "avg_rating" in sql
    assert "review_count" in sql
    assert "user_id" in sql
    assert "event_timestamp" in sql


def test_pull_all_empty_feature_cols_upstream_regression(repo_config, spark_source):
    """
    Regression guard: the upstream (unfixed) behaviour with feature_name_columns=[]
    produced a query that only selected join key + timestamp, dropping all columns
    the UDF needs. Verify the fixed code does NOT produce that broken query.

    Broken upstream SQL looked like:
      SELECT user_id, event_timestamp FROM ... WHERE ...
    """
    sql = _run_pull_all(repo_config, spark_source, feature_name_columns=[])

    projection = sql.split("FROM")[0]
    assert "user_id" not in projection, (
        "Upstream bug: query projected only 'user_id, event_timestamp', "
        "silently dropping all columns needed by the BFV UDF. "
        "Fixed query should use SELECT *."
    )


@pytest.mark.parametrize(
    "feature_cols,expect_star",
    [
        ([], True),
        (["f1"], False),
        (["f1", "f2", "f3"], False),
    ],
)
def test_pull_all_select_star_only_when_feature_cols_empty(
    repo_config, spark_source, feature_cols, expect_star
):
    sql = _run_pull_all(repo_config, spark_source, feature_name_columns=feature_cols)
    has_star = sql.strip().upper().startswith("SELECT *")
    assert has_star == expect_star, (
        f"feature_cols={feature_cols!r}: expected SELECT *={expect_star}, got SQL: {sql[:100]!r}"
    )
