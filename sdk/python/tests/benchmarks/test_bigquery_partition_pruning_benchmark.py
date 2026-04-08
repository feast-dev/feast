import os
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStore,
    BigQueryOfflineStoreConfig,
)
from feast.infra.offline_stores.bigquery_source import BigQuerySource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.repo_config import RepoConfig

__doc__ = """
Environment variables:
  FEAST_BQ_BENCH_PROJECT: BigQuery project to run dry-run queries in
  FEAST_BQ_BENCH_TABLE: BigQuery table in Feast format: project:dataset.table
  FEAST_BQ_BENCH_TIMESTAMP_FIELD: event timestamp column name used by Feast
  FEAST_BQ_BENCH_PARTITION_COLUMN: partition column to prune (e.g. _PARTITIONDATE)
  FEAST_BQ_BENCH_LOCATION: optional BigQuery location
  FEAST_BQ_BENCH_START: optional ISO datetime (e.g. 2026-01-01T00:00:00+00:00)
  FEAST_BQ_BENCH_END: optional ISO datetime
  FEAST_BQ_BENCH_REQUIRE_REDUCTION: if truthy, requires strict byte reduction
"""


def _required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        pytest.skip(f"Missing env var {name}")
    return val


def _optional_iso_datetime(name: str) -> datetime | None:
    val = os.environ.get(name)
    if not val:
        return None
    return datetime.fromisoformat(val.replace("Z", "+00:00"))


def _estimate_bytes_processed(project: str, location: str | None, sql: str) -> int:
    try:
        from google.cloud import bigquery
    except Exception as e:
        pytest.skip(str(e))
    client = bigquery.Client(project=project, location=location)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=job_config)
    return int(job.total_bytes_processed or 0)


@pytest.mark.benchmark(group="bigquery_partition_pruning")
@patch("feast.infra.offline_stores.bigquery._get_bigquery_client")
def test_bigquery_partition_pruning_bytes_processed(
    mock_get_bigquery_client, benchmark
):
    mock_get_bigquery_client.return_value = Mock()

    project = _required_env("FEAST_BQ_BENCH_PROJECT")
    table = _required_env("FEAST_BQ_BENCH_TABLE")
    timestamp_field = _required_env("FEAST_BQ_BENCH_TIMESTAMP_FIELD")
    partition_column = _required_env("FEAST_BQ_BENCH_PARTITION_COLUMN")
    location = os.environ.get("FEAST_BQ_BENCH_LOCATION")

    end = _optional_iso_datetime("FEAST_BQ_BENCH_END")
    if end is None:
        end = datetime.now(tz=timezone.utc).replace(microsecond=0)
    start = _optional_iso_datetime("FEAST_BQ_BENCH_START")
    if start is None:
        start = end - timedelta(days=7)

    repo_config = RepoConfig(
        registry="gs://ml-test/repo/registry.db",
        project="bench",
        provider="gcp",
        online_store=SqliteOnlineStoreConfig(type="sqlite"),
        offline_store=BigQueryOfflineStoreConfig(type="bigquery", dataset="feast"),
    )

    source_without_partition = BigQuerySource(
        table=table,
        timestamp_field=timestamp_field,
    )
    source_with_partition = BigQuerySource(
        table=table,
        timestamp_field=timestamp_field,
        date_partition_column=partition_column,
    )

    job_without = BigQueryOfflineStore.pull_all_from_table_or_query(
        config=repo_config,
        data_source=source_without_partition,
        join_key_columns=[],
        feature_name_columns=[],
        timestamp_field=timestamp_field,
        start_date=start,
        end_date=end,
    )
    job_with = BigQueryOfflineStore.pull_all_from_table_or_query(
        config=repo_config,
        data_source=source_with_partition,
        join_key_columns=[],
        feature_name_columns=[],
        timestamp_field=timestamp_field,
        start_date=start,
        end_date=end,
    )

    sql_without = job_without.to_sql()
    sql_with = job_with.to_sql()

    def measure():
        bytes_without = _estimate_bytes_processed(project, location, sql_without)
        bytes_with = _estimate_bytes_processed(project, location, sql_with)
        return bytes_without, bytes_with

    bytes_without, bytes_with = benchmark(measure)
    benchmark.extra_info["total_bytes_processed_without_partition_filter"] = (
        bytes_without
    )
    benchmark.extra_info["total_bytes_processed_with_partition_filter"] = bytes_with
    if bytes_without > 0:
        benchmark.extra_info["bytes_ratio_with_over_without"] = (
            bytes_with / bytes_without
        )

    if os.environ.get("FEAST_BQ_BENCH_REQUIRE_REDUCTION", "").lower() in (
        "1",
        "true",
        "yes",
    ):
        assert bytes_with < bytes_without
    else:
        assert bytes_with <= bytes_without
