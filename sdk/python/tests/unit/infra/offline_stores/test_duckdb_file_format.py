"""DuckDB offline store handling of a FileSource without a ``file_format``.

``FileSource(path=...)`` leaves ``file_format`` as ``None``. The DuckDB write
path, ``FileSource.get_table_column_names_and_types`` and the Dask store all read
such a source as Parquet. The DuckDB reader did so only when the path ended in
``.parquet``, and the DuckDB monitoring queries not at all.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast.data_format import FileFormat
from feast.infra.offline_stores.duckdb import (
    DuckDBOfflineStore,
    DuckDBOfflineStoreConfig,
    _read_data_source,
)
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig

EVENT_TS = datetime(2026, 1, 1, 11)


def _repo_config(tmp_path) -> RepoConfig:
    return RepoConfig(
        project="duckdb_test",
        registry=str(tmp_path / "registry.db"),
        provider="local",
        offline_store=DuckDBOfflineStoreConfig(),
        entity_key_serialization_version=3,
        repo_path=tmp_path,
    )


def _write_stats(path) -> None:
    pd.DataFrame(
        {
            "driver_id": [1000, 1001],
            "conv_rate": [0.5, 0.7],
            "event_timestamp": [EVENT_TS, EVENT_TS],
        }
    ).to_parquet(path)


def _parquet_directory_source(tmp_path) -> FileSource:
    directory = tmp_path / "driver_stats"
    directory.mkdir()
    _write_stats(directory / "part-0.parquet")
    return FileSource(
        name="driver_stats_source",
        path=str(directory),
        timestamp_field="event_timestamp",
    )


@pytest.mark.parametrize("layout", ["directory", "pq_extension"])
def test_read_source_without_file_format_as_parquet(tmp_path, layout):
    if layout == "directory":
        source = _parquet_directory_source(tmp_path)
    else:
        path = tmp_path / "driver_stats.pq"
        _write_stats(path)
        source = FileSource(path=str(path), timestamp_field="event_timestamp")
    assert source.file_format is None

    table = _read_data_source(source, str(tmp_path))

    assert table is not None
    assert sorted(table.to_pandas()["driver_id"].tolist()) == [1000, 1001]


def test_pull_latest_from_directory_source_without_file_format(tmp_path):
    source = _parquet_directory_source(tmp_path)

    df = DuckDBOfflineStore.pull_latest_from_table_or_query(
        config=_repo_config(tmp_path),
        data_source=source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=EVENT_TS - timedelta(days=1),
        end_date=EVENT_TS + timedelta(days=1),
    ).to_df()

    assert sorted(df["driver_id"].tolist()) == [1000, 1001]


def test_read_source_with_unsupported_file_format_raises(tmp_path):
    class _UnsupportedFormat(FileFormat):
        def to_proto(self):
            raise NotImplementedError

    source = FileSource(
        name="unsupported_source",
        path=str(tmp_path / "driver_stats.csv"),
        timestamp_field="event_timestamp",
    )
    source.file_options.file_format = _UnsupportedFormat()

    with pytest.raises(NotImplementedError, match="unsupported_source"):
        _read_data_source(source, str(tmp_path))


def test_monitoring_reads_source_without_file_format_as_parquet(tmp_path):
    path = tmp_path / "driver_stats.parquet"
    _write_stats(path)
    source = FileSource(path=str(path), timestamp_field="event_timestamp")
    assert source.file_format is None
    config = _repo_config(tmp_path)

    max_ts = DuckDBOfflineStore.get_monitoring_max_timestamp(
        config=config, data_source=source, timestamp_field="event_timestamp"
    )
    assert max_ts is not None
    assert max_ts.replace(tzinfo=None) == EVENT_TS

    metrics = DuckDBOfflineStore.compute_monitoring_metrics(
        config=config,
        data_source=source,
        feature_columns=[("conv_rate", "numeric")],
        timestamp_field="event_timestamp",
    )
    assert [m["feature_name"] for m in metrics] == ["conv_rate"]
    assert metrics[0]["row_count"] == 2
