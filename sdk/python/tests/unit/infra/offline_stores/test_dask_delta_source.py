"""Unit tests for DeltaFormat FileSource support in the Dask offline store.

The Dask offline store previously read every FileSource through
``dd.read_parquet`` on the source directory, ignoring ``file_format``. For
Delta Lake tables this returns stale or duplicated rows after overwrites or
compaction, because tombstoned ``part-*.parquet`` files of earlier Delta
versions stay on disk until they are vacuumed. These tests pin the
snapshot-correct behavior for reads (historical retrieval, materialization
pulls, monitoring) and the explicit failure for writes.
"""

import glob
import json
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from feast.data_format import DeltaFormat, FileFormat, ParquetFormat
from feast.entity import Entity
from feast.feature_view import FeatureView, Field
from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores.dask import (
    DaskOfflineStore,
    DaskOfflineStoreConfig,
    DaskRetrievalJob,
)
from feast.infra.offline_stores.file_source import FileSource, SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.repo_config import RepoConfig
from feast.types import Float32, Int32, Int64, UnixTimestamp, ValueType

DeltaTable = pytest.importorskip("deltalake").DeltaTable
write_deltalake = pytest.importorskip("deltalake.writer").write_deltalake


class _UnsupportedFileFormat(FileFormat):
    """A FileFormat outside the formats the Dask offline store can read."""

    def to_proto(self) -> None:
        raise NotImplementedError

    def __str__(self) -> str:
        return "unsupported"


# Values in the initial and replacement snapshots.
STALE_CONV_RATES = [0.10, 0.20]
CURRENT_CONV_RATES = [0.90, 0.80]


def _write_snapshot(
    path: str, day: int, conv_rates: list[float], partition_by: list[str] | None = None
) -> None:
    df = pd.DataFrame(
        {
            "driver_id": [1001, 1002],
            "conv_rate": conv_rates,
            "event_timestamp": pd.to_datetime(
                [datetime(2026, 1, day, tzinfo=timezone.utc)] * 2
            ),
        }
    )
    write_deltalake(path, df, mode="overwrite", partition_by=partition_by)


def _make_overwritten_delta_table(
    tmp_path: Path, partition_by: list[str] | None = None
) -> str:
    """Create a Delta table whose first version was fully overwritten.

    The tombstoned files of version 0 remain on disk, so reading the
    directory with a plain Parquet reader returns both versions.
    """
    delta_path = str(tmp_path / "driver_stats_delta")
    os.makedirs(delta_path)
    _write_snapshot(
        delta_path, day=1, conv_rates=STALE_CONV_RATES, partition_by=partition_by
    )
    _write_snapshot(
        delta_path, day=2, conv_rates=CURRENT_CONV_RATES, partition_by=partition_by
    )
    return delta_path


def _delta_file_source(delta_path: str) -> FileSource:
    return FileSource(
        name="driver_stats_source",
        path=delta_path,
        file_format=DeltaFormat(),
        timestamp_field="event_timestamp",
    )


def _mock_repo_config(repo_path: Path | None = None) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        repo_path=repo_path,
        offline_store=DaskOfflineStoreConfig(type="dask"),
    )


def _mock_entity() -> list[Entity]:
    return [
        Entity(
            name="driver_id",
            join_keys=["driver_id"],
            description="Driver ID",
            value_type=ValueType.INT64,
        )
    ]


def _mock_feature_view(source: FileSource) -> FeatureView:
    return FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="conv_rate", dtype=Float32),
        ],
        source=source,
        ttl=None,
    )


def _physical_part_files(delta_path: str) -> list[str]:
    return glob.glob(os.path.join(delta_path, "**", "part-*.parquet"), recursive=True)


# ------------------------------------------------------------------ #
# Read path: _read_datasource (historical retrieval + materialization)
# ------------------------------------------------------------------ #


def test_read_datasource_returns_current_delta_snapshot_only(tmp_path: Path) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path)

    # Sanity: the tombstoned files of the overwritten version are still on
    # disk, so a directory-level Parquet read would see both versions.
    assert len(_physical_part_files(delta_path)) == 2
    assert DeltaTable(delta_path).version() == 1
    assert len(DeltaTable(delta_path).file_uris()) == 1

    source = _delta_file_source(delta_path)
    result = dask_mod._read_datasource(source, None).compute()

    assert len(result) == 2
    assert sorted(float(v) for v in result["conv_rate"]) == sorted(CURRENT_CONV_RATES)


def test_read_datasource_delta_partitioned_restores_partition_column(
    tmp_path: Path,
) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path, partition_by=["driver_id"])

    source = _delta_file_source(delta_path)
    result = dask_mod._read_datasource(source, None).compute()

    assert len(result) == 2
    # Partition values live in the Delta log and the directory layout, not in
    # the Parquet files themselves; the reader must still surface them.
    assert sorted(int(v) for v in result["driver_id"]) == [1001, 1002]
    assert sorted(float(v) for v in result["conv_rate"]) == sorted(CURRENT_CONV_RATES)


@pytest.mark.parametrize("segments", [["001"], ["001", "002", "east/west", None]])
def test_delta_partition_values_come_from_the_log(
    tmp_path: Path, segments: list[str | None]
) -> None:
    path = str(tmp_path / "delta")
    table = pa.table(
        {"row_id": range(len(segments)), "segment": pa.array(segments, pa.string())}
    )
    write_deltalake(path, table, partition_by=["segment"])

    # A relative source path uses the repo root, and projecting the partition
    # column must retain the declared type and values (not path-inferred ints).
    frame = dask_mod._read_datasource(_delta_file_source("delta"), tmp_path)
    assert frame.npartitions == len(segments)
    rows = frame[["row_id", "segment"]].compute().sort_values("row_id")
    assert [None if pd.isna(value) else value for value in rows["segment"]] == segments


def test_delta_rejects_an_unsupported_reader_protocol(tmp_path: Path) -> None:
    from deltalake.exceptions import DeltaProtocolError

    path = Path(_make_overwritten_delta_table(tmp_path))
    # The pinned reader cannot apply deletion vectors. A raw Parquet scan
    # would silently return results from a table it cannot interpret.
    action = {
        "protocol": {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"],
            "writerFeatures": ["deletionVectors"],
        }
    }
    (path / "_delta_log" / "00000000000000000002.json").write_text(
        json.dumps(action) + "\n", encoding="utf-8"
    )
    source = _delta_file_source(str(path))
    with pytest.raises(DeltaProtocolError, match="deletionVectors"):
        dask_mod._read_datasource(source, None).compute()
    with pytest.raises(DeltaProtocolError, match="deletionVectors"):
        DaskOfflineStore.get_monitoring_max_timestamp(
            _mock_repo_config(tmp_path), source, "event_timestamp"
        )
    with pytest.raises(DeltaProtocolError, match="deletionVectors"):
        DaskOfflineStore.compute_monitoring_metrics(
            _mock_repo_config(tmp_path),
            source,
            [("conv_rate", "numeric")],
            "event_timestamp",
        )


def test_read_datasource_delta_empty_snapshot_keeps_schema(tmp_path: Path) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path)
    delta_table = DeltaTable(delta_path)
    # Delete every row without vacuuming: an empty snapshot that still knows
    # its schema through the Delta log.
    delta_table.delete()

    source = _delta_file_source(delta_path)
    result = dask_mod._read_datasource(source, None).compute()

    assert len(result) == 0
    assert set(result.columns) == {"driver_id", "conv_rate", "event_timestamp"}


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("tz", [None, timezone.utc], ids=["naive", "aware"])
@pytest.mark.parametrize("bounds", ["start", "end", "both"])
def test_delta_timestamp_bounds_respect_utc(
    tmp_path: Path, empty: bool, tz: timezone | None, bounds: str
) -> None:
    path = str(tmp_path / "delta")
    write_deltalake(
        path,
        pa.table(
            {
                "driver_id": [1001, 1002],
                "conv_rate": [0.25, 0.75],
                "event_timestamp": pa.array(
                    [datetime(2026, 1, 1, tzinfo=tz), datetime(2026, 1, 2, tzinfo=tz)],
                    pa.timestamp("us", "UTC" if tz else None),
                ),
            }
        ),
    )
    if empty:
        DeltaTable(path).delete()
    source = _delta_file_source(path)
    config = _mock_repo_config(tmp_path)
    bounds_tz = None if tz else timezone.utc
    start = datetime(2026, 1, 2, tzinfo=bounds_tz) if bounds != "end" else None
    end = (
        datetime(2026, 1, 1 if bounds == "end" else 2, tzinfo=bounds_tz)
        if bounds != "start"
        else None
    )
    expected = 0.25 if bounds == "end" else 0.75
    job = DaskOfflineStore.pull_all_from_table_or_query(
        config,
        source,
        ["driver_id"],
        ["conv_rate"],
        "event_timestamp",
        start_date=start,
        end_date=end,
    )
    assert job.to_df()["conv_rate"].tolist() == ([] if empty else [expected])
    metrics = DaskOfflineStore.compute_monitoring_metrics(
        config,
        source,
        [("conv_rate", "numeric")],
        "event_timestamp",
        start_date=start,
        end_date=end,
    )
    assert metrics[0]["row_count"] == (0 if empty else 1)
    assert metrics[0]["mean"] == (None if empty else expected)


def test_delta_nullable_integers_keep_precision_and_arrow_type(tmp_path: Path) -> None:
    path = str(tmp_path / "delta")
    large = 2**53 + 1
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for ids, flags in (([large, None], [True, None]), ([large + 2], [False])):
        write_deltalake(
            path,
            pa.table(
                {
                    "driver_id": pa.array(ids, pa.int64()),
                    "is_active": pa.array(flags, pa.bool_()),
                    "event_timestamp": [timestamp] * len(ids),
                }
            ),
            mode="append",
        )
    source = _delta_file_source(path)
    table = DaskOfflineStore.pull_all_from_table_or_query(
        _mock_repo_config(tmp_path), source, ["driver_id"], [], "event_timestamp"
    ).to_arrow()
    ids = table["driver_id"]
    assert ids.type == pa.int64()
    assert ids.null_count == 1
    assert sorted(v for v in ids.to_pylist() if v is not None) == [large, large + 2]
    assert table["is_active"].type == pa.bool_()
    assert dict(zip(ids.to_pylist(), table["is_active"].to_pylist())) == {
        large: True,
        large + 2: False,
        None: None,
    }


def test_read_datasource_unknown_file_format_raises(tmp_path: Path) -> None:
    parquet_path = str(tmp_path / "data.parquet")
    pq.write_table(pa.table({"event_timestamp": [datetime(2026, 1, 1)]}), parquet_path)
    source = FileSource(
        name="unsupported_source",
        path=parquet_path,
        file_format=_UnsupportedFileFormat(),
        timestamp_field="event_timestamp",
    )

    with pytest.raises(NotImplementedError, match="only Parquet and Delta"):
        dask_mod._read_datasource(source, None)
    with pytest.raises(NotImplementedError, match="only Parquet and Delta"):
        DaskOfflineStore.get_monitoring_max_timestamp(
            _mock_repo_config(tmp_path), source, "event_timestamp"
        )
    with pytest.raises(NotImplementedError, match="only Parquet and Delta"):
        DaskOfflineStore.compute_monitoring_metrics(
            _mock_repo_config(tmp_path), source, [], "event_timestamp"
        )


@pytest.mark.parametrize("file_format", [None, ParquetFormat()])
def test_parquet_file_source_is_unchanged(
    tmp_path: Path, file_format: FileFormat | None
) -> None:
    table = pa.table(
        {
            "driver_id": [1001],
            "conv_rate": [0.5],
            "event_timestamp": [datetime(2026, 1, 1, tzinfo=timezone.utc)],
        }
    )
    pq.write_table(table, tmp_path / "data.parquet")
    source = FileSource(
        path="data.parquet", file_format=file_format, timestamp_field="event_timestamp"
    )
    config = _mock_repo_config(tmp_path)

    assert dask_mod._read_datasource(source, tmp_path).compute()[
        "conv_rate"
    ].tolist() == [0.5]
    assert DaskOfflineStore.get_monitoring_max_timestamp(
        config, source, "event_timestamp"
    ) == datetime(2026, 1, 1, tzinfo=timezone.utc)
    metrics = DaskOfflineStore.compute_monitoring_metrics(
        config, source, [("conv_rate", "numeric")], "event_timestamp"
    )
    assert metrics[0]["row_count"] == 1
    assert metrics[0]["mean"] == 0.5
    DaskOfflineStore.offline_write_batch(
        config, _mock_feature_view(source), table, None
    )
    assert pq.read_table(tmp_path / "data.parquet").num_rows == 2


# ------------------------------------------------------------------ #
# End-to-end: historical retrieval and materialization pulls
# ------------------------------------------------------------------ #


def test_historical_retrieval_reads_delta_snapshot(tmp_path: Path) -> None:
    # The tombstoned version has a later event time, so PIT deduplication
    # would select stale values if the reader included its rows.
    delta_path = str(tmp_path / "driver_stats_delta")
    _write_snapshot(delta_path, day=2, conv_rates=STALE_CONV_RATES)
    _write_snapshot(delta_path, day=1, conv_rates=CURRENT_CONV_RATES)

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)
    fv = _mock_feature_view(source)
    entity_df = pd.DataFrame(
        {
            "driver_id": [1001, 1002],
            "event_timestamp": pd.to_datetime(
                [datetime(2026, 1, 3, tzinfo=timezone.utc)] * 2
            ),
        }
    )

    job = DaskOfflineStore.get_historical_features(
        config=repo_config,
        feature_views=[fv],
        feature_refs=["driver_stats:conv_rate"],
        entity_df=entity_df,
        registry=MagicMock(),
        project="test_project",
        full_feature_names=False,
    )
    result = job.to_df()

    assert len(result) == 2
    assert sorted(float(v) for v in result["conv_rate"]) == sorted(CURRENT_CONV_RATES)


@pytest.mark.parametrize("full_feature_names", [False, True])
def test_delta_historical_requested_timestamp_keeps_pit_filter(
    tmp_path: Path, full_feature_names: bool
) -> None:
    path = str(tmp_path / "delta")
    past = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    future = datetime(2026, 1, 3, tzinfo=timezone.utc)
    write_deltalake(
        path,
        pa.table(
            {
                "driver_id": [1001, 1001, 1001],
                "event_timestamp": [past, past, future],
                "created": [cutoff, future, cutoff],
            }
        ),
    )
    source = FileSource(
        path=path,
        file_format=DeltaFormat(),
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
    )
    fv = FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(name="created", dtype=UnixTimestamp),
        ],
        source=source,
    )
    entity_df = pd.DataFrame({"driver_id": [1001], "request_time": [cutoff]})
    result = DaskOfflineStore.get_historical_features(
        _mock_repo_config(tmp_path),
        [fv],
        ["driver_stats:event_timestamp", "driver_stats:created"],
        entity_df,
        MagicMock(),
        "test_project",
        full_feature_names=full_feature_names,
        filter_by_created_timestamp=True,
    ).to_df()
    prefix = "driver_stats__" if full_feature_names else ""
    assert result[f"{prefix}event_timestamp"].tolist() == [past]
    assert result[f"{prefix}created"].tolist() == [cutoff]
    assert result["request_time"].tolist() == [cutoff]


@pytest.mark.parametrize("all_unmatched", [False, True])
def test_delta_historical_retains_requests_without_eligible_rows(
    tmp_path: Path, all_unmatched: bool
) -> None:
    path = str(tmp_path / "delta")
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    past = datetime(2026, 1, 1, tzinfo=timezone.utc)
    future = datetime(2026, 1, 3, tzinfo=timezone.utc)
    for ids, times, scores in (
        ([1001, 1002], [past, None], [10, 20]),
        ([1001, 1003], [future, future], [30, 40]),
    ):
        write_deltalake(
            path,
            pa.table(
                {
                    "driver_id": ids,
                    "event_timestamp": pa.array(times, pa.timestamp("us", "UTC")),
                    "score": pa.array(scores, pa.int32()),
                }
            ),
            mode="append",
        )
    fv = FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[Field(name="driver_id", dtype=Int64), Field(name="score", dtype=Int32)],
        source=_delta_file_source(path),
    )
    # One missing entity, one null source timestamp, and one future-only entity.
    ids = [1001, 1002, 1003, 1004] if not all_unmatched else [1004, 1005]
    requests = pd.DataFrame(
        {"driver_id": ids, "event_timestamp": [cutoff] * len(ids), "label": ids}
    )
    table = (
        DaskOfflineStore.get_historical_features(
            _mock_repo_config(tmp_path),
            [fv],
            ["driver_stats:score"],
            requests,
            MagicMock(),
            "test_project",
        )
        .to_arrow()
        .sort_by("driver_id")
    )
    assert table["driver_id"].to_pylist() == ids
    assert table["label"].to_pylist() == ids
    assert table["event_timestamp"].to_pylist() == [cutoff] * len(ids)
    assert table["score"].type == pa.int32()
    assert table["score"].to_pylist() == (
        [None, None] if all_unmatched else [10, None, None, None]
    )


@pytest.mark.parametrize("full_feature_names", [False, True])
def test_delta_historical_timestamp_alias_does_not_shadow_feature(
    tmp_path: Path, full_feature_names: bool
) -> None:
    path = str(tmp_path / "delta")
    past = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    future = datetime(2026, 1, 3, tzinfo=timezone.utc)
    write_deltalake(
        path,
        pa.table(
            {
                "driver_id": [1001, 1001, 9999],
                "event_timestamp": [past, future, cutoff],
                "__event_timestamp": [10, 20, 30],
            }
        ),
    )
    fv = FeatureView(
        name="driver_stats",
        entities=_mock_entity(),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="__event_timestamp", dtype=Int64),
        ],
        source=FileSource(
            path=path,
            file_format=DeltaFormat(),
            timestamp_field="event_timestamp",
        ),
    )
    result = DaskOfflineStore.get_historical_features(
        _mock_repo_config(tmp_path),
        [fv],
        ["driver_stats:__event_timestamp"],
        pd.DataFrame({"driver_id": [1001], "event_timestamp": [cutoff]}),
        MagicMock(),
        "test_project",
        full_feature_names=full_feature_names,
    ).to_df()
    assert result.columns.is_unique
    output = (
        "driver_stats____event_timestamp" if full_feature_names else "__event_timestamp"
    )
    assert result[output].tolist() == [10]
    assert result["event_timestamp"].tolist() == [cutoff]


@pytest.mark.parametrize("has_entity_df", [False, True])
def test_delta_empty_historical_snapshot(tmp_path: Path, has_entity_df: bool) -> None:
    path = _make_overwritten_delta_table(tmp_path)
    DeltaTable(path).delete()
    cutoff = datetime(2026, 1, 3, tzinfo=timezone.utc)
    entity_df = pd.DataFrame({"driver_id": [1001], "event_timestamp": [cutoff]})
    result = DaskOfflineStore.get_historical_features(
        _mock_repo_config(tmp_path),
        [_mock_feature_view(_delta_file_source(path))],
        ["driver_stats:conv_rate"],
        entity_df if has_entity_df else None,
        MagicMock(),
        "test_project",
        end_date=cutoff,
    ).to_df()
    assert list(result["driver_id"]) == ([1001] if has_entity_df else [])
    assert result["conv_rate"].isna().all()


def test_delta_non_entity_snapshot_requires_a_timestamp(tmp_path: Path) -> None:
    path = str(tmp_path / "delta")
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    write_deltalake(
        path,
        pa.table(
            {
                "driver_id": [1001, 1002],
                "conv_rate": [0.25, 0.75],
                "event_timestamp": [event_time, None],
            }
        ),
    )
    result = DaskOfflineStore.get_historical_features(
        _mock_repo_config(tmp_path),
        [_mock_feature_view(_delta_file_source(path))],
        ["driver_stats:conv_rate"],
        None,
        MagicMock(),
        "test_project",
        end_date=event_time,
    ).to_df()
    assert result["driver_id"].tolist() == [1001]
    assert result["conv_rate"].tolist() == [0.25]


@pytest.mark.parametrize(
    "pull",
    [
        DaskOfflineStore.pull_latest_from_table_or_query,
        DaskOfflineStore.pull_all_from_table_or_query,
    ],
    ids=["latest", "all"],
)
def test_materialization_pulls_read_delta_snapshot(
    tmp_path: Path, pull: Callable[..., RetrievalJob]
) -> None:
    delta_path = str(tmp_path / "driver_stats_delta")
    _write_snapshot(delta_path, day=2, conv_rates=STALE_CONV_RATES)
    _write_snapshot(delta_path, day=1, conv_rates=CURRENT_CONV_RATES)

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)

    job = pull(
        config=repo_config,
        data_source=source,
        join_key_columns=["driver_id"],
        feature_name_columns=["conv_rate"],
        timestamp_field="event_timestamp",
        created_timestamp_column=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    result = job.to_df()

    assert len(result) == 2
    assert sorted(float(v) for v in result["conv_rate"]) == sorted(CURRENT_CONV_RATES)


@pytest.mark.parametrize("all_null", [False, True])
@pytest.mark.parametrize("bounded", [False, True])
def test_delta_pulls_handle_null_timestamp_fragments(
    tmp_path: Path, all_null: bool, bounded: bool
) -> None:
    path = str(tmp_path / "delta")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    for ids, times, values in (
        ([1001, 1001], [None, None] if all_null else [start, end], [10, 20]),
        ([1002], [None], [30]),
    ):
        write_deltalake(
            path,
            pa.table(
                {
                    "driver_id": ids,
                    "event_timestamp": pa.array(times, pa.timestamp("us", "UTC")),
                    "conv_rate": values,
                }
            ),
            mode="append",
        )
    args = (
        _mock_repo_config(tmp_path),
        _delta_file_source(path),
        ["driver_id"],
        ["conv_rate"],
        "event_timestamp",
        None,
    )
    bounds = {"start_date": start, "end_date": end} if bounded else {}
    rows = DaskOfflineStore.pull_all_from_table_or_query(*args, **bounds).to_df()
    latest = DaskOfflineStore.pull_latest_from_table_or_query(*args, **bounds).to_df()
    if all_null:
        assert rows["event_timestamp"].isna().all()
        assert sorted(rows["conv_rate"]) == ([] if bounded else [10, 20, 30])
        assert sorted(latest["driver_id"]) == ([] if bounded else [1001, 1002])
    else:
        assert rows["conv_rate"].tolist() == ([10, 20] if bounded else [10, 20, 30])
        assert sorted(latest["conv_rate"]) == ([20] if bounded else [20, 30])


# ------------------------------------------------------------------ #
# Monitoring
# ------------------------------------------------------------------ #


def test_monitoring_max_timestamp_ignores_tombstoned_files(tmp_path: Path) -> None:
    # The overwritten version carries a NEWER timestamp than the current
    # snapshot, so a directory-level read would report the wrong max.
    delta_path = str(tmp_path / "driver_stats_delta")
    os.makedirs(delta_path)
    _write_snapshot(
        delta_path, day=20, conv_rates=STALE_CONV_RATES
    )  # tombstoned, ts 2026-01-20
    _write_snapshot(
        delta_path, day=2, conv_rates=CURRENT_CONV_RATES
    )  # current, ts 2026-01-02

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)

    max_ts = DaskOfflineStore.get_monitoring_max_timestamp(
        config=repo_config,
        data_source=source,
        timestamp_field="event_timestamp",
    )

    assert max_ts == datetime(2026, 1, 2, tzinfo=timezone.utc)


def test_monitoring_max_timestamp_empty_delta_snapshot_returns_none(
    tmp_path: Path,
) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path)
    DeltaTable(delta_path).delete()

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)

    assert (
        DaskOfflineStore.get_monitoring_max_timestamp(
            config=repo_config,
            data_source=source,
            timestamp_field="event_timestamp",
        )
        is None
    )


def test_monitoring_missing_delta_table_returns_no_data(
    tmp_path: Path,
) -> None:
    source = _delta_file_source(str(tmp_path / "not_written_yet_delta"))
    repo_config = _mock_repo_config(repo_path=tmp_path)

    assert (
        DaskOfflineStore.get_monitoring_max_timestamp(
            config=repo_config,
            data_source=source,
            timestamp_field="event_timestamp",
        )
        is None
    )
    assert (
        DaskOfflineStore.compute_monitoring_metrics(
            config=repo_config,
            data_source=source,
            feature_columns=[("conv_rate", "numeric")],
            timestamp_field="event_timestamp",
        )
        == []
    )


def test_missing_delta_table_raises_for_retrieval(tmp_path: Path) -> None:
    from deltalake.exceptions import TableNotFoundError

    job = DaskOfflineStore.pull_all_from_table_or_query(
        _mock_repo_config(tmp_path),
        _delta_file_source(str(tmp_path / "not_written_yet_delta")),
        ["driver_id"],
        [],
        "event_timestamp",
    )
    with pytest.raises(TableNotFoundError):
        job.to_arrow()


def test_delta_monitoring_max_invalid_timestamp_column_raises(tmp_path: Path) -> None:
    source = _delta_file_source(_make_overwritten_delta_table(tmp_path))
    with pytest.raises(pa.ArrowInvalid, match="missing_timestamp"):
        DaskOfflineStore.get_monitoring_max_timestamp(
            _mock_repo_config(tmp_path), source, "missing_timestamp"
        )


@pytest.mark.parametrize(
    "credentials, endpoint, expected",
    [
        (None, None, None),
        (
            None,
            "https://s3.example.invalid",
            {"AWS_ENDPOINT_URL": "https://s3.example.invalid"},
        ),
        (
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",  # pragma: allowlist secret
                "AWS_SESSION_TOKEN": "test-token",
                "AWS_DEFAULT_REGION": "us-west-2",
            },
            "http://s3.example.invalid",
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",  # pragma: allowlist secret
                "AWS_SESSION_TOKEN": "test-token",
                "AWS_DEFAULT_REGION": "us-west-2",
                "AWS_ENDPOINT_URL": "http://s3.example.invalid",
                "AWS_ALLOW_HTTP": "true",
            },
        ),
    ],
)
def test_delta_s3_reader_options(
    monkeypatch: pytest.MonkeyPatch,
    credentials: dict[str, str] | None,
    endpoint: str | None,
    expected: dict[str, str] | None,
) -> None:
    path = "s3://bucket/table"
    source = FileSource(
        path=path, file_format=DeltaFormat(), s3_endpoint_override=endpoint
    )
    credentials_resolver = MagicMock(return_value=credentials)
    monkeypatch.setattr(source, "resolve_credentials", credentials_resolver)
    constructor = MagicMock()
    constructor.return_value.to_pyarrow_dataset.return_value = pa.dataset.dataset(
        pa.table({"event_timestamp": pa.array([], pa.timestamp("us", "UTC"))})
    )
    monkeypatch.setattr("deltalake.DeltaTable", constructor)

    dask_mod._read_datasource(source, None)

    credentials_resolver.assert_called_once_with()
    constructor.assert_called_once_with(path, storage_options=expected)


def test_compute_monitoring_metrics_reads_delta_snapshot(tmp_path: Path) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path)

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)

    metrics = DaskOfflineStore.compute_monitoring_metrics(
        config=repo_config,
        data_source=source,
        feature_columns=[("conv_rate", "numeric")],
        timestamp_field="event_timestamp",
    )

    assert len(metrics) == 1
    conv_metrics = metrics[0]
    assert conv_metrics["feature_name"] == "conv_rate"
    assert conv_metrics["row_count"] == 2
    assert conv_metrics["mean"] == pytest.approx(sum(CURRENT_CONV_RATES) / 2)


# ------------------------------------------------------------------ #
# Write guard
# ------------------------------------------------------------------ #


def test_offline_write_batch_delta_raises_not_implemented(tmp_path: Path) -> None:
    delta_path = _make_overwritten_delta_table(tmp_path)

    source = _delta_file_source(delta_path)
    repo_config = _mock_repo_config(repo_path=tmp_path)
    fv = _mock_feature_view(source)

    table = pa.Table.from_pandas(
        pd.DataFrame(
            {
                "driver_id": pd.array([1001], dtype="int64"),
                "conv_rate": pd.array([0.5], dtype="float32"),
                "event_timestamp": pd.to_datetime(
                    [datetime(2026, 1, 3, tzinfo=timezone.utc)]
                ),
            }
        )
    )

    old_version = DeltaTable(delta_path).version()
    old_files = set(_physical_part_files(delta_path))
    with pytest.raises(NotImplementedError, match="DeltaFormat"):
        DaskOfflineStore.offline_write_batch(
            config=repo_config,
            feature_view=fv,
            table=table,
            progress=None,
        )
    assert DeltaTable(delta_path).version() == old_version
    assert set(_physical_part_files(delta_path)) == old_files


# ------------------------------------------------------------------ #
# Saved datasets: persist guard and create/retrieve roundtrip
# ------------------------------------------------------------------ #


def _retrieval_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "driver_id": pd.array([1001, 1002], dtype="int64"),
            "conv_rate": pd.array([0.5, 0.6], dtype="float32"),
            "event_timestamp": pd.to_datetime(
                [datetime(2026, 1, 4, tzinfo=timezone.utc)] * 2
            ),
        }
    )


def _retrieval_job(repo_path: Path) -> DaskRetrievalJob:
    return DaskRetrievalJob(
        evaluation_function=lambda: dd.from_pandas(_retrieval_df(), npartitions=1),
        full_feature_names=False,
        repo_path=str(repo_path),
    )


def test_persist_rejects_delta_saved_dataset_storage(tmp_path: Path) -> None:
    job = _retrieval_job(tmp_path)
    storage = SavedDatasetFileStorage(
        path=str(tmp_path / "saved_dataset"), file_format=DeltaFormat()
    )

    with pytest.raises(NotImplementedError, match="DeltaFormat"):
        job.persist(storage)

    # The mismatch is rejected before anything is written.
    assert not os.path.exists(str(tmp_path / "saved_dataset"))


def test_saved_dataset_parquet_roundtrip(tmp_path: Path) -> None:
    job = _retrieval_job(tmp_path)
    storage = SavedDatasetFileStorage(path=str(tmp_path / "saved_dataset"))

    job.persist(storage)

    # Retrieval reads the persisted dataset back through the reader the
    # offline store uses for historical retrieval.
    persisted_source = storage.to_data_source()
    assert isinstance(persisted_source, FileSource)
    retrieved = dask_mod._read_datasource(persisted_source, tmp_path)
    retrieved = retrieved.compute().sort_values("driver_id").reset_index(drop=True)
    original = _retrieval_df().sort_values("driver_id").reset_index(drop=True)
    assert retrieved["driver_id"].tolist() == original["driver_id"].tolist()
    assert retrieved["conv_rate"].tolist() == original["conv_rate"].tolist()
    assert len(retrieved) == len(original)


def test_delta_labelled_parquet_directory_read_error_gives_recovery_guidance(
    tmp_path: Path,
) -> None:
    from deltalake.exceptions import TableNotFoundError

    # A saved-dataset directory as written by DaskRetrievalJob.persist before
    # this change: plain Parquet files saved under a DeltaFormat declaration.
    parquet_dir = str(tmp_path / "legacy_saved_dataset")
    os.makedirs(parquet_dir)
    pq.write_table(
        pa.table({"driver_id": pa.array([1001], type=pa.int64())}),
        os.path.join(parquet_dir, "part-0.parquet"),
    )

    with pytest.raises(TableNotFoundError, match="earlier Feast version"):
        dask_mod._read_datasource(_delta_file_source(parquet_dir), tmp_path)
