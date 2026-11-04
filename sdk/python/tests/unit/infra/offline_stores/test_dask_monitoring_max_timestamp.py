from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.offline_stores import dask as dask_mod
from feast.infra.offline_stores.dask import DaskOfflineStore, DaskOfflineStoreConfig
from feast.infra.offline_stores.file_source import FileSource
from feast.repo_config import RepoConfig


def _config(tmp_path) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        offline_store=DaskOfflineStoreConfig(type="dask"),
        repo_path=tmp_path,
    )


def _max_ts(tmp_path, path):
    return DaskOfflineStore.get_monitoring_max_timestamp(
        config=_config(tmp_path),
        data_source=FileSource(path=str(path), timestamp_field="event_timestamp"),
        timestamp_field="event_timestamp",
    )


def test_returns_the_latest_event_timestamp(tmp_path):
    path = tmp_path / "events.parquet"
    pd.DataFrame(
        {
            "event_timestamp": pd.to_datetime(
                ["2026-09-01T00:00:00Z", "2026-09-30T12:00:00Z"]
            ),
            "conv_rate": [0.1, 0.2],
        }
    ).to_parquet(path)

    assert _max_ts(tmp_path, path) == datetime(2026, 9, 30, 12, tzinfo=timezone.utc)


def test_missing_file_is_still_no_data(tmp_path):
    assert _max_ts(tmp_path, tmp_path / "not_written_yet.parquet") is None


def test_unreadable_file_raises(tmp_path):
    # A file that exists but cannot be read is not "no data for this feature view".
    path = tmp_path / "events.parquet"
    path.write_bytes(b"not a parquet file")

    with pytest.raises(pa.ArrowInvalid, match="magic bytes"):
        _max_ts(tmp_path, path)


def test_storage_error_raises(tmp_path, monkeypatch):
    path = tmp_path / "events.parquet"
    pd.DataFrame(
        {"event_timestamp": pd.to_datetime(["2026-09-30T12:00:00Z"])}
    ).to_parquet(path)

    def _unreachable(*args, **kwargs):
        raise OSError(
            "When reading information for key 'events.parquet': AWS Error NETWORK_CONNECTION"
        )

    monkeypatch.setattr(dask_mod.pq, "read_table", _unreachable)

    with pytest.raises(OSError, match="NETWORK_CONNECTION"):
        _max_ts(tmp_path, path)
