import pytest

from feast.infra.offline_stores.duckdb import _read_data_source
from feast.infra.offline_stores.file_source import FileSource


def test_read_data_source_raises_on_unresolvable_file_format():
    data_source = FileSource(
        name="driver_hourly_stats_source",
        path="data/driver_stats.csv",
        timestamp_field="event_timestamp",
    )

    with pytest.raises(ValueError, match="Unable to determine the file format"):
        _read_data_source(data_source, repo_path=".")
