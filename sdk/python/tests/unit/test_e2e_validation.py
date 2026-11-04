from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from feast import FeatureStore, RepoConfig
from feast.infra.offline_stores.file_source import FileSource
from tests.data.data_creator import create_basic_driver_dataset
from tests.universal.feature_repos.universal.entities import driver
from tests.universal.feature_repos.universal.feature_views import driver_feature_view
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.parametrize("setup_seconds", [0, 2, 3602])
def test_consistency_validation_survives_hour_rollover(
    tmp_path: Path, setup_seconds: int
) -> None:
    fixture_time = datetime.now(timezone.utc).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(seconds=1)
    with patch("tests.data.data_creator._utc_now", return_value=fixture_time):
        df = create_basic_driver_dataset()
    source_path = tmp_path / "drivers.parquet"
    df.to_parquet(source_path)
    source = FileSource(
        path=str(source_path),
        timestamp_field="ts",
        created_timestamp_column="created_ts",
        field_mapping={"ts_1": "ts"},
    )
    fv = driver_feature_view(data_source=source, infer_features=True)
    fs = FeatureStore(
        config=RepoConfig(
            project="clock_rollover",
            registry=str(tmp_path / "registry.db"),
            provider="local",
            online_store={"type": "sqlite", "path": str(tmp_path / "online.db")},
            offline_store={"type": "file"},
        )
    )
    try:
        fs.apply([driver(), fv])
        split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)
        with (
            patch(
                "tests.utils.e2e_test_validation._utc_now",
                return_value=fixture_time + timedelta(seconds=setup_seconds),
            ),
            patch("tests.utils.e2e_test_validation.time.sleep"),
        ):
            validate_offline_online_store_consistency(fs, fv, split_dt)
    finally:
        fs.teardown()
