import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import yaml

from feast import FeatureStore, FeatureView, RepoConfig
from feast.utils import _utc_now
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
    FileParquetDatasetSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)


def validate_offline_online_store_consistency(
    fs: FeatureStore, fv: FeatureView, split_dt: datetime
) -> None:
    now = _utc_now()

    full_feature_names = True
    check_offline_store: bool = True

    # Run materialize()
    # use both tz-naive & tz-aware timestamps to test that they're both correctly handled
    start_date = (now - timedelta(hours=5)).replace(tzinfo=timezone.utc)
    end_date = split_dt
    fs.materialize(feature_views=[fv.name], start_date=start_date, end_date=end_date)

    time.sleep(10)

    # check result of materialize()
    _check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=1,
        event_timestamp=end_date,
        expected_value=0.3,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )

    _check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=2,
        event_timestamp=end_date,
        expected_value=None,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )

    # check prior value for materialize_incremental()
    _check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=3,
        event_timestamp=end_date,
        expected_value=4,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )

    # run materialize_incremental()
    fs.materialize_incremental(feature_views=[fv.name], end_date=now)
    updated_fv = fs.registry.get_feature_view(fv.name, fs.project)

    # Check if materialization_intervals was updated by the registry
    assert (
        len(updated_fv.materialization_intervals) == 2
        and updated_fv.materialization_intervals[0][0] == start_date
        and updated_fv.materialization_intervals[0][1] == end_date
        and updated_fv.materialization_intervals[1][0] == end_date
        and updated_fv.materialization_intervals[1][1]
        == now.replace(tzinfo=timezone.utc)
    )

    # check result of materialize_incremental()
    _check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=3,
        event_timestamp=now,
        expected_value=5,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )


def _check_offline_and_online_features(
    fs: FeatureStore,
    fv: FeatureView,
    driver_id: int,
    event_timestamp: datetime,
    expected_value: Optional[float],
    full_feature_names: bool,
    check_offline_store: bool = True,
) -> None:
    # Check online store
    response_dict = fs.get_online_features(
        [f"{fv.name}:value"],
        [{"driver_id": driver_id}],
        full_feature_names=full_feature_names,
    ).to_dict()

    # Wait for materialization to occur
    if not response_dict[f"{fv.name}__value"][0]:
        # Deal with flake with a retry
        time.sleep(10)
        response_dict = fs.get_online_features(
            [f"{fv.name}:value"],
            [{"driver_id": driver_id}],
            full_feature_names=full_feature_names,
        ).to_dict()

    if full_feature_names:
        if expected_value:
            assert response_dict[f"{fv.name}__value"][0], f"Response: {response_dict}"
            assert abs(response_dict[f"{fv.name}__value"][0] - expected_value) < 1e-6, (
                f"Response: {response_dict}, Expected: {expected_value}"
            )
        else:
            assert response_dict[f"{fv.name}__value"][0] is None
    else:
        if expected_value:
            assert response_dict["value"][0], f"Response: {response_dict}"
            assert abs(response_dict["value"][0] - expected_value) < 1e-6, (
                f"Response: {response_dict}, Expected: {expected_value}"
            )
        else:
            assert response_dict["value"][0] is None

    # Check offline store
    if check_offline_store:
        df = fs.get_historical_features(
            entity_df=pd.DataFrame.from_dict(
                {"driver_id": [driver_id], "event_timestamp": [event_timestamp]}
            ),
            features=[f"{fv.name}:value"],
            full_feature_names=full_feature_names,
        ).to_df()

        if full_feature_names:
            if expected_value:
                assert (
                    abs(
                        df.to_dict(orient="list")[f"{fv.name}__value"][0]
                        - expected_value
                    )
                    < 1e-6
                )
            else:
                assert not df.to_dict(orient="list")[f"{fv.name}__value"] or math.isnan(
                    df.to_dict(orient="list")[f"{fv.name}__value"][0]
                )
        else:
            if expected_value:
                assert (
                    abs(df.to_dict(orient="list")["value"][0] - expected_value) < 1e-6
                )
            else:
                assert not df.to_dict(orient="list")["value"] or math.isnan(
                    df.to_dict(orient="list")["value"][0]
                )


def make_feature_store_yaml(
    project,
    repo_dir_name: Path,
    offline_creator: DataSourceCreator,
    provider: str,
    online_store: Optional[Union[str, Dict]],
):
    offline_store_config = offline_creator.create_offline_store_config()

    config = RepoConfig(
        registry=str(Path(repo_dir_name) / "registry.db"),
        project=project,
        provider=provider,
        offline_store=offline_store_config,
        online_store=online_store,
        repo_path=str(Path(repo_dir_name)),
        entity_key_serialization_version=3,
    )
    config_dict = config.model_dump(by_alias=True)
    if (
        isinstance(config_dict["online_store"], dict)
        and "redis_type" in config_dict["online_store"]
    ):
        if str(config_dict["online_store"]["redis_type"]) == "RedisType.redis_cluster":
            config_dict["online_store"]["redis_type"] = "redis_cluster"
        elif str(config_dict["online_store"]["redis_type"]) == "RedisType.redis":
            config_dict["online_store"]["redis_type"] = "redis"
    config_dict["repo_path"] = str(config_dict["repo_path"])
    return yaml.safe_dump(config_dict)


NULLABLE_ONLINE_STORE_CONFIGS: List[IntegrationTestRepoConfig] = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=FileDataSourceCreator,
        online_store=None,
    ),
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=FileParquetDatasetSourceCreator,
        online_store=None,
    ),
]

# Only test if this is NOT a local test
if os.getenv("FEAST_IS_LOCAL_TEST", "False") != "True":
    NULLABLE_ONLINE_STORE_CONFIGS.extend(
        [
            IntegrationTestRepoConfig(
                provider="gcp",
                offline_store_creator=BigQueryDataSourceCreator,
                online_store=None,
            ),
            IntegrationTestRepoConfig(
                provider="aws",
                offline_store_creator=RedshiftDataSourceCreator,
                online_store=None,
            ),
        ]
    )
