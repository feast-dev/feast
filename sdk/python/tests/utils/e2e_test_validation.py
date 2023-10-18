import math
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pytest
import yaml
from pytz import utc

from feast import FeatureStore, FeatureView, FileSource, RepoConfig
from feast.data_format import ParquetFormat
from feast.entity import Entity
from feast.field import Field
from feast.infra.registry.registry import Registry
from feast.types import Array, Bytes, Int64, String
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
    now = datetime.utcnow()

    full_feature_names = True
    check_offline_store: bool = True

    # Run materialize()
    # use both tz-naive & tz-aware timestamps to test that they're both correctly handled
    start_date = (now - timedelta(hours=5)).replace(tzinfo=utc)
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
            assert (
                abs(response_dict[f"{fv.name}__value"][0] - expected_value) < 1e-6
            ), f"Response: {response_dict}, Expected: {expected_value}"
        else:
            assert response_dict[f"{fv.name}__value"][0] is None
    else:
        if expected_value:
            assert response_dict["value"][0], f"Response: {response_dict}"
            assert (
                abs(response_dict["value"][0] - expected_value) < 1e-6
            ), f"Response: {response_dict}, Expected: {expected_value}"
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
    test_repo_config,
    repo_dir_name: Path,
    offline_creator: DataSourceCreator,
):

    offline_store_config = offline_creator.create_offline_store_config()
    online_store = test_repo_config.online_store

    config = RepoConfig(
        registry=str(Path(repo_dir_name) / "registry.db"),
        project=project,
        provider=test_repo_config.provider,
        offline_store=offline_store_config,
        online_store=online_store,
        repo_path=str(Path(repo_dir_name)),
        entity_key_serialization_version=2,
    )
    config_dict = config.dict()
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


def validate_registry_data_source_apply(test_registry: Registry):
    # Create Feature Views
    batch_source = FileSource(
        name="test_source",
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )

    entity = Entity(name="fs1_my_entity_1", join_keys=["test"])

    fv1 = FeatureView(
        name="my_feature_view_1",
        schema=[
            Field(name="fs1_my_feature_1", dtype=Int64),
            Field(name="fs1_my_feature_2", dtype=String),
            Field(name="fs1_my_feature_3", dtype=Array(String)),
            Field(name="fs1_my_feature_4", dtype=Array(Bytes)),
        ],
        entities=[entity],
        tags={"team": "matchmaking"},
        source=batch_source,
        ttl=timedelta(minutes=5),
    )

    project = "project"

    # Register data source and feature view
    test_registry.apply_data_source(batch_source, project, commit=False)
    test_registry.apply_feature_view(fv1, project, commit=True)

    registry_feature_views = test_registry.list_feature_views(project)
    registry_data_sources = test_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_data_source = registry_data_sources[0]
    assert registry_data_source == batch_source

    # Check that change to batch source propagates
    batch_source.timestamp_field = "new_ts_col"
    test_registry.apply_data_source(batch_source, project, commit=False)
    test_registry.apply_feature_view(fv1, project, commit=True)
    registry_feature_views = test_registry.list_feature_views(project)
    registry_data_sources = test_registry.list_data_sources(project)
    assert len(registry_feature_views) == 1
    assert len(registry_data_sources) == 1
    registry_feature_view = registry_feature_views[0]
    assert registry_feature_view.batch_source == batch_source
    registry_batch_source = test_registry.list_data_sources(project)[0]
    assert registry_batch_source == batch_source

    test_registry.teardown()

    # Will try to reload registry, which will fail because the file has been deleted
    with pytest.raises(FileNotFoundError):
        test_registry._get_registry_proto(project=project)
