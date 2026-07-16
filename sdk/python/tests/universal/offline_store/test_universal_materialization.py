from datetime import timedelta

import pytest

from feast import Entity, FeatureView, Field
from feast.types import Float32
from tests.data.data_creator import create_basic_driver_dataset
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("materialization_pull_latest", [True, False])
def test_universal_materialization_consistency(
    environment, materialization_pull_latest
):
    environment.materialization.pull_latest_features = materialization_pull_latest

    fs = environment.feature_store
    df = create_basic_driver_dataset()
    ds = environment.data_source_creator.create_data_source(
        df,
        fs.project,
        field_mapping={"ts_1": "ts"},
    )
    driver = Entity(
        name="driver_id",
        join_keys=["driver_id"],
    )
    driver_stats_fv = FeatureView(
        name="driver_hourly_stats",
        entities=[driver],
        ttl=timedelta(weeks=52),
        schema=[Field(name="value", dtype=Float32)],
        source=ds,
    )
    fs.apply([driver, driver_stats_fv])
    split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)
    validate_offline_online_store_consistency(fs, driver_stats_fv, split_dt)
