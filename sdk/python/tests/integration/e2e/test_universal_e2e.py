import math
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import pytest
from pytz import utc

from feast import FeatureStore, FeatureView
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view


@pytest.mark.integration
@pytest.mark.parametrize("infer_features", [True, False])
def test_e2e_consistency(environment, e2e_data_sources, infer_features):
    fs = environment.feature_store
    df, data_source = e2e_data_sources
    fv = driver_feature_view(data_source=data_source, infer_features=infer_features)

    entity = driver()
    fs.apply([fv, entity])

    run_offline_online_store_consistency_test(fs, fv)


def check_offline_and_online_features(
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
        [{"driver": driver_id}],
        full_feature_names=full_feature_names,
    ).to_dict()

    if full_feature_names:
        if expected_value:
            assert abs(response_dict[f"{fv.name}__value"][0] - expected_value) < 1e-6
        else:
            assert response_dict[f"{fv.name}__value"][0] is None
    else:
        if expected_value:
            assert abs(response_dict["value"][0] - expected_value) < 1e-6
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
                assert abs(df.to_dict()[f"{fv.name}__value"][0] - expected_value) < 1e-6
            else:
                assert math.isnan(df.to_dict()[f"{fv.name}__value"][0])
        else:
            if expected_value:
                assert abs(df.to_dict()["value"][0] - expected_value) < 1e-6
            else:
                assert math.isnan(df.to_dict()["value"][0])


def run_offline_online_store_consistency_test(
    fs: FeatureStore, fv: FeatureView
) -> None:
    now = datetime.now()

    full_feature_names = True
    check_offline_store: bool = True

    # Run materialize()
    # use both tz-naive & tz-aware timestamps to test that they're both correctly handled
    start_date = (now - timedelta(hours=5)).replace(tzinfo=utc)
    end_date = now - timedelta(hours=2)
    fs.materialize(feature_views=[fv.name], start_date=start_date, end_date=end_date)

    # check result of materialize()
    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=1,
        event_timestamp=end_date,
        expected_value=0.3,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )

    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=2,
        event_timestamp=end_date,
        expected_value=None,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )

    # check prior value for materialize_incremental()
    check_offline_and_online_features(
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
    check_offline_and_online_features(
        fs=fs,
        fv=fv,
        driver_id=3,
        event_timestamp=now,
        expected_value=5,
        full_feature_names=full_feature_names,
        check_offline_store=check_offline_store,
    )
