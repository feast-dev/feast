from datetime import timedelta

import pytest

from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view
from tests.utils.e2e_test_validation import validate_offline_online_store_consistency


@pytest.mark.integration
@pytest.mark.universal_online_stores
@pytest.mark.parametrize("infer_features", [True, False])
def test_e2e_consistency(environment, e2e_data_sources, infer_features):
    fs = environment.feature_store
    df, data_source = e2e_data_sources
    fv = driver_feature_view(
        name=f"test_consistency_{'with_inference' if infer_features else ''}",
        data_source=data_source,
        infer_features=infer_features,
    )

    entity = driver()
    fs.apply([fv, entity])

    # materialization is run in two steps and
    # we use timestamp from generated dataframe as a split point
    split_dt = df["ts_1"][4].to_pydatetime() - timedelta(seconds=1)

    validate_offline_online_store_consistency(fs, fv, split_dt)
