import pytest

from feast import Feature, ValueType
from feast.errors import SpecifiedFeaturesNotPresentError
from tests.integration.feature_repos.universal.entities import customer, driver
from tests.integration.feature_repos.universal.feature_views import (
    conv_rate_plus_100_feature_view,
    create_conv_rate_request_data_source,
    create_driver_hourly_stats_feature_view,
)


@pytest.mark.integration
@pytest.mark.parametrize("infer_features", [True, False], ids=lambda v: str(v))
def test_infer_odfv_features(environment, universal_data_sources, infer_features):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources

    driver_hourly_stats = create_driver_hourly_stats_feature_view(
        data_sources["driver"]
    )
    request_data_source = create_conv_rate_request_data_source()
    driver_odfv = conv_rate_plus_100_feature_view(
        {"driver": driver_hourly_stats, "input_request": request_data_source},
        infer_features=infer_features,
    )

    feast_objects = [driver_hourly_stats, driver_odfv, driver(), customer()]
    store.apply(feast_objects)
    odfv = store.get_on_demand_feature_view("conv_rate_plus_100")
    assert len(odfv.features) == 2


@pytest.mark.integration
def test_infer_odfv_features_with_error(environment, universal_data_sources):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources

    features = [Feature("conv_rate_plus_200", ValueType.DOUBLE)]
    driver_hourly_stats = create_driver_hourly_stats_feature_view(
        data_sources["driver"]
    )
    request_data_source = create_conv_rate_request_data_source()
    driver_odfv = conv_rate_plus_100_feature_view(
        {"driver": driver_hourly_stats, "input_request": request_data_source},
        features=features,
    )

    feast_objects = [driver_hourly_stats, driver_odfv, driver(), customer()]
    with pytest.raises(SpecifiedFeaturesNotPresentError):
        store.apply(feast_objects)
