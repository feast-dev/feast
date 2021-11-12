from datetime import datetime

import pandas as pd
import pytest

from feast import Feature, ValueType
from feast.errors import SpecifiedFeaturesNotPresentError
from feast.infra.offline_stores.file_source import FileSource
from tests.integration.feature_repos.universal.entities import customer, driver, item
from tests.integration.feature_repos.universal.feature_views import (
    conv_rate_plus_100_feature_view,
    create_conv_rate_request_data_source,
    create_driver_hourly_stats_feature_view,
    create_item_embeddings_feature_view,
    create_similarity_request_data_source,
    similarity_feature_view,
)


@pytest.mark.integration
@pytest.mark.universal
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
    assert len(odfv.features) == 3


@pytest.mark.integration
@pytest.mark.parametrize("infer_features", [True, False], ids=lambda v: str(v))
def test_infer_odfv_list_features(environment, infer_features, tmp_path):
    fake_embedding = [1.0, 1.0]
    items_df = pd.DataFrame(
        data={
            "item_id": [0],
            "embedding_float": [fake_embedding],
            "embedding_double": [fake_embedding],
            "event_timestamp": [pd.Timestamp(datetime.utcnow())],
            "created": [pd.Timestamp(datetime.utcnow())],
        }
    )
    output_path = f"{tmp_path}/items.parquet"
    items_df.to_parquet(output_path)
    fake_items_src = FileSource(
        path=output_path,
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    items = create_item_embeddings_feature_view(fake_items_src)
    sim_odfv = similarity_feature_view(
        {"items": items, "input_request": create_similarity_request_data_source()},
        infer_features=infer_features,
    )
    store = environment.feature_store
    store.apply([item(), items, sim_odfv])
    odfv = store.get_on_demand_feature_view("similarity")
    assert len(odfv.features) == 2


@pytest.mark.integration
@pytest.mark.universal
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
