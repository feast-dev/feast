import datetime

import pandas as pd
import pytest

from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import location


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_push_features_and_read(environment, universal_data_sources):
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    location_fv = feature_views.pushed_locations
    store.apply([location(), location_fv])

    data = {
        "location_id": [1],
        "temperature": [4],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_ingest = pd.DataFrame(data)

    store.push("location_stats_push_source", df_ingest)

    online_resp = store.get_online_features(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 1}],
    )
    online_resp_dict = online_resp.to_dict()
    assert online_resp_dict["location_id"] == [1]
    assert online_resp_dict["temperature"] == [4]
