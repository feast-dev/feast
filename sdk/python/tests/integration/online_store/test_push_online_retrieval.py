import datetime

import pandas as pd
import pytest

from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_push_features_and_read(environment, universal_data_sources):
    store = environment.feature_store

    (_, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    store.apply([driver(), customer(), location(), *feature_views.values()])

    # TODO(felixwang9817): Note that we choose an entity value of 101 here since it is not included
    # in the existing range of entity values (1-49). This allows us to push data for this test
    # without affecting other tests. This decision is tech debt, and should be resolved by finding a
    # better way to isolate data sources across tests.
    data = {
        "location_id": [101],
        "temperature": [4],
        "event_timestamp": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
        "created": [pd.Timestamp(datetime.datetime.utcnow()).round("ms")],
    }
    df_ingest = pd.DataFrame(data)

    store.push("location_stats_push_source", df_ingest)

    online_resp = store.get_online_features(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 101}],
    )
    online_resp_dict = online_resp.to_dict()
    assert online_resp_dict["location_id"] == [101]
    assert online_resp_dict["temperature"] == [4]
