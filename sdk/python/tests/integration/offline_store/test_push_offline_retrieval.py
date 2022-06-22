import datetime

import numpy as np
import pandas as pd
import pytest

from feast.data_source import PushMode
from tests.integration.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    location,
)


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.universal_online_stores(only=["sqlite"])
def test_push_features_and_read_from_offline_store(environment, universal_data_sources):
    store = environment.feature_store

    (_, _, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    now = pd.Timestamp(datetime.datetime.utcnow()).round("ms")

    store.apply([driver(), customer(), location(), *feature_views.values()])
    entity_df = pd.DataFrame.from_dict({"location_id": [1], "event_timestamp": [now]})

    before_df = store.get_historical_features(
        entity_df=entity_df,
        features=["pushable_location_stats:temperature"],
        full_feature_names=False,
    ).to_df()

    data = {
        "event_timestamp": [now],
        "location_id": [1],
        "temperature": [4],
        "created": [now],
    }
    df_ingest = pd.DataFrame(data)
    assert np.where(
        before_df["location_id"].reset_index(drop=True)
        == df_ingest["location_id"].reset_index(drop=True)
    )
    assert np.where(
        before_df["temperature"].reset_index(drop=True)
        != df_ingest["temperature"].reset_index(drop=True)
    )

    store.push("location_stats_push_source", df_ingest, to=PushMode.OFFLINE)

    df = store.get_historical_features(
        entity_df=entity_df,
        features=["pushable_location_stats:temperature"],
        full_feature_names=False,
    ).to_df()
    assert np.where(
        df["location_id"].reset_index(drop=True)
        == df_ingest["location_id"].reset_index(drop=True)
    )
    assert np.where(
        df["temperature"].reset_index(drop=True)
        == df_ingest["temperature"].reset_index(drop=True)
    )
