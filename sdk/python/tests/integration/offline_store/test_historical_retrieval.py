import os
import random
import string
import time
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest
from google.cloud import bigquery
from pandas.testing import assert_frame_equal
from pytz import utc

import feast.driver_test_data as driver_data
from feast import BigQuerySource, FeatureService, FileSource, RepoConfig, utils
from feast.entity import Entity
from feast.errors import FeatureNameCollisionError
from feast.feature import Feature
from feast.feature_store import FeatureStore, _validate_feature_refs
from feast.feature_view import FeatureView
from feast.infra.offline_stores.bigquery import (
    BigQueryOfflineStoreConfig,
    _write_df_to_bq,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.value_type import ValueType

np.random.seed(0)

def test_feature_name_collision_on_historical_retrieval():

    # _validate_feature_refs is the function that checks for colliding feature names
    # check when feature names collide and 'full_feature_names=False'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=False,
        )

    expected_error_message = (
        "Duplicate features named avg_daily_trips found.\n"
        "To resolve this collision, either use the full feature name by setting "
        "'full_feature_names=True', or ensure that the features in question have different names."
    )

    assert str(error.value) == expected_error_message

    # check when feature names collide and 'full_feature_names=True'
    with pytest.raises(FeatureNameCollisionError) as error:
        _validate_feature_refs(
            feature_refs=[
                "driver_stats:conv_rate",
                "driver_stats:avg_daily_trips",
                "driver_stats:avg_daily_trips",
                "customer_profile:current_balance",
                "customer_profile:avg_passenger_count",
                "customer_profile:lifetime_trip_count",
                "customer_profile:avg_daily_trips",
            ],
            full_feature_names=True,
        )

    expected_error_message = (
        "Duplicate features named driver_stats__avg_daily_trips found.\n"
        "To resolve this collision, please ensure that the features in question "
        "have different names."
    )
    assert str(error.value) == expected_error_message
