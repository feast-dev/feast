import random
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast.entity import Entity
from feast.feature import Feature
from feast.feature_view import FeatureView
from feast.infra.offline_stores.bigquery import _get_bigquery_client
from feast.value_type import ValueType
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import (
    construct_test_environment,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)

NOW = datetime.now()
TOMORROW = datetime.now() + timedelta(days=1)
YESTERDAY = datetime.now() - timedelta(days=1)


@pytest.fixture
def driver_input_df():
    return pd.DataFrame(
        data=[
            {
                "driver_id": 1,
                "avg_daily_trips": 10.0,
                "event_timestamp": NOW,
                "created_ts": NOW,
            },
            {
                "driver_id": 1,
                "avg_daily_trips": 20.0,
                "event_timestamp": TOMORROW,
                "created_ts": NOW,
            },
            {
                "driver_id": 1,
                "avg_daily_trips": 30.0,
                "event_timestamp": YESTERDAY,
                "created_ts": NOW,
            },
        ]
    )


@pytest.fixture
def entity_df():
    return pd.DataFrame(data=[{"driver_id": 1, "event_timestamp": NOW}])


@pytest.fixture
def expected_df():
    return pd.DataFrame(
        data=[{"driver_id": 1, "avg_daily_trips": 10.0, "event_timestamp": NOW}]
    )


@pytest.fixture
def environment():
    bigquery_env = IntegrationTestRepoConfig(
        provider="gcp",
        offline_store_creator=BigQueryDataSourceCreator,
        online_store="datastore",
    )
    with construct_test_environment(bigquery_env) as e:
        yield e


def get_bigquery_offline_retrieval_job(
    environment: IntegrationTestRepoConfig,
    input_data: pd.DataFrame,
    entity_df: pd.DataFrame,
):
    store = environment.feature_store
    driver_stats_data_source = environment.data_source_creator.create_data_source(
        df=input_data,
        destination_name=f"test_driver_stats_{int(time.time_ns())}_{random.randint(1000, 9999)}",
        event_timestamp_column="event_timestamp",
    )

    driver = Entity(name="driver", join_key="driver_id", value_type=ValueType.INT64)
    driver_fv = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=[Feature(name="avg_daily_trips", dtype=ValueType.INT32)],
        batch_source=driver_stats_data_source,
        ttl=None,
    )

    store.apply([driver, driver_fv])
    return store.get_historical_features(
        entity_df=entity_df,
        features=["driver_stats:avg_daily_trips"],
        full_feature_names=False,
    )


@pytest.mark.integration
def test_bigquery_to_df(environment, driver_input_df, entity_df, expected_df):
    offline_job = get_bigquery_offline_retrieval_job(
        environment=environment, input_data=driver_input_df, entity_df=entity_df
    )
    actual_df = offline_job.to_df()
    pd.testing.assert_frame_equal(
        expected_df.sort_values(by=["driver_id"]).reset_index(drop=True),
        actual_df[expected_df.columns]
        .sort_values(by=["driver_id"])
        .reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.integration
def test_bigquery_to_arrow(environment, driver_input_df, entity_df, expected_df):
    offline_job = get_bigquery_offline_retrieval_job(
        environment=environment, input_data=driver_input_df, entity_df=entity_df
    )
    actual_df = offline_job.to_arrow().to_pandas()
    pd.testing.assert_frame_equal(
        expected_df.sort_values(by=["driver_id"]).reset_index(drop=True),
        actual_df[expected_df.columns]
        .sort_values(by=["driver_id"])
        .reset_index(drop=True),
        check_dtype=False,
    )


@pytest.mark.integration
def test_bigquery_to_bigquery(environment, driver_input_df, entity_df, expected_df):
    offline_job = get_bigquery_offline_retrieval_job(
        environment=environment, input_data=driver_input_df, entity_df=entity_df
    )
    actual_bq_dataset = offline_job.to_bigquery()
    client = _get_bigquery_client()
    actual_df = (
        client.query(f"SELECT * FROM {actual_bq_dataset}").result().to_dataframe()
    )
    pd.testing.assert_frame_equal(
        expected_df.sort_values(by=["driver_id"]).reset_index(drop=True),
        actual_df[expected_df.columns]
        .sort_values(by=["driver_id"])
        .reset_index(drop=True),
        check_dtype=False,
    )
