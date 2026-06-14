import pandas as pd
import pytest

from feast.utils import _utc_now
from tests.universal.feature_repos.repo_configuration import (
    construct_universal_feature_views,
)
from tests.universal.feature_repos.universal.entities import location


@pytest.fixture
def store(environment, universal_data_sources):
    store = environment.feature_store
    _, _, data_sources = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)
    location_fv = feature_views.pushed_locations
    store.apply([location(), location_fv])
    return store


def _ingest_df():
    data = {
        "location_id": [1],
        "temperature": [4],
        "event_timestamp": [pd.Timestamp(_utc_now()).round("ms")],
        "created": [pd.Timestamp(_utc_now()).round("ms")],
    }
    return pd.DataFrame(data)


def assert_response(online_resp):
    online_resp_dict = online_resp.to_dict()
    assert online_resp_dict["location_id"] == [1]
    assert online_resp_dict["temperature"] == [4]


@pytest.mark.integration
@pytest.mark.universal_online_stores
def test_push_features_and_read(store):
    store.push("location_stats_push_source", _ingest_df())

    online_resp = store.get_online_features(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 1}],
    )
    assert_response(online_resp)


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["mongodb"])
async def test_push_features_and_read_async(store):
    await store.push_async("location_stats_push_source", _ingest_df())

    online_resp = await store.get_online_features_async(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 1}],
    )
    assert_response(online_resp)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.universal_online_stores
async def test_push_features_and_read_async_dynamodb(dynamodb_local_environment):
    """Async push + async read for DynamoDB with a credential-isolated environment.

    DynamoDB Local 2.x rejects requests that carry an expired AWS session
    token.  In CI, real (possibly expired) STS credentials exist in the
    environment.  The shared ``environment`` fixture resolves credentials
    before the async client is created, so those bad credentials bleed in.

    This test uses ``dynamodb_local_environment``, which sets dummy
    credentials *before* any boto client is instantiated, guaranteeing that
    both the sync boto3 table-provisioning client and the async aiobotocore
    client start with clean, token-free credentials.
    """
    environment, universal_test_data = dynamodb_local_environment
    store = environment.feature_store
    _, _, data_sources = universal_test_data

    feature_views = construct_universal_feature_views(data_sources)
    location_fv = feature_views.pushed_locations
    store.apply([location(), location_fv])

    await store.push_async("location_stats_push_source", _ingest_df())

    online_resp = await store.get_online_features_async(
        features=["pushable_location_stats:temperature"],
        entity_rows=[{"location_id": 1}],
    )
    assert_response(online_resp)
