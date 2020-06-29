import pytest
from feast.client import Client

import uuid

from ds_example_feature_data import (
    PRODUCT_IMAGE_FEATURE_SET, create_product_image_features_df,
    PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET, create_product_text_attributes_df,
)
from ds_fraud_feature_data import (
    FRAUD_COUNTS_FEATURE_SET, create_fraud_counts_df,
)

PROJECT_NAME = 'ds_' + uuid.uuid4().hex.upper()[0:6]


@pytest.fixture(scope='module')
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope='module')
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope='module')
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption(
        "allow_dirty").lower() == "true" else False


@pytest.fixture(scope='module')
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url, grpc_connection_timeout_default=20,
                grpc_connection_timeout_apply_key=1200, batch_feature_request_wait_time_seconds=1200)
    client.create_project(PROJECT_NAME)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets(project=PROJECT_NAME)
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.mark.timeout(1200)
@pytest.mark.parametrize("data_frame_generator,feature_set", [
#    (create_product_image_features_df, PRODUCT_IMAGE_FEATURE_SET),
    (create_product_text_attributes_df, PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET),
    (create_fraud_counts_df, FRAUD_COUNTS_FEATURE_SET),
])
def test_ingestion(client, data_frame_generator, feature_set):
    client.set_project(PROJECT_NAME)
    client.apply(feature_set)
    data_frame = data_frame_generator()
    client.ingest(feature_set, data_frame, timeout=1200)
