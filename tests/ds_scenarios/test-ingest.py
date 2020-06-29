import sys
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

@pytest.fixture(scope='module')
def project_name(pytestconfig):
    return pytestconfig.getoption("project_name")


@pytest.fixture(scope='module')
def initial_entity_id(pytestconfig):
    return int(pytestconfig.getoption("initial_entity_id"))


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
def client(core_url, serving_url, allow_dirty, project_name):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url)

    if not project_name:
      project_name = 'ds_' + uuid.uuid4().hex.upper()[0:6]

    client.create_project(project_name)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets(project=project_name)
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.mark.timeout(600)
@pytest.mark.parametrize("data_frame_generator,feature_set", [
#    (create_product_image_features_df, PRODUCT_IMAGE_FEATURE_SET),
    (create_product_text_attributes_df, PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET),
    (create_fraud_counts_df, FRAUD_COUNTS_FEATURE_SET),
])
def test_ingestion(client, initial_entity_id, data_frame_generator, feature_set):
    client.apply(feature_set)
    data_frame = data_frame_generator(initial_entity_id)
    client.ingest(feature_set, data_frame)
