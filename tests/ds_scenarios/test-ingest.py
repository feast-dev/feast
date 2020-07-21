import sys
import uuid

import pytest
from feast.client import Client

from ds_example_feature_data import (
    PRODUCT_IMAGE_FEATURE_SET,
    PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET,
    create_product_image_features_df,
    create_product_text_attributes_df,
)
from ds_fraud_feature_data import FRAUD_COUNTS_FEATURE_SET, create_fraud_counts_df


@pytest.fixture(scope="module")
def project_name(pytestconfig):
    return pytestconfig.getoption("project_name")


@pytest.fixture(scope="module")
def initial_entity_id(pytestconfig):
    return int(pytestconfig.getoption("initial_entity_id"))


_GRPC_CONNECTION_TIMEOUT_DEFAULT = 20

_GRPC_CONNECTION_TIMEOUT_APPLY_KEY = 1200


@pytest.fixture(scope="module")
def core_url(pytestconfig):
    return pytestconfig.getoption("core_url")


@pytest.fixture(scope="module")
def serving_url(pytestconfig):
    return pytestconfig.getoption("serving_url")


@pytest.fixture(scope="module")
def allow_dirty(pytestconfig):
    return True if pytestconfig.getoption("allow_dirty").lower() == "true" else False


@pytest.fixture(scope="module")
def client(core_url, serving_url, allow_dirty, project_name):
    # Get client for core and serving
    client = Client(
        core_url=core_url,
        serving_url=serving_url,
        grpc_connection_timeout_default=_GRPC_CONNECTION_TIMEOUT_DEFAULT,
        grpc_connection_timeout_apply_key=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY,
        batch_feature_request_wait_time_seconds=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY,
    )

    if not project_name:
        project_name = "ds_" + uuid.uuid4().hex.upper()[0:6]

    if project_name not in client.list_projects():
        client.create_project(project_name)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets(project=project_name)
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.mark.timeout(1200)
@pytest.mark.parametrize(
    "data_frame_generator,feature_set",
    [
        #    (create_product_image_features_df, PRODUCT_IMAGE_FEATURE_SET),
        (create_product_text_attributes_df, PRODUCT_TEXT_ATTRIBUTE_FEATURE_SET),
        (create_fraud_counts_df, FRAUD_COUNTS_FEATURE_SET),
    ],
)
def test_ingestion(
    project_name, client, initial_entity_id, data_frame_generator, feature_set
):
    client.set_project(project_name)
    client.apply(feature_set)
    data_frame = data_frame_generator(initial_entity_id)
    client.ingest(feature_set, data_frame, timeout=_GRPC_CONNECTION_TIMEOUT_APPLY_KEY)
