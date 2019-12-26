import random
import time
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest
import pytz
from feast.client import Client
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet
from feast.type_map import ValueType
from google.protobuf.duration_pb2 import Duration

pd.set_option('display.max_columns', None)

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
def client(core_url, serving_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url, serving_url=serving_url)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception("Feast cannot have existing feature sets registered. Exiting tests.")

    return client


def test_order_by_creation_time(client):
    proc_time_fs = FeatureSet(
        "processing_time",
        features=[Feature("feature_value", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(proc_time_fs)
    time.sleep(10)
    proc_time_fs = client.get_feature_set(name="processing_time", version=1)

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    N_ROWS = 10
    incorrect_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value": ["WRONG"] * N_ROWS,
        }
    )
    correct_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value": ["CORRECT"] * N_ROWS,
        }
    )
    client.ingest(proc_time_fs, incorrect_df)
    time.sleep(10)
    client.ingest(proc_time_fs, correct_df)
    feature_retrieval_job = client.get_batch_features(
        entity_rows=incorrect_df[["datetime", "entity_id"]], feature_ids=["processing_time:1:feature_value"]
    )
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["processing_time_v1_feature_value"].to_list() == ["CORRECT"] * N_ROWS


def test_additional_columns_in_entity_table(client):
    add_cols_fs = FeatureSet(
        "additional_columns",
        features=[Feature("feature_value", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(add_cols_fs)
    time.sleep(10)
    add_cols_fs = client.get_feature_set(name="additional_columns", version=1)

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_df = pd.DataFrame(
        {"datetime": [time_offset] * N_ROWS, "entity_id": [i for i in range(N_ROWS)], "feature_value": ["abc"] * N_ROWS}
    )
    client.ingest(add_cols_fs, features_df)

    entity_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "additional_string_col": ["hello im extra"] * N_ROWS,
            "additional_float_col": [random.random() for i in range(N_ROWS)],
        }
    )
    feature_retrieval_job = client.get_batch_features(
        entity_rows=entity_df, feature_ids=["additional_columns:1:feature_value"]
    )
    output = feature_retrieval_job.to_dataframe().sort_values(by=["entity_id"])
    print(output.head(10))

    assert np.allclose(output["additional_float_col"], entity_df["additional_float_col"])
    assert output["additional_string_col"].to_list() == entity_df["additional_string_col"].to_list()
    assert output["additional_columns_v1_feature_value"].to_list() == features_df["feature_value"].to_list()


def test_point_in_time_correctness_join(client):
    historical_fs = FeatureSet(
        "historical",
        features=[Feature("feature_value", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(historical_fs)
    time.sleep(10)
    historical_fs = client.get_feature_set(name="historical", version=1)

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    N_EXAMPLES = 10
    historical_df = pd.DataFrame(
        {
            "datetime": [
                time_offset - timedelta(seconds=50),
                time_offset - timedelta(seconds=30),
                time_offset - timedelta(seconds=10),
            ]
            * N_EXAMPLES,
            "entity_id": [i for i in range(N_EXAMPLES) for _ in range(3)],
            "feature_value": ["WRONG", "WRONG", "CORRECT"] * N_EXAMPLES,
        }
    )
    entity_df = pd.DataFrame(
        {"datetime": [time_offset - timedelta(seconds=10)] * N_EXAMPLES, "entity_id": [i for i in range(N_EXAMPLES)]}
    )

    client.ingest(historical_fs, historical_df)

    feature_retrieval_job = client.get_batch_features(entity_rows=entity_df, feature_ids=["historical:1:feature_value"])
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["historical_v1_feature_value"].to_list() == ["CORRECT"] * N_EXAMPLES


def test_multiple_featureset_joins(client):
    fs1 = FeatureSet(
        "feature_set_1",
        features=[Feature("feature_value", ValueType.STRING)],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )

    fs2 = FeatureSet(
        "feature_set_2",
        features=[Feature("other_feature_value", ValueType.INT64)],
        entities=[Entity("other_entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )

    client.apply(fs1)
    time.sleep(10)
    fs1 = client.get_feature_set(name="feature_set_1", version=1)

    client.apply(fs2)
    time.sleep(10)
    fs2 = client.get_feature_set(name="feature_set_2", version=1)

    N_ROWS = 10
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    features_1_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "feature_value": [f"{i}" for i in range(N_ROWS)],
        }
    )
    client.ingest(fs1, features_1_df)

    features_2_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "other_entity_id": [i for i in range(N_ROWS)],
            "other_feature_value": [i for i in range(N_ROWS)],
        }
    )
    client.ingest(fs2, features_2_df)

    entity_df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "other_entity_id": [N_ROWS - 1 - i for i in range(N_ROWS)],
        }
    )
    feature_retrieval_job = client.get_batch_features(
        entity_rows=entity_df, feature_ids=["feature_set_1:1:feature_value", "feature_set_2:1:other_feature_value"]
    )
    output = feature_retrieval_job.to_dataframe()
    print(output.head())

    assert output["entity_id"].to_list() == [int(i) for i in output["feature_set_1_v1_feature_value"].to_list()]
    assert output["other_entity_id"].to_list() == output["feature_set_2_v1_other_feature_value"].to_list()
