import os
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
import pytest
import pytz
import tensorflow_data_validation as tfdv
from google.protobuf.duration_pb2 import Duration

from bq.testutils import (
    assert_stats_equal,
    clear_unsupported_agg_fields,
    clear_unsupported_fields,
)
from feast.client import Client
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet
from feast.type_map import ValueType

pd.set_option("display.max_columns", None)

PROJECT_NAME = "batch_" + uuid.uuid4().hex.upper()[0:6]
STORE_NAME = "historical"
os.environ["CUDA_VISIBLE_DEVICES"] = "0"


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
def gcs_path(pytestconfig):
    return pytestconfig.getoption("gcs_path")


@pytest.fixture(scope="module")
def client(core_url, allow_dirty):
    # Get client for core and serving
    client = Client(core_url=core_url)
    client.create_project(PROJECT_NAME)
    client.set_project(PROJECT_NAME)

    # Ensure Feast core is active, but empty
    if not allow_dirty:
        feature_sets = client.list_feature_sets()
        if len(feature_sets) > 0:
            raise Exception(
                "Feast cannot have existing feature sets registered. Exiting tests."
            )

    return client


@pytest.fixture(scope="module")
def feature_stats_feature_set(client):
    fv_fs = FeatureSet(
        "feature_stats",
        features=[
            Feature("strings", ValueType.STRING),
            Feature("ints", ValueType.INT64),
            Feature("floats", ValueType.FLOAT),
        ],
        entities=[Entity("entity_id", ValueType.INT64)],
        max_age=Duration(seconds=100),
    )
    client.apply(fv_fs)
    return fv_fs


@pytest.fixture(scope="module")
def feature_stats_dataset_basic(client, feature_stats_feature_set):

    n_rows = 20

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    df = pd.DataFrame(
        {
            "datetime": [time_offset] * n_rows,
            "entity_id": [i for i in range(n_rows)],
            "strings": ["a", "b"] * int(n_rows / 2),
            "ints": [int(i) for i in range(n_rows)],
            "floats": [10.5 - i for i in range(n_rows)],
        }
    )

    expected_stats = tfdv.generate_statistics_from_dataframe(
        df[["strings", "ints", "floats"]]
    )
    clear_unsupported_fields(expected_stats)

    # Since TFDV computes population std dev
    for feature in expected_stats.datasets[0].features:
        if feature.HasField("num_stats"):
            name = feature.path.step[0]
            std = df[name].std()
            feature.num_stats.std_dev = std

    ingestion_id = client.ingest(feature_stats_feature_set, df)
    time.sleep(10)
    return {
        "df": df,
        "id": ingestion_id,
        "date": datetime(time_offset.year, time_offset.month, time_offset.day).replace(
            tzinfo=pytz.utc
        ),
        "stats": expected_stats,
    }


@pytest.fixture(scope="module")
def feature_stats_dataset_agg(client, feature_stats_feature_set):
    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    start_date = time_offset - timedelta(days=10)
    end_date = time_offset - timedelta(days=7)
    df1 = pd.DataFrame(
        {
            "datetime": [start_date] * 5,
            "entity_id": [i for i in range(5)],
            "strings": ["a", "b", "b", "b", "a"],
            "ints": [4, 3, 2, 6, 3],
            "floats": [2.1, 5.2, 4.3, 0.6, 0.1],
        }
    )
    ingestion_id_1 = client.ingest(feature_stats_feature_set, df1)
    df2 = pd.DataFrame(
        {
            "datetime": [start_date + timedelta(days=1)] * 3,
            "entity_id": [i for i in range(3)],
            "strings": ["a", "b", "c"],
            "ints": [2, 6, 7],
            "floats": [1.6, 2.4, 2],
        }
    )
    ingestion_id_2 = client.ingest(feature_stats_feature_set, df2)

    combined_df = pd.concat([df1, df2])[["strings", "ints", "floats"]]
    expected_stats = tfdv.generate_statistics_from_dataframe(combined_df)
    clear_unsupported_agg_fields(expected_stats)

    # Since TFDV computes population std dev
    for feature in expected_stats.datasets[0].features:
        if feature.HasField("num_stats"):
            name = feature.path.step[0]
            std = combined_df[name].std()
            feature.num_stats.std_dev = std

    time.sleep(10)

    return {
        "ids": [ingestion_id_1, ingestion_id_2],
        "start_date": datetime(
            start_date.year, start_date.month, start_date.day
        ).replace(tzinfo=pytz.utc),
        "end_date": datetime(end_date.year, end_date.month, end_date.day).replace(
            tzinfo=pytz.utc
        ),
        "stats": expected_stats,
    }


def test_feature_stats_retrieval_by_single_dataset(client, feature_stats_dataset_basic):
    stats = client.get_statistics(
        "feature_stats",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        ingestion_ids=[feature_stats_dataset_basic["id"]],
    )

    assert_stats_equal(feature_stats_dataset_basic["stats"], stats)


def test_feature_stats_by_date(client, feature_stats_dataset_basic):
    stats = client.get_statistics(
        "feature_stats",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        start_date=feature_stats_dataset_basic["date"],
        end_date=feature_stats_dataset_basic["date"] + timedelta(days=1),
    )
    assert_stats_equal(feature_stats_dataset_basic["stats"], stats)


def test_feature_stats_agg_over_datasets(client, feature_stats_dataset_agg):
    stats = client.get_statistics(
        "feature_stats",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        ingestion_ids=feature_stats_dataset_agg["ids"],
    )
    assert_stats_equal(feature_stats_dataset_agg["stats"], stats)


def test_feature_stats_agg_over_dates(client, feature_stats_dataset_agg):
    stats = client.get_statistics(
        "feature_stats",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        start_date=feature_stats_dataset_agg["start_date"],
        end_date=feature_stats_dataset_agg["end_date"],
    )
    assert_stats_equal(feature_stats_dataset_agg["stats"], stats)


def test_feature_stats_force_refresh(
    client, feature_stats_dataset_basic, feature_stats_feature_set
):
    df = feature_stats_dataset_basic["df"]

    df2 = pd.DataFrame(
        {
            "datetime": [df.iloc[0].datetime],
            "entity_id": [10],
            "strings": ["c"],
            "ints": [2],
            "floats": [1.3],
        }
    )
    client.ingest(feature_stats_feature_set, df2)
    time.sleep(10)

    actual_stats = client.get_statistics(
        "feature_stats",
        features=["strings", "ints", "floats"],
        store="historical",
        start_date=feature_stats_dataset_basic["date"],
        end_date=feature_stats_dataset_basic["date"] + timedelta(days=1),
        force_refresh=True,
    )

    combined_df = pd.concat([df, df2])
    expected_stats = tfdv.generate_statistics_from_dataframe(combined_df)

    clear_unsupported_fields(expected_stats)

    # Since TFDV computes population std dev
    for feature in expected_stats.datasets[0].features:
        if feature.HasField("num_stats"):
            name = feature.path.step[0]
            std = combined_df[name].std()
            feature.num_stats.std_dev = std

    assert_stats_equal(expected_stats, actual_stats)
