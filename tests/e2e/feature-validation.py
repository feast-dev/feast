import pandas as pd
import pytest
import pytz
import uuid
from datetime import datetime, timedelta

from feast.client import Client
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_set import FeatureSet
from feast.type_map import ValueType
from google.protobuf.duration_pb2 import Duration
import tensorflow_data_validation as tfdv
from deepdiff import DeepDiff
from google.protobuf.json_format import MessageToDict


pd.set_option("display.max_columns", None)

PROJECT_NAME = "batch_" + uuid.uuid4().hex.upper()[0:6]
STORE_NAME = "historical"


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
def feature_validation_feature_set(client):
    fv_fs = FeatureSet(
        "feature_validation",
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
def dataset_basic(client, feature_validation_feature_set):

    N_ROWS = 20

    time_offset = datetime.utcnow().replace(tzinfo=pytz.utc)
    df = pd.DataFrame(
        {
            "datetime": [time_offset] * N_ROWS,
            "entity_id": [i for i in range(N_ROWS)],
            "strings": ["a", "b"] * int(N_ROWS / 2),
            "ints": [int(i) for i in range(N_ROWS)],
            "floats": [10.5 - i for i in range(N_ROWS)],
        }
    )

    expected_stats = tfdv.generate_statistics_from_dataframe(
        df[["entity_id", "strings", "ints", "floats"]]
    )
    clear_unsupported_fields(expected_stats)

    return {
        "df": df,
        "id": client.ingest(feature_validation_feature_set, df),
        "date": datetime(time_offset.year, time_offset.month, time_offset.day).replace(
            tzinfo=pytz.utc
        ),
        "stats": expected_stats,
    }


@pytest.fixture(scope="module")
def dataset_agg(client, feature_validation_feature_set):
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
    dataset_id_1 = client.ingest(feature_validation_feature_set, df1)
    df2 = pd.DataFrame(
        {
            "datetime": [start_date + timedelta(days=1)] * 3,
            "entity_id": [i for i in range(3)],
            "strings": ["a", "b", "c"],
            "ints": [2, 6, 7],
            "floats": [1.6, 2.4, 2],
        }
    )
    dataset_id_2 = client.ingest(feature_validation_feature_set, df2)

    combined_df = pd.concat([df1, df2])[["entity_id", "strings", "ints", "floats"]]
    expected_stats = tfdv.generate_statistics_from_dataframe(combined_df)
    clear_unsupported_agg_fields(expected_stats)

    # Temporary until TFDV fixes their std dev computation
    for feature in expected_stats.datasets[0].features:
        if feature.HasField("num_stats"):
            name = feature.path.step[0]
            std = combined_df[name].std()
            feature.num_stats.std_dev = std

    return {
        "ids": [dataset_id_1, dataset_id_2],
        "start_date": datetime(
            start_date.year, start_date.month, start_date.day
        ).replace(tzinfo=pytz.utc),
        "end_date": datetime(end_date.year, end_date.month, end_date.day).replace(
            tzinfo=pytz.utc
        ),
        "stats": expected_stats,
    }


def test_basic_retrieval_by_single_dataset(client, dataset_basic):
    stats = client.get_statistics(
        f"{PROJECT_NAME}/feature_validation:1",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        dataset_ids=[dataset_basic["id"]],
    )

    assert_stats_equal(dataset_basic["stats"], stats)


def test_basic_by_date(client, dataset_basic):
    stats = client.get_statistics(
        f"{PROJECT_NAME}/feature_validation:1",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        start_date=dataset_basic["date"],
        end_date=dataset_basic["date"] + timedelta(days=1),
    )
    assert_stats_equal(dataset_basic["stats"], stats)


def test_agg_over_datasets(client, dataset_agg):
    stats = client.get_statistics(
        f"{PROJECT_NAME}/feature_validation:1",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        dataset_ids=[dataset_basic["ids"]],
    )
    assert_stats_equal(dataset_basic["stats"], stats)


def test_agg_over_dates(client, dataset_agg):
    stats = client.get_statistics(
        f"{PROJECT_NAME}/feature_validation:1",
        features=["strings", "ints", "floats"],
        store=STORE_NAME,
        start_date=dataset_agg["start_date"],
        end_date=dataset_agg["end_date"],
    )
    assert_stats_equal(dataset_agg["stats"], stats)


def test_force_refresh(client, dataset_basic, feature_validation_feature_set):
    df = dataset_basic["df"]

    df2 = pd.DataFrame(
        {
            "datetime": [df.iloc[0].datetime],
            "entity_id": [10],
            "strings": ["c"],
            "ints": [2],
            "floats": [1.3],
        }
    )
    client.ingest(feature_validation_feature_set, df2)

    actual_stats = client.get_statistics(
        f"{PROJECT_NAME}/feature_validation:1",
        features=["strings", "ints", "floats"],
        store="historical",
        start_date=dataset_basic["date"],
        end_date=dataset_basic["date"] + timedelta(days=1),
        force_refresh=True,
    )

    combined_df = pd.concat([df, df2])
    expected_stats = tfdv.generate_statistics_from_dataframe(combined_df)
    clear_unsupported_fields(expected_stats)

    assert_stats_equal(expected_stats, actual_stats)


def clear_unsupported_fields(datasets):
    dataset = datasets.datasets[0]
    for feature in dataset.features:
        if feature.HasField("num_stats"):
            feature.num_stats.common_stats.ClearField("num_values_histogram")
        elif feature.HasField("string_stats"):
            feature.string_stats.common_stats.ClearField("num_values_histogram")
            for bucket in feature.string_stats.rank_histogram.buckets:
                bucket.ClearField("low_rank")
                bucket.ClearField("high_rank")
        elif feature.HasField("struct_stats"):
            feature.string_stats.struct_stats.ClearField("num_values_histogram")
        elif feature.HasField("bytes_stats"):
            feature.string_stats.bytes_stats.ClearField("num_values_histogram")


def clear_unsupported_agg_fields(datasets):
    dataset = datasets.datasets[0]
    for feature in dataset.features:
        if feature.HasField("num_stats"):
            feature.num_stats.common_stats.ClearField("num_values_histogram")
            feature.num_stats.ClearField("histograms")
        elif feature.HasField("string_stats"):
            feature.string_stats.common_stats.ClearField("num_values_histogram")
            feature.string_stats.ClearField("histograms")
            feature.string_stats.ClearField("rank_histogram")
            feature.string_stats.ClearField("top_values")
            feature.string_stats.ClearField("unique")
        elif feature.HasField("struct_stats"):
            feature.string_stats.struct_stats.ClearField("num_values_histogram")
        elif feature.HasField("bytes_stats"):
            feature.string_stats.bytes_stats.ClearField("num_values_histogram")


def assert_stats_equal(left, right):
    left_stats = MessageToDict(left)["datasets"][0]
    right_stats = MessageToDict(right)["datasets"][0]
    assert (
        left_stats["numExamples"] == right_stats["numExamples"]
    ), f"Number of examples do not match. Expected {left_stats['numExamples']}, got {right_stats['numExamples']}"

    left_features = sorted(left_stats["features"], key=lambda k: k["path"]["step"][0])
    right_features = sorted(right_stats["features"], key=lambda k: k["path"]["step"][0])
    diff = DeepDiff(left_features, right_features)
    assert len(diff) == 0, f"Statistics do not match: \n{diff}"
