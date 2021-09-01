from datetime import datetime, timedelta

import pandas as pd
from data.data_creator import get_feature_values_for_dtype
from integration.feature_repos.test_repo_configuration import (
    Environment,
    parametrize_types_no_materialize_test,
    parametrize_types_no_materialize_test_no_list,
)
from integration.feature_repos.universal.entities import driver
from integration.feature_repos.universal.feature_views import driver_feature_view

from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.type_map import python_type_to_feast_value_type
from feast.value_type import ValueType


# TODO: change parametrization to allow for other providers aside from gcp
@parametrize_types_no_materialize_test
def test_entity_inference_types_match(environment: Environment):
    feature_dtype, feature_is_list, fs, fv = get_test_fixtures(environment)
    # Don't specify value type in entity to force inference
    entity = driver(value_type=ValueType.UNKNOWN)
    fs.apply([fv, entity])

    entities = fs.list_entities()
    entity_type_to_expected_inferred_entity_type = {
        ValueType.INT32: ValueType.INT64,
        ValueType.INT64: ValueType.INT64,
        ValueType.FLOAT: ValueType.DOUBLE,
        ValueType.STRING: ValueType.STRING,
    }
    for entity in entities:
        assert (
            entity.value_type
            == entity_type_to_expected_inferred_entity_type[environment.entity_type]
        )


@parametrize_types_no_materialize_test
def test_feature_get_historical_features_types_match(environment: Environment):
    feature_dtype, feature_is_list, fs, fv = get_test_fixtures(environment)
    entity = driver()
    fs.apply([fv, entity])

    features = [f"{fv.name}:value"]
    df = pd.DataFrame()
    df["driver_id"] = (
        ["1", "3"] if environment.entity_type == ValueType.STRING else [1, 3]
    )
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")
    df["ts"] = [
        ts - timedelta(hours=4),
        ts - timedelta(hours=2),
    ]
    historical_features = fs.get_historical_features(entity_df=df, features=features,)

    # TODO(adchia): pandas doesn't play well with nan values in ints. BQ will also coerce to floats if there are NaNs
    historical_features_df = historical_features.to_df()
    print(historical_features_df)
    if feature_is_list:
        assert_feature_list_types(feature_dtype, historical_features_df)
    else:
        assert_expected_historical_feature_types(feature_dtype, historical_features_df)
    assert_expected_arrow_types(feature_dtype, feature_is_list, historical_features)


@parametrize_types_no_materialize_test_no_list
def test_feature_get_online_features_types_match(environment: Environment):
    feature_dtype, feature_is_list, fs, fv = get_test_fixtures(environment)
    if feature_is_list:
        pass

    features = [fv.name + ":value"]
    entity = driver(value_type=ValueType.UNKNOWN)
    fs.apply([fv, entity])
    fs.materialize(environment.start_date, environment.end_date)
    driver_id_value = "1" if environment.entity_type == ValueType.STRING else 1
    online_features = fs.get_online_features(
        features=features, entity_rows=[{"driver": driver_id_value}],
    ).to_dict()

    feature_list_dtype_to_expected_online_response_value_type = {
        "int32": "int",
        "int64": "int",
        "float": "float",
        "string": "str",
        "bool": "bool",
    }
    assert (
        type(online_features["value"][0]).__name__
        == feature_list_dtype_to_expected_online_response_value_type[feature_dtype]
    )


def get_test_fixtures(environment: Environment):
    feature_dtype = environment.feature_dtype
    feature_is_list = environment.feature_is_list
    fs, fv = (
        environment.feature_store,
        driver_feature_view(
            environment.data_source,
            value_type=python_type_to_feast_value_type(
                feature_dtype,
                value=get_feature_values_for_dtype(feature_dtype, feature_is_list)[0],
            ),
        ),
    )
    return feature_dtype, feature_is_list, fs, fv


def assert_expected_historical_feature_types(
    feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature types")
    feature_dtype_to_expected_historical_feature_dtype = {
        "int32": "int64",
        "int64": "int64",
        "float": "float64",
        "string": "object",
        "bool": "bool",
    }
    assert (
        str(historical_features_df.dtypes["value"])
        == feature_dtype_to_expected_historical_feature_dtype[feature_dtype]
    )


def assert_feature_list_types(feature_dtype: str, historical_features_df: pd.DataFrame):
    print("Asserting historical feature list types")
    # Note, these expected values only hold for BQ
    feature_list_dtype_to_expected_historical_feature_list_dtype = {
        "int32": "int",
        "int64": "int",
        "float": "float",
        "string": "str",
        "bool": "bool",
    }
    assert str(historical_features_df.dtypes["value"]) == "object"
    # Note, this struct schema is only true for BQ and not for other stores
    assert (
        type(historical_features_df.value[0]["list"][0]["item"]).__name__
        == feature_list_dtype_to_expected_historical_feature_list_dtype[feature_dtype]
    )


def assert_expected_arrow_types(
    feature_dtype: str, feature_is_list: bool, historical_features: RetrievalJob
):
    print("Asserting historical feature arrow types")
    historical_features_arrow = historical_features.to_arrow()
    print(historical_features_arrow)
    feature_list_dtype_to_expected_historical_feature_arrow_type = {
        "int32": "int64",
        "int64": "int64",
        "float": "double",
        "string": "string",
        "bool": "bool",
    }
    arrow_type = feature_list_dtype_to_expected_historical_feature_arrow_type[
        feature_dtype
    ]
    if feature_is_list:
        assert (
            str(historical_features_arrow.schema.field_by_name("value").type)
            == f"struct<list: list<item: struct<item: {arrow_type}>> not null>"
        )
    else:
        assert (
            str(historical_features_arrow.schema.field_by_name("value").type)
            == arrow_type
        )
