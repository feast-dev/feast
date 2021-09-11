from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytest

from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.type_map import python_type_to_feast_value_type
from feast.value_type import ValueType
from tests.data.data_creator import create_dataset, get_feature_values_for_dtype
from tests.integration.feature_repos.repo_configuration import (
    FULL_REPO_CONFIGS,
    REDIS_CONFIG,
    IntegrationTestRepoConfig,
    construct_test_environment,
)
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view


def populate_test_configs(offline: bool):
    entity_type_feature_dtypes = [
        (ValueType.INT32, "int32"),
        (ValueType.INT64, "int64"),
        (ValueType.STRING, "float"),
        (ValueType.STRING, "bool"),
    ]
    configs: List[TypeTestConfig] = []
    for test_repo_config in FULL_REPO_CONFIGS:
        for entity_type, feature_dtype in entity_type_feature_dtypes:
            for feature_is_list in [True, False]:
                # Redshift doesn't support list features
                if test_repo_config.provider == "aws" and feature_is_list is True:
                    continue
                # For offline tests, don't need to vary for online store
                if offline and test_repo_config.online_store == REDIS_CONFIG:
                    continue
                # TODO(https://github.com/feast-dev/feast/issues/1839): Fix BQ materialization of list features
                if (
                    not offline
                    and test_repo_config.provider == "gcp"
                    and feature_is_list is True
                ):
                    continue
                configs.append(
                    TypeTestConfig(
                        entity_type=entity_type,
                        feature_dtype=feature_dtype,
                        feature_is_list=feature_is_list,
                        test_repo_config=test_repo_config,
                    )
                )
    return configs


@dataclass(frozen=True, repr=True)
class TypeTestConfig:
    entity_type: ValueType
    feature_dtype: str
    feature_is_list: bool
    test_repo_config: IntegrationTestRepoConfig


OFFLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=True)
ONLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=False)


@pytest.fixture(
    params=OFFLINE_TYPE_TEST_CONFIGS,
    scope="session",
    ids=[str(c) for c in OFFLINE_TYPE_TEST_CONFIGS],
)
def offline_types_test_fixtures(request):
    yield from get_fixtures(request)


@pytest.fixture(
    params=ONLINE_TYPE_TEST_CONFIGS,
    scope="session",
    ids=[str(c) for c in ONLINE_TYPE_TEST_CONFIGS],
)
def online_types_test_fixtures(request):
    yield from get_fixtures(request)


def get_fixtures(request):
    config: TypeTestConfig = request.param
    # Lower case needed because Redshift lower-cases all table names
    test_project_id = f"{config.entity_type}{config.feature_dtype}{config.feature_is_list}".replace(
        ".", ""
    ).lower()
    with construct_test_environment(
        test_repo_config=config.test_repo_config,
        test_suite_name=f"test_{test_project_id}",
    ) as type_test_environment:
        config = request.param
        df = create_dataset(
            config.entity_type, config.feature_dtype, config.feature_is_list
        )
        data_source = type_test_environment.data_source_creator.create_data_source(
            df,
            destination_name=type_test_environment.feature_store.project,
            field_mapping={"ts_1": "ts"},
        )
        fv = create_feature_view(
            config.feature_dtype, config.feature_is_list, data_source
        )
        yield type_test_environment, config, data_source, fv
        type_test_environment.data_source_creator.teardown()


@pytest.mark.integration
def test_entity_inference_types_match(offline_types_test_fixtures):
    environment, config, data_source, fv = offline_types_test_fixtures
    fs = environment.feature_store

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
            == entity_type_to_expected_inferred_entity_type[config.entity_type]
        )


@pytest.mark.integration
def test_feature_get_historical_features_types_match(offline_types_test_fixtures):
    environment, config, data_source, fv = offline_types_test_fixtures
    fs = environment.feature_store
    fv = create_feature_view(config.feature_dtype, config.feature_is_list, data_source)
    entity = driver()
    fs.apply([fv, entity])

    features = [f"{fv.name}:value"]
    entity_df = pd.DataFrame()
    entity_df["driver_id"] = (
        ["1", "3"] if config.entity_type == ValueType.STRING else [1, 3]
    )
    now = datetime.utcnow()
    ts = pd.Timestamp(now).round("ms")
    entity_df["ts"] = [
        ts - timedelta(hours=4),
        ts - timedelta(hours=2),
    ]
    historical_features = fs.get_historical_features(
        entity_df=entity_df, features=features,
    )
    # Note: Pandas doesn't play well with nan values in ints. BQ will also coerce to floats if there are NaNs
    historical_features_df = historical_features.to_df()
    print(historical_features_df)

    if config.feature_is_list:
        assert_feature_list_types(
            environment.test_repo_config.provider,
            config.feature_dtype,
            historical_features_df,
        )
    else:
        assert_expected_historical_feature_types(
            config.feature_dtype, historical_features_df
        )
    assert_expected_arrow_types(
        environment.test_repo_config.provider,
        config.feature_dtype,
        config.feature_is_list,
        historical_features,
    )


@pytest.mark.integration
def test_feature_get_online_features_types_match(online_types_test_fixtures):
    environment, config, data_source, fv = online_types_test_fixtures
    fv = create_feature_view(config.feature_dtype, config.feature_is_list, data_source)
    fs = environment.feature_store
    features = [fv.name + ":value"]
    entity = driver(value_type=ValueType.UNKNOWN)
    fs.apply([fv, entity])
    fs.materialize(environment.start_date, environment.end_date)

    driver_id_value = "1" if config.entity_type == ValueType.STRING else 1
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
    if config.feature_is_list:
        assert type(online_features["value"][0]).__name__ == "list"
        assert (
            type(online_features["value"][0][0]).__name__
            == feature_list_dtype_to_expected_online_response_value_type[
                config.feature_dtype
            ]
        )
    else:
        assert (
            type(online_features["value"][0]).__name__
            == feature_list_dtype_to_expected_online_response_value_type[
                config.feature_dtype
            ]
        )


def create_feature_view(feature_dtype, feature_is_list, data_source):
    return driver_feature_view(
        data_source,
        value_type=python_type_to_feast_value_type(
            feature_dtype,
            value=get_feature_values_for_dtype(feature_dtype, feature_is_list)[0],
        ),
    )


def assert_expected_historical_feature_types(
    feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature types")
    feature_dtype_to_expected_historical_feature_dtype = {
        "int32": "int64",
        "int64": "int64",
        "float": "float64",
        "string": {"string", "object"},
        "bool": {"bool", "object"},
    }
    assert (
        str(historical_features_df.dtypes["value"])
        in feature_dtype_to_expected_historical_feature_dtype[feature_dtype]
    )


def assert_feature_list_types(
    provider: str, feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature list types")
    feature_list_dtype_to_expected_historical_feature_list_dtype = {
        "int32": "int",
        "int64": "int",
        "float": "float",
        "string": "str",
        "bool": "bool",
    }
    assert str(historical_features_df.dtypes["value"]) == "object"
    if provider == "gcp":
        assert (
            feature_list_dtype_to_expected_historical_feature_list_dtype[feature_dtype]
            in type(historical_features_df.value[0]["list"][0]["item"]).__name__
        )
    else:
        assert (
            feature_list_dtype_to_expected_historical_feature_list_dtype[feature_dtype]
            in type(historical_features_df.value[0][0]).__name__
        )


def assert_expected_arrow_types(
    provider: str,
    feature_dtype: str,
    feature_is_list: bool,
    historical_features: RetrievalJob,
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
        if provider == "gcp":
            assert str(
                historical_features_arrow.schema.field_by_name("value").type
            ) in [
                f"struct<list: list<item: struct<item: {arrow_type}>> not null>",
                f"struct<list: list<item: struct<item: {arrow_type}>>>",
            ]
        else:
            assert (
                str(historical_features_arrow.schema.field_by_name("value").type)
                == f"list<item: {arrow_type}>"
            )
    else:
        assert (
            str(historical_features_arrow.schema.field_by_name("value").type)
            == arrow_type
        )
