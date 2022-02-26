import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.value_type import ValueType
from tests.data.data_creator import create_dataset
from tests.integration.feature_repos.repo_configuration import (
    FULL_REPO_CONFIGS,
    REDIS_CONFIG,
    IntegrationTestRepoConfig,
    construct_test_environment,
)
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view

logger = logging.getLogger(__name__)


def populate_test_configs(offline: bool):
    entity_type_feature_dtypes = [
        (ValueType.INT32, "int32"),
        (ValueType.INT64, "int64"),
        (ValueType.STRING, "float"),
        (ValueType.STRING, "bool"),
        (ValueType.INT32, "datetime"),
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
                for has_empty_list in [True, False]:
                    # For non list features `has_empty_list` does nothing
                    if feature_is_list is False and has_empty_list is True:
                        continue
                    configs.append(
                        TypeTestConfig(
                            entity_type=entity_type,
                            feature_dtype=feature_dtype,
                            feature_is_list=feature_is_list,
                            has_empty_list=has_empty_list,
                            test_repo_config=test_repo_config,
                        )
                    )
    return configs


@dataclass(frozen=True, repr=True)
class TypeTestConfig:
    entity_type: ValueType
    feature_dtype: str
    feature_is_list: bool
    has_empty_list: bool
    test_repo_config: IntegrationTestRepoConfig


OFFLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=True)
ONLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=False)


@pytest.fixture(
    params=OFFLINE_TYPE_TEST_CONFIGS,
    scope="session",
    ids=[str(c) for c in OFFLINE_TYPE_TEST_CONFIGS],
)
def offline_types_test_fixtures(request):
    return get_fixtures(request)


@pytest.fixture(
    params=ONLINE_TYPE_TEST_CONFIGS,
    scope="session",
    ids=[str(c) for c in ONLINE_TYPE_TEST_CONFIGS],
)
def online_types_test_fixtures(request):
    return get_fixtures(request)


def get_fixtures(request):
    config: TypeTestConfig = request.param
    # Lower case needed because Redshift lower-cases all table names
    test_project_id = f"{config.entity_type}{config.feature_dtype}{config.feature_is_list}".replace(
        ".", ""
    ).lower()
    type_test_environment = construct_test_environment(
        test_repo_config=config.test_repo_config,
        test_suite_name=f"test_{test_project_id}",
    )
    config = request.param
    df = create_dataset(
        config.entity_type,
        config.feature_dtype,
        config.feature_is_list,
        config.has_empty_list,
    )
    data_source = type_test_environment.data_source_creator.create_data_source(
        df,
        destination_name=type_test_environment.feature_store.project,
        field_mapping={"ts_1": "ts"},
    )
    fv = create_feature_view(
        request.fixturename,
        config.feature_dtype,
        config.feature_is_list,
        config.has_empty_list,
        data_source,
    )

    def cleanup():
        try:
            type_test_environment.data_source_creator.teardown()
        except Exception:  # noqa
            logger.exception("DataSourceCreator teardown has failed")

        type_test_environment.feature_store.teardown()

    request.addfinalizer(cleanup)

    return type_test_environment, config, data_source, fv


@pytest.mark.integration
@pytest.mark.universal
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
@pytest.mark.universal
def test_feature_get_historical_features_types_match(offline_types_test_fixtures):
    environment, config, data_source, fv = offline_types_test_fixtures
    fs = environment.feature_store
    fv = create_feature_view(
        "get_historical_features_types_match",
        config.feature_dtype,
        config.feature_is_list,
        config.has_empty_list,
        data_source,
    )
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
@pytest.mark.universal
def test_feature_get_online_features_types_match(online_types_test_fixtures):
    environment, config, data_source, fv = online_types_test_fixtures
    fv = create_feature_view(
        "get_online_features_types_match",
        config.feature_dtype,
        config.feature_is_list,
        config.has_empty_list,
        data_source,
    )
    fs = environment.feature_store
    features = [fv.name + ":value"]
    entity = driver(value_type=config.entity_type)
    fs.apply([fv, entity])
    fs.materialize(
        environment.start_date,
        environment.end_date
        - timedelta(hours=1)  # throwing out last record to make sure
        # we can successfully infer type even from all empty values
    )

    driver_id_value = "1" if config.entity_type == ValueType.STRING else 1
    online_features = fs.get_online_features(
        features=features, entity_rows=[{"driver": driver_id_value}],
    ).to_dict()

    feature_list_dtype_to_expected_online_response_value_type = {
        "int32": int,
        "int64": int,
        "float": float,
        "string": str,
        "bool": bool,
        "datetime": datetime,
    }
    expected_dtype = feature_list_dtype_to_expected_online_response_value_type[
        config.feature_dtype
    ]

    assert len(online_features["value"]) == 1

    if config.feature_is_list:
        for feature in online_features["value"]:
            assert isinstance(feature, list), "Feature value should be a list"
            assert (
                config.has_empty_list or len(feature) > 0
            ), "List of values should not be empty"
            for element in feature:
                assert isinstance(element, expected_dtype)
    else:
        for feature in online_features["value"]:
            assert isinstance(feature, expected_dtype)


def create_feature_view(
    name, feature_dtype, feature_is_list, has_empty_list, data_source
):
    if feature_is_list is True:
        if feature_dtype == "int32":
            value_type = ValueType.INT32_LIST
        elif feature_dtype == "int64":
            value_type = ValueType.INT64_LIST
        elif feature_dtype == "float":
            value_type = ValueType.FLOAT_LIST
        elif feature_dtype == "bool":
            value_type = ValueType.BOOL_LIST
        elif feature_dtype == "datetime":
            value_type = ValueType.UNIX_TIMESTAMP_LIST
    else:
        if feature_dtype == "int32":
            value_type = ValueType.INT32
        elif feature_dtype == "int64":
            value_type = ValueType.INT64
        elif feature_dtype == "float":
            value_type = ValueType.FLOAT
        elif feature_dtype == "bool":
            value_type = ValueType.BOOL
        elif feature_dtype == "datetime":
            value_type = ValueType.UNIX_TIMESTAMP

    return driver_feature_view(data_source, name=name, value_type=value_type,)


def assert_expected_historical_feature_types(
    feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature types")
    feature_dtype_to_expected_historical_feature_dtype = {
        "int32": (pd.api.types.is_integer_dtype,),
        "int64": (pd.api.types.is_int64_dtype,),
        "float": (pd.api.types.is_float_dtype,),
        "string": (pd.api.types.is_string_dtype,),
        "bool": (pd.api.types.is_bool_dtype, pd.api.types.is_object_dtype),
        "datetime": (pd.api.types.is_datetime64_any_dtype,),
    }
    dtype_checkers = feature_dtype_to_expected_historical_feature_dtype[feature_dtype]
    assert any(
        check(historical_features_df.dtypes["value"]) for check in dtype_checkers
    )


def assert_feature_list_types(
    provider: str, feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature list types")
    feature_list_dtype_to_expected_historical_feature_list_dtype: Dict[
        str, Union[type, Tuple[Union[type, Tuple[Any, ...]], ...]]
    ] = {
        "int32": (
            int,
            np.int64,
        ),  # Can be `np.int64` if from `np.array` rather that `list`
        "int64": (
            int,
            np.int64,
        ),  # Can be `np.int64` if from `np.array` rather that `list`
        "float": float,
        "string": str,
        "bool": (
            bool,
            np.bool_,
        ),  # Can be `np.bool_` if from `np.array` rather that `list`
        "datetime": np.datetime64,
    }
    expected_dtype = feature_list_dtype_to_expected_historical_feature_list_dtype[
        feature_dtype
    ]
    assert pd.api.types.is_object_dtype(historical_features_df.dtypes["value"])
    for feature in historical_features_df.value:
        assert isinstance(feature, (np.ndarray, list))
        for element in feature:
            assert isinstance(element, expected_dtype)


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
        "int32": pa.types.is_int64,
        "int64": pa.types.is_int64,
        "float": pa.types.is_float64,
        "string": pa.types.is_string,
        "bool": pa.types.is_boolean,
        "date": pa.types.is_date,
        "datetime": pa.types.is_timestamp,
    }
    arrow_type_checker = feature_list_dtype_to_expected_historical_feature_arrow_type[
        feature_dtype
    ]
    pa_type = historical_features_arrow.schema.field("value").type

    if feature_is_list:
        assert pa.types.is_list(pa_type)
        assert arrow_type_checker(pa_type.value_type)
    else:
        assert arrow_type_checker(pa_type)
