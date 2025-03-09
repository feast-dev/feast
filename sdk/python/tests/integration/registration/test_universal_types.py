import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.types import (
    Array,
    Bool,
    FeastType,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    UnixTimestamp,
)
from feast.utils import _utc_now
from tests.data.data_creator import create_basic_driver_dataset
from tests.integration.feature_repos.universal.entities import driver
from tests.integration.feature_repos.universal.feature_views import driver_feature_view

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("entity_type", [Int32, Int64, String])
def test_entity_inference_types_match(environment, entity_type):
    fs = environment.feature_store

    # Don't specify value type in entity to force inference
    df = create_basic_driver_dataset(
        entity_type,
        feature_dtype="int32",
    )
    data_source = environment.data_source_creator.create_data_source(
        df,
        destination_name=f"entity_type_{entity_type.name.lower()}",
        field_mapping={"ts_1": "ts"},
    )
    fv = driver_feature_view(
        data_source=data_source,
        name=f"fv_entity_type_{entity_type.name.lower()}",
        infer_entities=True,  # Forces entity inference by not including a field for the entity.
        dtype=_get_feast_type("int32", False),
        entity_type=entity_type,
    )

    entity = driver()
    fs.apply([fv, entity])

    entity_type_to_expected_inferred_entity_type = {
        Int32: {Int32, Int64},
        Int64: {Int32, Int64},
        Float32: {Float64},
        String: {String},
    }

    entity_columns = list(
        filter(lambda x: x.name == entity.join_key, fv.entity_columns)
    )
    assert len(entity_columns) == 1
    entity_column = entity_columns[0]
    assert (
        entity_column.dtype in entity_type_to_expected_inferred_entity_type[entity_type]
    )


@pytest.mark.integration
@pytest.mark.universal_offline_stores
def test_feature_get_historical_features_types_match(
    offline_types_test_fixtures, environment
):
    """
    Note: to make sure this test works, we need to ensure that get_historical_features
    returns at least one non-null row to make sure type inferral works. This can only
    be achieved by carefully matching entity_df to the data fixtures.
    """
    config, data_source, fv = offline_types_test_fixtures
    fs = environment.feature_store
    entity = driver()
    fv = driver_feature_view(
        data_source=data_source,
        name="get_historical_features_types_match",
        dtype=_get_feast_type(config.feature_dtype, config.feature_is_list),
    )
    fs.apply([fv, entity])

    entity_df = pd.DataFrame()
    entity_df["driver_id"] = [1, 3]
    ts = pd.Timestamp(_utc_now()).round("ms")
    entity_df["ts"] = [
        ts - timedelta(hours=4),
        ts - timedelta(hours=2),
    ]
    features = [f"{fv.name}:value"]

    historical_features = fs.get_historical_features(
        entity_df=entity_df,
        features=features,
    )
    # Note: Pandas doesn't play well with nan values in ints. BQ will also coerce to floats if there are NaNs
    historical_features_df = historical_features.to_df()
    print(historical_features_df)

    if config.feature_is_list:
        assert_feature_list_types(
            environment.provider,
            config.feature_dtype,
            historical_features_df,
        )
    else:
        assert_expected_historical_feature_types(
            config.feature_dtype, historical_features_df
        )
    assert_expected_arrow_types(
        environment.provider,
        config.feature_dtype,
        config.feature_is_list,
        historical_features,
    )


@pytest.mark.integration
@pytest.mark.universal_online_stores(only=["sqlite"])
def test_feature_get_online_features_types_match(
    online_types_test_fixtures, environment
):
    config, data_source, fv = online_types_test_fixtures
    entity = driver()
    fv = driver_feature_view(
        data_source=data_source,
        name="get_online_features_types_match",
        dtype=_get_feast_type(config.feature_dtype, config.feature_is_list),
    )
    fs = environment.feature_store
    features = [fv.name + ":value"]
    fs.apply([fv, entity])
    fs.materialize(
        environment.start_date,
        environment.end_date
        - timedelta(hours=1),  # throwing out last record to make sure
        # we can successfully infer type even from all empty values
    )

    online_features = fs.get_online_features(
        features=features,
        entity_rows=[{"driver_id": 1}],
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
            assert config.has_empty_list or len(feature) > 0, (
                "List of values should not be empty"
            )
            for element in feature:
                assert isinstance(element, expected_dtype)
    else:
        for feature in online_features["value"]:
            assert isinstance(feature, expected_dtype)


def _get_feast_type(feature_dtype: str, feature_is_list: bool) -> FeastType:
    dtype: Optional[FeastType] = None
    if feature_is_list is True:
        if feature_dtype == "int32":
            dtype = Array(Int32)
        elif feature_dtype == "int64":
            dtype = Array(Int64)
        elif feature_dtype == "float":
            dtype = Array(Float32)
        elif feature_dtype == "bool":
            dtype = Array(Bool)
        elif feature_dtype == "datetime":
            dtype = Array(UnixTimestamp)
    else:
        if feature_dtype == "int32":
            dtype = Int32
        elif feature_dtype == "int64":
            dtype = Int64
        elif feature_dtype == "float":
            dtype = Float32
        elif feature_dtype == "bool":
            dtype = Bool
        elif feature_dtype == "datetime":
            dtype = UnixTimestamp
    assert dtype
    return dtype


def assert_expected_historical_feature_types(
    feature_dtype: str, historical_features_df: pd.DataFrame
):
    print("Asserting historical feature types")
    feature_dtype_to_expected_historical_feature_dtype = {
        "int32": (pd.api.types.is_integer_dtype,),
        "int64": (pd.api.types.is_integer_dtype,),
        "float": (pd.api.types.is_float_dtype,),
        "string": (pd.api.types.is_string_dtype,),
        "bool": (pd.api.types.is_bool_dtype, pd.api.types.is_object_dtype),
        "datetime": (pd.api.types.is_datetime64_any_dtype,),
    }
    dtype_checkers = feature_dtype_to_expected_historical_feature_dtype[feature_dtype]
    assert any(
        check(historical_features_df.dtypes["value"]) for check in dtype_checkers
    ), (
        f"Failed to match feature type {historical_features_df.dtypes['value']} with checkers {dtype_checkers}"
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
        "datetime": (
            np.datetime64,
            datetime,
        ),  # datetime.datetime
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
        "int32": pa.types.is_signed_integer,  # different offline stores could interpret integers differently
        "int64": pa.types.is_signed_integer,  # eg, Snowflake chooses the smallest possible (like int8)
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


def populate_test_configs(offline: bool):
    feature_dtypes = [
        "int32",
        "int64",
        "float",
        "bool",
        "datetime",
    ]
    configs: List[TypeTestConfig] = []
    for feature_dtype in feature_dtypes:
        for feature_is_list in [True, False]:
            for has_empty_list in [True, False]:
                # For non list features `has_empty_list` does nothing
                if feature_is_list is False and has_empty_list is True:
                    continue

                configs.append(
                    TypeTestConfig(
                        feature_dtype=feature_dtype,
                        feature_is_list=feature_is_list,
                        has_empty_list=has_empty_list,
                    )
                )
    return configs


@dataclass(frozen=True, repr=True)
class TypeTestConfig:
    feature_dtype: str
    feature_is_list: bool
    has_empty_list: bool


OFFLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=True)
ONLINE_TYPE_TEST_CONFIGS: List[TypeTestConfig] = populate_test_configs(offline=False)


@pytest.fixture(
    params=OFFLINE_TYPE_TEST_CONFIGS,
    ids=[str(c) for c in OFFLINE_TYPE_TEST_CONFIGS],
)
def offline_types_test_fixtures(request, environment):
    config: TypeTestConfig = request.param
    if environment.provider == "aws" and config.feature_is_list is True:
        pytest.skip("Redshift doesn't support list features")

    return get_fixtures(request, environment)


@pytest.fixture(
    params=ONLINE_TYPE_TEST_CONFIGS,
    ids=[str(c) for c in ONLINE_TYPE_TEST_CONFIGS],
)
def online_types_test_fixtures(request, environment):
    return get_fixtures(request, environment)


def get_fixtures(request, environment):
    config: TypeTestConfig = request.param
    # Lower case needed because Redshift lower-cases all table names
    destination_name = (
        f"feature_type_{config.feature_dtype}{config.feature_is_list}".replace(
            ".", ""
        ).lower()
    )
    config = request.param
    df = create_basic_driver_dataset(
        Int64,
        config.feature_dtype,
        config.feature_is_list,
        config.has_empty_list,
    )
    data_source = environment.data_source_creator.create_data_source(
        df,
        destination_name=destination_name,
        field_mapping={"ts_1": "ts"},
    )
    fv = driver_feature_view(
        data_source=data_source,
        name=destination_name,
        dtype=_get_feast_type(config.feature_dtype, config.feature_is_list),
    )

    return config, data_source, fv
