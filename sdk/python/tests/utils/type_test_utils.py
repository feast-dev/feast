from dataclasses import dataclass
from typing import List, Optional

from feast.types import (
    Array,
    Bool,
    FeastType,
    Float32,
    Int32,
    Int64,
    UnixTimestamp,
)
from tests.data.data_creator import create_basic_driver_dataset
from tests.integration.feature_repos.universal.feature_views import driver_feature_view


@dataclass(frozen=True, repr=True)
class TypeTestConfig:
    feature_dtype: str
    feature_is_list: bool
    has_empty_list: bool


def get_feast_type(feature_dtype: str, feature_is_list: bool) -> FeastType:
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


def populate_test_configs():
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


def get_type_test_fixtures(request, environment):
    config: TypeTestConfig = request.param
    # Lower case needed because Redshift lower-cases all table names
    destination_name = (
        f"feature_type_{config.feature_dtype}{config.feature_is_list}".replace(
            ".", ""
        ).lower()
    )
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
        dtype=get_feast_type(config.feature_dtype, config.feature_is_list),
    )

    return config, data_source, fv
