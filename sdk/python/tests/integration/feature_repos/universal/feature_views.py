from datetime import timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from feast import Feature, FeatureView, OnDemandFeatureView, ValueType
from feast.data_source import DataSource, RequestDataSource
from feast.request_feature_view import RequestFeatureView


def driver_feature_view(
    data_source: DataSource,
    name="test_correctness",
    infer_features: bool = False,
    value_type: ValueType = ValueType.FLOAT,
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=["driver"],
        features=None if infer_features else [Feature("value", value_type)],
        ttl=timedelta(days=5),
        batch_source=data_source,
    )


def global_feature_view(
    data_source: DataSource,
    name="test_entityless",
    infer_features: bool = False,
    value_type: ValueType = ValueType.INT32,
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[],
        features=None if infer_features else [Feature("entityless_value", value_type)],
        ttl=timedelta(days=5),
        batch_source=data_source,
    )


def conv_rate_plus_100(features_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_100"] = features_df["conv_rate"] + 100
    df["conv_rate_plus_val_to_add"] = (
        features_df["conv_rate"] + features_df["val_to_add"]
    )
    df["conv_rate_plus_100_rounded"] = (
        df["conv_rate_plus_100"].astype("float").round().astype(pd.Int32Dtype())
    )
    return df


def conv_rate_plus_100_feature_view(
    inputs: Dict[str, Union[RequestDataSource, FeatureView]],
    infer_features: bool = False,
    features: Optional[List[Feature]] = None,
) -> OnDemandFeatureView:
    _features = features or [
        Feature("conv_rate_plus_100", ValueType.DOUBLE),
        Feature("conv_rate_plus_val_to_add", ValueType.DOUBLE),
        Feature("conv_rate_plus_100_rounded", ValueType.INT32),
    ]
    return OnDemandFeatureView(
        name=conv_rate_plus_100.__name__,
        inputs=inputs,
        features=[] if infer_features else _features,
        udf=conv_rate_plus_100,
    )


def similarity(features_df: pd.DataFrame) -> pd.DataFrame:
    if features_df.size == 0:
        # give hint to Feast about return type
        df = pd.DataFrame({"cos_double": [0.0]})
        df["cos_float"] = df["cos_double"].astype(np.float32)
        return df
    vectors_a = features_df["embedding_double"].apply(np.array)
    vectors_b = features_df["vector_double"].apply(np.array)
    dot_products = vectors_a.mul(vectors_b).apply(sum)
    norms_q = vectors_a.apply(np.linalg.norm)
    norms_doc = vectors_b.apply(np.linalg.norm)
    df = pd.DataFrame()
    df["cos_double"] = dot_products / (norms_q * norms_doc)
    df["cos_float"] = df["cos_double"].astype(np.float32)
    return df


def similarity_feature_view(
    inputs: Dict[str, Union[RequestDataSource, FeatureView]],
    infer_features: bool = False,
    features: Optional[List[Feature]] = None,
) -> OnDemandFeatureView:
    _features = features or [
        Feature("cos_double", ValueType.DOUBLE),
        Feature("cos_float", ValueType.FLOAT),
    ]
    return OnDemandFeatureView(
        name=similarity.__name__,
        inputs=inputs,
        features=[] if infer_features else _features,
        udf=similarity,
    )


def create_driver_age_request_feature_view():
    return RequestFeatureView(
        name="driver_age",
        request_data_source=RequestDataSource(
            name="driver_age_source", schema={"driver_age": ValueType.INT32}
        ),
    )


def create_conv_rate_request_data_source():
    return RequestDataSource(
        name="conv_rate_input", schema={"val_to_add": ValueType.INT32}
    )


def create_similarity_request_data_source():
    return RequestDataSource(
        name="similarity_input",
        schema={
            "vector_double": ValueType.DOUBLE_LIST,
            "vector_float": ValueType.FLOAT_LIST,
        },
    )


def create_item_embeddings_feature_view(source, infer_features: bool = False):
    item_embeddings_feature_view = FeatureView(
        name="item_embeddings",
        entities=["item"],
        features=None
        if infer_features
        else [
            Feature(name="embedding_double", dtype=ValueType.DOUBLE_LIST),
            Feature(name="embedding_float", dtype=ValueType.FLOAT_LIST),
        ],
        batch_source=source,
        ttl=timedelta(hours=2),
    )
    return item_embeddings_feature_view


def create_driver_hourly_stats_feature_view(source, infer_features: bool = False):
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=["driver"],
        features=None
        if infer_features
        else [
            Feature(name="conv_rate", dtype=ValueType.FLOAT),
            Feature(name="acc_rate", dtype=ValueType.FLOAT),
            Feature(name="avg_daily_trips", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(hours=2),
    )
    return driver_stats_feature_view


def create_customer_daily_profile_feature_view(source, infer_features: bool = False):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=["customer_id"],
        features=None
        if infer_features
        else [
            Feature(name="current_balance", dtype=ValueType.FLOAT),
            Feature(name="avg_passenger_count", dtype=ValueType.FLOAT),
            Feature(name="lifetime_trip_count", dtype=ValueType.INT32),
        ],
        batch_source=source,
        ttl=timedelta(days=2),
    )
    return customer_profile_feature_view


def create_global_stats_feature_view(source, infer_features: bool = False):
    global_stats_feature_view = FeatureView(
        name="global_stats",
        entities=[],
        features=None
        if infer_features
        else [
            Feature(name="num_rides", dtype=ValueType.INT32),
            Feature(name="avg_ride_length", dtype=ValueType.FLOAT),
        ],
        batch_source=source,
        ttl=timedelta(days=2),
    )
    return global_stats_feature_view


def create_order_feature_view(source, infer_features: bool = False):
    return FeatureView(
        name="order",
        entities=["driver", "customer_id"],
        features=None
        if infer_features
        else [Feature(name="order_is_success", dtype=ValueType.INT32)],
        batch_source=source,
        ttl=timedelta(days=2),
    )


def create_location_stats_feature_view(source, infer_features: bool = False):
    location_stats_feature_view = FeatureView(
        name="location_stats",
        entities=["location_id"],
        features=None
        if infer_features
        else [Feature(name="temperature", dtype=ValueType.INT32)],
        batch_source=source,
        ttl=timedelta(days=2),
    )
    return location_stats_feature_view


def create_field_mapping_feature_view(source):
    return FeatureView(
        name="field_mapping",
        entities=[],
        features=[Feature(name="feature_name", dtype=ValueType.INT32)],
        batch_source=source,
        ttl=timedelta(days=2),
    )
