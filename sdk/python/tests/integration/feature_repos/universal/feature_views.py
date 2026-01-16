from datetime import timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from feast import (
    BatchFeatureView,
    Feature,
    FeatureView,
    Field,
    OnDemandFeatureView,
    PushSource,
    StreamFeatureView,
)
from feast.data_source import DataSource, RequestSource
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import PandasTransformation
from feast.types import Array, FeastType, Float32, Float64, Int32, Int64, String
from tests.integration.feature_repos.universal.entities import (
    customer,
    driver,
    item,
    location,
)

TAGS = {"release": "production"}


def driver_feature_view(
    data_source: DataSource,
    name="test_correctness",
    infer_entities: bool = False,
    infer_features: bool = False,
    dtype: FeastType = Float32,
    entity_type: FeastType = Int64,
) -> FeatureView:
    d = driver()
    return FeatureView(
        name=name,
        entities=[d],
        schema=([] if infer_entities else [Field(name=d.join_key, dtype=entity_type)])
        + ([] if infer_features else [Field(name="value", dtype=dtype)]),
        ttl=timedelta(days=5),
        source=data_source,
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
    sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]],
    infer_features: bool = False,
    features: Optional[List[Field]] = None,
) -> OnDemandFeatureView:
    # Test that positional arguments and Features still work for ODFVs.
    _features = features or [
        Field(name="conv_rate_plus_100", dtype=Float64),
        Field(name="conv_rate_plus_val_to_add", dtype=Float64),
        Field(name="conv_rate_plus_100_rounded", dtype=Int32),
    ]
    return OnDemandFeatureView(
        name=conv_rate_plus_100.__name__,
        schema=[] if infer_features else _features,
        sources=sources,
        feature_transformation=PandasTransformation(
            udf=conv_rate_plus_100,
            udf_string="raw udf source",  # type: ignore
        ),
        mode="pandas",
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
    sources: Dict[str, Union[RequestSource, FeatureView]],
    infer_features: bool = False,
    features: Optional[List[Feature]] = None,
) -> OnDemandFeatureView:
    _fields = [
        Field(name="cos_double", dtype=Float64),
        Field(name="cos_float", dtype=Float32),
    ]
    if features is not None:
        _fields = [Field.from_feature(feature) for feature in features]

    return OnDemandFeatureView(
        name=similarity.__name__,
        sources=sources,  # type: ignore
        schema=[] if infer_features else _fields,
        feature_transformation=PandasTransformation(
            udf=similarity,
            udf_string="similarity raw udf",  # type: ignore
        ),
    )


def create_conv_rate_request_source():
    return RequestSource(
        name="conv_rate_input",
        schema=[Field(name="val_to_add", dtype=Int32)],
    )


def create_similarity_request_source():
    return RequestSource(
        name="similarity_input",
        schema=[
            Field(name="vector_double", dtype=Array(Float64)),
            Field(name="vector_float", dtype=Array(Float32)),
        ],
    )


def create_item_embeddings_feature_view(source, infer_features: bool = False):
    item_embeddings_feature_view = FeatureView(
        name="item_embeddings",
        entities=[item()],
        schema=None
        if infer_features
        else [
            Field(
                name="embedding_float",
                dtype=Array(Float32),
                vector_index=True,
                vector_search_metric="L2",
            ),
            Field(name="string_feature", dtype=String),
            Field(name="float_feature", dtype=Float32),
        ],
        source=source,
        ttl=timedelta(hours=2),
    )
    return item_embeddings_feature_view


def create_item_embeddings_batch_feature_view(
    source, infer_features: bool = False
) -> BatchFeatureView:
    item_embeddings_feature_view = BatchFeatureView(
        name="item_embeddings",
        entities=[item()],
        schema=None
        if infer_features
        else [
            Field(name="embedding_double", dtype=Array(Float64)),
            Field(name="embedding_float", dtype=Array(Float32)),
        ],
        source=source,
        ttl=timedelta(hours=2),
        udf=lambda x: x,
    )
    return item_embeddings_feature_view


def create_driver_hourly_stats_feature_view(source, infer_features: bool = False):
    # TODO(felixwang9817): Figure out why not adding an entity field here
    # breaks type tests.
    d = driver()
    driver_stats_feature_view = FeatureView(
        name="driver_stats",
        entities=[d],
        schema=None
        if infer_features
        else [
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int32),
            Field(name=d.join_key, dtype=Int64),
        ],
        source=source,
        ttl=timedelta(hours=2),
        tags=TAGS,
    )
    return driver_stats_feature_view


def create_driver_hourly_stats_batch_feature_view(
    source, infer_features: bool = False
) -> BatchFeatureView:
    driver_stats_feature_view = BatchFeatureView(
        name="driver_stats",
        entities=[driver()],
        schema=None
        if infer_features
        else [
            Field(name="conv_rate", dtype=Float32),
            Field(name="acc_rate", dtype=Float32),
            Field(name="avg_daily_trips", dtype=Int32),
        ],
        source=source,
        ttl=timedelta(hours=2),
        tags=TAGS,
        udf=lambda x: x,
    )
    return driver_stats_feature_view


def create_customer_daily_profile_feature_view(source, infer_features: bool = False):
    customer_profile_feature_view = FeatureView(
        name="customer_profile",
        entities=[customer()],
        schema=None
        if infer_features
        else [
            Field(name="current_balance", dtype=Float32),
            Field(name="avg_passenger_count", dtype=Float32),
            Field(name="lifetime_trip_count", dtype=Int32),
        ],
        source=source,
        ttl=timedelta(days=2),
        tags=TAGS,
    )
    return customer_profile_feature_view


def create_global_stats_feature_view(source, infer_features: bool = False):
    global_stats_feature_view = FeatureView(
        name="global_stats",
        entities=[],
        schema=None
        if infer_features
        else [
            Field(name="num_rides", dtype=Int32),
            Field(name="avg_ride_length", dtype=Float32),
        ],
        source=source,
        ttl=timedelta(days=2),
        tags=TAGS,
    )
    return global_stats_feature_view


def create_order_feature_view(source, infer_features: bool = False):
    return FeatureView(
        name="order",
        entities=[customer(), driver()],
        schema=None
        if infer_features
        else [
            Field(name="order_is_success", dtype=Int32),
            Field(name="driver_id", dtype=Int64),
        ],
        source=source,
        ttl=timedelta(days=2),
    )


def create_location_stats_feature_view(source, infer_features: bool = False):
    location_stats_feature_view = FeatureView(
        name="location_stats",
        entities=[location()],
        schema=None
        if infer_features
        else [
            Field(name="temperature", dtype=Int32),
            Field(name="location_id", dtype=Int64),
        ],
        source=source,
        ttl=timedelta(days=2),
    )
    return location_stats_feature_view


def create_field_mapping_feature_view(source):
    return FeatureView(
        name="field_mapping",
        entities=[],
        schema=[Field(name="feature_name", dtype=Int32)],
        source=source,
        ttl=timedelta(days=2),
    )


def create_pushable_feature_view(batch_source: DataSource):
    push_source = PushSource(
        name="location_stats_push_source",
        batch_source=batch_source,
    )
    return StreamFeatureView(
        name="pushable_location_stats",
        entities=[location()],
        schema=[
            Field(name="temperature", dtype=Int32),
            Field(name="location_id", dtype=Int64),
        ],
        ttl=timedelta(days=2),
        source=push_source,
    )
