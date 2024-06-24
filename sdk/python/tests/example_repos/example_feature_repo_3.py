from datetime import timedelta
from typing import Any

from feast import Entity, FeatureView, Field, FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64, String
from tests.integration.feature_repos.universal.feature_views import TAGS

# Note that file source paths are not validated, so there doesn't actually need to be any data
# at the paths for these file sources. Since these paths are effectively fake, this example
# feature repo should not be used for historical retrieval.

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_keys=["customer_id"],
    tags=TAGS,
)

customer_profile_source = FileSource(
    name="customer_profile_source",
    path="data/customer_profiles.parquet",
    timestamp_field="event_timestamp",
)

customer_predictions_source = FileSource(
    name="customer_predictions",
    path="data/customer_predictions.parquet",
    timestamp_field="event_timestamp",
)

predictions_source = FileSource(
    name="predictions_source",
    path="data/driver_locations.parquet",
    timestamp_field="event_timestamp",
)

customer_profile = FeatureView(
    name="customer_profile",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_orders_day", dtype=Float32),
        Field(name="name", dtype=String),
        Field(name="age", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_profile_source,
    tags={},
)


stored_customer_predictions = FeatureView(
    name="stored_customer_predictions",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="predictions", dtype=Float32),
        Field(name="model_version", dtype=String),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_predictions_source,
    tags={},
)


@on_demand_feature_view(
    sources=[customer_profile],
    schema=[
        Field(name="predictions", dtype=Float64),
        Field(name="model_version", dtype=String),
    ],
    mode="python",
)
def risk_score_calculator(inputs: dict[str, Any]) -> dict[str, Any]:
    outputs = {
        "predictions": [sum(values) for values in zip(*(inputs[k] for k in ["avg_orders_day", "age"]))],
        "model_version": ["1.0.0"] * len(inputs["avg_orders_day"])
    }
    return outputs
