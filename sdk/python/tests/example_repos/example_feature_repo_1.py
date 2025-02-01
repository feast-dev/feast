from datetime import timedelta

import pandas as pd

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Array, Float32, Int64, String
from tests.integration.feature_repos.universal.feature_views import TAGS

# Note that file source paths are not validated, so there doesn't actually need to be any data
# at the paths for these file sources. Since these paths are effectively fake, this example
# feature repo should not be used for historical retrieval.

driver_locations_source = FileSource(
    path="data/driver_locations.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

customer_profile_source = FileSource(
    name="customer_profile_source",
    path="data/customer_profiles.parquet",
    timestamp_field="event_timestamp",
)

customer_driver_combined_source = FileSource(
    path="data/customer_driver_combined.parquet",
    timestamp_field="event_timestamp",
)

driver_locations_push_source = PushSource(
    name="driver_locations_push",
    batch_source=driver_locations_source,
)

rag_documents_source = FileSource(
    name="rag_documents_source",
    path="data/rag_documents.parquet",
    timestamp_field="event_timestamp",
)

driver = Entity(
    name="driver",  # The name is derived from this argument, not object name.
    join_keys=["driver_id"],
    description="driver id",
    tags=TAGS,
)

customer = Entity(
    name="customer",  # The name is derived from this argument, not object name.
    join_keys=["customer_id"],
    tags=TAGS,
)

item = Entity(
    name="item_id",  # The name is derived from this argument, not object name.
    join_keys=["item_id"],
)

driver_locations = FeatureView(
    name="driver_locations",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="lat", dtype=Float32),
        Field(name="lon", dtype=String),
        Field(name="driver_id", dtype=Int64),
    ],
    online=True,
    source=driver_locations_source,
    tags={},
)

pushed_driver_locations = FeatureView(
    name="pushed_driver_locations",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="driver_lat", dtype=Float32),
        Field(name="driver_long", dtype=String),
        Field(name="driver_id", dtype=Int64),
    ],
    online=True,
    source=driver_locations_push_source,
    tags={},
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

customer_driver_combined = FeatureView(
    name="customer_driver_combined",
    entities=[customer, driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="trips", dtype=Int64),
        Field(name="driver_id", dtype=Int64),
        Field(name="customer_id", dtype=String),
    ],
    online=True,
    source=customer_driver_combined_source,
    tags={},
)

document_embeddings = FeatureView(
    name="document_embeddings",
    entities=[item],
    schema=[
        Field(
            name="Embeddings",
            dtype=Array(Float32),
            vector_index=True,
            vector_search_metric="L2",
        ),
        Field(name="item_id", dtype=String),
    ],
    source=rag_documents_source,
    ttl=timedelta(hours=24),
)


@on_demand_feature_view(
    sources=[customer_profile],
    schema=[Field(name="on_demand_age", dtype=Int64)],
    mode="pandas",
)
def customer_profile_pandas_odfv(inputs: pd.DataFrame) -> pd.DataFrame:
    outputs = pd.DataFrame()
    outputs["on_demand_age"] = inputs["age"] + 1
    return outputs


all_drivers_feature_service = FeatureService(
    name="driver_locations_service",
    features=[driver_locations],
    tags=TAGS,
)
