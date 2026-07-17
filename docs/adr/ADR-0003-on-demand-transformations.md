# ADR-0003: On-Demand Transformations

## Status

Accepted

## Context

For many ML use cases, it is not possible or feasible to precompute and persist feature values for serving:

- **Transactional use cases**: Inputs are part of the transaction/booking/order event.
- **Clickstream use cases**: User event data contains raw data used for feature engineering.
- **Location-based use cases**: Distance calculations between feature views (e.g., customer and driver locations).
- **Time-dependent features**: e.g., `user_account_age = current_time - account_creation_time`.
- **Crossed features**: e.g., user-user, user-tweet based features where the keyspace is too large to precompute.

Additionally, Feast did not provide a means for post-processing features, forcing all feature development to upstream systems.

## Decision

Introduce **On-Demand Feature Views** as a feature transformation layer with the following properties:

- Transformations execute at retrieval time (post-processing step after reading from the store).
- The calling client can input data as part of the retrieval request via a `RequestSource`.
- Users define arbitrary transformations on both stored features and request-time input data.
- Transformations are row-level operations only (no aggregations).

### Definition API

Uses the `@on_demand_feature_view` decorator (Option 3 from the RFC was chosen):

```python
from feast import on_demand_feature_view, Field, RequestSource
from feast.types import Float64, String

input_request = RequestSource(
    name="transaction",
    schema=[Field(name="input_lat", dtype=Float64), Field(name="input_lon", dtype=Float64)],
)

@on_demand_feature_view(
    sources=[driver_fv, input_request],
    schema=[Field(name="distance", dtype=Float64)],
)
def driver_distance(inputs: pd.DataFrame) -> pd.DataFrame:
    from haversine import haversine
    df = pd.DataFrame()
    df["distance"] = inputs.apply(
        lambda r: haversine((r["lat"], r["lon"]), (r["input_lat"], r["input_lon"])),
        axis=1,
    )
    return df
```

### Retrieval

```python
# Online - request data passed as entity rows
features = store.get_online_features(
    features=["driver_distance:distance"],
    entity_rows=[{"driver_id": 1001, "input_lat": 1.234, "input_lon": 5.678}],
).to_dict()

# Offline - request data columns included in entity_df
df = store.get_historical_features(
    entity_df=entity_df_with_request_columns,
    features=["driver_distance:distance"],
).to_df()
```

### Key Decisions

- **Decorator approach** chosen over adding transforms to FeatureService or FeatureView directly. This avoids changing existing APIs and keeps transformations self-contained.
- **Pandas DataFrames** as the input/output type to support vectorized operations.
- **All imports must be self-contained** within the function block for serialization.
- **Offline transformations** initially execute client-side using Dask for scalability.
- **Feature Transformation Server (FTS)** handles online transformations via HTTP/REST, deployed at `apply` time.

## Consequences

### Positive

- Enables real-time feature engineering that depends on request-time data.
- Keeps feature logic co-located with feature definitions in the repository.
- Provides a consistent interface for both online and offline feature retrieval.
- The FTS allows horizontal scaling independent of feature serving.

### Negative

- Adds computational overhead to the serving path since transformations run at read time.
- On-demand feature views are limited to row-level transformations (no aggregations).
- Python function serialization requires self-contained imports within function blocks.

## References

- Original RFC: [Feast RFC-021: On-Demand Transformations](https://docs.google.com/document/d/1lgfIw0Drc65LpaxbUu49RCeJgMew547meSJttnUqz7c/edit)
- Implementation: `sdk/python/feast/on_demand_feature_view.py`
