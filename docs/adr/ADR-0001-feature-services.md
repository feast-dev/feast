# ADR-0001: Feature Services

## Status

Accepted

## Context

Feast's Feature Views allowed for storage-level grouping of features based on how they are produced. However, there was no concept of a retrieval-level grouping of features that maps to models. Without this:

- There was no way to track which features were used to train a model or serve a specific model.
- Retrieving features during training required a complete list of features to be provided and persisted manually, which was error-prone.
- There was no way to ensure consumers wouldn't face breaking changes when feature views changed.

## Decision

Introduce a `FeatureService` object that allows users to define which features to use for a specific ML use case. A feature service groups features from one or more feature views for model training and online serving.

### API Design

Feature services use a Pandas-like API where feature views can be referenced directly:

```python
from feast import FeatureService

feature_service = FeatureService(
    name="my_model_v1",
    features=[
        shop_raw,                                           # select all features
        customer_sales[["average_order_value", "max_order_value"]],  # select specific features
    ],
)
```

Feature selection with aliasing:

```python
feature_service = FeatureService(
    name="my_model_v1",
    features=[
        shop_raw,
        customer_sales[["average_order_value", "max_order_value"]]
            .alias({"average_order_value": "avg_o_val"}),
    ],
)
```

### Retrieval

```python
# Online inference
row = store.get_online_features(
    feature_service="my_model_v1",
    entity_rows=[{"customer_id": 123, "shop_id": 456}],
).to_dict()

# Training
historical_df = store.get_historical_features(
    feature_service="my_model_v1",
    entity_df=entity_df,
)
```

### Key Decisions

- **Name**: `FeatureService` was chosen over `FeatureSet` because it conveys the concept of a serving layer bridging models and data. `FeatureService` is analogous to model services in model serving systems.
- **Mutability**: Feature services are mutable. Immutability may be considered in the future.
- **Versioning**: Not included in the first version; users manage versions through naming conventions.

## Consequences

### Positive

- Users can track which features are used for training and serving specific models.
- Provides a consistent interface for both online and offline feature retrieval.
- Reduces error-prone manual feature list management.
- Enables future functionality like logging, monitoring, and endpoint provisioning.

### Negative

- Adds another abstraction layer to the Feast data model.
- Feature services are mutable, which may lead to inconsistencies if not carefully managed.

## References

- Original RFC: [Feast RFC-015: Feature Services](https://docs.google.com/document/d/1jC0RJbyYLilXTOrLVBeR22PYLK5fe2JmQK1mKdZ-eno/edit)
- Implementation: `sdk/python/feast/feature_service.py`
