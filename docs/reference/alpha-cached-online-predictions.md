# [Alpha] Cached Online Predictions
**Warning**: This is an _experimental_ feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview

In Machine Learning architectures, model serving/inference is typically done on demand (i.e., at request time) but this 
is often unnecessary and adds additional latency. In many cases, the model predictions change only when the underlying
data changes. The `get_online_predictions` method allows MLEs to store their model predictions in the Feature Store and retrieve 
the precomputed output, which will enable a much better user experience. Additionally, we allow the user to force
recomputation of the model predictions if needed.

This works by creating two feature views: one for the cached model predictions and one for the on-demand model predictions.
The on demand feature view is used for calculating the model predictions on demand, while the cached feature view is used for
fast retrieval and storing the score after the model has been computed. The `get_online_predictions` method then retrieves the
cached model predictions if they exist, and falls back to the on-demand model predictions if they do not.

This is a good option for small models where the artifact can fit into the registry and the service can support the 
additional bloat of the supporting libraries and CPU overhead of running them. 

**This is *not* the recommended option for scaling to large models, models that require GPU acceleration, or serving
several medium sized models. For those cases, we recommend using a full model inference service such as 
[KServe](https://kserve.github.io/website/latest/).**

## Why use cached predictions?
It is well known that users of the internet do not like to wait. Running inference on demand when not necessary adds
latency. Models depend on features and features depend on data. If the data is not changing, the model predictions will 
not change, so precomputing the model predictions and storing them in the Feature Store can save time, resources, and 
improve the user experience.

## Is Model inference in a Feature Transformation an abuse of Feature Transformation?
Yes, but if you understand the limitations and use this approach as a light-weight solution to drive impactful work while 
building a full platform, it can be a useful tool. It is an imperfect compromise that can be useful in the right 
context (e.g., small teams that may not have the need to run large models).

## Method Overview

The `get_online_predictions` method requires the following parameters:

- `features`: A list of features to retrieve.
- `entity_rows`: A list of dictionaries where each dictionary represents a single entity row.
- `cached_model_feature_reference`: A string representing the feature reference of the cached model feature.
- `on_demand_model_feature_reference`: A string representing the feature reference of the on-demand model feature.
- `force_recompute`: A boolean value indicating whether to force the re-computation of the model feature.
- `log_features`: A boolean value indicating whether to log the features.

This method returns a dictionary containing the requested features and their corresponding values.

## Endpoint Overview

The `/get-online-predictions` endpoint in the Feast Feature Server is designed to interact with the 
`get_online_predictions` method. This endpoint accepts a POST request with a JSON body containing the parameters 
required by the `get_online_predictions` method.

## Example

Here's an example of how to use the `get_online_predictions` method, taken from the `test_get_online_predictions` 
function in the `test_online_retrieval.py` file:

```python
result = store.get_online_predictions(
    features=[
        "customer_profile:avg_orders_day",
        "customer_profile:age",
    ],
    entity_rows=[
        {"customer_id": "5"},
        {"customer_id": 5},
    ],
    cached_model_feature_reference="stored_customer_predictions:predictions",
    on_demand_model_feature_reference="risk_score_calculator:predictions",
    force_recompute=False,
    log_features=True,
).to_dict()
```

In this example, the method is used to retrieve the average orders per day and the age for customers with IDs "5" and 5. 
The cached model predictions are retrieved from the stored_customer_predictions:predictions feature, and the on-demand 
model predictions are computed using the risk_score_calculator:predictions feature. The force_recompute parameter is 
set to False, indicating that the method should use the cached predictions if available. The log_features parameter is 
set to True, indicating that the features should be logged.

## Alternatives / Expanding Scope

In the future, we will look to add support for calling KServe or another model inference endpoint in the 
On Demand Feature View directly. This will allow for more flexibility in the models that can be served and still get the
benefits of caching scores in the Feature Store.