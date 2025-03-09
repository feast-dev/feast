# Feast 0.13 adds on-demand transforms, feature servers, and feature views without entities

*October 2, 2021* | *Danny Chiao, Tsotne Tabidze, Achal Shah, and Felix Wang*

We are delighted to announce the release of [Feast 0.13](https://github.com/feast-dev/feast/releases/tag/v0.13.0), which introduces:

* [Experimental] On demand feature views, which allow for consistently applied transformations in both training and online paths. This also introduces the concept of request data, which is data only available at the time of the prediction request, as potential inputs into these transformations
* [Experimental] Python feature servers, which allow you to quickly deploy a local HTTP server to serve online features. Serverless deployments and java feature servers to come soon!
* Feature views without entities, which allow you to specify features that should only be joined on event timestamps. You do not need lists of entities / entity values when defining and retrieving features from these feature views.

Experimental features are subject to API changes in the near future as we collect feedback. If you have thoughts, please don't hesitate to reach out to the Feast team!

### [Experimental] On demand feature views

On demand feature views allows users to use existing features and request data to transform and create new features. Users define Python transformation logic which is executed in both historical retrieval and online retrieval paths.‌ This unlocks many use cases including fraud detection and recommender systems, and reduces training / serving skew by allowing for consistently applied transformations. Example features may include:

* Transactional features such as `transaction_amount_greater_than_7d_average` where the inputs to features are part of the transaction, booking, or order event.
* Features requiring the current location or time such as `user_account_age`, `distance_driver_customer`
* Feature crosses where the keyspace is too large to precompute such as `movie_category_x_movie_rating` or `lat_bucket_x_lon_bucket`

Currently, these transformations are executed locally. Future milestones include building a feature transformation server for executing transformations at higher scale.

First, we define the transformations:

```python
# Define a request data source which encodes features / information only 
# available at request time (e.g. part of the user initiated HTTP request)
input_request = RequestDataSource(
    name="vals_to_add",
    schema={
        "val_to_add": ValueType.INT64,
    }
)
```

See [On demand feature view](https://docs.feastsite.wpenginepowered.com/reference/on-demand-feature-view) for detailed info on how to use this functionality.

### [Experimental] Python feature server

The Python feature server provides an HTTP endpoint that serves features from the feature store. This enables users to retrieve features from Feast using any programming language that can make HTTP requests. As of now, it's only possible to run the server locally. A remote serverless feature server is currently being developed. Additionally, a low latency java feature server is in development.

```bash
$ feast init feature_repo
Creating a new Feast repository in /home/tsotne/feast/feature_repo.
```
