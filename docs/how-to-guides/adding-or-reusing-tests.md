# Adding or reusing tests

## Overview

This guide will go over:

1. how Feast tests are setup
2. how to extend the test suite to test new functionality
3. how to use the existing test suite to test a new custom offline / online store.

## Test suite overview

Let's inspect the test setup as is:

```bash
$ tree

.
├── e2e
│   └── test_universal_e2e.py
├── feature_repos
│   ├── repo_configuration.py
│   └── universal
│       ├── data_source_creator.py
│       ├── data_sources
│       │   ├── bigquery.py
│       │   ├── file.py
│       │   └── redshift.py
│       ├── entities.py
│       └── feature_views.py
├── offline_store
│   ├── test_s3_custom_endpoint.py
│   └── test_universal_historical_retrieval.py
├── online_store
│   ├── test_e2e_local.py
│   ├── test_feature_service_read.py
│   ├── test_online_retrieval.py
│   └── test_universal_online.py
├── registration
│   ├── test_cli.py
│   ├── test_cli_apply_duplicated_featureview_names.py
│   ├── test_cli_chdir.py
│   ├── test_feature_service_apply.py
│   ├── test_feature_store.py
│   ├── test_inference.py
│   ├── test_registry.py
│   ├── test_universal_odfv_feature_inference.py
│   └── test_universal_types.py
└── scaffolding
    ├── test_init.py
    ├── test_partial_apply.py
    ├── test_repo_config.py
    └── test_repo_operations.py

8 directories, 27 files
```

`feature_repos` has setup files for most tests in the test suite and sets up pytest fixtures for other tests. Crucially, this parametrizes on different offline stores, different online stores, etc and abstracts away store specific implementations so tests don't need to rewrite e.g. uploading dataframes to a specific store for setup.

## Understanding an example test

Let's look at a sample test using the universal repo:

{% tabs %}
{% tab title="Python" %}
```python
@pytest.mark.integration
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: str(v))
def test_historical_features(environment, universal_data_sources, full_feature_names):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources
    feature_views = construct_universal_feature_views(data_sources)

    customer_df, driver_df, orders_df, global_df, entity_df = (
        datasets["customer"],
        datasets["driver"],
        datasets["orders"],
        datasets["global"],
        datasets["entity"],
    )

    # ... more test code

    customer_fv, driver_fv, driver_odfv, order_fv, global_fv = (
        feature_views["customer"],
        feature_views["driver"],
        feature_views["driver_odfv"],
        feature_views["order"],
        feature_views["global"],
    )

    feature_service = FeatureService(
        "convrate_plus100",
        features=[
            feature_views["driver"][["conv_rate"]], 
            feature_views["driver_odfv"]
        ],
    )

    feast_objects = []
    feast_objects.extend(
        [
            customer_fv,
            driver_fv,
            driver_odfv,
            order_fv,
            global_fv,
            driver(),
            customer(),
            feature_service,
        ]
    )
    store.apply(feast_objects)

    # ... more test code

    job_from_df = store.get_historical_features(
        entity_df=entity_df_with_request_data,
        features=[
            "driver_stats:conv_rate",
            "driver_stats:avg_daily_trips",
            "customer_profile:current_balance",
            "customer_profile:avg_passenger_count",
            "customer_profile:lifetime_trip_count",
            "conv_rate_plus_100:conv_rate_plus_100",
            "conv_rate_plus_100:conv_rate_plus_val_to_add",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
        ],
        full_feature_names=full_feature_names,
    )
    actual_df_from_df_entities = job_from_df.to_df()

    # ... more test code

    assert_frame_equal(
        expected_df, actual_df_from_df_entities, check_dtype=False,
    )

    # ... more test code
```
{% endtab %}
{% endtabs %}

The key fixtures are the `environment` and `universal_data_sources` fixtures, which are defined in the `feature_repos` directories. This by default pulls in a standard dataset with driver and customer entities, certain feature views, and feature values. By including the environment as a parameter, the test automatically parametrizes across other offline / online store combinations.

## Writing a new test or reusing existing tests

To:

* Include a new offline store: 
  * extend `data_source_creator.py` for your offline store
  * in `repo_configuration.py` add a new`IntegrationTestRepoConfig` or two (depending on how many online stores you want to test)
  * Run the full test suite with `make test-python-integration`
  * See more guidelines at [https://github.com/feast-dev/feast/issues/1892](https://github.com/feast-dev/feast/issues/1892)
* Include a new online store:
  * in `repo_configuration.py` add a new config that maps to a serialized version of configuration you need in `feature_store.yaml` to setup the online store.
  * in `repo_configuration.py`, add new`IntegrationTestRepoConfig` for offline stores you want to test
  * Run the full test suite with `make test-python-integration`
  * See more guidelines at [https://github.com/feast-dev/feast/issues/1892](https://github.com/feast-dev/feast/issues/1892)
* Add a new test to an existing test file:
  * Use the same function signatures as an existing test (e.g. have environment as an argument) to include the relevant test fixtures. 
  * We prefer to expand what an individual test covers due to the cost of standing up offline / online stores
* Using custom data in a new test:
  * This is used in several places such as `test_universal_types.py` 

```python
@pytest.mark.integration
def your_test(environment: Environment):
    df = #...#
    data_source = environment.data_source_creator.create_data_source(
        df,
        destination_name=environment.feature_store.project
    )
    your_fv = driver_feature_view(data_source)
    entity = driver(value_type=ValueType.UNKNOWN)
    fs.apply([fv, entity])

    # ... run test

    environment.data_source_creator.teardown()
```
