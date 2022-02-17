# Adding or reusing tests

## Overview

This guide will go over:

1. how Feast tests are setup
2. how to extend the test suite to test new functionality
3. how to use the existing test suite to test a new custom offline / online store.

## Test suite overview

Let's inspect the test setup in `sdk/python/tests/integration`:

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

`feature_repos` has setup files for most tests in the test suite and pytest fixtures for other tests. These fixtures parametrize on different offline stores, online stores, etc. and thus abstract away store specific implementations so tests don't need to rewrite e.g. uploading dataframes to a specific store for setup.

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

### To add a new test to an existing test file

* Use the same function signatures as an existing test (e.g. use `environment` as an argument) to include the relevant test fixtures.
* If possible, expand an individual test instead of writing a new test, due to the cost of standing up offline / online stores.

### To test a new offline / online store from a plugin repo

* Install Feast in editable mode with `pip install -e`.
* The core tests for offline / online store behavior are parametrized by the `FULL_REPO_CONFIGS` variable defined in `feature_repos/repo_configuration.py`. To overwrite this variable without modifying the Feast repo, create your own file that contains a `FULL_REPO_CONFIGS` (which will require adding a new `IntegrationTestRepoConfig` or two) and set the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file. Then the core offline / online store tests can be run with `make test-python-universal`.
* See the [custom offline store demo](https://github.com/feast-dev/feast-custom-offline-store-demo) and the [custom online store demo](https://github.com/feast-dev/feast-custom-online-store-demo) for examples.

### To include a new offline / online store in the main Feast repo

* Extend `data_source_creator.py` for your offline store.
* In `repo_configuration.py` add a new`IntegrationTestRepoConfig` or two (depending on how many online stores you want to test).
* Run the full test suite with `make test-python-integration.`

### To include a new online store

* In `repo_configuration.py` add a new config that maps to a serialized version of configuration you need in `feature_store.yaml` to setup the online store.
* In `repo_configuration.py`, add new`IntegrationTestRepoConfig` for offline stores you want to test.
* Run the full test suite with `make test-python-integration`

### To use custom data in a new test

* Check `test_universal_types.py` for an example of how to do this.

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
```

### Running your own redis cluster for testing

* Install redis on your computer. If you are a mac user, you should be able to `brew install redis`.
    * Running `redis-server --help` and `redis-cli --help` should show corresponding help menus.
* Run `cd scripts/create-cluster` and run `./create-cluster start` then `./create-cluster create` to start the server. You should see output that looks like this:
~~~~
Starting 6001
Starting 6002
Starting 6003
Starting 6004
Starting 6005
Starting 6006
~~~~
* You should be able to run the integration tests and have the redis cluster tests pass.
* If you would like to run your own redis cluster, you can run the above commands with your own specified ports and connect to the newly configured cluster.
* To stop the cluster, run `./create-cluster stop` and then `./create-cluster clean`.

