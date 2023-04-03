# Adding or reusing tests

## Overview

This guide will go over:

1. how Feast tests are setup
2. how to extend the test suite to test new functionality
3. how to use the existing test suite to test a new custom offline / online store

## Test suite overview

Unit tests are contained in `sdk/python/tests/unit`.
Integration tests are contained in `sdk/python/tests/integration`.
Let's inspect the structure of `sdk/python/tests/integration`:

```bash
$ tree
.
├── e2e
│   ├── test_go_feature_server.py
│   ├── test_python_feature_server.py
│   ├── test_universal_e2e.py
│   ├── test_usage_e2e.py
│   └── test_validation.py
├── feature_repos
│   ├── integration_test_repo_config.py
│   ├── repo_configuration.py
│   └── universal
│       ├── catalog
│       ├── data_source_creator.py
│       ├── data_sources
│       │   ├── __init__.py
│       │   ├── bigquery.py
│       │   ├── file.py
│       │   ├── redshift.py
│       │   └── snowflake.py
│       ├── entities.py
│       ├── feature_views.py
│       ├── online_store
│       │   ├── __init__.py
│       │   ├── datastore.py
│       │   ├── dynamodb.py
│       │   ├── hbase.py
│       │   └── redis.py
│       └── online_store_creator.py
├── materialization
│   └── test_lambda.py
├── offline_store
│   ├── test_feature_logging.py
│   ├── test_offline_write.py
│   ├── test_push_features_to_offline_store.py
│   ├── test_s3_custom_endpoint.py
│   └── test_universal_historical_retrieval.py
├── online_store
│   ├── test_push_features_to_online_store.py
│   └── test_universal_online.py
└── registration
    ├── test_feature_store.py
    ├── test_inference.py
    ├── test_registry.py
    ├── test_universal_cli.py
    ├── test_universal_odfv_feature_inference.py
    └── test_universal_types.py

```

* `feature_repos` has setup files for most tests in the test suite.
* `conftest.py` (in the parent directory) contains the most common [fixtures](https://docs.pytest.org/en/6.2.x/fixture.html), which are designed as an abstraction on top of specific offline/online stores, so tests do not need to be rewritten for different stores. Individual test files also contain more specific fixtures.
* The tests are organized by which Feast component(s) they test.

## Structure of the test suite

### Universal feature repo

The universal feature repo refers to a set of fixtures (e.g. `environment` and `universal_data_sources`) that can be parametrized to cover various combinations of offline stores, online stores, and providers.
This allows tests to run against all these various combinations without requiring excess code.
The universal feature repo is constructed by fixtures in `conftest.py` with help from the various files in `feature_repos`.

### Integration vs. unit tests

Tests in Feast are split into integration and unit tests.
If a test requires external resources (e.g. cloud resources on GCP or AWS), it is an integration test.
If a test can be run purely locally (where locally includes Docker resources), it is a unit test.

* Integration tests test non-local Feast behavior. For example, tests that require reading data from BigQuery or materializing data to DynamoDB are integration tests. Integration tests also tend to involve more complex Feast functionality.
* Unit tests test local Feast behavior. For example, tests that only require registering feature views are unit tests. Unit tests tend to only involve simple Feast functionality.

### Main types of tests

#### Integration tests

1. E2E tests
    * E2E tests test end-to-end functionality of Feast over the various codepaths (initialize a feature store, apply, and materialize).
    * The main codepaths include:
        * basic e2e tests for offline stores
            * `test_universal_e2e.py`
        * go feature server
            * `test_go_feature_server.py`
        * python http server
            * `test_python_feature_server.py`
        * usage tracking
            * `test_usage_e2e.py`
        * data quality monitoring feature validation
            * `test_validation.py`
2. Offline and Online Store Tests
    * Offline and online store tests mainly test for the offline and online retrieval functionality.
    * The various specific functionalities that are tested include:
        * push API tests
            * `test_push_features_to_offline_store.py`
            * `test_push_features_to_online_store.py`
            * `test_offline_write.py`
        * historical retrieval tests
            *  `test_universal_historical_retrieval.py`
        * online retrieval tests
            * `test_universal_online.py`
        * data quality monitoring feature logging tests
            * `test_feature_logging.py`
        * online store tests
            * `test_universal_online.py`
3. Registration Tests
    * The registration folder contains all of the registry tests and some universal cli tests. This includes:
        * CLI Apply and Materialize tests tested against on the universal test suite
        * Data type inference tests
        * Registry tests
4. Miscellaneous Tests
    * AWS Lambda Materialization Tests (Currently do not work)
        * `test_lambda.py`

#### Unit tests

1. Registry Diff Tests
    * These are tests for the infrastructure and registry diff functionality that Feast uses to determine if changes to the registry or infrastructure is needed.
2. Local CLI Tests and Local Feast Tests
    * These tests test all of the cli commands against the local file offline store.
3. Infrastructure Unit Tests
    * DynamoDB tests with dynamo mocked out
    * Repository configuration tests
    * Schema inference unit tests
    * Key serialization tests
    * Basic provider unit tests
4. Feature Store Validation Tests
    * These test mainly contain class level validation like hashing tests, protobuf and class serialization, and error and warning handling.
        * Data source unit tests
        * Feature service unit tests
        * Feature service, feature view, and feature validation tests
        * Protobuf/json tests for Feast ValueTypes
        * Serialization tests
            * Type mapping
            * Feast types
            * Serialization tests due to this [issue](https://github.com/feast-dev/feast/issues/2345)
        * Feast usage tracking unit tests

#### Docstring tests

Docstring tests are primarily smoke tests to make sure imports and setup functions can be executed without errors.

## Understanding the test suite with an example test

### Example test

Let's look at a sample test using the universal repo:

{% tabs %}
{% tab code="sdk/python/tests/integration/offline_store/test_universal_historical_retrieval.py" %}
```python
@pytest.mark.integration
@pytest.mark.universal_offline_stores
@pytest.mark.parametrize("full_feature_names", [True, False], ids=lambda v: f"full:{v}")
def test_historical_features(environment, universal_data_sources, full_feature_names):
    store = environment.feature_store

    (entities, datasets, data_sources) = universal_data_sources

    feature_views = construct_universal_feature_views(data_sources)

    entity_df_with_request_data = datasets.entity_df.copy(deep=True)
    entity_df_with_request_data["val_to_add"] = [
        i for i in range(len(entity_df_with_request_data))
    ]
    entity_df_with_request_data["driver_age"] = [
        i + 100 for i in range(len(entity_df_with_request_data))
    ]

    feature_service = FeatureService(
        name="convrate_plus100",
        features=[feature_views.driver[["conv_rate"]], feature_views.driver_odfv],
    )
    feature_service_entity_mapping = FeatureService(
        name="entity_mapping",
        features=[
            feature_views.location.with_name("origin").with_join_key_map(
                {"location_id": "origin_id"}
            ),
            feature_views.location.with_name("destination").with_join_key_map(
                {"location_id": "destination_id"}
            ),
        ],
    )

    store.apply(
        [
            driver(),
            customer(),
            location(),
            feature_service,
            feature_service_entity_mapping,
            *feature_views.values(),
        ]
    )
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
            "conv_rate_plus_100:conv_rate_plus_100_rounded",
            "conv_rate_plus_100:conv_rate_plus_val_to_add",
            "order:order_is_success",
            "global_stats:num_rides",
            "global_stats:avg_ride_length",
            "field_mapping:feature_name",
        ],
        full_feature_names=full_feature_names,
    )

    if job_from_df.supports_remote_storage_export():
        files = job_from_df.to_remote_storage()
        print(files)
        assert len(files) > 0  # This test should be way more detailed

    start_time = datetime.utcnow()
    actual_df_from_df_entities = job_from_df.to_df()
    # ... more test code

    validate_dataframes(
        expected_df,
        table_from_df_entities,
        sort_by=[event_timestamp, "order_id", "driver_id", "customer_id"],
        event_timestamp = event_timestamp,
    )
    # ... more test code
```
{% endtab %}
{% endtabs %}

* The key fixtures are the `environment` and `universal_data_sources` fixtures, which are defined in the `feature_repos` directories and the `conftest.py` file. This by default pulls in a standard dataset with driver and customer entities (that we have pre-defined), certain feature views, and feature values.
    * The `environment` fixture sets up a feature store, parametrized by the provider and the online/offline store. It allows the test to query against that feature store without needing to worry about the underlying implementation or any setup that may be involved in creating instances of these datastores.
    * Each fixture creates a different integration test with its own `IntegrationTestRepoConfig` which is used by pytest to generate a unique test testing one of the different environments that require testing.

* Feast tests also use a variety of markers:
    * The `@pytest.mark.integration` marker is used to designate integration tests which will cause the test to be run when you call `make test-python-integration`.
    * The `@pytest.mark.universal_offline_stores` marker will parametrize the test on all of the universal offline stores including file, redshift, bigquery and snowflake.
    * The `full_feature_names` parametrization defines whether or not the test should reference features as their full feature name (fully qualified path) or just the feature name itself.


## Writing a new test or reusing existing tests

### To add a new test to an existing test file

* Use the same function signatures as an existing test (e.g. use `environment` and `universal_data_sources` as an argument) to include the relevant test fixtures.
* If possible, expand an individual test instead of writing a new test, due to the cost of starting up offline / online stores.
* Use the `universal_offline_stores` and `universal_online_store` markers to parametrize the test against different offline store and online store combinations. You can also designate specific online and offline stores to test by using the `only` parameter on the marker.

```python
@pytest.mark.universal_online_stores(only=["redis"])
```
### To test a new offline / online store from a plugin repo

* Install Feast in editable mode with `pip install -e`.
* The core tests for offline / online store behavior are parametrized by the `FULL_REPO_CONFIGS` variable defined in `feature_repos/repo_configuration.py`. To overwrite this variable without modifying the Feast repo, create your own file that contains a `FULL_REPO_CONFIGS` (which will require adding a new `IntegrationTestRepoConfig` or two) and set the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file. Then the core offline / online store tests can be run with `make test-python-universal`.
* See the [custom offline store demo](https://github.com/feast-dev/feast-custom-offline-store-demo) and the [custom online store demo](https://github.com/feast-dev/feast-custom-online-store-demo) for examples.

### What are some important things to keep in mind when adding a new offline / online store?

#### Type mapping/Inference

Many problems arise when implementing your data store's type conversion to interface with Feast datatypes.
1. You will need to correctly update `inference.py` so that Feast can infer your datasource schemas
2. You also need to update `type_map.py` so that Feast knows how to convert your datastores types to Feast-recognized types in `feast/types.py`.

#### Historical and online retrieval

The most important functionality in Feast is historical and online retrieval. Most of the e2e and universal integration test test this functionality in some way. Making sure this functionality works also indirectly asserts that reading and writing from your datastore works as intended.


### To include a new offline / online store in the main Feast repo

* Extend `data_source_creator.py` for your offline store.
* In `repo_configuration.py` add a new `IntegrationTestRepoConfig` or two (depending on how many online stores you want to test).
    * Generally, you should only need to test against sqlite. However, if you need to test against a production online store, then you can also test against Redis or dynamodb.
* Run the full test suite with `make test-python-integration.`

### Including a new offline / online store in the main Feast repo from external plugins with community maintainers.

* This folder is for plugins that are officially maintained with community owners. Place the APIs in `feast/infra/offline_stores/contrib/`.
* Extend `data_source_creator.py` for your offline store and implement the required APIs.
* In `contrib_repo_configuration.py` add a new `IntegrationTestRepoConfig` (depending on how many online stores you want to test).
* Run the test suite on the contrib test suite with `make test-python-contrib-universal`.

### To include a new online store

* In `repo_configuration.py` add a new config that maps to a serialized version of configuration you need in `feature_store.yaml` to setup the online store.
* In `repo_configuration.py`, add new `IntegrationTestRepoConfig` for online stores you want to test.
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

### Running your own Redis cluster for testing

* Install Redis on your computer. If you are a mac user, you should be able to `brew install redis`.
    * Running `redis-server --help` and `redis-cli --help` should show corresponding help menus.
* * Run `./infra/scripts/redis-cluster.sh start` then `./infra/scripts/redis-cluster.sh create` to start the Redis cluster locally. You should see output that looks like this:
~~~~
Starting 6001
Starting 6002
Starting 6003
Starting 6004
Starting 6005
Starting 6006
~~~~
* You should be able to run the integration tests and have the Redis cluster tests pass.
* If you would like to run your own Redis cluster, you can run the above commands with your own specified ports and connect to the newly configured cluster.
* To stop the cluster, run `./infra/scripts/redis-cluster.sh stop` and then `./infra/scripts/redis-cluster.sh clean`.
