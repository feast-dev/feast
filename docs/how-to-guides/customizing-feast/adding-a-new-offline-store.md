# Adding a new offline store

## Overview

Feast makes adding support for a new offline store easy. Developers can simply implement the [OfflineStore](../../../sdk/python/feast/infra/offline\_stores/offline\_store.py#L41) interface to add support for a new store (other than the existing stores like Parquet files, Redshift, and Bigquery).

In this guide, we will show you how to extend the existing File offline store and use in a feature repo. While we will be implementing a specific store, this guide should be representative for adding support for any new offline store.

The full working code for this guide can be found at [feast-dev/feast-custom-offline-store-demo](https://github.com/feast-dev/feast-custom-offline-store-demo).

The process for using a custom offline store consists of 8 steps:

1. Defining an `OfflineStore` class.
2. Defining an `OfflineStoreConfig` class.
3. Defining a `RetrievalJob` class for this offline store.
4. Defining a `DataSource` class for the offline store
5. Referencing the `OfflineStore` in a feature repo's `feature_store.yaml` file.
6. Testing the `OfflineStore` class.
7. Updating dependencies.
8. Adding documentation.

## 1. Defining an OfflineStore class

{% hint style="info" %}
OfflineStore class names must end with the OfflineStore suffix!
{% endhint %}

### Contrib offline stores

New offline stores go in `sdk/python/feast/infra/offline_stores/contrib/`.

#### What is a contrib plugin?

* Not guaranteed to implement all interface methods
* Not guaranteed to be stable.
* Should have warnings for users to indicate this is a contrib plugin that is not maintained by the maintainers.

#### How do I make a contrib plugin an "official" plugin?

To move an offline store plugin out of contrib, you need:

* GitHub actions (i.e `make test-python-integration`) is setup to run all tests against the offline store and pass.
* At least two contributors own the plugin (ideally tracked in our `OWNERS` / `CODEOWNERS` file).

#### Define the offline store class

The OfflineStore class contains a couple of methods to read features from the offline store. Unlike the OnlineStore class, Feast does not manage any infrastructure for the offline store.

To fully implement the interface for the offline store, you will need to implement these methods:

* `pull_latest_from_table_or_query` is invoked when running materialization (using the `feast materialize` or `feast materialize-incremental` commands, or the corresponding `FeatureStore.materialize()` method. This method pull data from the offline store, and the `FeatureStore` class takes care of writing this data into the online store.
* `get_historical_features` is invoked when reading values from the offline store using the `FeatureStore.get_historical_features()` method. Typically, this method is used to retrieve features when training ML models.
* (optional) `offline_write_batch` is a method that supports directly pushing a pyarrow table to a feature view. Given a feature view with a specific schema, this function should write the pyarrow table to the batch source defined. More details about the push api can be found [here](../docs/reference/data-sources/push.md). This method only needs implementation if you want to support the push api in your offline store.
* (optional) `pull_all_from_table_or_query` is a method that pulls all the data from an offline store from a specified start date to a specified end date. This method is only used for **SavedDatasets** as part of data quality monitoring validation.
* (optional) `write_logged_features` is a method that takes a pyarrow table or a path that points to a parquet file and writes the data to a defined source defined by `LoggingSource` and `LoggingConfig`. This method is only used internally for **SavedDatasets**.

{% code title="feast_custom_offline_store/file.py" %}
```python
    # Only prints out runtime warnings once.
    warnings.simplefilter("once", RuntimeWarning)

    def get_historical_features(self,
                                config: RepoConfig,
                                feature_views: List[FeatureView],
                                feature_refs: List[str],
                                entity_df: Union[pd.DataFrame, str],
                                registry: Registry, project: str,
                                full_feature_names: bool = False) -> RetrievalJob:
        """ Perform point-in-time correct join of features onto an entity dataframe(entity key and timestamp). More details about how this should work at https://docs.feast.dev/v/v0.6-branch/user-guide/feature-retrieval#3.-historical-feature-retrieval.
        print("Getting historical features from my offline store")."""
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        # Implementation here.
        pass

    def pull_latest_from_table_or_query(self,
                                        config: RepoConfig,
                                        data_source: DataSource,
                                        join_key_columns: List[str],
                                        feature_name_columns: List[str],
                                        timestamp_field: str,
                                        created_timestamp_column: Optional[str],
                                        start_date: datetime,
                                        end_date: datetime) -> RetrievalJob:
        """ Pulls data from the offline store for use in materialization."""
        print("Pulling latest features from my offline store")
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        # Implementation here.
        pass

    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """ Optional method that returns a Retrieval Job for all join key columns, feature name columns, and the event timestamp columns that occur between the start_date and end_date."""
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        # Implementation here.
        pass

    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        """ Optional method to have Feast support logging your online features."""
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        # Implementation here.
        pass

    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        """ Optional method to have Feast support the offline push api for your offline store."""
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        # Implementation here.
        pass
```
{% endcode %}

### 1.1 Type Mapping

Most offline stores will have to perform some custom mapping of offline store datatypes to feast value types.

* The function to implement here are `source_datatype_to_feast_value_type` and `get_column_names_and_types` in your `DataSource` class.
* `source_datatype_to_feast_value_type` is used to convert your DataSource's datatypes to feast value types.
* `get_column_names_and_types` retrieves the column names and corresponding datasource types.

Add any helper functions for type conversion to `sdk/python/feast/type_map.py`.

* Be sure to implement correct type mapping so that Feast can process your feature columns without casting incorrectly that can potentially cause loss of information or incorrect data.

## 2. Defining an OfflineStoreConfig class

Additional configuration may be needed to allow the OfflineStore to talk to the backing store. For example, Redshift needs configuration information like the connection information for the Redshift instance, credentials for connecting to the database, etc.

To facilitate configuration, all OfflineStore implementations are **required** to also define a corresponding OfflineStoreConfig class in the same file. This OfflineStoreConfig class should inherit from the `FeastConfigBaseModel` class, which is defined [here](../../../sdk/python/feast/repo\_config.py#L44).

The `FeastConfigBaseModel` is a [pydantic](https://pydantic-docs.helpmanual.io) class, which parses yaml configuration into python objects. Pydantic also allows the model classes to define validators for the config classes, to make sure that the config classes are correctly defined.

This config class **must** container a `type` field, which contains the fully qualified class name of its corresponding OfflineStore class.

Additionally, the name of the config class must be the same as the OfflineStore class, with the `Config` suffix.

An example of the config class for the custom file offline store :

{% code title="feast_custom_offline_store/file.py" %}
```python
class CustomFileOfflineStoreConfig(FeastConfigBaseModel):
    """ Custom offline store config for local (file-based) store """

    type: Literal["feast_custom_offline_store.file.CustomFileOfflineStore"] \
        = "feast_custom_offline_store.file.CustomFileOfflineStore"

    uri: str # URI for your offline store(in this case it would be a path)
```
{% endcode %}

This configuration can be specified in the `feature_store.yaml` as follows:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: feast_custom_offline_store.file.CustomFileOfflineStore
    uri: <File URI>
online_store:
    path: data/online_store.db
```
{% endcode %}

This configuration information is available to the methods of the OfflineStore, via the `config: RepoConfig` parameter which is passed into the methods of the OfflineStore interface, specifically at the `config.offline_store` field of the `config` parameter. This fields in the `feature_store.yaml` should map directly to your `OfflineStoreConfig` class that is detailed above in Section 2.

{% code title="feast_custom_offline_store/file.py" %}
```python
    def get_historical_features(self,
                                config: RepoConfig,
                                feature_views: List[FeatureView],
                                feature_refs: List[str],
                                entity_df: Union[pd.DataFrame, str],
                                registry: Registry, project: str,
                                full_feature_names: bool = False) -> RetrievalJob:
        warnings.warn(
            "This offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        offline_store_config = config.offline_store
        assert isinstance(offline_store_config, CustomFileOfflineStoreConfig)
        store_type = offline_store_config.type
```
{% endcode %}

## 3. Defining a RetrievalJob class

The offline store methods aren't expected to perform their read operations eagerly. Instead, they are expected to execute lazily, and they do so by returning a `RetrievalJob` instance, which represents the execution of the actual query against the underlying store.

Custom offline stores may need to implement their own instances of the `RetrievalJob` interface.

The `RetrievalJob` interface exposes two methods - `to_df` and `to_arrow`. The expectation is for the retrieval job to be able to return the rows read from the offline store as a parquet DataFrame, or as an Arrow table respectively.

Users who want to have their offline store support **scalable batch materialization** for online use cases (detailed in this [RFC](https://docs.google.com/document/d/1J7XdwwgQ9dY\_uoV9zkRVGQjK9Sy43WISEW6D5V9qzGo/edit#heading=h.9gaqqtox9jg6)) will also need to implement `to_remote_storage` to distribute the reading and writing of offline store records to blob storage (such as S3). This may be used by a custom [Materialization Engine](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/materialization/batch\_materialization\_engine.py#L72) to parallelize the materialization of data by processing it in chunks. If this is not implemented, Feast will default to local materialization (pulling all records into memory to materialize).

{% code title="feast_custom_offline_store/file.py" %}
```python
class CustomFileRetrievalJob(RetrievalJob):
    def __init__(self, evaluation_function: Callable):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function

    def to_df(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        print("Getting a pandas DataFrame from a File is easy!")
        df = self.evaluation_function()
        return df

    def to_arrow(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        print("Getting a pandas DataFrame from a File is easy!")
        df = self.evaluation_function()
        return pyarrow.Table.from_pandas(df)

    def to_remote_storage(self):
        # Optional method to write to an offline storage location to support scalable batch materialization.
        pass
```
{% endcode %}

## 4. Defining a DataSource class for the offline store

Before this offline store can be used as the batch source for a feature view in a feature repo, a subclass of the `DataSource` [base class](https://rtd.feast.dev/en/master/index.html?highlight=DataSource#feast.data\_source.DataSource) needs to be defined. This class is responsible for holding information needed by specific feature views to support reading historical values from the offline store. For example, a feature view using Redshift as the offline store may need to know which table contains historical feature values.

The data source class should implement two methods - `from_proto`, and `to_proto`.

For custom offline stores that are not being implemented in the main feature repo, the `custom_options` field should be used to store any configuration needed by the data source. In this case, the implementer is responsible for serializing this configuration into bytes in the `to_proto` method and reading the value back from bytes in the `from_proto` method.

{% code title="feast_custom_offline_store/file.py" %}
```python
class CustomFileDataSource(FileSource):
    """Custom data source class for local files"""
    def __init__(
        self,
        timestamp_field: Optional[str] = "",
        path: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        created_timestamp_column: Optional[str] = "",
        date_partition_column: Optional[str] = "",
    ):
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        super(CustomFileDataSource, self).__init__(
            timestamp_field=timestamp_field,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._path = path


    @staticmethod
    def from_proto(data_source: DataSourceProto):
        custom_source_options = str(
            data_source.custom_options.configuration, encoding="utf8"
        )
        path = json.loads(custom_source_options)["path"]
        return CustomFileDataSource(
            field_mapping=dict(data_source.field_mapping),
            path=path,
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        config_json = json.dumps({"path": self.path})
        data_source_proto = DataSourceProto(
            type=DataSourceProto.CUSTOM_SOURCE,
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto
```
{% endcode %}

## 5. Using the custom offline store

After implementing these classes, the custom offline store can be used by referencing it in a feature repo's `feature_store.yaml` file, specifically in the `offline_store` field. The value specified should be the fully qualified class name of the OfflineStore.

As long as your OfflineStore class is available in your Python environment, it will be imported by Feast dynamically at runtime.

To use our custom file offline store, we can use the following `feature_store.yaml`:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
offline_store:
    # Make sure to specify the type as the fully qualified path that Feast can import.
    type: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

If additional configuration for the offline store is **not** required, then we can omit the other fields and only specify the `type` of the offline store class as the value for the `offline_store`.

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
offline_store: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

Finally, the custom data source class can be use in the feature repo to define a data source, and refer to in a feature view definition.

{% code title="feature_repo/repo.py" %}
```python
driver_hourly_stats = CustomFileDataSource(
    path="feature_repo/data/driver_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)


driver_hourly_stats_view = FeatureView(
    source=driver_hourly_stats,
    ...
)
```
{% endcode %}

## 6. Testing the OfflineStore class

### Integrating with the integration test suite and unit test suite.

Even if you have created the `OfflineStore` class in a separate repo, you can still test your implementation against the Feast test suite, as long as you have Feast as a submodule in your repo.

1. In order to test against the test suite, you need to create a custom `DataSourceCreator` that implement our testing infrastructure methods, `create_data_source` and optionally, `created_saved_dataset_destination`.
   * `create_data_source` should create a datasource based on the dataframe passed in. It may be implemented by uploading the contents of the dataframe into the offline store and returning a datasource object pointing to that location. See `BigQueryDataSourceCreator` for an implementation of a data source creator.
   * `created_saved_dataset_destination` is invoked when users need to save the dataset for use in data validation. This functionality is still in alpha and is **optional**.
2.  Make sure that your offline store doesn't break any unit tests first by running:

    ```
    make test-python-unit
    ```
3.  Next, set up your offline store to run the universal integration tests. These are integration tests specifically intended to test offline and online stores against Feast API functionality, to ensure that the Feast APIs works with your offline store.

    * Feast parametrizes integration tests using the `FULL_REPO_CONFIGS` variable defined in `sdk/python/tests/integration/feature_repos/repo_configuration.py` which stores different offline store classes for testing.
    * To overwrite the default configurations to use your own offline store, you can simply create your own file that contains a `FULL_REPO_CONFIGS` dictionary, and point Feast to that file by setting the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file. The module should add new `IntegrationTestRepoConfig` classes to the `AVAILABLE_OFFLINE_STORES` by defining an offline store that you would like Feast to test with.

    A sample `FULL_REPO_CONFIGS_MODULE` looks something like this:

    ```python
    # Should go in sdk/python/feast/infra/offline_stores/contrib/postgres_repo_configuration.py
    from feast.infra.offline_stores.contrib.postgres_offline_store.tests.data_source import (
        PostgreSQLDataSourceCreator,
    )

    AVAILABLE_OFFLINE_STORES = [("local", PostgreSQLDataSourceCreator)]
    ```
4.  You should swap out the `FULL_REPO_CONFIGS` environment variable and run the integration tests against your offline store. In the example repo, the file that overwrites `FULL_REPO_CONFIGS` is `feast_custom_offline_store/feast_tests.py`, so you would run:

    ```bash
    export FULL_REPO_CONFIGS_MODULE='feast_custom_offline_store.feast_tests'
    make test-python-universal
    ```

    If the integration tests fail, this indicates that there is a mistake in the implementation of this offline store!

5. Remember to add your datasource to `repo_config.py` similar to how we added `spark`, `trino`, etc, to the dictionary `OFFLINE_STORE_CLASS_FOR_TYPE`. This will allow Feast to load your class from the `feature_store.yaml`.

6. Finally, add a Makefile target to the Makefile to run your datastore specific tests by setting the `FULL_REPO_CONFIGS_MODULE` and `PYTEST_PLUGINS` environment variable. The `PYTEST_PLUGINS` environment variable allows pytest to load in the `DataSourceCreator` for your datasource. You can remove certain tests that are not relevant or still do not work for your datastore using the `-k` option.

{% code title="Makefile" %}
```Makefile
test-python-universal-spark:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.offline_stores.contrib.spark_repo_configuration \
	PYTEST_PLUGINS=feast.infra.offline_stores.contrib.spark_offline_store.tests \
    IS_TEST=True \
 	python -m pytest -n 8 --integration \
 	 	-k "not test_historical_retrieval_fails_on_validation and \
			not test_historical_retrieval_with_validation and \
			not test_historical_features_persisting and \
			not test_historical_retrieval_fails_on_validation and \
			not test_universal_cli and \
			not test_go_feature_server and \
			not test_feature_logging and \
			not test_reorder_columns and \
			not test_logged_features_validation and \
			not test_lambda_materialization_consistency and \
			not test_offline_write and \
			not test_push_features_to_offline_store.py and \
			not gcs_registry and \
			not s3_registry and \
			not test_universal_types" \
 	 sdk/python/tests
```
{% endcode %}

### 7. Dependencies

Add any dependencies for your offline store to our `sdk/python/setup.py` under a new `<OFFLINE_STORE>__REQUIRED` list with the packages and add it to the setup script so that if your offline store is needed, users can install the necessary python packages. These packages should be defined as extras so that they are not installed by users by default. You will need to regenerate our requirements files:

```
make lock-python-ci-dependencies-all
```

### 8. Add Documentation

Remember to add documentation for your offline store.

1. Add a new markdown file to `docs/reference/offline-stores/` and `docs/reference/data-sources/`. Use these files to document your offline store functionality similar to how the other offline stores are documented.
2. You should also add a reference in `docs/reference/data-sources/README.md` and `docs/SUMMARY.md` to these markdown files.

**NOTE**: Be sure to document the following things about your offline store:

* How to create the datasource and most what configuration is needed in the `feature_store.yaml` file in order to create the datasource.
* Make sure to flag that the datasource is in alpha development.
* Add some documentation on what the data model is for the specific offline store for more clarity.
* Finally, generate the python code docs by running:

```bash
make build-sphinx
```
