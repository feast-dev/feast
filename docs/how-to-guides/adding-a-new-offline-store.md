# Adding a new offline store

## Overview

Feast makes adding support for a new offline store (database) easy. Developers can simply implement the [OfflineStore](../../sdk/python/feast/infra/offline\_stores/offline\_store.py#L41) interface to add support for a new store (other than the existing stores like Parquet files, Redshift, and Bigquery).&#x20;

In this guide, we will show you how to extend the existing File offline store and use in a feature repo. While we will be implementing a specific store, this guide should be representative for adding support for any new offline store.

The full working code for this guide can be found at [feast-dev/feast-custom-offline-store-demo](https://github.com/feast-dev/feast-custom-offline-store-demo) and an example of a custom offline store that was contributed by developers can be found [here](https://github.com/feast-dev/feast/pull/2401).

The process for using a custom offline store consists of 6 steps:

1. Defining an `OfflineStore` class.
2. Defining an `OfflineStoreConfig` class.
3. Defining a `RetrievalJob` class for this offline store.
4. Defining a `DataSource` class for the offline store
5. Referencing the `OfflineStore` in a feature repo's `feature_store.yaml` file.
6. Testing the `OfflineStore` class.

## 1. Defining an OfflineStore class

{% hint style="info" %}
&#x20;OfflineStore class names must end with the OfflineStore suffix!
{% endhint %}

The OfflineStore class contains a couple of methods to read features from the offline store. Unlike the OnlineStore class, Feast does not manage any infrastructure for the offline store.&#x20;

There are two methods that deal with reading data from the offline stores`get_historical_features`and `pull_latest_from_table_or_query`.

* `pull_latest_from_table_or_query` is invoked when running materialization (using the `feast materialize` or `feast materialize-incremental` commands, or the corresponding `FeatureStore.materialize()` method. This method pull data from the offline store, and the `FeatureStore` class takes care of writing this data into the online store.
* `get_historical_features` is invoked when reading values from the offline store using the `FeatureStore.get_historical_features()` method. Typically, this method is used to retrieve features when training ML models.
* `pull_all_from_table_or_query` is a method that pulls all the data from an offline store from a specified start date to a specified end date.
* `write_logged_features` is a method that takes a pyarrow table or a path that points to a parquet file and writes the data to a defined source defined by `LoggingSource` and `LoggingConfig`.
* `offline_write_batch` is a method that supports directly pushing a pyarrow table to a feature view. Given a feature view with a specific schema, this function should write the pyarrow table to the batch source defined. More details about the push api can be found [here](docs/reference/data-sources/push.md).

{% code title="feast_custom_offline_store/file.py" %}
```python
    def get_historical_features(self,
                                config: RepoConfig,
                                feature_views: List[FeatureView],
                                feature_refs: List[str],
                                entity_df: Union[pd.DataFrame, str],
                                registry: Registry, project: str,
                                full_feature_names: bool = False) -> RetrievalJob:
        print("Getting historical features from my offline store")
        return super().get_historical_features(config,
                                               feature_views,
                                               feature_refs,
                                               entity_df,
                                               registry,
                                               project,
                                               full_feature_names)

    def pull_latest_from_table_or_query(self,
                                        config: RepoConfig,
                                        data_source: DataSource,
                                        join_key_columns: List[str],
                                        feature_name_columns: List[str],
                                        timestamp_field: str,
                                        created_timestamp_column: Optional[str],
                                        start_date: datetime,
                                        end_date: datetime) -> RetrievalJob:
        print("Pulling latest features from my offline store")
        return super().pull_latest_from_table_or_query(config,
                                                       data_source,
                                                       join_key_columns,
                                                       feature_name_columns,
                                                       timestamp_field=timestamp_field,
                                                       created_timestamp_column,
                                                       start_date,
                                                       end_date)
```
{% endcode %}

## 2. Defining an OfflineStoreConfig class

Additional configuration may be needed to allow the OfflineStore to talk to the backing store. For example, Redshift needs configuration information like the connection information for the Redshift instance, credentials for connecting to the database, etc.

To facilitate configuration, all OfflineStore implementations are **required** to also define a corresponding OfflineStoreConfig class in the same file. This OfflineStoreConfig class should inherit from the `FeastConfigBaseModel` class, which is defined [here](../../sdk/python/feast/repo\_config.py#L44).&#x20;

The `FeastConfigBaseModel` is a [pydantic](https://pydantic-docs.helpmanual.io) class, which parses yaml configuration into python objects. Pydantic also allows the model classes to define validators for the config classes, to make sure that the config classes are correctly defined.

This config class **must** container a `type` field, which contains the fully qualified class name of its corresponding OfflineStore class.&#x20;

Additionally, the name of the config class must be the same as the OfflineStore class, with the `Config` suffix.

An example of the config class for the custom file offline store :

{% code title="feast_custom_offline_store/file.py" %}
```python
class CustomFileOfflineStoreConfig(FeastConfigBaseModel):
    """ Custom offline store config for local (file-based) store """

    type: Literal["feast_custom_offline_store.file.CustomFileOfflineStore"] \
        = "feast_custom_offline_store.file.CustomFileOfflineStore"
```
{% endcode %}

This configuration can be specified in the `feature_store.yaml` as follows:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
type: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

This configuration information is available to the methods of the OfflineStore, via the`config: RepoConfig` parameter which is passed into the methods of the OfflineStore interface, specifically at the `config.offline_store` field of the `config` parameter.&#x20;

{% code title="feast_custom_offline_store/file.py" %}
```python
    def get_historical_features(self,
                                config: RepoConfig,
                                feature_views: List[FeatureView],
                                feature_refs: List[str],
                                entity_df: Union[pd.DataFrame, str],
                                registry: Registry, project: str,
                                full_feature_names: bool = False) -> RetrievalJob:

        offline_store_config = config.offline_store
        assert isinstance(offline_store_config, CustomFileOfflineStoreConfig)
        store_type = offline_store_config.type
```
{% endcode %}

## 3. Defining a RetrievalJob class

The offline store methods aren't expected to perform their read operations eagerly. Instead, they are expected to execute lazily, and they do so by returning a `RetrievalJob` instance, which represents the execution of the actual query against the underlying store.

Custom offline stores may need to implement their own instances of the `RetrievalJob` interface.

The `RetrievalJob` interface exposes two methods - `to_df` and `to_arrow`. The expectation is for the retrieval job to be able to return the rows read from the offline store as a parquet DataFrame, or as an Arrow table respectively.

Users who want to have their offline store support batch materialization (detailed in this [RFC](https://docs.google.com/document/d/1J7XdwwgQ9dY_uoV9zkRVGQjK9Sy43WISEW6D5V9qzGo/edit#heading=h.9gaqqtox9jg6)) will also need to implement `to_remote_storage` to write distribute the reading and writing of offline store records to a distributed framework. If this functionality is not needed, the RetrievalJob will default to local materialization.

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
```
{% endcode %}

## 4. Defining a DataSource class for the offline store

Before this offline store can be used as the batch source for a feature view in a feature repo, a subclass of the `DataSource` [base class](https://rtd.feast.dev/en/master/index.html?highlight=DataSource#feast.data\_source.DataSource) needs to be defined. This class is responsible for holding information needed by specific feature views to support reading historical values from the offline store. For example, a feature view using Redshift as the offline store may need to know which table contains historical feature values.

The data source class should implement two methods - `from_proto`, and `to_proto`.&#x20;

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

## 5. Using the custom offline store&#x20;

After implementing these classes, the custom offline store can be used by referencing it in a feature repo's `feature_store.yaml` file, specifically in the `offline_store` field. The value specified should be the fully qualified class name of the OfflineStore.&#x20;

As long as your OfflineStore class is available in your Python environment, it will be imported by Feast dynamically at runtime. It is crucial to specify the type as the package that Feast can import.

To use our custom file offline store, we can use the following `feature_store.yaml`:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
offline_store:
    type: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

If additional configuration for the offline store is **not **required, then we can omit the other fields and only specify the `type` of the offline store class as the value for the `offline_store`.

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
pdriver_hourly_stats = CustomFileDataSource(
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

### Contrib

Generally, new offline stores should go in the contrib folder and have alpha functionality tags. The contrib folder is designated for community contributions that are not fully maintained by Feast maintainers and may contain potential instability and have API changes. It is recommended to add warnings to users that the offline store functionality is still in alpha development and is not fully stable. In order to be classified as fully stable and be moved to the main offline store folder, the offline store should integrate with the full integration test suite in Feast and pass all of the test cases.

### Integrating with the integration test suite and unit test suite.

Even if you have created the `OfflineStore` class in a separate repo, you can still test your implementation against the Feast test suite, as long as you have Feast as a submodule in your repo. In the Feast submodule, we can run all the unit tests with:

In order to test against the test suite, you need to create a custom `DataSourceCreator`. This class will need to implement our testing infrastructure methods, `create_data_source` and `created_saved_dataset_destination`. `create_data_source` creates a datasource for testing from the dataframe given that will register the dataframe with your offline store and return a datasource pointing to that location. See `BigQueryDataSourceCreator` for an implementation of a datawarehouse data source creator. **Saved datasets** are special datasets used for data validation and is Feast's way of snapshotting your data for future data validation.

```
make test-python
```
This should run the unit tests and the unit tests should all pass. Please add unit tests for your data source that test out basic functionality of reading and writing to and from the datasource. This should just be class level functionality that ensures that the methods you implemented for the OfflineStore and the DataSource associated with it work as expected. In order to be approved to merge into Feast, these unit tests should all pass and demonstrate that the DataSource works as intended.

The universal tests, which are integration tests specifically intended to test offline and online stores, will be run against Feast to ensure that the Feast APIs works with your offline store. The universal tests can be run by running the following commands:

```
make test-python-integration-container
make test-python-universal
```

The unit tests should succeed, but the universal tests will likely fail. The tests are parametrized based on the `FULL_REPO_CONFIGS` variable defined in `sdk/python/tests/integration/feature_repos/repo_configuration.py`. To overwrite these configurations, you can simply create your own file that contains a `FULL_REPO_CONFIGS`, and point Feast to that file by setting the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file. The module should add new `IntegrationTestRepoConfig` classes to the `AVAILABLE_OFFLINE_STORES` by defining an offline and online store. In general, use the sqlite online store to test your offline store so that you don't need to setup any other online store containers. The main challenge there will be to write a `DataSourceCreator` for the offline store. In this repo, the file that overwrites `FULL_REPO_CONFIGS` is `feast_custom_offline_store/feast_tests.py`, so you would run

```
export FULL_REPO_CONFIGS_MODULE='feast_custom_offline_store.feast_tests'
make test-python-universal
```

to test the offline store against the Feast universal tests. You should notice that some of the tests actually fail. If you have configured the offline stores and only that offline store is being used in `FULL_REPO_CONFIGS`, `make test-python-integration-container` should work as it tests the offline store and online stores that are containerized in Docker. All of the containerized integration tests should pass and if they don't, this indicates that there is a mistake in the implementation of this offline store!

Remember to add your datasource to `repo_config.py` similar to how we added `spark`, `trino`, etc, to the dictionary `OFFLINE_STORE_CLASS_FOR_TYPE` and add the necessary configuration to `repo_configuration.py`. Namely, `AVAILABLE_OFFLINE_STORES` should load your repo configuration module.

### Dependencies

Finally, if your offline store requires special packages, add them to our `sdk/python/setup.py` under a new `<OFFLINE>_STORE_REQUIRED` list with the packages and add it to the setup script so that if your offline store is needed, users can install the necessary python packages.
You will need to regenerate our requirements files. To do this, create separate pyenv environments for python 3.8, 3.9, and 3.10. In each environment, run the following commands:

```
export PYTHON=<version>
make lock-python-ci-dependencies
make lock-python-dependencies
```


### Documentation

Remember to update the documentation for your offline store. This can be found in `docs/reference/offline-stores/` and `docs/reference/data-sources/`. You should also add a reference in `docs/reference/data-sources/README.md` and `docs/SUMMARY.md`. Add a new markdown documentation and document the functions similar to how the other offline stores are documented. Be sure to cover how to create the datasource and most importantly, what configuration is needed in the `feature_store.yaml` file in order to create the datasource and also make sure to flag that the datasource is in alpha development.


An example of a full pull request for adding a custom offline store can be found [here](https://github.com/feast-dev/feast/pull/2401).


