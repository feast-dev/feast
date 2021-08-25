# Adding a new offline store

## Overview

Feast makes adding support for a new offline store \(database\) easy. Developers can simply implement the [OfflineStore](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/offline_store.py#L41) interface to add support for a new store \(other than the existing stores like Parquet files, Redshift, and Bigquery\). 

In this guide, we will show you how to extend the existing File offline store and use in a feature repo. While we will be implementing a specific store, this guide should be representative for adding support for any new offline store.

The full working code for this guide can be found at [feast-dev/feast-custom-offline-store-demo](https://github.com/feast-dev/feast-custom-offline-store-demo).

The process for using a custom offline store consists of 4 steps:

1. Defining an `OfflineStore` class.
2. Defining an `OfflineStoreConfig` class.
3. Defining a `RetrievalJob` class for this offline store.
4. Referencing the `OfflineStore` in a feature repo's `feature_store.yaml` file.

## 1. Defining an OfflineStore class

{% hint style="info" %}
 OfflineStore class names must end with the OfflineStore suffix!
{% endhint %}

The OfflineStore class contains a couple of methods to read features from the offline store. Unlike the OnlineStore class, Feast does not manage any infrastructure for the offline store. 

There are two methods that deal with reading data from the offline stores`get_historical_features`and `pull_latest_from_table_or_query`.

* `pull_latest_from_table_or_query` is invoked when running materialization \(using the `feast materialize` or `feast materialize-incremental` commands, or the corresponding `FeatureStore.materialize()` method. This method pull data from the offline store, and the `FeatureStore` class takes care of writing this data into the online store.
* `get_historical_features` is invoked when reading values from the offline store using the `FeatureStore.get_historica_features()` method. Typically, this method is used to retrieve features when training ML models.

{% code title="feast\_custom\_offline\_store/file.py" %}
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
                                        event_timestamp_column: str,
                                        created_timestamp_column: Optional[str],
                                        start_date: datetime,
                                        end_date: datetime) -> RetrievalJob:
        print("Pulling latest features from my offline store")
        return super().pull_latest_from_table_or_query(config,
                                                       data_source,
                                                       join_key_columns,
                                                       feature_name_columns,
                                                       event_timestamp_column,
                                                       created_timestamp_column,
                                                       start_date,
                                                       end_date)

```
{% endcode %}

## 2. Defining an OfflineStoreConfig class

Additional configuration may be needed to allow the OfflineStore to talk to the backing store. For example, Redshift needs configuration information like the connection information for the Redshift instance, credentials for connecting to the database, etc.

To facilitate configuration, all OfflineStore implementations are **required** to also define a corresponding OfflineStoreConfig class in the same file. This OfflineStoreConfig class should inherit from the `FeastConfigBaseModel` class, which is defined [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/repo_config.py#L44). 

The `FeastConfigBaseModel` is a [pydantic](https://pydantic-docs.helpmanual.io/) class, which parses yaml configuration into python objects. Pydantic also allows the model classes to define validators for the config classes, to make sure that the config classes are correctly defined.

This config class **must** container a `type` field, which contains the fully qualified class name of its corresponding OfflineStore class. 

Additionally, the name of the config class must be the same as the OfflineStore class, with the `Config` suffix.

An example of the config class for the custom file offline store :

{% code title="feast\_custom\_offline\_store/file.py" %}
```python
class CustomFileOfflineStoreConfig(FeastConfigBaseModel):
    """ Custom offline store config for local (file-based) store """

    type: Literal["feast_custom_offline_store.file.CustomFileOfflineStore"] \
        = "feast_custom_offline_store.file.CustomFileOfflineStore"

```
{% endcode %}

This configuration can be specified in the `feature_store.yaml` as follows:

{% code title="feature\_repo/feature\_store.yaml" %}
```yaml
    type: feast_custom_offline_store.file.CustomFileOfflineStore

```
{% endcode %}

This configuration information is available to the methods of the OfflineStore, via the`config: RepoConfig` parameter which is passed into the methods of the OfflineStore interface, specifically at the `config.offline_store` field of the `config` parameter. 

{% code title="feast\_custom\_offline\_store/file.py" %}
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

{% code title="feast\_custom\_offline\_store/file.py" %}
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

## 4. Using the custom offline store 

After implementing these classes, the custom offline store can be used by referencing it in a feature repo's `feature_store.yaml` file, specifically in the `offline_store` field. The value specified should be the fully qualified class name of the OfflineStore. 

As long as your OfflineStore class is available in your Python environment, it will be imported by Feast dynamically at runtime.

To use our custom file offline store, we can use the following `feature_store.yaml`:

{% code title="feature\_repo/feature\_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
offline_store: 
    type: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

If additional configuration for the offline store is **not** required, then we can omit the other fields and only specify the `type` of the offline store class as the value for the `offline_store`.

{% code title="feature\_repo/feature\_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
offline_store: feast_custom_offline_store.file.CustomFileOfflineStore
```
{% endcode %}

