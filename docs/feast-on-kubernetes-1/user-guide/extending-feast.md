# Extending Feast

## Custom OnlineStore 

Feast allow users to create their own OnlineStore implementations, allowing Feast to read and write feature values to stores other than first-party implementations already in Feast directly. The interface for the is found at [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/online_stores/online_store.py), and consists of four methods that need to be implemented.

### Update/Teardown methods

The `update` method is should be set up any state in the OnlineStore that is required before any data can be ingested into it. This can be things like tables in sqlite, or keyspaces in Cassandra, etc. The update method should be idempotent. Similarly, the `teardown` method should remove any state in the online store.

```python
def update(
    self,
    config: RepoConfig,
    tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
    tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
    entities_to_delete: Sequence[Entity],
    entities_to_keep: Sequence[Entity],
    partial: bool,
):
    ...

def teardown(
    self,
    config: RepoConfig,
    tables: Sequence[Union[FeatureTable, FeatureView]],
    entities: Sequence[Entity],
):
    ...

```

### Write/Read methods

The `online_write_batch` method is responsible for writing the data into the online store - and `online_read` method is responsible for reading data from the online store. 

```python
def online_write_batch(
    self,
    config: RepoConfig,
    table: Union[FeatureTable, FeatureView],
    data: List[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
    ],
    progress: Optional[Callable[[int], Any]],
) -> None:

    ...

def online_read(
    self,
    config: RepoConfig,
    table: Union[FeatureTable, FeatureView],
    entity_keys: List[EntityKeyProto],
    requested_features: Optional[List[str]] = None,
) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
    ...
```

## Custom OfflineStore

Feast allow users to create their own OfflineStore implementations, allowing Feast to read and write feature values to stores other than first-party implementations already in Feast directly. The interface for the is found at [here](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/offline_store.py), and consists of two methods that need to be implemented.

### Write method

The `pull_latest_from_table_or_query` method is used to read data from a source for materialization into the OfflineStore.

```python
def pull_latest_from_table_or_query(
    data_source: DataSource,
    join_key_columns: List[str],
    feature_name_columns: List[str],
    event_timestamp_column: str,
    created_timestamp_column: Optional[str],
    start_date: datetime,
    end_date: datetime,
) -> pyarrow.Table:
    ...

```

### Read method

The read method is responsible for reading historical features from the OfflineStore. The feature retrieval may be asynchronous, so the read method is expected to return an object that should produce a DataFrame representing the historical features once the feature retrieval job is complete. 

```python
class RetrievalJob:

    @abstractmethod
    def to_df(self):
        pass

def get_historical_features(
    config: RepoConfig,
    feature_views: List[FeatureView],
    feature_refs: List[str],
    entity_df: Union[pd.DataFrame, str],
    registry: Registry,
    project: str,
) -> RetrievalJob:
    pass

```



