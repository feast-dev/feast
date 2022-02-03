# Dataset

Feast datasets allow for conveniently saving dataframes that include both features and entities to be subsequently used for data analysis and model training.
[Data Quality Monitoring](https://docs.google.com/document/d/110F72d4NTv80p35wDSONxhhPBqWRwbZXG4f9mNEMd98) was the primary motivation for creating dataset concept.

Dataset's metadata is stored in the Feast registry and raw data (features, entities, additional input keys and timestamp) is stored in the [offline store](../architecture-and-components/offline-store.md).

Dataset can be created from:
1. Results of historical retrieval
2. [planned] Logging request (including input for [on demand transformation](../../reference/alpha-on-demand-feature-view.md)) and response during feature serving
3. [planned] Logging features during writing to online store (from batch source or stream)


### Creating Saved Dataset from Historical Retrieval

To create a saved dataset from historical features for later retrieval or analysis, a user needs to call `get_historical_features` method first and then pass the returned retrieval job to `create_saved_dataset` method.
`create_saved_dataset` will trigger provided retrieval job (by calling `.persist()` on it) to store the data using specified `storage`.
Storage type must be the same as globally configured offline store (eg, it's impossible to persist data to Redshift with BigQuery source).
`create_saved_dataset` will also create SavedDataset object with all related metadata and will write it to the registry.

```python
from feast import FeatureStore
from feast.infra.offline_stores.bigquery_source import SavedDatasetBigQueryStorage

store = FeatureStore()

historical_job = store.get_historical_features(
    features=["driver:avg_trip"],
    entity_df=...,
)

dataset = store.create_saved_dataset(
    from_=historical_job,
    name='my_training_dataset',
    storage=SavedDatasetBigQueryStorage(table_ref='<gcp-project>.<gcp-dataset>.my_training_dataset'),
    tags={'author': 'oleksii'}
)

dataset.to_df()
```

Saved dataset can be later retrieved using `get_saved_dataset` method:
```python
dataset = store.get_saved_dataset('my_training_dataset')
dataset.to_df()
```

---

Check out our [tutorial on validating historical features](../../tutorials/validating-historical-features.md) to see how this concept can be applied in real-world use case.