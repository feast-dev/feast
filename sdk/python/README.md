# Feast Python SDK Quickstart Guide

## Pre-requisites

* A working Feast Cluster: Consult your Feast admin or [install your own](install.md).

## Where to get it

Binary installers for the latest released version are available at the
 [PyPI](https://pypi.org/project/feast/):

```sh
 pip install feast
```

## Getting started

### Configuring Feast client
All interaction with feast cluster happens via an instance of `feast.sdk.client.Client`. 
It should be pointed to correct core/serving urls of the feast cluster:
```python
from feast.sdk.client import Client

fs = Client(
    core_url=FEAST_CORE_URL, 
    serving_url=FEAST_SERVING_URL, 
    verbose=True)
```

If you are running a local feast cluster, then `FEAST_CORE_URL` and `FEAST_SERVING_URL`
could be something like:
```python
FEAST_CORE_URL="localhost:8080"
FEAST_SERVING_URL="localhost:8081"
```

### Registering an entity and features
Entities and features could be registered explicitly beforehand or during the data ingestion.

Register your first entity:
```python
from feast.sdk.resources.entity import Entity

entity = Entity(
    name='word',
    description='word found in shakespearean works'
)

fs.apply(entity)
```
And a feature, that belongs to this entity:
```python
from feast.sdk.resources.feature import Feature, Datastore, ValueType

word_count_feature = Feature(
    entity='word',
    name='count',
    value_type=ValueType.INT32,                      
    description='number of times the word appears',
    tags=['tag1', 'tag2'],
    owner='bob@feast.com',
    uri='https://github.com/bob/example',
    warehouse_store=Datastore(id='WAREHOUSE'),
    serving_store=Datastore(id='SERVING')    
)

fs.apply(word_count_feature)
```
Read more on entity/feature fields here: [Entity Spec](../../docs/specs.md#entity-spec)


### Ingest data for your feature
Let's create a simple [pandas](https://pandas.pydata.org/) dataframe
```python
import pandas as pd

words_df = pd.DataFrame({
    'word': ['the', 'and', 'i', 'to', 'of', 'a', 'you', 'my', 'in', 'that', 'is', 'not', 'with', 'me', 'it'],
    'count': [28944, 27317, 21120, 20136, 17181, 14945, 13989, 12949, 11513, 11488, 9545, 8855, 8293, 8043, 8003]
    })
```
And import it into the feast store:
```python
from datetime import datetime
from feast.sdk.importer import Importer

STAGING_LOCATION = 'gs://your-bucket'  

importer = Importer.from_df(words_df, 
                           entity='word', 
                           owner='bob@feast.com',
                           id_column='word',
                           timestamp_value=datetime(2018, 1, 1),
                           staging_location=STAGING_LOCATION,
                           serving_store=Datastore(id='SERVING'),
                           warehouse_store=Datastore(id='WAREHOUSE'))
    
fs.run(importer)
```
You can also import data from a CSV file (`Importer.from_csv(...)`) 
or a BigQuery (`Importer.from_bq(...)`)