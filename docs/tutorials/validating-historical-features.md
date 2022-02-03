# Validating historical features with Great Expectations

In this tutorial, we will use the public dataset of Chicago taxi trips to present data validation capabilities of Feast. 
- The original dataset is stored in BigQuery and consists of raw data for each taxi trip (one row per trip) since 2013. 
- We will generate several training datasets (aka historical features in Feast) for different periods and evaluate expectations made on one dataset against another.

Types of features we're ingesting and generating:
- Features that aggregate raw data with daily intervals (eg, trips per day, average fare or speed for a specific day, etc.). 
- Features using SQL while pulling data from BigQuery (like total trips time or total miles travelled). 
- Features calculated on the fly when requested using Feast's on-demand transformations

Our plan:

0. Prepare environment
1. Pull data from BigQuery (optional)
2. Declare & apply features and feature views in Feast
3. Generate reference dataset
4. Develop & test profiler function
5. Run validation on different dataset using reference dataset & profiler


> The original notebook and datasets for this tutorial can be found on [GitHub](https://github.com/feast-dev/dqm-tutorial).

### 0. Setup

Install Feast Python SDK and great expectations:


```python
!pip install 'feast[ge]'
```


### 1. Dataset preparation (Optional) 

**You can skip this step if you don't have GCP account. Please use parquet files that are coming with this tutorial instead**


```python
!pip install google-cloud-bigquery
```


```python
import pyarrow.parquet

from google.cloud.bigquery import Client
```


```python
bq_client = Client(project='kf-feast')
```

Running some basic aggregations while pulling data from BigQuery. Grouping by taxi_id and day:


```python
data_query = """SELECT 
    taxi_id,
    TIMESTAMP_TRUNC(trip_start_timestamp, DAY) as day,
    SUM(trip_miles) as total_miles_travelled,
    SUM(trip_seconds) as total_trip_seconds,
    SUM(fare) as total_earned,
    COUNT(*) as trip_count
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` 
WHERE 
    trip_miles > 0 AND trip_seconds > 60 AND
    trip_start_timestamp BETWEEN '2019-01-01' and '2020-12-31' AND
    trip_total < 1000
GROUP BY taxi_id, TIMESTAMP_TRUNC(trip_start_timestamp, DAY)"""
```


```python
driver_stats_table = bq_client.query(data_query).to_arrow()

# Storing resulting dataset into parquet file
pyarrow.parquet.write_table(driver_stats_table, "trips_stats.parquet")
```


```python
def entities_query(year):
    return f"""SELECT
    distinct taxi_id
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips` 
WHERE
    trip_miles > 0 AND trip_seconds > 0 AND
    trip_start_timestamp BETWEEN '{year}-01-01' and '{year}-12-31'
"""
```


```python
entities_2019_table = bq_client.query(entities_query(2019)).to_arrow()

# Storing entities (taxi ids) into parquet file
pyarrow.parquet.write_table(entities_2019_table, "entities.parquet")
```


## 2. Declaring features


```python
import pyarrow.parquet
import pandas as pd

from feast import Feature, FeatureView, Entity, FeatureStore
from feast.value_type import ValueType
from feast.data_format import ParquetFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.offline_stores.file import SavedDatasetFileStorage

from google.protobuf.duration_pb2 import Duration
```


```python
batch_source = FileSource(
    event_timestamp_column="day",
    path="trips_stats.parquet",  # using parquet file that we created on previous step
    file_format=ParquetFormat()
)
```


```python
taxi_entity = Entity(name='taxi', join_key='taxi_id')
```


```python
trips_stats_fv = FeatureView(
    name='trip_stats',
    entities=['taxi'],
    features=[
        Feature("total_miles_travelled", ValueType.DOUBLE),
        Feature("total_trip_seconds", ValueType.DOUBLE),
        Feature("total_earned", ValueType.DOUBLE),
        Feature("trip_count", ValueType.INT64),
        
    ],
    ttl=Duration(seconds=86400),
    batch_source=batch_source,
)
```

*Read more about feature views in [Feast docs](https://docs.feast.dev/getting-started/concepts/feature-view)*


```python
@on_demand_feature_view(
    features=[
        Feature("avg_fare", ValueType.DOUBLE),
        Feature("avg_speed", ValueType.DOUBLE),
        Feature("avg_trip_seconds", ValueType.DOUBLE),
        Feature("earned_per_hour", ValueType.DOUBLE),
    ],
    inputs={
        "stats": trips_stats_fv
    }
)
def on_demand_stats(inp):
    out = pd.DataFrame()
    out["avg_fare"] = inp["total_earned"] / inp["trip_count"]
    out["avg_speed"] = 3600 * inp["total_miles_travelled"] / inp["total_trip_seconds"]
    out["avg_trip_seconds"] = inp["total_trip_seconds"] / inp["trip_count"]
    out["earned_per_hour"] = 3600 * inp["total_earned"] / inp["total_trip_seconds"]
    return out
```

*Read more about on demand feature views [here](https://docs.feast.dev/reference/alpha-on-demand-feature-view)*


```python
store = FeatureStore(".")  # using feature_store.yaml that stored in the same directory
```


```python
store.apply([taxi_entity, trips_stats_fv, on_demand_stats])  # writing to the registry
```


## 3. Generating training (reference) dataset


```python
taxi_ids = pyarrow.parquet.read_table("entities.parquet").to_pandas()
```

Generating range of timestamps with daily frequency:


```python
timestamps = pd.DataFrame()
timestamps["event_timestamp"] = pd.date_range("2019-06-01", "2019-07-01", freq='D')
```

Cross merge (aka relation multiplication) produces entity dataframe with each taxi_id repeated for each timestamp:


```python
entity_df = pd.merge(taxi_ids, timestamps, how='cross')
entity_df
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>taxi_id</th>
      <th>event_timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2019-06-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2019-06-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2019-06-03</td>
    </tr>
    <tr>
      <th>3</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2019-06-04</td>
    </tr>
    <tr>
      <th>4</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2019-06-05</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>156979</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2019-06-27</td>
    </tr>
    <tr>
      <th>156980</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2019-06-28</td>
    </tr>
    <tr>
      <th>156981</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2019-06-29</td>
    </tr>
    <tr>
      <th>156982</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2019-06-30</td>
    </tr>
    <tr>
      <th>156983</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2019-07-01</td>
    </tr>
  </tbody>
</table>
<p>156984 rows × 2 columns</p>
</div>



Retrieving historical features for resulting entity dataframe and persisting output as a saved dataset:


```python
job = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "trip_stats:total_miles_travelled",
        "trip_stats:total_trip_seconds",
        "trip_stats:total_earned",
        "trip_stats:trip_count",
        "on_demand_stats:avg_fare",
        "on_demand_stats:avg_trip_seconds",
        "on_demand_stats:avg_speed",
        "on_demand_stats:earned_per_hour",
    ]
)

store.create_saved_dataset(
    from_=job,
    name='my_training_ds',
    storage=SavedDatasetFileStorage(path='my_training_ds.parquet')
)
```

```python
<SavedDataset(name = my_training_ds, features = ['trip_stats:total_miles_travelled', 'trip_stats:total_trip_seconds', 'trip_stats:total_earned', 'trip_stats:trip_count', 'on_demand_stats:avg_fare', 'on_demand_stats:avg_trip_seconds', 'on_demand_stats:avg_speed', 'on_demand_stats:earned_per_hour'], join_keys = ['taxi_id'], storage = <feast.infra.offline_stores.file_source.SavedDatasetFileStorage object at 0x1276e7950>, full_feature_names = False, tags = {}, _retrieval_job = <feast.infra.offline_stores.file.FileRetrievalJob object at 0x12716fed0>, min_event_timestamp = 2019-06-01 00:00:00, max_event_timestamp = 2019-07-01 00:00:00)>
```


## 4. Developing dataset profiler

Dataset profiler is a function that accepts dataset and generates set of its characteristics. This charasteristics will be then used to evaluate (validate) next datasets.

**Important: datasets are not compared to each other! 
Feast use a reference dataset and a profiler function to generate a reference profile. 
This profile will be then used during validation of the tested dataset.**


```python
import numpy as np

from feast.dqm.profilers.ge_profiler import ge_profiler

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.dataset import PandasDataset
```


Loading saved dataset first and exploring the data:


```python
ds = store.get_saved_dataset('my_training_ds')
ds.to_df()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total_earned</th>
      <th>avg_trip_seconds</th>
      <th>taxi_id</th>
      <th>total_miles_travelled</th>
      <th>trip_count</th>
      <th>earned_per_hour</th>
      <th>event_timestamp</th>
      <th>total_trip_seconds</th>
      <th>avg_fare</th>
      <th>avg_speed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>68.25</td>
      <td>2270.000000</td>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>24.70</td>
      <td>2.0</td>
      <td>54.118943</td>
      <td>2019-06-01 00:00:00+00:00</td>
      <td>4540.0</td>
      <td>34.125000</td>
      <td>19.585903</td>
    </tr>
    <tr>
      <th>1</th>
      <td>221.00</td>
      <td>560.500000</td>
      <td>7a4a6162eaf27805aef407d25d5cb21fe779cd962922cb...</td>
      <td>54.18</td>
      <td>24.0</td>
      <td>59.143622</td>
      <td>2019-06-01 00:00:00+00:00</td>
      <td>13452.0</td>
      <td>9.208333</td>
      <td>14.499554</td>
    </tr>
    <tr>
      <th>2</th>
      <td>160.50</td>
      <td>1010.769231</td>
      <td>f4c9d05b215d7cbd08eca76252dae51cdb7aca9651d4ef...</td>
      <td>41.30</td>
      <td>13.0</td>
      <td>43.972603</td>
      <td>2019-06-01 00:00:00+00:00</td>
      <td>13140.0</td>
      <td>12.346154</td>
      <td>11.315068</td>
    </tr>
    <tr>
      <th>3</th>
      <td>183.75</td>
      <td>697.550000</td>
      <td>c1f533318f8480a59173a9728ea0248c0d3eb187f4b897...</td>
      <td>37.30</td>
      <td>20.0</td>
      <td>47.415956</td>
      <td>2019-06-01 00:00:00+00:00</td>
      <td>13951.0</td>
      <td>9.187500</td>
      <td>9.625116</td>
    </tr>
    <tr>
      <th>4</th>
      <td>217.75</td>
      <td>1054.076923</td>
      <td>455b6b5cae6ca5a17cddd251485f2266d13d6a2c92f07c...</td>
      <td>69.69</td>
      <td>13.0</td>
      <td>57.206451</td>
      <td>2019-06-01 00:00:00+00:00</td>
      <td>13703.0</td>
      <td>16.750000</td>
      <td>18.308692</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>156979</th>
      <td>38.00</td>
      <td>1980.000000</td>
      <td>0cccf0ec1f46d1e0beefcfdeaf5188d67e170cdff92618...</td>
      <td>14.90</td>
      <td>1.0</td>
      <td>69.090909</td>
      <td>2019-07-01 00:00:00+00:00</td>
      <td>1980.0</td>
      <td>38.000000</td>
      <td>27.090909</td>
    </tr>
    <tr>
      <th>156980</th>
      <td>135.00</td>
      <td>551.250000</td>
      <td>beefd3462e3f5a8e854942a2796876f6db73ebbd25b435...</td>
      <td>28.40</td>
      <td>16.0</td>
      <td>55.102041</td>
      <td>2019-07-01 00:00:00+00:00</td>
      <td>8820.0</td>
      <td>8.437500</td>
      <td>11.591837</td>
    </tr>
    <tr>
      <th>156981</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>9a3c52aa112f46cf0d129fafbd42051b0fb9b0ff8dcb0e...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2019-07-01 00:00:00+00:00</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>156982</th>
      <td>63.00</td>
      <td>815.000000</td>
      <td>08308c31cd99f495dea73ca276d19a6258d7b4c9c88e43...</td>
      <td>19.96</td>
      <td>4.0</td>
      <td>69.570552</td>
      <td>2019-07-01 00:00:00+00:00</td>
      <td>3260.0</td>
      <td>15.750000</td>
      <td>22.041718</td>
    </tr>
    <tr>
      <th>156983</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2019-07-01 00:00:00+00:00</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>156984 rows × 10 columns</p>
</div>



Feast uses [Great Expectations](https://docs.greatexpectations.io/docs/) as a validation engine and [ExpectationSuite](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/core/expectation_suite/index.html#great_expectations.core.expectation_suite.ExpectationSuite) as a dataset's profile. Hence, we need to develop a function that will generate ExpectationSuite. This function will receive instance of [PandasDataset](https://legacy.docs.greatexpectations.io/en/latest/autoapi/great_expectations/dataset/index.html?highlight=pandasdataset#great_expectations.dataset.PandasDataset) (wrapper around pandas.DataFrame) so we can utilize both Pandas DataFrame API and some helper functions from PandasDataset during profiling.


```python
DELTA = 0.1  # controlling allowed window in fraction of the value on scale [0, 1]

@ge_profiler
def stats_profiler(ds: PandasDataset) -> ExpectationSuite:
    # simple checks on data consistency
    ds.expect_column_values_to_be_between(
        "avg_speed",
        min_value=0,
        max_value=60,
        mostly=0.99  # allow some outliers
    )
    
    ds.expect_column_values_to_be_between(
        "total_miles_travelled",
        min_value=0,
        max_value=500,
        mostly=0.99  # allow some outliers
    )
    
    # expectation of means based on observed values
    observed_mean = ds.trip_count.mean()
    ds.expect_column_mean_to_be_between("trip_count",
                                        min_value=observed_mean * (1 - DELTA),
                                        max_value=observed_mean * (1 + DELTA))
    
    observed_mean = ds.earned_per_hour.mean()
    ds.expect_column_mean_to_be_between("earned_per_hour",
                                        min_value=observed_mean * (1 - DELTA),
                                        max_value=observed_mean * (1 + DELTA))
    
    
    # expectation of quantiles
    qs = [0.5, 0.75, 0.9, 0.95]
    observed_quantiles = ds.avg_fare.quantile(qs)
    
    ds.expect_column_quantile_values_to_be_between(
        "avg_fare",
        quantile_ranges={
            "quantiles": qs,
            "value_ranges": [[None, max_value] for max_value in observed_quantiles]
        })                                     
    
    return ds.get_expectation_suite()
```

Testing our profiler function:


```python
ds.get_profile(profiler=stats_profiler)
```
    02/02/2022 02:43:47 PM INFO:	5 expectation(s) included in expectation_suite. result_format settings filtered.
    <GEProfile with expectations: [
      {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {
          "column": "avg_speed",
          "min_value": 0,
          "max_value": 60,
          "mostly": 0.99
        },
        "meta": {}
      },
      {
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {
          "column": "total_miles_travelled",
          "min_value": 0,
          "max_value": 500,
          "mostly": 0.99
        },
        "meta": {}
      },
      {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {
          "column": "trip_count",
          "min_value": 10.387244591346153,
          "max_value": 12.695521167200855
        },
        "meta": {}
      },
      {
        "expectation_type": "expect_column_mean_to_be_between",
        "kwargs": {
          "column": "earned_per_hour",
          "min_value": 52.320624975640214,
          "max_value": 63.94743052578249
        },
        "meta": {}
      },
      {
        "expectation_type": "expect_column_quantile_values_to_be_between",
        "kwargs": {
          "column": "avg_fare",
          "quantile_ranges": {
            "quantiles": [
              0.5,
              0.75,
              0.9,
              0.95
            ],
            "value_ranges": [
              [
                null,
                16.4
              ],
              [
                null,
                26.229166666666668
              ],
              [
                null,
                36.4375
              ],
              [
                null,
                42.0
              ]
            ]
          }
        },
        "meta": {}
      }
    ]>



**Verify that all expectations that we coded in our profiler are present here. Otherwise (if you can't find some expectations) it means that it failed to pass on the reference dataset (do it silently is default behavior of Great Expectations).**

Now we can create validation reference from dataset and profiler function:


```python
validation_reference = ds.as_reference(profiler=stats_profiler)
```

and test it against our existing retrieval job


```python
_ = job.to_df(validation_reference=validation_reference)
```

    02/02/2022 02:43:52 PM INFO: 5 expectation(s) included in expectation_suite. result_format settings filtered.
    02/02/2022 02:43:53 PM INFO: Validating data_asset_name None with expectation_suite_name default


Validation successfully passed as no exception were raised.


### 5. Validating new historical retrieval 

Creating new timestamps for Dec 2020:


```python
from feast.dqm.errors import ValidationFailed
```


```python
timestamps = pd.DataFrame()
timestamps["event_timestamp"] = pd.date_range("2020-12-01", "2020-12-07", freq='D')
```


```python
entity_df = pd.merge(taxi_ids, timestamps, how='cross')
entity_df
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>taxi_id</th>
      <th>event_timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2020-12-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2020-12-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2020-12-03</td>
    </tr>
    <tr>
      <th>3</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2020-12-04</td>
    </tr>
    <tr>
      <th>4</th>
      <td>91d5288487e87c5917b813ba6f75ab1c3a9749af906a2d...</td>
      <td>2020-12-05</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>35443</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2020-12-03</td>
    </tr>
    <tr>
      <th>35444</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2020-12-04</td>
    </tr>
    <tr>
      <th>35445</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2020-12-05</td>
    </tr>
    <tr>
      <th>35446</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2020-12-06</td>
    </tr>
    <tr>
      <th>35447</th>
      <td>7ebf27414a0c7b128e7925e1da56d51a8b81484f7630cf...</td>
      <td>2020-12-07</td>
    </tr>
  </tbody>
</table>
<p>35448 rows × 2 columns</p>
</div>


```python
job = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "trip_stats:total_miles_travelled",
        "trip_stats:total_trip_seconds",
        "trip_stats:total_earned",
        "trip_stats:trip_count",
        "on_demand_stats:avg_fare",
        "on_demand_stats:avg_trip_seconds",
        "on_demand_stats:avg_speed",
        "on_demand_stats:earned_per_hour",
    ]
)
```

Execute retrieval job with validation reference:


```python
try:
    df = job.to_df(validation_reference=validation_reference)
except ValidationFailed as exc:
    print(exc.validation_report)
```

    02/02/2022 02:43:58 PM INFO: 5 expectation(s) included in expectation_suite. result_format settings filtered.
    02/02/2022 02:43:59 PM INFO: Validating data_asset_name None with expectation_suite_name default

    [
      {
        "expectation_config": {
          "expectation_type": "expect_column_mean_to_be_between",
          "kwargs": {
            "column": "trip_count",
            "min_value": 10.387244591346153,
            "max_value": 12.695521167200855,
            "result_format": "COMPLETE"
          },
          "meta": {}
        },
        "meta": {},
        "result": {
          "observed_value": 6.692920555429092,
          "element_count": 35448,
          "missing_count": 31055,
          "missing_percent": 87.6071992778154
        },
        "exception_info": {
          "raised_exception": false,
          "exception_message": null,
          "exception_traceback": null
        },
        "success": false
      },
      {
        "expectation_config": {
          "expectation_type": "expect_column_mean_to_be_between",
          "kwargs": {
            "column": "earned_per_hour",
            "min_value": 52.320624975640214,
            "max_value": 63.94743052578249,
            "result_format": "COMPLETE"
          },
          "meta": {}
        },
        "meta": {},
        "result": {
          "observed_value": 68.99268345164135,
          "element_count": 35448,
          "missing_count": 31055,
          "missing_percent": 87.6071992778154
        },
        "exception_info": {
          "raised_exception": false,
          "exception_message": null,
          "exception_traceback": null
        },
        "success": false
      },
      {
        "expectation_config": {
          "expectation_type": "expect_column_quantile_values_to_be_between",
          "kwargs": {
            "column": "avg_fare",
            "quantile_ranges": {
              "quantiles": [
                0.5,
                0.75,
                0.9,
                0.95
              ],
              "value_ranges": [
                [
                  null,
                  16.4
                ],
                [
                  null,
                  26.229166666666668
                ],
                [
                  null,
                  36.4375
                ],
                [
                  null,
                  42.0
                ]
              ]
            },
            "result_format": "COMPLETE"
          },
          "meta": {}
        },
        "meta": {},
        "result": {
          "observed_value": {
            "quantiles": [
              0.5,
              0.75,
              0.9,
              0.95
            ],
            "values": [
              19.5,
              28.1,
              38.0,
              44.125
            ]
          },
          "element_count": 35448,
          "missing_count": 31055,
          "missing_percent": 87.6071992778154,
          "details": {
            "success_details": [
              false,
              false,
              false,
              false
            ]
          }
        },
        "exception_info": {
          "raised_exception": false,
          "exception_message": null,
          "exception_traceback": null
        },
        "success": false
      }
    ]


Validation failed since several expectations didn't pass:
* Trip count (mean) decreased more than 10% (which is expected when comparing Dec 2020 vs June 2019)
* Average Fare increased - all quantiles are higher than expected
* Earn per hour (mean) increased more than 10% (most probably due to increased fare)

