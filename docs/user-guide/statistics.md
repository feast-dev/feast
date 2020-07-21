# Statistics

Data is a first-class citizen in machine learning projects, it is critical to have tests and validations around data. To that end, Feast avails various feature statistics to users in order to give users visibility into the data that has been ingested into the system.

![overview](../.gitbook/assets/statistics-sources%20%281%29.png)

Feast exposes feature statistics at two points in the Feast system: 1. Inflight feature statistics from the population job 2. Historical feature statistics from the warehouse stores

## Historical Feature Statistics

Feast supports the computation of feature statistics over data already written to warehouse stores. These feature statistics, which can be retrieved over distinct sets of historical data, are fully compatible with [TFX's Data Validation](https://tensorflow.google.cn/tfx/tutorials/data_validation/tfdv_basic).

### Retrieving Statistics

Statistics can be retrieved from Feast using the python SDK's `get_statistics` method. This requires a connection to Feast core.

Feature statistics can be retrieved for a single feature set, from a single valid warehouse store. Users can opt to either retrieve feature statistics for a discrete subset of data by providing an `ingestion_id` , a unique id generated for a dataset when it is ingested into feast:

```text
# A unique ingestion id is returned for each batch ingestion
ingestion_id=client.ingest(feature_set,df)

stats = client.get_statistics( 
    feature_set_id='project/feature_set', 
    store='warehouse', 
    features=['feature_1', 'feature_2'], 
    ingestion_ids=[ingestion_id])
```

Or by selecting data within a time range by providing a `start_date` and `end_date` \(the start date is inclusive, the end date is not\):

```text
start_date=datetime(2020,10,1,0,0,0)
end_date=datetime(2020,10,2,0,0,0)

stats = client.get_statistics(
feature_set_id = 'project/feature_set',
    store='warehouse',
    features=['feature_1', 'feature_2'],
    start_date=start_date,
    end_date=end_date)
```

{% hint style="info" %}
Although `get_statistics` accepts python `datetime` objects for `start_date` and `end_date`, statistics are computed at the day granularity.
{% endhint %}

Note that when providing a time range, Feast will NOT filter out duplicated rows. It is therefore highly recommended to provide `ingestion_id`s whenever possible.

Feast returns the statistics in the form of the protobuf [DatasetFeatureStatisticsList](https://github.com/tensorflow/metadata/blob/master/tensorflow_metadata/proto/v0/statistics.proto#L36), which can be subsequently passed to TFDV methods to [validate the dataset](https://www.tensorflow.org/tfx/data_validation/get_started#checking_the_data_for_errors)...

```text
anomalies = tfdv.validate_statistics(
    statistics=stats_2, schema=feature_set.export_tfx_schema())
tfdv.display_anomalies(anomalies)
```

Or [visualise the statistics](https://www.tensorflow.org/tfx/data_validation/get_started#computing_descriptive_data_statistics) in [facets](https://github.com/PAIR-code/facets).

```text
tfdv.visualize_statistics(stats)
```

Refer to the [example notebook](https://github.com/feast-dev/feast/blob/master/examples/statistics/Historical%20Feature%20Statistics%20with%20Feast,%20TFDV%20and%20Facets.ipynb) for an end-to-end example showcasing Feast's integration with TFDV and Facets.

### Aggregating Statistics

Feast supports retrieval of feature statistics across multiple datasets or days.

```text
stats = client.get_statistics( 
    feature_set_id='project/feature_set', 
    store='warehouse', 
    features=['feature_1', 'feature_2'], 
    ingestion_ids=[ingestion_id_1, ingestion_id_2])
```

However, when querying across multiple datasets, Feast computes the statistics for each dataset independently \(for caching purposes\), and aggregates the results. As a result of this, certain un-aggregatable statistics are dropped in the process, such as medians, uniqueness counts, and histograms.

Refer to the table below for the list of statistics that will be dropped.

### Caching

Feast caches the results of all feature statistics requests, and will, by default, retrieve and return the cached results. To recompute previously computed feature statistics, set `force_refresh` to `true` when retrieving the statistics:

```text
stats=client.get_statistics(
    feature_set_id='project/feature_set',
    store='warehouse',
    features=['feature_1', 'feature_2'],
    dataset_ids=[dataset_id],
    force_refresh=True)
```

This will force Feast to recompute the statistics, and replace any previously cached values.

### Supported Statistics

Feast supports most, but not all of the feature statistics defined in TFX's [FeatureNameStatistics](https://github.com/tensorflow/metadata/blob/master/tensorflow_metadata/proto/v0/statistics.proto#L147). For the definition of each statistic and information about how each one is computed, refer to the [protobuf definition](https://github.com/tensorflow/metadata/blob/master/tensorflow_metadata/proto/v0/statistics.proto#L147).

| Type | Statistic | Supported | Aggregateable |
| :--- | :--- | :--- | :--- |
| Common | NumNonMissing | ✔ | ✔ |
|  | NumMissing | ✔ | ✔ |
|  | MinNumValues | ✔ | ✔ |
|  | MaxNumValues | ✔ | ✔ |
|  | AvgNumValues | ✔ | ✔ |
|  | TotalNumValues | ✔ | ✔ |
|  | NumValuesHist |  |  |
| Numeric | Min | ✔ | ✔ |
|  | Max | ✔ | ✔ |
|  | Median | ✔ |  |
|  | Mean | ✔ | ✔ |
|  | Stdev | ✔ | ✔ |
|  | NumZeroes | ✔ | ✔ |
|  | Quantiles | ✔ |  |
|  | Histogram | ✔ |  |
| String | RankHistogram | ✔ |  |
|  | TopValues | ✔ |  |
|  | Unique | ✔ |  |
|  | AvgLength | ✔ | ✔ |
| Bytes | MinNumBytes | ✔ | ✔ |
|  | MaxNumBytes | ✔ | ✔ |
|  | AvgNumBytes | ✔ | ✔ |
|  | Unique | ✔ |  |
| Struct/List | - \(uses common statistics only\) | - | - |

## Inflight Feature Statistics

For insight into data currently flowing into Feast through the population jobs, [statsd](https://github.com/statsd/statsd) is used to capture feature value statistics.

Inflight feature statistics are windowed \(default window length is 30s\) and computed at two points in the feature population pipeline:

1. Prior to store writes, after successful validation 
2. After successful store writes

The following metrics are written at the end of each window as [statsd gauges](https://github.com/statsd/statsd/blob/master/docs/metric_types.md#gauges):

```text
feast_ingestion_feature_value_min
feast_ingestion_feature_value_max
feast_ingestion_feature_value_mean
feast_ingestion_feature_value_percentile_25 
feast_ingestion_feature_value_percentile_50 
feast_ingestion_feature_value_percentile_90 
feast_ingestion_feature_value_percentile_95 
feast_ingestion_feature_value_percentile_99
```

{% hint style="info" %}
the gauge metric type is used over histogram because statsd only supports positive values for histogram metric types, while numerical feature values can be of any double value.
{% endhint %}

The metrics are tagged with and can be aggregated by the following keys:

| key | description |
| :--- | :--- |
| feast\_store | store the population job is writing to |
| feast\_project\_name | feast project name |
| feast\_featureSet\_name | feature set name |
| feast\_feature\_name | feature name |
| ingestion\_job\_name | id of the population job writing the feature values. |
| metrics\_namespace | either `Inflight` or `WriteToStoreSuccess` |

