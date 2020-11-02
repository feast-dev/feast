# Getting training features

Feast provides a historical retrieval interface for exporting feature data to train machine learning models. Essentially, users are able to retrieve features from any feature tables and join them together in a single response dataset. The only requirement is that the user provides the correct entities and timestamps in order to look up the features.

Historical feature retrieval can be done through the [Feast SDK](https://api.docs.feast.dev/python).

{% hint style="warning" %}
Historical Retrieval currently pulls from batch sources for Feast v0.8, and offline storage support will not be available until v0.9.
{% endhint %}

{% hint style="info" %}
By default, Feast infers that the features specified belong to the `default` project. To retrieve from another project, specify the `project` parameter when retrieving features.
{% endhint %}

## **Point-in-time-correct Join**

Feast does a point in time correct query from a single feature table. For each entity key and event timestamp combination that is provided by `entity_source`, Feast determines the values of all the features in the `feature_refs` list at that respective point in time and then joins features values to that specific entity value and event timestamp, and repeats this process for all timestamps.

This is called a point in time correct join.

Below is an example of how a point-in-time-correct join works. We have two DataFrames. The first is the `entity dataframe` that contains timestamps, entities, and labels. The user would like to have driver features joined onto this `entity dataframe` from the `driver dataframe` to produce a `joined dataframe` upon materializing the view that contains both labels and features. They would then like to train their model on this output

![](../.gitbook/assets/point_in_time_join%20%281%29.png)

Typically the `input 1` DataFrame would be provided by the user through `entity_source`, and the `input 2` DataFrame would already be ingested into Feast. To join these two, the user would call Feast as follows:

```python
# Feature references with target feature
feature_refs = [
    "driver_trips:average_daily_rides",
    "driver_trips:maximum_daily_rides",
    "driver_trips:rating",
    "trip_completed",
]

# Define entity source
entity_source = FileSource(
   "event_timestamp",
   ParquetFormat(),
   "gs://some-bucket/customer"
)

# Retrieve historical dataset from Feast.
historical_feature_retrieval_job = client.get_historical_features(
    feature_refs=feature_refs,
    entity_rows=entity_source
)

# Retrieve the output uri to materialize the dataset object into a Pandas DataFrame etc.
# Eg. gs://some-bucket/output/, s3://*, file://*
output_file_uri = historical_feature_retrieval_job.get_output_file_uri()
```

Feast is able to intelligently join feature data with different timestamps to a single basis table in a point-in-time-correct way. This allows users to join daily batch data with high-frequency event data transparently. They simply need to provide the feature references.

{% hint style="info" %}
Feast can retrieve features from any amount of feature tables, as long as they occur on the same entities.
{% endhint %}

Point-in-time-correct joins also prevents the occurrence of feature leakage by trying to accurate the state of the world at a single point in time, instead of just joining features based on the nearest timestamps.

