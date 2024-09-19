# Tags

## Overview

Tags is a way to filter Feast objects when listing the objects in the UI, Cli or when directly querying the registry.

The way to define tags on the feast objects is through the definition file or directly in the object that will be applied to the feature store.

## Examples

In this example we define a Feature View in a definition file that has a tag:
```python
driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Average daily trips"),
    ],
    online=True,
    source=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "driver_performance"},
)
```

In this example we define a Stream Feature View that has a tag, in the code:
```python
    sfv = StreamFeatureView(
        name="test kafka stream feature view",
        entities=[entity],
        schema=[],
        description="desc",
        timestamp_field="event_timestamp",
        source=stream_source,
        tags={"team": "driver_performance"},
```

An example of listing feature-views with the tag `team:driver_performance`:
```commandline
$ feast feature-views list --tags team:driver_performance              
NAME                       ENTITIES    TYPE
driver_hourly_stats        {'driver'}  FeatureView
driver_hourly_stats_fresh  {'driver'}  FeatureView
```

The same example of listing feature-views without tag filtering:
```commandline
$ feast feature-views list
NAME                       ENTITIES    TYPE
driver_hourly_stats        {'driver'}  FeatureView
driver_hourly_stats_fresh  {'driver'}  FeatureView
transformed_conv_rate_fresh  {'driver'}  OnDemandFeatureView
transformed_conv_rate        {'driver'}  OnDemandFeatureView
```

