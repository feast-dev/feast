# Quick Start Example

This quickstart gives a simple overview of querying Feast's 

- Online Batch Feature View
- On Demand Feature View (sourced from a batch file)
- On Demand Feature View (sourced from a stream)

To get this to run properly set your working directory to 
```bash
cd examples/quickstart
```
Then run `feast apply` and run the materialization
```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast apply
...
... [stuff will happen here]
...
feast materialize-incremental $CURRENT_TIME
```

Then you can run the simple demo python script
```bash
[on-demand-plus-stream-demo]% python get_features_demo.py 
/Users/francisco.arceo/github/feast/sdk/python/feast/repo_config.py:222: RuntimeWarning: `entity_key_serialization_version` is either not specified in the feature_store.yaml, or is specified to a value <= 1.This serialization version may cause errors when trying to write fields with the `Long` data type into the online store. Specifying `entity_key_serialization_version` to 2 is recommended for new projects. 
  RuntimeWarning,

begin demo...
batch_features  {'driver_id': {0: 1001}, 'event_timestamp': {0: Timestamp('2022-12-24 20:33:30.408881+0000', tz='UTC')}, 'int_val': {0: 100}, 'created': {0: Timestamp('2022-08-24 12:09:14.378000+0000', tz='UTC')}, 'acc_rate': {0: 0.010148861445486546}, 'conv_rate': {0: 0.5983744263648987}, 'avg_daily_trips': {0: 23}} 

on demand features = {'driver_id': [1001], 'conv_rate': [0.5983744263648987], 'avg_daily_trips': [23], 'acc_rate': [0.010148861445486546], 'created': [1661342954378000000], 'output': [100.5983744263649], 'seconds_since_last_created_date': [10743856.145642001], 'days_since_last_created_date': [124], 'created_ts': [datetime.datetime(2022, 8, 24, 12, 9, 14, 378000)]} 

stream features = {'driver_id': [1001], 'conv_rate': [1.0], 'avg_daily_trips': [1000], 'acc_rate': [1.0], 'created': [datetime.datetime(2022, 12, 26, 20, 17, 30, tzinfo=datetime.timezone.utc)], 'output': [101.0], 'seconds_since_last_created_date': [960.543448], 'days_since_last_created_date': [0]} 

...end demo
```

Note, that the data was created on 08/24/2022 so for the on demand features, the `seconds` and `days` since last created features will be relative to that date.