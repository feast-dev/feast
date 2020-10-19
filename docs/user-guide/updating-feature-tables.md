# Updating Feature Tables

In order to facilitate the need for feature table definitions to change over time, a limited set of changes can be made to existing feature tables.

To apply changes to a feature table:

```python
# With existing feature table
driver_trips_ft = FeatureTable.from_yaml("driver_feature_table.yaml")

# Add new feature, maximum_daily_rides
driver_trips_ft.add_feature(Feature(name="maximum_daily_rides", dtype=ValueType.INT32))

# Apply changed feature table
client.apply(driver_trips_ft)
```

Permitted changes include:

* Adding new features
* Deleting existing features \(note that features are tombstoned and remain on record, rather than removed completely; as a result, new features will not be able to take the names of these deleted features\)
* Changing features' TFX schemas
* Changing the feature table's source, max age and labels

Note that the following are **not** allowed:

* Changes to project or name of the feature table.
* Changes to entities related to the feature table.
* Changes to names and types of existing features.

