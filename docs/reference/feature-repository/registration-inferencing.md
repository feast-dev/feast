# Registration Inferencing

## Overview

* FeatureView - When the `features` parameter is left out of the FeatureView definition, upon a `feast apply` call, Feast will automatically consider every column in the data source as a feature to be registered other than the specific timestamp columns associated with the underlying data source definition (e.g. event_timestamp_column) and the columns associated with the Feature View's entities.
* DataSource - When the `event_timestamp_column` is left out of the DataSource definition, upon a 'feast apply' call, Feast will automatically find the sole timestamp column in the table underlying the data source and use that as the `event_timestamp_column`. If there are no columns of timestamp type or multiple columns of timestamp type, `feast apply` will throw an exception.
* Entity - When the `value_type` is left out of the Entity definition, upon a `feast apply` call, Feast will automatically find the column corresponding with the Entity's `join_key` and take that column's data type to be the `value_type`. If the column doesn't exist, `feast apply` will throw an exception.
