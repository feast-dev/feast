# Registration Inferencing

## Overview

* FeatureView - When the `features` parameter is left out of the feature view definition, upon a `feast apply` call, Feast will automatically consider every column in the data source as a feature to be registered other than the specific timestamp columns associated with the underlying data source definition (e.g. timestamp_field) and the columns associated with the feature view's entities.
* DataSource - When the `timestamp_field` parameter is left out of the data source definition, upon a 'feast apply' call, Feast will automatically find the sole timestamp column in the table underlying the data source and use that as the `timestamp_field`. If there are no columns of timestamp type or multiple columns of timestamp type, `feast apply` will throw an exception.
* Entity - When the `value_type` parameter is left out of the entity definition, upon a `feast apply` call, Feast will automatically find the column corresponding with the entity's `join_key` and take that column's data type to be the `value_type`. If the column doesn't exist, `feast apply` will throw an exception.
