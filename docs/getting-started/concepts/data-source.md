# Data source

The data source refers to raw underlying data \(e.g. a table in BigQuery\). 

Feast uses a time-series data model to represent data. This data model is used to interpret feature data in data sources in order to build training datasets or when materializing features into an online store.

Below is an example data source with a single entity \(`driver`\) and two features \(`trips_today`, and `rating`\).

![Ride-hailing data source](../../.gitbook/assets/image%20%2816%29.png)



