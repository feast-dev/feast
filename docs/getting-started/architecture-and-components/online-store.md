# Online store

The Feast online store is used for low-latency online feature value lookups. Feature values are loaded into the online store from data sources in feature views using the `materialize` command.

The storage schema of features within the online store mirrors that of the data source used to populate the online store. One key difference between the online store and data sources is that only the latest feature values are stored per entity key. No historical values are stored.

Example batch data source

![](../../.gitbook/assets/image%20%286%29.png)

Once the above data source is materialized into Feast \(using `feast materialize`\), the feature values will be stored as follows:

![](../../.gitbook/assets/image%20%285%29.png)

Features can also be written to the online store via [push sources](https://docs.feast.dev/reference/data-sources/push) 