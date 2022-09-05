# Online store

Feast uses online stores to serve features at low latency.
Feature values are loaded from data sources into the online store through _materialization_, which can be triggered through the `materialize` command.

The storage schema of features within the online store mirrors that of the original data source.
One key difference is that for each [entity key](../concepts/entity.md), only the latest feature values are stored.
No historical values are stored.

Here is an example batch data source:

![](../../.gitbook/assets/image%20%286%29.png)

Once the above data source is materialized into Feast (using `feast materialize`), the feature values will be stored as follows:

![](../../.gitbook/assets/image%20%285%29.png)

Features can also be written directly to the online store via [push sources](../../reference/data-sources/push.md) .