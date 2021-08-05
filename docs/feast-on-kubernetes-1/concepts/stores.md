# Stores

In Feast, a store is a database that is populated with feature data that will ultimately be served to models.

## Offline \(Historical\) Store

The offline store maintains historical copies of feature values. These features are grouped and stored in feature tables. During retrieval of historical data, features are queries from these feature tables in order to produce training datasets.

## Online Store

The online store maintains only the latest values for a specific feature.

* Feature values are stored based on their [entity keys]()
* Feast currently supports Redis as an online store.
* Online stores are meant for very high throughput writes from ingestion jobs and very low latency access to features during online serving.

{% hint style="info" %}
Feast only supports a single online store in production
{% endhint %}

