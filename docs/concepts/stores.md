# Stores

{% hint style="danger" %}
We strongly encourage all users to upgrade from Feast 0.9 to Feast 0.10+. Please see [this](https://docs.feast.dev/v/master/project/feast-0.9-vs-feast-0.10+) for an explanation of the differences between the two versions. A guide to upgrading can be found [here](https://docs.google.com/document/d/1AOsr_baczuARjCpmZgVd8mCqTF4AZ49OEyU4Cn-uTT0/edit#heading=h.9gb2523q4jlh). 
{% endhint %}

In Feast, a store is a database that is populated with feature data that will ultimately be served to models.

## Offline \(Historical\) Store

The offline store maintains historical copies of feature values. These features are grouped and stored in feature tables. During retrieval of historical data, features are queries from these feature tables in order to produce training datasets.

{% hint style="warning" %}
Feast 0.8 does not support offline storage. Support will be added in Feast 0.9.
{% endhint %}

## Online Store

The online store maintains only the latest values for a specific feature.

* Feature values are stored based on their [entity keys](glossary.md#entity-key)
* Feast currently supports Redis as an online store.
* Online stores are meant for very high throughput writes from ingestion jobs and very low latency access to features during online serving.

{% hint style="info" %}
Feast only supports a single online store in production
{% endhint %}

