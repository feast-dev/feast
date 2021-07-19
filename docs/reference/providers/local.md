# Local

## Description

* Offline Store: Uses the File offline store by default. Also supports BigQuery as the offline store.
* Online Store: Uses the Sqlite online store by default. Also supports Redis and Datastore as online stores.

## Example

{% code title="feature\_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
```
{% endcode %}

