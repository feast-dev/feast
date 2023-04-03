# Entity

An entity is a collection of semantically related features. Users define entities to map to the domain of their use case. For example, a ride-hailing service could have customers and drivers as their entities, which group related features that correspond to these customers and drivers.

```python
driver = Entity(name='driver', join_keys=['driver_id'])
```

The _entity name_ is used to uniquely identify the entity (for example to show in the experimental Web UI). The _join key_ is used to identify the physical primary key on which feature values should be joined together to be retrieved during feature retrieval.

Entities are used by Feast in many contexts, as we explore below:

### Use case #1: Defining and storing features

Feast's primary object for defining features is a _feature view,_ which is a collection of features. Feature views map to 0 or more entities, since a feature can be associated with:

* zero entities (e.g. a global feature like _num\_daily\_global\_transactions_)
* one entity (e.g. a user feature like _user\_age_ or _last\_5\_bought\_items_)
* multiple entities, aka a composite key (e.g. a user + merchant category feature like _num\_user\_purchases\_in\_merchant\_category)_

Feast refers to this collection of entities for a feature view as an **entity key**.

![](<../../.gitbook/assets/image (15).png>)

Entities should be reused across feature views. This helps with discovery of features, since it enables data scientists understand how other teams build features for the entity they are most interested in.

Feast will use the feature view concept to then define the schema of groups of features in a low-latency online store.

### Use case #2: Retrieving features

At _training time_, users control what entities they want to look up, for example corresponding to train / test / validation splits. A user specifies a list of _entity keys + timestamps_ they want to fetch [point-in-time](./point-in-time-joins.md) correct features for to generate a training dataset.

At _serving time_, users specify _entity key(s)_ to fetch the latest feature values which can power real-time model prediction (e.g. a fraud detection model that needs to fetch the latest transaction user's features to make a prediction).

{% hint style="info" %}
**Q: Can I retrieve features for all entities?**

Kind of.

In practice, this is most relevant for _batch scoring models_ (e.g. predict user churn for all existing users) that are offline only. For these use cases, Feast supports generating features for a SQL-backed list of entities. There is an [open GitHub issue](https://github.com/feast-dev/feast/issues/1611) that welcomes contribution to make this a more intuitive API.

For _real-time feature retrieval_, there is no out of the box support for this because it would promote expensive and slow scan operations which can affect the performance of other operations on your data sources. Users can still pass in a large list of entities for retrieval, but this does not scale well.
{% endhint %}
