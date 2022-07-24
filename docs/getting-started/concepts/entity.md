# Entity

An entity is a collection of semantically related features. Users define entities to map to the domain of their use case. For example, a ride-hailing service could have customers and drivers as their entities, which group related features that correspond to these customers and drivers.

```python
driver = Entity(name='driver', value_type=ValueType.STRING, join_keys=['driver_id'])
```

Entities are typically defined as part of feature views. Entity name is used to reference the entity from a feature view definition and join key is used to identify the physical primary key on which feature values should be stored and retrieved. These keys are used during the lookup of feature values from the online store and the join process in point-in-time joins. It is possible to define composite entities \(more than one entity object\) in a feature view. It is also possible for feature views to have zero entities. See [feature view](feature-view.md) for more details.

Entities should be reused across feature views.

## **Entity key**

A related concept is an entity key. These are one or more entity values that uniquely describe a feature view record. In the case of an entity \(like a `driver`\) that only has a single entity field, the entity _is_ an entity key. However, it is also possible for an entity key to consist of multiple entity values. For example, a feature view with the composite entity of \(customer, country\) might have an entity key of \(1001, 5\).

![](../../.gitbook/assets/image%20%2815%29.png)

Entity keys act as primary keys. They are used during the lookup of features from the online store, and they are also used to match feature rows across feature views during point-in-time joins.



