# Entities

An entity is any domain object that can be modelled and that information can be stored about. Entities are usually recognisable concepts, either concrete or abstract, such as persons, places, things, or events which have relevance to the modelled system.

More formally, an entity is an instance of an entity type. An entity type is the class of entities where entities are the instances.

* Examples of entity types in the context of ride-hailing and food delivery: `customer`, `order`, `driver`, `restaurant`, `dish`, `area`.
* A specific driver, for example a driver with ID `D011234` would be an entity of the entity type `driver`

An entity is the object on which features are observed. For example we could have a feature `total_trips_24h` on the driver `D01123` with a feature value of `11`.

In the context of Feast, entities are important because they are used as keys when looking up feature values. Entities are also used when joining feature values between different feature tables in order to build one large data set to train a model, or to serve a model.

Entities can be created through the [Feast SDK](../getting-started/connecting-to-feast-1/connecting-to-feast.md) as follows:

```python
from feast import Entity, ValueType

# Create a customer entity
customer = Entity(
    "customer_id",
    "Customer id for ride customer",
    ValueType.INT64
)
```

Please see the [EntitySpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#EntitySpecV2) for the entity specification API.

