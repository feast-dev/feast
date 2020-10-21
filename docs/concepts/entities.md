# Entities

## Overview

An entity is any domain object that can be modelled and that information can be stored about. Entities are usually recognisable concepts, either concrete or abstract, such as persons, places, things, or events which have relevance to the modelled system.

* Examples of entity types in the context of ride-hailing and food delivery: `customer`, `order`, `driver`, `restaurant`, `dish`, `area`.
* A specific driver, for example a driver with ID `D011234` would be an entity of the entity type `driver`

An entity is the object on which features are observed. For example, we could have a feature `total_trips_24h` on the driver `D01123` with a feature value of `11`.

In the context of Feast, entities are important because they are used as keys when looking up feature values. Entities are also used when joining feature values between different feature tables in order to build one large data set to train a model, or to serve a model.

## Structure of an Entity

When creating an entity specification, the following fields should be considered:

* **name**: Name of the entity.
* **description**: Description of the entity.
* **value\_type**: Value type of the entity.
* **labels**: User-defined metadata.

A valid entity specification is shown below:

```python
from feast import Entity, ValueType

# Create a customer entity
customer = Entity(
    "customer_id",
    "Customer id for ride customer",
    ValueType.INT64
)
```

## Working with an Entity

#### Creating an Entity

```python
# Create a customer entity
customer_entity = Entity(name="customer_id", description="ID of car customer")
client.apply_entity(customer_entity)
```

#### Updating an Entity

```python
# Update a customer entity
customer_entity = client.get_entity("customer_id")
customer_entity.description = "ID of bike customer"
client.apply_entity(customer_entity)
```

Permitted changes include:

* Changing the entity's description, labels.

Note that the following are **not** allowed:

* Changes to project or name of the entity.
* Changes to types of the entity.

Please see the [EntitySpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#EntitySpecV2) for the entity specification API.

