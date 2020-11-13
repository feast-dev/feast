# Entities

### Overview

An entity is any domain object that can be modeled and about which information can be stored. Entities are usually recognizable concepts, either concrete or abstract, such as persons, places, things, or events which have relevance to the modeled system.

* Examples of entity types in the context of ride-hailing and food delivery: `customer`, `order`, `driver`, `restaurant`, `dish`, `area`.
* A specific driver, for example a driver with ID `D011234` would be an entity of the entity type `driver`

An entity is the domain object on which features are observed. For example, we could have a feature `total_trips_24h` for driver `D011234` with a feature value of `11`.

Entities are important for Feast because they are used as keys when searching for feature values. Entities are also used when joining feature values from different feature tables to build a large data set that is used to train or serve models.

### Structure of an Entity

When creating an entity specification, consider the following fields:

* **name**: Name of the entity
* **description**: Description of the entity
* **value\_type**: Value type of the entity
* **labels**: User-defined metadata

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

### Working with an Entity

Creating an Entity:

```python
# Create a customer entity
customer_entity = Entity(name="customer_id", description="ID of car customer")
client.apply_entity(customer_entity)
```

Updating an Entity:

```python
# Update a customer entity
customer_entity = client.get_entity("customer_id")
customer_entity.description = "ID of bike customer"
client.apply_entity(customer_entity)
```

Permitted changes include:

* The entity's description and labels

{% hint style="warning" %}
You **cannot** change the following:

* Project or name of an entity
* Types of entity
{% endhint %}

Visit [EntitySpec](https://api.docs.feast.dev/grpc/feast.core.pb.html#EntitySpecV2) for the entity-specification API.

