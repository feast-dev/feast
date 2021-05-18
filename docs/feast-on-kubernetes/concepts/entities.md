# Entities

## Overview

An entity is any domain object that can be modeled and about which information can be stored. Entities are usually recognizable concepts, either concrete or abstract, such as persons, places, things, or events.

Examples of entities in the context of ride-hailing and food delivery: `customer`, `order`, `driver`, `restaurant`, `dish`, `area`.

Entities are important in the context of feature stores since features are always properties of a specific entity. For example, we could have a feature `total_trips_24h` for driver `D011234` with a feature value of `11`.

Feast uses entities in the following way:

* Entities serve as the keys used to look up features for producing training datasets and online feature values.
* Entities serve as a natural grouping of features in a feature table. A feature table must belong to an entity \(which could be a composite entity\)

## Structure of an Entity

When creating an entity specification, consider the following fields:

* **Name**: Name of the entity
* **Description**: Description of the entity
* **Value Type**: Value type of the entity. Feast will attempt to coerce entity columns in your data sources into this type.
* **Labels**: Labels are maps that allow users to attach their own metadata to entities

A valid entity specification is shown below:

```python
customer = Entity(
    name="customer_id",
    description="Customer id for ride customer",
    value_type=ValueType.INT64,
    labels={}
)
```

## Working with an Entity

### Creating an Entity:

```python
# Create a customer entity
customer_entity = Entity(name="customer_id", description="ID of car customer")
client.apply(customer_entity)
```

### Updating an Entity:

```python
# Update a customer entity
customer_entity = client.get_entity("customer_id")
customer_entity.description = "ID of bike customer"
client.apply(customer_entity)
```

Permitted changes include:

* The entity's description and labels

The following changes are not permitted:

* Project
* Name of an entity
* Type

