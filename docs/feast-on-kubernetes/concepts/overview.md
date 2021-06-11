# Overview

### Concepts

[Entities](entities.md) are objects in an organization like customers, transactions, and drivers, products, etc.

[Sources](sources.md) are external sources of data where feature data can be found.

[Feature Tables](feature-tables.md) are objects that define logical groupings of features, data sources, and other related metadata.

### Concept Hierarchy

![](../../.gitbook/assets/image%20%284%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%282%29%20%283%29%20%283%29%20%283%29%20%283%29%20%281%29.png)

Feast contains the following core concepts:

* **Projects:** Serve as a top level namespace for all Feast resources. Each project is a completely independent environment in Feast. Users can only work in a single project at a time.
* **Entities:** Entities are the objects in an organization on which features occur. They map to your business domain \(users, products, transactions, locations\).
* **Feature Tables:** Defines a group of features that occur on a specific entity.
* **Features:** Individual feature within a feature table.

