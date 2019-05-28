# Concepts

### What is Feast?
Feast is a Feature Storage platform for Machine Learning features with the following  attributes:

1. Ingestion and storage of ML features via batch or stream
2. Retrieval of ML features for serving via API, or via Google BigQuery to create training datasets
3. Maintaining of a feature catalog, including additional feature attribute information and discovery via API 

Feast solves a need for standardising how features are stored, served and accessed, and encourages sharing and reuse of created features amongst data science teams.

Feast does not prescribe how Features should be created. It allows for ingestion via batch or stream in a number of formats, e.g. batch import from CSV, BigQuery tables, streaming via Pub/Sub etc.


### What is a Feature?

A Feature is an individual measurable property or characteristic of an Entity. In the context of Feast a Feature has the following attributes: 

* Entity - It must be associated with a known Entity within Feast
* ValueType - The feature type must be defined, e.g. String, Bytes, Int64, Int32, Float etc.
* Requirements - Properties related to how a feature should be stored for serving and training
* StorageType - For both serving and training a storage type must be defined

Feast needs to know these attributes in order to be able to ingest, store and serve a feature. A Feature is only a feature when Feast knows about it; This seems contrite, but it introduces a best practice whereby a feature only becomes available for ingestion, serving and training in production when Feast has added the feature to its catalog.

### What is an Entity?

An entity is a type with an associated key which generally maps onto a known domain object, e.g. Driver, Customer, Area, Merchant etc. An entity can also be a composite of other entities, with the corresponding composite key, e.g. DriverArea.

An entity determines how a feature may be retrieved. e.g. for a Driver entity all driver features must be looked up with an associated driver id entity key.
