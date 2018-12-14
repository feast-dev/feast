
# Components

### Core

Feast Core is the central component that manages Feast and all other components within the system. It allows for the registration and management of entities, features, data stores, and other system resources. Core also manages the execution of feature ingestion jobs from batch and streaming sources, and provides the other Feast components with feature related information.

### Stores

Feast maintains data stores for the purposes of model training and serving features to models in production. Features are loaded into these stores by ingestion jobs from both streaming and batch sources. 

Two kinds of data stores are supported:

Warehouse: The feature warehouse maintains all historical feature data. The warehouse can be queried for batch datasets which are then used for model training. 

Supported warehouse: __BigQuery__

Serving: Feast supports multiple serving stores which maintain feature values for access in a production serving environment.

Supported serving stores: __Redis__, __Bigtable__

### Serving

Feast Serving is an API used for for the retrieval of feature values by models in production. It allows for low latency and high throughput access to feature values from serving stores using Feast client libraries. The API abstracts away data access, allowing users to simultaneously query from multiple stores with a single gRPC or HTTP request.

### Client Libraries

Feast provides multiple client libraries for interacting with a Feast deployment.
    
| Functionality                | CLI | Go  | Java | Python (WIP)|
|------------------------------|-----|-----|------|-------------|
| Feature Management           | yes | no  | no   | yes         |
| Data Ingestion (Jobs)        | yes | no  | no   | yes         |
| Feature Retrieval (Training) | no  | no  | no   | yes         |
| Feature Retrieval (Serving)  | no  | yes | yes  | yes         |
