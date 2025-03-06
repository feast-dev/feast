# Writing Data to Feast

Feast uses a [Push Model](push-vs-pull-model.md) to push features to the online store.

This has two important consequences: (1) communication patterns between the Data Producer (i.e., the client) and Feast (i.e,. the server) and (2) feature computation and 
_feature value_ write patterns to Feast's online store.

Data Producers (i.e., services that generate data) send data to Feast so that Feast can write feature values to the online store. That data can
be either raw data where Feast computes and stores the feature values or precomputed feature values.

## Communication Patterns

There are two ways a client (or Data Producer) can *_send_* data to the online store: 

1. Synchronously
   - Using a synchronous API call for a small number of entities or a single entity (e.g., using the [`push` or `write_to_online_store` methods](../../reference/data-sources/push.md#pushing-data)) or the Feature Server's [`push` endpoint](../../reference/feature-servers/python-feature-server.md#pushing-features-to-the-online-and-offline-stores))
2. Asynchronously 
   - Using an asynchronous API call for a small number of entities or a single entity (e.g., using the [`push` or `write_to_online_store` methods](../../reference/data-sources/push.md#pushing-data)) or the Feature Server's [`push` endpoint](../../reference/feature-servers/python-feature-server.md#pushing-features-to-the-online-and-offline-stores))
   - Using a "batch job" for a large number of entities (e.g., using a [batch materialization engine](../components/batch-materialization-engine.md))

Note, in some contexts, developers may "batch" a group of entities together and write them to the online store in a 
single API call. This is a common pattern when writing data to the online store to reduce write loads but we would 
not qualify this as a batch job.

## Feature Value Write Patterns

Writing feature values to the online store (i.e., the server) can be done in two ways: Precomputing the transformations client-side or Computing the transformations On Demand server-side. 

### Combining Approaches

In some scenarios, a combination of Precomputed and On Demand transformations may be optimal.

When selecting feature value write patterns, one must consider the specific requirements of your application, the acceptable correctness of the data, the latency tolerance, and the computational resources available. Making deliberate choices can help the performance and reliability of your service.

There are two ways the client can write *feature values* to the online store:

1. Precomputing transformations
2. Computing transformations On Demand
3. Hybrid (Precomputed + On Demand)

### 1. Precomputing Transformations
Precomputed transformations can happen outside of Feast (e.g., via some batch job or streaming application) or inside of the Feast feature server when writing to the online store via the `push` or `write-to-online-store` api. 

### 2. Computing Transformations On Demand
On Demand transformations can only happen inside of Feast at either (1) the time of the client's request or (2) when the data producer writes to the online store.

### 3. Hybrid (Precomputed + On Demand)
The hybrid approach allows for precomputed transformations to happen inside or outside of Feast and have the On Demand transformations happen at client request time. This is particularly convenient for "Time Since Last" types of features (e.g., time since purchase).

## Tradeoffs

When deciding between synchronous and asynchronous data writes, several tradeoffs should be considered:

- **Data Consistency**: Asynchronous writes allow Data Producers to send data without waiting for the write operation to complete, which can lead to situations where the data in the online store is stale. This might be acceptable in scenarios where absolute freshness is not critical. However, for critical operations, such as calculating loan amounts in financial applications, stale data can lead to incorrect decisions, making synchronous writes essential.
- **Correctness**: The risk of data being out-of-date must be weighed against the operational requirements. For instance, in a lending application, having up-to-date feature data can be crucial for correctness (depending upon the features and raw data), thus favoring synchronous writes. In less sensitive contexts, the eventual consistency offered by asynchronous writes might be sufficient.
- **Service Coupling**: Synchronous writes result in tighter coupling between services. If a write operation fails, it can cause the dependent service operation to fail as well, which might be a significant drawback in systems requiring high reliability and independence between services.
- **Application Latency**: Asynchronous writes typically reduce the perceived latency from the client's perspective because the client does not wait for the write operation to complete. This can enhance the user experience and efficiency in environments where operations are not critically dependent on immediate data freshness.

The table below can help guide the most appropriate data write and feature computation strategies based on specific application needs and data sensitivity.

| Data Write Type | Feature Computation | Scenario | Recommended Approach |
|----------|-----------------|---------------------|----------------------|
| Asynchronous | On Demand | Data-intensive applications tolerant to staleness | Opt for asynchronous writes with on-demand computation to balance load and manage resource usage efficiently. |
| Asynchronous | Precomputed | High volume, non-critical data processing | Use asynchronous batch jobs with precomputed transformations for efficiency and scalability. |
| Synchronous | On Demand | High-stakes decision making | Use synchronous writes with on-demand feature computation to ensure data freshness and correctness. |
| Synchronous | Precomputed | User-facing applications requiring quick feedback | Use synchronous writes with precomputed features to reduce latency and improve user experience. |
| Synchronous | Hybrid (Precomputed + On Demand) | High-stakes decision making that want to optimize for latency under constraints| Use synchronous writes with precomputed features where possible and a select set of on demand computations to reduce latency and improve user experience. |
