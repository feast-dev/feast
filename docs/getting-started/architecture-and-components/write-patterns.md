# Writing Data to Feast

Feast uses a [Push Model](getting-started/architecture-and-components/push-vs-pull-model.md) to push features to the online store.

This means Data Producers (i.e., services that generate data) have to push data to Feast. 
Said another way, users have to send Feast data to Feast so Feast can write it to the online store.

## Communication Patterns

There are two ways to *_send_* data to the online store: 

1. Synchronously
   - Using an API call for a small number of entities or a single entity
2. Asynchronously 
   - Using an API call for a small number of entities or a single entity
   - Using a "batch job" for a large number of entities

It is worth noting that, in some contexts, developers may "batch" a group of entities together and write them to the 
online store in a single API call. This is a common pattern when writing data to the online store to reduce write loads
but this would not qualify as a batch job.

## Feature Value Write Patterns
There are two ways to write *feature values* to the online store:

1. Precomputing the transformations
2. Computing the transformations "On Demand"

## Tradeoffs

When deciding between synchronous and asynchronous data writes, several tradeoffs related to data consistency and operational impacts should be considered:

- **Data Consistency**: Asynchronous writes allow data producers to send data without waiting for the write operation to complete, which can lead to situations where the data in the online store is stale. This might be acceptable in scenarios where absolute freshness is not critical. However, for critical operations, such as calculating loan amounts in financial applications, stale data can lead to incorrect decisions, making synchronous writes essential.
- **Service Coupling**: Synchronous writes result in tighter coupling between services. If a write operation fails, it can cause the dependent service operation to fail as well, which might be a significant drawback in systems requiring high reliability and independence between services.
- **Application Latency**: Asynchronous writes typically reduce the perceived latency from the client's perspective because the client does not wait for the write operation to complete. This can enhance the user experience and efficiency in environments where operations are not critically dependent on immediate data freshness.
- **Correctness**: The risk of data being out-of-date must be weighed against the operational requirements. For instance, in a lending application, having up-to-date feature data can be crucial for correctness (depending upon the features and raw data), thus favoring synchronous writes. In less sensitive contexts, the eventual consistency offered by asynchronous writes might be sufficient.

## Decision Matrix

Given these considerations, the following matrix can help decide the most appropriate data write and feature computation strategies based on specific application needs and data sensitivity:

| Scenario | Data Write Type | Feature Computation | Recommended Approach |
|----------|-----------------|---------------------|----------------------|
| Real-time, high-stakes decision making | Synchronous | On Demand | Use synchronous writes with on-demand feature computation to ensure data freshness and correctness. |
| High volume, non-critical data processing | Asynchronous | Precomputed | Use asynchronous batch jobs with precomputed transformations for efficiency and scalability. |
| User-facing applications requiring quick feedback | Synchronous | Precomputed | Use synchronous writes with precomputed features to reduce latency and improve user experience. |
| Data-intensive applications tolerant to staleness | Asynchronous | On Demand | Opt for asynchronous writes with on-demand computation to balance load and manage resource usage efficiently. |

Each scenario balances the tradeoffs differently, depending on the application's tolerance for staleness versus its need for immediacy and accuracy.

