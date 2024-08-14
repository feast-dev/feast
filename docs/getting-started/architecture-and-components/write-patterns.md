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
There are two ways to write feature values to the online store:

1. Precomputing the transformations
2. Computing the transformations "On Demand"

