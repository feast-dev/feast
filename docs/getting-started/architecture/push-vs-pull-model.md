# Push vs Pull Model

Feast uses a [Push Model](https://en.wikipedia.org/wiki/Push_technology), i.e., 
Data Producers push data to the feature store and Feast stores the feature values 
in the online store, to serve features in real-time. 

In a [Pull Model](https://en.wikipedia.org/wiki/Pull_technology), Feast would 
pull data from the data producers at request time and store the feature values in 
the online store before serving them (storing them would actually be unnecessary). 
This approach would incur additional network latency as Feast would need to orchestrate 
a request to each data producer, which would mean the latency would be at least as long as 
your slowest call. So, in order to serve features as fast as possible, we push data to 
Feast and store the feature values in the online store.

The trade-off with the Push Model is that strong consistency is not guaranteed out 
of the box. Instead, strong consistency has to be explicitly designed for in orchestrating 
the updates to Feast and the client usage.

The significant advantage with this approach is that Feast is read-optimized for low-latency 
feature retrieval.

# How to Push

Implicit in the Push model are decisions about _how_ and _when_ to push feature values to the online store. 

From a developer's perspective, there are three ways to push feature values to the online store with different tradeoffs.

They are discussed further in the [Write Patterns](write-patterns.md) section.
