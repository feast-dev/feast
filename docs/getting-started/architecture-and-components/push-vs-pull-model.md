# Push vs Pull Model

Feast uses a [Push Model](https://en.wikipedia.org/wiki/Push_technology) (i.e., the feature store pushes feature values to the online store) to serve features in 
real-time. This model is more efficient than a pull model (i.e., the model serving system pulls feature values from the 
feature store) because it reduces the latency of feature retrieval. 

In a [Pull Model](https://en.wikipedia.org/wiki/Pull_technology), the model serving system must make 
a request to the feature store to retrieve feature values, which can introduce latency. In a push model, the feature 
store pushes feature values to the online store, which reduces the latency of feature retrieval.

