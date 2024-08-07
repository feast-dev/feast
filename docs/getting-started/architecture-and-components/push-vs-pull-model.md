# Push vs Pull Model

Feast uses a [Push Model](https://en.wikipedia.org/wiki/Push_technology) (i.e., Data Producers push data to the feature store and Feast stores the feature values in the online store) to serve features in 
real-time. This model is more efficient than a pull model (i.e., Feast pulls data from the data producers) because it reduces the latency of feature retrieval. 

In a [Pull Model](https://en.wikipedia.org/wiki/Pull_technology), the model serving system must make 
a request to the feature store to retrieve feature values, which can introduce latency. In a push model, the feature 
store pushes feature values to the online store, which reduces the latency of feature retrieval.

Pros:
- **Low Latency**: The feature store pushes feature values to the online store, which reduces the latency of feature retrieval.
- **Scalability**: The feature store can push feature values to multiple online stores, which allows for horizontal scaling.
- **Efficiency**: The feature store can push feature values to the online store in batches, which reduces the number of requests made by the model serving system.
- **Consistency**: The feature store can push feature values to the online store in a consistent manner, which ensures that the feature values are up-to-date.
- **Reliability**: The feature store can push feature values to the online store in a reliable manner, which ensures that the feature values are not lost.
- **Flexibility**: The feature store can push feature values to the online store in a flexible manner, which allows for different types of feature retrieval.
- **Security**: The feature store can push feature values to the online store in a secure manner, which ensures that the feature values are not compromised.
- **Cost-Effective**: The feature store can push feature values to the online store in a cost-effective manner, which reduces the cost of feature retrieval.
- **Ease of Use**: The feature store can push feature values to the online store in an easy-to-use manner, which simplifies the process of feature retrieval.

Cons:
- **Consistency**: Data consistency can be a challenge in a push model, as the feature store must ensure that the feature values are pushed to the online store in a consistent manner. 
- **Write Amplification**: The feature store must write feature values to the online store, which can result in write amplification.
- **Data Producer Ownership**: Data Producers own the push/writes to the Feature Store. This means more work on their part when things go wrong.