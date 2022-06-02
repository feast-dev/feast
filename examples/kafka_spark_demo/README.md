# Stream Feature View Demo with Kafka and Spark

This demo is based heavily on the feast spark kafka demo [here](https://github.com/feast-dev/feast-workshop/tree/main/module_1).

The Kafka docker container takes data from the feature repo and takes data(specifically the "miles_driven" column) and streams it as a kafka producer as fresher features.

The stream feature view will register this kafka topic and stream the fresher features and update the online store.