# Building streaming features

Feast supports registering streaming feature views and Kafka and Kinesis streaming sources. It also provides an interface for stream processing called the `Stream Processor`. An example Kafka/Spark StreamProcessor is implemented in the contrib folder. For more details, please see the [RFC](https://docs.google.com/document/d/1UzEyETHUaGpn0ap4G82DHluiCj7zEbrQLkJJkKSv4e8/edit?usp=sharing) for more details.

Please see [here](https://github.com/feast-dev/streaming-tutorial) for a tutorial on how to build a versioned streaming pipeline that registers your transformations, features, and data sources in Feast.
