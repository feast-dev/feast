# Stream Processor

A Stream Processor is responsible for consuming data from stream sources (such as Kafka, Kinesis, etc.) and loading it directly into the online (and optionally the offline store).

A Stream Processor abstracts over specific technologies or frameworks that are used to materialize data. An experimental Spark Processor for Kafka is available in Feast. 

If the built-in processor is not sufficient, you can create your own custom processor. Please see [this tutorial](../../tutorials/building-streaming-features.md) for more details.

