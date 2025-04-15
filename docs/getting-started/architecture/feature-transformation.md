# Feature Transformation

A *feature transformation* is a function that takes some set of input data and
returns some set of output data. Feature transformations can happen on either raw data or derived data.

## Feature Transformation Engines
Feature transformations can be executed by three types of "transformation engines":

1. The Feast Feature Server
2. An Offline Store (e.g., Snowflake, BigQuery, DuckDB, Spark, etc.)
3. [A Compute Engine](../../reference/compute-engine/README.md)

The three transformation engines are coupled with the [communication pattern used for writes](write-patterns.md).

Importantly, this implies that different feature transformation code may be 
used under different transformation engines, so understanding the tradeoffs of 
when to use which transformation engine/communication pattern is extremely critical to 
the success of your implementation.

In general, we recommend transformation engines and network calls to be chosen by aligning it with what is most 
appropriate for the data producer, feature/model usage, and overall product.