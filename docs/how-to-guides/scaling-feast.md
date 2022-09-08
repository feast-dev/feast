# Scaling Feast

## Overview

Feast is designed to be easy to use and understand out of the box, with as few infrastructure dependencies as possible. However, there are components used by default that may not scale well. 
Since Feast is designed to be modular, it's possible to swap such components with more performant components, at the cost of Feast depending on additional infrastructure.


### Scaling Feast Registry

The default Feast [registry](../getting-started/concepts/registry.md) is a file-based registry. Any changes to the feature repo, or materializing data into the online store, results in a mutation to the registry.

However, there are inherent limitations with a file-based registry, since changing a single field in the registry requires re-writing the whole registry file.
With multiple concurrent writers, this presents a risk of data loss, or bottlenecks writes to the registry since all changes have to be serialized (e.g. when running materialization for multiple feature views or time ranges concurrently).

The recommended solution in this case is to use the [SQL based registry](../tutorials/using-scalable-registry.md), which allows concurrent, transactional, and fine-grained updates to the registry. This registry implementation requires access to an existing database (such as MySQL, Postgres, etc).

### Scaling Materialization

The default Feast materialization process is an in-memory process, which pulls data from the offline store before writing it to the online store.
However, this process does not scale for large data sets, since it's executed on a single-process.

Feast supports pluggable [Materialization Engines](../getting-started/architecture-and-components/batch-materialization-engine.md), that allow the materialization process to be scaled up.
Aside from the local process, Feast supports a [Lambda-based materialization engine](https://rtd.feast.dev/en/master/#alpha-lambda-based-engine), and a [Bytewax-based materialization engine](https://rtd.feast.dev/en/master/#bytewax-engine).

Users may also be able to build an engine to scale up materialization using existing infrastructure in their organizations.