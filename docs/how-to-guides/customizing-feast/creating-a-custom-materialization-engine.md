# Adding a custom batch materialization engine

### Overview

Feast batch materialization operations (`materialize` and `materialize-incremental`) execute through a `BatchMaterializationEngine`.

Custom batch materialization engines allow Feast users to extend Feast to customize the materialization process. Examples include:

* Setting up custom materialization-specific infrastructure during `feast apply` (e.g. setting up Spark clusters or Lambda Functions)
* Launching custom batch ingestion (materialization) jobs (Spark, Beam, AWS Lambda)
* Tearing down custom materialization-specific infrastructure during `feast teardown` (e.g. tearing down Spark clusters, or deleting Lambda Functions)

Feast comes with built-in materialization engines, e.g, `LocalMaterializationEngine`, and an experimental `LambdaMaterializationEngine`. However, users can develop their own materialization engines by creating a class that implements the contract in the [BatchMaterializationEngine class](https://github.com/feast-dev/feast/blob/6d7b38a39024b7301c499c20cf4e7aef6137c47c/sdk/python/feast/infra/materialization/batch\_materialization\_engine.py#L72).

### Guide

The fastest way to add custom logic to Feast is to extend an existing materialization engine. The most generic engine is the `LocalMaterializationEngine` which contains no cloud-specific logic. The guide that follows will extend the `LocalProvider` with operations that print text to the console. It is up to you as a developer to add your custom code to the engine methods, but the guide below will provide the necessary scaffolding to get you started.

#### Step 1: Define an Engine class

The first step is to define a custom materialization engine class. We've created the `MyCustomEngine` below.

```python
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.infra.materialization import LocalMaterializationEngine, LocalMaterializationJob, MaterializationTask
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig


class MyCustomEngine(LocalMaterializationEngine):
    def __init__(
            self,
            *,
            repo_config: RepoConfig,
            offline_store: OfflineStore,
            online_store: OnlineStore,
            **kwargs,
    ):
        super().__init__(
            repo_config=repo_config,
            offline_store=offline_store,
            online_store=online_store,
            **kwargs,
        )

    def update(
            self,
            project: str,
            views_to_delete: Sequence[
                Union[BatchFeatureView, StreamFeatureView, FeatureView]
            ],
            views_to_keep: Sequence[
                Union[BatchFeatureView, StreamFeatureView, FeatureView]
            ],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
    ):
        print("Creating new infrastructure is easy here!")
        pass

    def materialize(
        self, registry, tasks: List[MaterializationTask]
    ) -> List[LocalMaterializationJob]:
        print("Launching custom batch jobs or multithreading things is pretty easy...")
        return [
            self._materialize_one(
                registry,
                task.feature_view,
                task.start_time,
                task.end_time,
                task.project,
                task.tqdm_builder,
            )
            for task in tasks
        ]
```

Notice how in the above engine we have only overwritten two of the methods on the `LocalMaterializatinEngine`, namely `update` and `materialize`. These two methods are convenient to replace if you are planning to launch custom batch jobs.

#### Step 2: Configuring Feast to use the engine

Configure your [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md) file to point to your new engine class:

```yaml
project: repo
registry: registry.db
batch_engine: feast_custom_engine.MyCustomEngine
online_store:
    type: sqlite
    path: online_store.db
offline_store:
    type: file
```

Notice how the `batch_engine` field above points to the module and class where your engine can be found.

#### Step 3: Using the engine

Now you should be able to use your engine by running a Feast command:

```bash
feast apply
```

```
Registered entity driver_id
Registered feature view driver_hourly_stats
Deploying infrastructure for driver_hourly_stats
Creating new infrastructure is easy here!
```

It may also be necessary to add the module root path to your `PYTHONPATH` as follows:

```bash
PYTHONPATH=$PYTHONPATH:/home/my_user/my_custom_engine feast apply
```

That's it. You should now have a fully functional custom engine!
