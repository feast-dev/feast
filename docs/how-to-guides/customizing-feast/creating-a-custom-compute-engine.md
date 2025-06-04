# Adding a custom compute engine

### Overview

Feast batch materialization operations (`materialize` and `materialize-incremental`), and get_historical_features are executed through a `ComputeEngine`.

Custom batch compute engines allow Feast users to extend Feast to customize the materialization and get_historical_features process. Examples include:

* Setting up custom materialization-specific infrastructure during `feast apply` (e.g. setting up Spark clusters or Lambda Functions)
* Launching custom batch ingestion (materialization) jobs (Spark, Beam, AWS Lambda)
* Tearing down custom materialization-specific infrastructure during `feast teardown` (e.g. tearing down Spark clusters, or deleting Lambda Functions)

Feast comes with built-in materialization engines, e.g, `LocalComputeEngine`, and an experimental `LambdaComputeEngine`. However, users can develop their own compute engines by creating a class that implements the contract in the [ComputeEngine class](https://github.com/feast-dev/feast/blob/85514edbb181df083e6a0d24672c00f0624dcaa3/sdk/python/feast/infra/compute_engines/base.py#L19).

### Guide

The fastest way to add custom logic to Feast is to implement the ComputeEngine. The guide that follows will extend the `LocalProvider` with operations that print text to the console. It is up to you as a developer to add your custom code to the engine methods, but the guide below will provide the necessary scaffolding to get you started.

#### Step 1: Define an Engine class

The first step is to define a custom compute engine class. We've created the `MyCustomEngine` below. This python file can be placed in your `feature_repo` directory if you're following the Quickstart guide.

```python
from typing import List, Sequence, Union

from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.compute_engines.local.job import LocalMaterializationJob
from feast.infra.compute_engines.base import ComputeEngine 
from feast.infra.common.materialization_job import MaterializationTask
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.online_stores.online_store import OnlineStore
from feast.repo_config import RepoConfig


class MyCustomEngine(ComputeEngine):
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

    def get_historical_features(self, task: HistoricalRetrievalTask) -> RetrievalJob:
        raise NotImplementedError
```

Notice how in the above engine we have only overwritten two of the methods on the `LocalComputeEngine`, namely `update` and `materialize`. These two methods are convenient to replace if you are planning to launch custom batch jobs.
If you want to use the compute to execute the get_historical_features method, you will need to implement the `get_historical_features` method as well.

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
