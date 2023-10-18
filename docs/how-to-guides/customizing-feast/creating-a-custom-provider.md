# Adding a custom provider

### Overview

All Feast operations execute through a `provider`. Operations like materializing data from the offline to the online store, updating infrastructure like databases, launching streaming ingestion jobs, building training datasets, and reading features from the online store.

Custom providers allow Feast users to extend Feast to execute any custom logic. Examples include:

* Launching custom streaming ingestion jobs (Spark, Beam)
* Launching custom batch ingestion (materialization) jobs (Spark, Beam)
* Adding custom validation to feature repositories during `feast apply`
* Adding custom infrastructure setup logic which runs during `feast apply`
* Extending Feast commands with in-house metrics, logging, or tracing

Feast comes with built-in providers, e.g, `LocalProvider`, `GcpProvider`, and `AwsProvider`. However, users can develop their own providers by creating a class that implements the contract in the [Provider class](https://github.com/feast-dev/feast/blob/745a1b43d20c0169b675b1f28039854205fb8180/sdk/python/feast/infra/provider.py#L22).

{% hint style="info" %}
This guide also comes with a fully functional [custom provider demo repository](https://github.com/feast-dev/feast-custom-provider-demo). Please have a look at the repository for a representative example of what a custom provider looks like, or fork the repository when creating your own provider.
{% endhint %}

### Guide

The fastest way to add custom logic to Feast is to extend an existing provider. The most generic provider is the `LocalProvider` which contains no cloud-specific logic. The guide that follows will extend the `LocalProvider` with operations that print text to the console. It is up to you as a developer to add your custom code to the provider methods, but the guide below will provide the necessary scaffolding to get you started.

#### Step 1: Define a Provider class

The first step is to define a custom provider class. We've created the `MyCustomProvider` below.

```python
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from feast.entity import Entity
from feast.feature_table import FeatureTable
from feast.feature_view import FeatureView
from feast.infra.local import LocalProvider
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.infra.registry.registry import Registry
from feast.repo_config import RepoConfig


class MyCustomProvider(LocalProvider):
    def __init__(self, config: RepoConfig, repo_path):
        super().__init__(config)
        # Add your custom init code here. This code runs on every Feast operation.

    def update_infra(
        self,
        project: str,
        tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
        tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        super().update_infra(
            project,
            tables_to_delete,
            tables_to_keep,
            entities_to_delete,
            entities_to_keep,
            partial,
        )
        print("Launching custom streaming jobs is pretty easy...")

    def materialize_single_feature_view(
        self,
        config: RepoConfig,
        feature_view: FeatureView,
        start_date: datetime,
        end_date: datetime,
        registry: Registry,
        project: str,
        tqdm_builder: Callable[[int], tqdm],
    ) -> None:
        super().materialize_single_feature_view(
            config, feature_view, start_date, end_date, registry, project, tqdm_builder
        )
        print("Launching custom batch jobs is pretty easy...")
```

Notice how in the above provider we have only overwritten two of the methods on the `LocalProvider`, namely `update_infra` and `materialize_single_feature_view`. These two methods are convenient to replace if you are planning to launch custom batch or streaming jobs. `update_infra` can be used for launching idempotent streaming jobs, and `materialize_single_feature_view` can be used for launching batch ingestion jobs.

It is possible to overwrite all the methods on the provider class. In fact, it isn't even necessary to subclass an existing provider like `LocalProvider`. The only requirement for the provider class is that it follows the [Provider contract](https://github.com/feast-dev/feast/blob/048c837b2fa741b38b0e35b8f8e534761a232561/sdk/python/feast/infra/provider.py#L22).

#### Step 2: Configuring Feast to use the provider

Configure your [feature\_store.yaml](../../reference/feature-repository/feature-store-yaml.md) file to point to your new provider class:

```yaml
project: repo
registry: registry.db
provider: feast_custom_provider.custom_provider.MyCustomProvider
online_store:
    type: sqlite
    path: online_store.db
offline_store:
    type: file
```

Notice how the `provider` field above points to the module and class where your provider can be found.

#### Step 3: Using the provider

Now you should be able to use your provider by running a Feast command:

```bash
feast apply
```

```
Registered entity driver_id
Registered feature view driver_hourly_stats
Deploying infrastructure for driver_hourly_stats
Launching custom streaming jobs is pretty easy...
```

It may also be necessary to add the module root path to your `PYTHONPATH` as follows:

```bash
PYTHONPATH=$PYTHONPATH:/home/my_user/my_custom_provider feast apply
```

That's it. You should now have a fully functional custom provider!

### Next steps

Have a look at the [custom provider demo repository](https://github.com/feast-dev/feast-custom-provider-demo) for a fully functional example of a custom provider. Feel free to fork it when creating your own custom provider!
