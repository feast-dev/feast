# Adding a new online store

## Overview

Feast makes adding support for a new online store (database) easy. Developers can simply implement the [OnlineStore](../../sdk/python/feast/infra/online\_stores/online\_store.py#L26) interface to add support for a new store (other than the existing stores like Redis, DynamoDB, SQLite, and Datastore).&#x20;

In this guide, we will show you how to integrate with MySQL as an online store. While we will be implementing a specific store, this guide should be representative for adding support for any new online store.

The full working code for this guide can be found at [feast-dev/feast-custom-online-store-demo](https://github.com/feast-dev/feast-custom-online-store-demo) and an example of a custom online store that was contributed by developers can be found [here](https://github.com/feast-dev/feast/pull/2590).


The process of using a custom online store consists of 4 steps:

1. Defining the `OnlineStore` class.
2. Defining the `OnlineStoreConfig` class.
3. Referencing the `OnlineStore` in a feature repo's `feature_store.yaml` file.
4. Testing the `OnlineStore` class.

## 1. Defining an OnlineStore class

### Contrib

Generally, new online stores should go in the contrib folder and have alpha functionality tags. The contrib folder is designated for community contributions that are not fully maintained by Feast maintainers and may contain potential instability and have API changes. It is recommended to add warnings to users that the online store functionality is still in alpha development and is not fully stable. In order to be classified as fully stable and be moved to the main online store folder, the online store should integrate with the full integration test suite in Feast and pass all of the test cases.

{% hint style="info" %}
&#x20;OnlineStore class names must end with the OnlineStore suffix!
{% endhint %}

The OnlineStore class broadly contains two sets of methods

* One set deals with managing infrastructure that the online store needed for operations
* One set deals with writing data into the store, and reading data from the store.&#x20;

### 1.1 Infrastructure Methods

There are two methods that deal with managing infrastructure for online stores, `update` and `teardown`

* `update` is invoked when users run `feast apply` as a CLI command, or the `FeatureStore.apply()` sdk method.

The `update` method should be used to perform any operations necessary before data can be written to or read from the store. The `update` method can be used to create MySQL tables in preparation for reads and writes to new feature views.&#x20;

* `teardown` is invoked when users run `feast teardown` or `FeatureStore.teardown()`.

The `teardown` method should be used to perform any clean-up operations. `teardown` can be used to drop MySQL indices and tables corresponding to the feature views being deleted.&#x20;

{% code title="feast_custom_online_store/mysql.py" %}
```python
# Only prints out runtime warnings once.
warnings.simplefilter("once", RuntimeWarning)

def update(
    self,
    config: RepoConfig,
    tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
    tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
    entities_to_delete: Sequence[Entity],
    entities_to_keep: Sequence[Entity],
    partial: bool,
):
    """
    An example of creating manging the tables needed for a mysql-backed online store.
    """
    warnings.warn(
        "This online store is an experimental feature in alpha development. "
        "Some functionality may still be unstable so functionality can change in the future.",
        RuntimeWarning,
    )
    conn = self._get_conn(config)
    cur = conn.cursor(buffered=True)

    project = config.project

    for table in tables_to_keep:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key VARCHAR(512), feature_name VARCHAR(256), value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
        )
        cur.execute(
            f"CREATE INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)} (entity_key);"
        )

    for table in tables_to_delete:
        cur.execute(
            f"DROP INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)};"
        )
        cur.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")


def teardown(
    self,
    config: RepoConfig,
    tables: Sequence[Union[FeatureTable, FeatureView]],
    entities: Sequence[Entity],
):
    warnings.warn(
        "This online store is an experimental feature in alpha development. "
        "Some functionality may still be unstable so functionality can change in the future.",
        RuntimeWarning,
    )
    conn = self._get_conn(config)
    cur = conn.cursor(buffered=True)
    project = config.project

    for table in tables:
        cur.execute(
            f"DROP INDEX {_table_id(project, table)}_ek ON {_table_id(project, table)};"
        )
        cur.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")
```
{% endcode %}

### 1.2 Read/Write Methods

There are two methods that deal with writing data to and from the online stores.`online_write_batch `and `online_read`.

* `online_write_batch `is invoked when running materialization (using the `feast materialize` or `feast materialize-incremental` commands, or the corresponding `FeatureStore.materialize()` method.
* `online_read `is invoked when reading values from the online store using the `FeatureStore.get_online_features()` method.

{% code title="feast_custom_online_store/mysql.py" %}
```python
# Only prints out runtime warnings once.
warnings.simplefilter("once", RuntimeWarning)

def online_write_batch(
    self,
    config: RepoConfig,
    table: Union[FeatureTable, FeatureView],
    data: List[
        Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
    ],
    progress: Optional[Callable[[int], Any]],
) -> None:
    warnings.warn(
        "This online store is an experimental feature in alpha development. "
        "Some functionality may still be unstable so functionality can change in the future.",
        RuntimeWarning,
    )
    conn = self._get_conn(config)
    cur = conn.cursor(buffered=True)

    project = config.project

    for entity_key, values, timestamp, created_ts in data:
        entity_key_bin = serialize_entity_key(entity_key).hex()
        timestamp = _to_naive_utc(timestamp)
        if created_ts is not None:
            created_ts = _to_naive_utc(created_ts)

        for feature_name, val in values.items():
            self.write_to_table(created_ts, cur, entity_key_bin, feature_name, project, table, timestamp, val)
        self._conn.commit()
        if progress:
            progress(1)

def online_read(
    self,
    config: RepoConfig,
    table: Union[FeatureTable, FeatureView],
    entity_keys: List[EntityKeyProto],
    requested_features: Optional[List[str]] = None,
) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
    warnings.warn(
        "This online store is an experimental feature in alpha development. "
        "Some functionality may still be unstable so functionality can change in the future.",
        RuntimeWarning,
    )
    conn = self._get_conn(config)
    cur = conn.cursor(buffered=True)

    result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

    project = config.project
    for entity_key in entity_keys:
        entity_key_bin = serialize_entity_key(entity_key).hex()
        print(f"entity_key_bin: {entity_key_bin}")

        cur.execute(
            f"SELECT feature_name, value, event_ts FROM {_table_id(project, table)} WHERE entity_key = %s",
            (entity_key_bin,),
        )

        res = {}
        res_ts = None
        for feature_name, val_bin, ts in cur.fetchall():
            val = ValueProto()
            val.ParseFromString(val_bin)
            res[feature_name] = val
            res_ts = ts

        if not res:
            result.append((None, None))
        else:
            result.append((res_ts, res))
    return result
```
{% endcode %}

## 2. Defining an OnlineStoreConfig class

Additional configuration may be needed to allow the OnlineStore to talk to the backing store. For example, MySQL may need configuration information like the host at which the MySQL instance is running, credentials for connecting to the database, etc.

To facilitate configuration, all OnlineStore implementations are **required** to also define a corresponding OnlineStoreConfig class in the same file. This OnlineStoreConfig class should inherit from the `FeastConfigBaseModel` class, which is defined [here](../../sdk/python/feast/repo\_config.py#L44).&#x20;

The `FeastConfigBaseModel` is a [pydantic](https://pydantic-docs.helpmanual.io) class, which parses yaml configuration into python objects. Pydantic also allows the model classes to define validators for the config classes, to make sure that the config classes are correctly defined.

This config class **must** container a `type` field, which contains the fully qualified class name of its corresponding OnlineStore class.&#x20;

Additionally, the name of the config class must be the same as the OnlineStore class, with the `Config` suffix.

An example of the config class for MySQL :

{% code title="feast_custom_online_store/mysql.py" %}
```python
class MySQLOnlineStoreConfig(FeastConfigBaseModel):
    type: Literal["feast_custom_online_store.mysql.MySQLOnlineStore"] = "feast_custom_online_store.mysql.MySQLOnlineStore"

    host: Optional[StrictStr] = None
    user: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
```
{% endcode %}

This configuration can be specified in the `feature_store.yaml` as follows:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
online_store:
    type: feast_custom_online_store.mysql.MySQLOnlineStore
    user: foo
    password: bar
```
{% endcode %}

This configuration information is available to the methods of the OnlineStore, via the`config: RepoConfig` parameter which is passed into all the methods of the OnlineStore interface, specifically at the `config.online_store` field of the `config` parameter.&#x20;

{% code title="feast_custom_online_store/mysql.py" %}
```python
def online_write_batch(
        self,
        config: RepoConfig,
        table: Union[FeatureTable, FeatureView],
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
) -> None:

    online_store_config = config.online_store
    assert isinstance(online_store_config, MySQLOnlineStoreConfig)

    connection = mysql.connector.connect(
        host=online_store_config.host or "127.0.0.1",
        user=online_store_config.user or "root",
        password=online_store_config.password,
        database=online_store_config.database or "feast",
        autocommit=True
    )
```
{% endcode %}

## 3. Using the custom online store&#x20;

After implementing both these classes, the custom online store can be used by referencing it in a feature repo's `feature_store.yaml` file, specifically in the `online_store` field. The value specified should be the fully qualified class name of the OnlineStore.&#x20;

As long as your OnlineStore class is available in your Python environment, it will be imported by Feast dynamically at runtime.

To use our MySQL online store, we can use the following `feature_store.yaml`:

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
online_store:
    # Make sure to specify the type as the fully qualified path that Feast can import.
    type: feast_custom_online_store.mysql.MySQLOnlineStore
    user: foo
    password: bar
```
{% endcode %}

If additional configuration for the online store is **not **required, then we can omit the other fields and only specify the `type` of the online store class as the value for the `online_store`.

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
online_store: feast_custom_online_store.mysql.MySQLOnlineStore
```
{% endcode %}

## 4. Testing the OnlineStore class

### Integrating with the integration test suite and unit test suite.

Even if you have created the `OnlineStore` class in a separate repo, you can still test your implementation against the Feast test suite, as long as you have Feast as a submodule in your repo. In the Feast submodule, we can run all the unit tests with:

```
make test-python
```

This should run the unit tests and the unit tests should all pass. Please add unit tests for your online store that test out basic functionality of reading and writing to and from the online store. This should just be class level functionality that ensures that the methods you implemented for the OnlineStore work as expected. In order to be approved to merge into Feast, these unit tests should all pass and demonstrate that the DataSource works as intended.

The universal tests, which are integration tests specifically intended to test offline and online stores, will be run against Feast to ensure that the Feast APIs works with your online store. To run the integration tests, you must parametrize the integration test suite based on the `FULL_REPO_CONFIGS` variable defined in `sdk/python/tests/integration/feature_repos/repo_configuration.py` to use your own custom online store. To overwrite these configurations, you can simply create your own file that contains a `FULL_REPO_CONFIGS`, and point Feast to that file by setting the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file. In this repo, the file that overwrites `FULL_REPO_CONFIGS` is `feast_custom_online_store/feast_tests.py`, so you would run

```
export FULL_REPO_CONFIGS_MODULE='feast_custom_online_store.feast_tests'
make test-python-universal
```

A sample `FULL_REPO_CONFIGS_MODULE` looks something like this:

```
from feast.infra.offline_stores.contrib.postgres_offline_store.tests.data_source import (
    PostgreSQLDataSourceCreator,
)

AVAILABLE_ONLINE_STORES = {"postgres": (None, PostgreSQLDataSourceCreator)}
```

to test the MySQL online store against the Feast universal tests. If there are some tests that fail, this indicates that there is a mistake in the implementation of this online store! If you are planning to start the online store up locally(e.g spin up a local Redis Instance), then the dictionary entry should be something like:

```
{
    "sqlite": ({"type": "sqlite"}, None),
}
```

The key is the name of the online store and the tuple contains the online store config represented as a dictionary. Make sure that the sqlite config defines the full package so that Feast can import the online store dynamically at runtime. The None type in the tuple can be replaced by an `OnlineStoreCreator` which is only useful for specific online stores that have containerized docker images. If you create a containerized docker image for testing, developers who are trying to test with your online store will not have to spin up their own instance of the online store(e.g a redis instance or dynamo instance) in order to perform testing. An example of an `OnlineStoreCreator` is shown below:

```
class RedisOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("redis").with_exposed_ports("6379")

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Ready to accept connections"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port("6379")
        return {"type": "redis", "connection_string": f"localhost:{exposed_port},db=0"}

    def teardown(self):
        self.container.stop()
```


### Add Dependencies

Finally, if your online store requires special packages, add them to our `sdk/python/setup.py` under a new `<ONLINE>_STORE_REQUIRED` list with the packages and add it to the setup script so that if your online store is needed, users can install the necessary python packages. These packages should be defined as extras so that they are not installed by users by default.
You will need to regenerate our requirements files. To do this, create separate pyenv environments for python 3.8, 3.9, and 3.10. In each environment, run the following commands:

```
export PYTHON=<version>
make lock-python-ci-dependencies
```


### Add Documentation

Remember to add the documentation for your online store. This should be added to `docs/reference/online-stores/`. You should also add a reference in `docs/reference/online-stores/README.md` and `docs/SUMMARY.md`. Add a new markdown documentation and document the functions similar to how the other online stores are documented. Be sure to cover how to create the datasource and most importantly, what configuration is needed in the `feature_store.yaml` file in order to create the datasource and also make sure to flag that the online store is in alpha development.

Finally, add the python code docs by running to document the functions:

```
make build-sphinx
```

An example of a full pull request for adding a custom online store can be found [here](https://github.com/feast-dev/feast/pull/2590).