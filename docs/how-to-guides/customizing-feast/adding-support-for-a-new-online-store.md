# Adding a new online store

## Overview

Feast makes adding support for a new online store (database) easy. Developers can simply implement the [OnlineStore](../../../sdk/python/feast/infra/online\_stores/online\_store.py#L26) interface to add support for a new store (other than the existing stores like Redis, DynamoDB, SQLite, and Datastore).

In this guide, we will show you how to integrate with MySQL as an online store. While we will be implementing a specific store, this guide should be representative for adding support for any new online store.

The full working code for this guide can be found at [feast-dev/feast-custom-online-store-demo](https://github.com/feast-dev/feast-custom-online-store-demo).

The process of using a custom online store consists of 6 steps:

1. Defining the `OnlineStore` class.
2. Defining the `OnlineStoreConfig` class.
3. Referencing the `OnlineStore` in a feature repo's `feature_store.yaml` file.
4. Testing the `OnlineStore` class.
5. Update dependencies.
6. Add documentation.

## 1. Defining an OnlineStore class

{% hint style="info" %}
OnlineStore class names must end with the OnlineStore suffix!
{% endhint %}

### Contrib online stores

New online stores go in `sdk/python/feast/infra/online_stores/`.

#### What is a contrib plugin?

* Not guaranteed to implement all interface methods
* Not guaranteed to be stable.
* Should have warnings for users to indicate this is a contrib plugin that is not maintained by the maintainers.

#### How do I make a contrib plugin an "official" plugin?

To move an online store plugin out of contrib, you need:

* GitHub actions (i.e `make test-python-integration`) is setup to run all tests against the online store and pass.
* At least two contributors own the plugin (ideally tracked in our `OWNERS` / `CODEOWNERS` file).

The OnlineStore class broadly contains two sets of methods

* One set deals with managing infrastructure that the online store needed for operations
* One set deals with writing data into the store, and reading data from the store.

### 1.1 Infrastructure Methods

There are two methods that deal with managing infrastructure for online stores, `update` and `teardown`

* `update` is invoked when users run `feast apply` as a CLI command, or the `FeatureStore.apply()` sdk method.

The `update` method should be used to perform any operations necessary before data can be written to or read from the store. The `update` method can be used to create MySQL tables in preparation for reads and writes to new feature views.

* `teardown` is invoked when users run `feast teardown` or `FeatureStore.teardown()`.

The `teardown` method should be used to perform any clean-up operations. `teardown` can be used to drop MySQL indices and tables corresponding to the feature views being deleted.

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
    An example of creating managing the tables needed for a mysql-backed online store.
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

There are two methods that deal with writing data to and from the online stores.`online_write_batch` and `online_read`.

* `online_write_batch` is invoked when running materialization (using the `feast materialize` or `feast materialize-incremental` commands, or the corresponding `FeatureStore.materialize()` method.
* `online_read` is invoked when reading values from the online store using the `FeatureStore.get_online_features()` method.

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
        entity_key_bin = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        ).hex()
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
        entity_key_bin = serialize_entity_key(
            entity_key,
            entity_key_serialization_version=config.entity_key_serialization_version,
        ).hex()
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

To facilitate configuration, all OnlineStore implementations are **required** to also define a corresponding OnlineStoreConfig class in the same file. This OnlineStoreConfig class should inherit from the `FeastConfigBaseModel` class, which is defined [here](../../../sdk/python/feast/repo\_config.py#L44).

The `FeastConfigBaseModel` is a [pydantic](https://pydantic-docs.helpmanual.io) class, which parses yaml configuration into python objects. Pydantic also allows the model classes to define validators for the config classes, to make sure that the config classes are correctly defined.

This config class **must** container a `type` field, which contains the fully qualified class name of its corresponding OnlineStore class.

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

This configuration information is available to the methods of the OnlineStore, via the`config: RepoConfig` parameter which is passed into all the methods of the OnlineStore interface, specifically at the `config.online_store` field of the `config` parameter.

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

## 3. Using the custom online store

After implementing both these classes, the custom online store can be used by referencing it in a feature repo's `feature_store.yaml` file, specifically in the `online_store` field. The value specified should be the fully qualified class name of the OnlineStore.

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

If additional configuration for the online store is \*\*not \*\*required, then we can omit the other fields and only specify the `type` of the online store class as the value for the `online_store`.

{% code title="feature_repo/feature_store.yaml" %}
```yaml
project: test_custom
registry: data/registry.db
provider: local
online_store: feast_custom_online_store.mysql.MySQLOnlineStore
```
{% endcode %}

## 4. Testing the OnlineStore class

### 4.1 Integrating with the integration test suite and unit test suite.

Even if you have created the `OnlineStore` class in a separate repo, you can still test your implementation against the Feast test suite, as long as you have Feast as a submodule in your repo.

1.  In the Feast submodule, we can run all the unit tests and make sure they pass:

    ```
    make test-python-unit
    ```
2. The universal tests, which are integration tests specifically intended to test offline and online stores, should be run against Feast to ensure that the Feast APIs works with your online store.
   * Feast parametrizes integration tests using the `FULL_REPO_CONFIGS` variable defined in `sdk/python/tests/integration/feature_repos/repo_configuration.py` which stores different online store classes for testing.
   * To overwrite these configurations, you can simply create your own file that contains a `FULL_REPO_CONFIGS` variable, and point Feast to that file by setting the environment variable `FULL_REPO_CONFIGS_MODULE` to point to that file.

A sample `FULL_REPO_CONFIGS_MODULE` looks something like this:

{% code title="sdk/python/feast/infra/online_stores/contrib/postgres_repo_configuration.py" %}
```python
from feast.infra.offline_stores.contrib.postgres_offline_store.tests.data_source import (
    PostgreSQLDataSourceCreator,
)

AVAILABLE_ONLINE_STORES = {"postgres": (None, PostgreSQLDataSourceCreator)}
```
{% endcode %}

If you are planning to start the online store up locally(e.g spin up a local Redis Instance) for testing, then the dictionary entry should be something like:

```python
{
    "sqlite": ({"type": "sqlite"}, None),
    # Specifies sqlite as the online store. The `None` object specifies to not use a containerized docker container.
}
```

If you are planning instead to use a Dockerized container to run your tests against your online store, you can define a `OnlineStoreCreator` and replace the `None` object above with your `OnlineStoreCreator` class. You should make this class available to pytest through the `PYTEST_PLUGINS` environment variable.

If you create a containerized docker image for testing, developers who are trying to test with your online store will not have to spin up their own instance of the online store for testing. An example of an `OnlineStoreCreator` is shown below:

{% code title="sdk/python/tests/integration/feature_repos/universal/online_store/redis.py" %}
```python
class RedisOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Ready to accept connections"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        self.container.stop()
```
{% endcode %}

3\. Add a Makefile target to the Makefile to run your datastore specific tests by setting the `FULL_REPO_CONFIGS_MODULE` environment variable. Add `PYTEST_PLUGINS` if pytest is having trouble loading your `DataSourceCreator`. You can remove certain tests that are not relevant or still do not work for your datastore using the `-k` option.

{% code title="Makefile" %}
```Makefile
test-python-universal-cassandra:
	PYTHONPATH='.' \
	FULL_REPO_CONFIGS_MODULE=sdk.python.feast.infra.online_stores.cassandra_online_store.cassandra_repo_configuration \
	PYTEST_PLUGINS=sdk.python.tests.integration.feature_repos.universal.online_store.cassandra \
	IS_TEST=True \
	python -m pytest -x --integration \
	sdk/python/tests
```
{% endcode %}

* If there are some tests that fail, this indicates that there is a mistake in the implementation of this online store!

### 5. Add Dependencies

Add any dependencies for your online store to our `sdk/python/setup.py` under a new `<ONLINE_STORE>_REQUIRED` list with the packages and add it to the setup script so that if your online store is needed, users can install the necessary python packages. These packages should be defined as extras so that they are not installed by users by default.

* You will need to regenerate our requirements files. To do this, create separate pyenv environments for python 3.10, 3.11, and 3.12. In each environment, run the following commands:

```
export PYTHON=<version>
make lock-python-ci-dependencies
```

### 6. Add Documentation

Remember to add the documentation for your online store.

1. Add a new markdown file to `docs/reference/online-stores/`.
2. You should also add a reference in `docs/reference/online-stores/README.md` and `docs/SUMMARY.md`. Add a new markdown document to document your online store functionality similar to how the other online stores are documented.

**NOTE**:Be sure to document the following things about your online store:

* Be sure to cover how to create the datasource and what configuration is needed in the `feature_store.yaml` file in order to create the datasource.
* Make sure to flag that the online store is in alpha development.
* Add some documentation on what the data model is for the specific online store for more clarity.
* Finally, generate the python code docs by running:

```bash
make build-sphinx
```
