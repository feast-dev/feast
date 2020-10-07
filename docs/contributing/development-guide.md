# Development Guide

## 1 Overview

The following guide will help you quickly run Feast in your local machine.

The main components of Feast are:

* **Feast Core:** Handles feature registration and ensures that Feast internal metadata is consistent.
* **Feast Job Controller:** Starts and manages ingestion jobs.
* **Feast Ingestion Jobs:** Subscribes to streams of FeatureRows and writes these as feature values to registered databases \(online, historical\) that can be read by Feast Serving.
* **Feast Serving:** Service that handles requests for features values, either online or batch.

## **2 Requirements**

### **2.1 Development environment**

The following software is required for Feast development

* Java SE Development Kit 11
* Python version 3.6 \(or above\) and pip
* [Maven](https://maven.apache.org/install.html) version 3.6.x

Additionally, [grpc\_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) is useful for debugging and quick testing of gRPC endpoints.

### **2.2 Services**

The following components/services are required to develop Feast:

* **Feast Core:** Requires PostgreSQL \(version 11 and above\) to store state, and requires a Kafka \(tested on version 2.x\) setup to allow for ingestion of FeatureRows.
* **Feast Serving:** Requires Redis \(tested on version 5.x\).

These services should be running before starting development. The following snippet will start the services using Docker:

```bash
# Start Postgres
docker run --name postgres --rm -it -d --net host -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=password postgres:12-alpine

# Start Redis
docker run --name redis --rm -it --net host -d redis:5-alpine

# Start Zookeeper (needed by Kafka)
docker run --rm \
  --net=host \
  --name=zookeeper \
  --env=ZOOKEEPER_CLIENT_PORT=2181 \
  --detach confluentinc/cp-zookeeper:5.2.1

# Start Kafka
docker run --rm \
  --net=host \
  --name=kafka \
  --env=KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
  --env=KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  --env=KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --detach confluentinc/cp-kafka:5.2.1
```

## 3 Testing and development

### 3.1 Running unit tests

```text
$ mvn test
```

### 3.2 Running integration tests

```text
$ mvn verify
```

### 3.3 Running components locally

The `core` and `serving` modules are Spring Boot applications. These may be run as usual for the Spring Boot Maven plugin:

```text
$ mvn --projects core spring-boot:run

# Or for short:
$ mvn -pl core spring-boot:run
```

Note that you should execute `mvn` from the Feast repository root directory, as there are intermodule dependencies that Maven will not resolve if you `cd` to subdirectories to run.

### 3.4 Running from IntelliJ

Compiling and running tests in IntelliJ should work as usual.

Running the Spring Boot apps may work out of the box in IDEA Ultimate, which has built-in support for Spring Boot projects, but the Community Edition needs a bit of help:

The Spring Boot Maven plugin automatically puts dependencies with `provided` scope on the runtime classpath when using `spring-boot:run`, such as its embedded Tomcat server. The "Play" buttons in the gutter or right-click menu of a `main()` method [do not do this](https://stackoverflow.com/questions/30237768/run-spring-boots-main-using-ide).

A solution to this is:

1. Open `View > Tool Windows > Maven`
2. Drill down to e.g. `Feast Core > Plugins > spring-boot:run`, right-click and `Create 'feast-core [spring-boot'…`
3. In the dialog that pops up, check the `Resolve Workspace artifacts` box
4. Click `OK`. You should now be able to select this run configuration for the Play button in the main toolbar, keyboard shortcuts, etc.

## **4** Validating your setup

The following section is a quick walk-through to test whether your local Feast deployment is functional for development purposes.

**4.1 Assumptions**

* PostgreSQL is running in `localhost:5432` and has a database called `postgres` which

  can be accessed with credentials user `postgres` and password `password`. Different database configurations can be supplied here via Feast Core and Job Controllers' `application.yml`.

* Redis is running locally and accessible from `localhost:6379`
* \(optional\) The local environment has been authentication with Google Cloud Platform and has full access to BigQuery. This is only necessary for BigQuery testing/development.

### 4.2 Clone Feast

```bash
git clone https://github.com/feast-dev/feast.git && cd feast && \
export FEAST_REPO=$(pwd)
```

Compile and package Feast components

```text
mvn package -Dmaven.test.skip=true
```

### 4.3 Starting Feast Core

To run Feast Core locally:

```bash
# Feast Core can be configured from the following .yml file
# $FEAST_REPO/core/src/main/resources/application.yml
java -jar core/target/feast-core-0.7.1-exec.jar
```

Test whether Feast Core is running

```text
grpc_cli call localhost:6565 ListStores ''
```

The output should list **no** stores since no Feast Serving has registered its stores to Feast Core:

```text
connecting to localhost:6565

Rpc succeeded with OK status
```

### 4.4 Starting Feast Job Controller

To run Feast Job Controller locally:

```bash
# Feast Job Controller can be configured from the following .yml file
# $FEAST_REPO/job-controller/src/main/resources/application.yml
java -jar job-controller/target/feast-job-controller-0.7.1-exec.jar
```

Test whether Feast Job Controller is running:

```text
grpc_cli call localhost:6570 ListIngestionJobs ''
```

The output should list **no** Ingestion Jobs since no Feature Sets have been registered yet

```text
connecting to localhost:6570

Rpc succeeded with OK status
```

### 4.5 Starting Feast Serving

Feast Serving is configured through the `$FEAST_REPO/serving/src/main/resources/application.yml`. Each Serving deployment must be configured with a store. Serving uses the store specified by native by `active-store` in `application.yml`

```text
# Indicates the active store. Only a single store in the last can be active at one time.
active_store: online
```

The default store `online` configured is Redis \(used for online serving\):

```text
  stores:
    - name: online # Name of the store (referenced by active_store)
      type: REDIS 
      config:  # store specific config. In this case specifies the host:port of redis.
        host: localhost
        port: 6379
     # ......
```

Once Feast Serving is started, it will register its store with Feast Core \(by name\) and start to subscribe to a feature sets based on its subscription.

Start Feast Serving server on localhost:6566:

```text
java -jar serving/target/feast-serving-0.7.1-exec.jar
```

Test connectivity to Feast Serving

```text
grpc_cli call localhost:6566 GetFeastServingInfo ''
```

```text
connecting to localhost:6566
version: "0.7.1"
type: FEAST_SERVING_TYPE_ONLINE

Rpc succeeded with OK status
```

Test Feast Core to see whether it is aware of the Feast Serving deployment

```text
grpc_cli call localhost:6565 ListStores ''
```

```text
connecting to localhost:6565
store {
  name: "online"
  type: REDIS
  subscriptions {
    name: "*"
    version: "*"
    project: "*"
  }
  redis_config {
    host: "localhost"
    port: 6379
  }
}

Rpc succeeded with OK status
```

In order to use BigQuery as a historical store, it is necessary to start Feast Serving with a different store configured. To do this we switch stores using the `active-store` property:

```text
active_store: historical
```

Configure the required BigQuery store properties:

```text
stores: 
- name: historical
  type: BIGQUERY
  config:
    # GCP Project
    project_id: my_project
    # BigQuery Dataset Id
    dataset_id: my_dataset
    # staging-location specifies the URI to store intermediate files for batch serving.
    # Feast Serving client is expected to have read access to this staging location
    # to download the batch features.
    # For example: gs://mybucket/myprefix
    # Please omit the trailing slash in the URI.
    staging_location: gs://mybucket/myprefix
# ..........
```

After making these changes, restart Feast Serving:

```text
mvn --projects serving spring-boot:run
```

You should see two stores registered:

```text
store {
  name: "online"
  type: REDIS
  subscriptions {
    name: "*"
    version: "*"
    project: "*"
  }
  redis_config {
    host: "localhost"
    port: 6379
  }
}
store {
  name: "historical"
  type: BIGQUERY
  subscriptions {
    name: "*"
    version: "*"
    project: "*"
  }
  bigquery_config {
    project_id: "my_project"
    dataset_id: "my_dataset"
  }
}
```

### 4.6 Registering a FeatureSet

Create a new FeatureSet in Feast by sending a request to Feast Core:

```text
# Example of registering a new driver feature set
# Note the source value, it assumes that you have access to a Kafka broker
# running on localhost:9092

grpc_cli call localhost:6565 ApplyFeatureSet '
feature_set {
  spec {
    project: "your_project_name"
    name: "driver"
    version: 1

    entities {
      name: "driver_id"
      value_type: INT64
    }

    features {
      name: "city"
      value_type: STRING
    }

    source {
      type: KAFKA
      kafka_source_config {
        bootstrap_servers: "localhost:9092"
        topic: "your-kafka-topic"
      }
    }
  }
}
'
```

Verify that the FeatureSet has been registered correctly.

```text
# To check that the FeatureSet has been registered correctly.
# You should also see logs from Feast Core of the ingestion job being started
grpc_cli call localhost:6565 GetFeatureSet '
  project: "your_project_name"
  name: "driver"
'
```

Or alternatively, list all feature sets

```text
grpc_cli call localhost:6565 ListFeatureSets '
  filter {
    project: "your_project_name"
    feature_set_name: "driver"
    feature_set_version: "1"
  }
'
```

When a feature set is successfully registered, Feast Core will start an **ingestion** job that listens for new features in the feature set.

{% hint style="info" %}
Note that Feast currently only supports source of type `KAFKA`, so you must have access to a running Kafka broker to register a FeatureSet successfully. It is possible to omit the `source` from a Feature Set, but Feast Core will still use Kafka behind the scenes, it is simply abstracted away from the user.
{% endhint %}

### 4.7 Ingestion and Population of Feature Values

```python
# Produce FeatureRow messages to Kafka so it will be ingested by Feast
# and written to the registered stores.
# Make sure the value here is the topic assigned to the feature set
# ... producer.send("feast-driver-features" ...)
#
# Install Python SDK to help writing FeatureRow messages to Kafka
cd $FEAST_HOMEDIR/sdk/python
pip3 install -e .
pip3 install pendulum

# Produce FeatureRow messages to Kafka so it will be ingested by Feast
# and written to the corresponding store.
# Make sure the value here is the topic assigned to the feature set
# ... producer.send("feast-test_feature_set-features" ...)
python3 - <<EOF
import logging
import pendulum
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from feast.types.FeatureRow_pb2 import FeatureRow
from feast.types.Field_pb2 import Field
from feast.types.Value_pb2 import Value, Int32List, BytesList

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

row = FeatureRow()

fields = [
    Field(name="driver_id", value=Value(int64_val=1234)),
    Field(name="city", value=Value(string_val="JAKARTA")),
]
row.fields.MergeFrom(fields)

timestamp = Timestamp()
timestamp.FromJsonString(
    pendulum.now("UTC").to_iso8601_string()
)
row.event_timestamp.CopyFrom(timestamp)

# The format is [PROJECT_NAME]/[FEATURE_NAME]
row.feature_set = "your_project_name/driver"

producer.send("your-kafka-topic", row.SerializeToString())
producer.flush()
logger.info(row)
EOF
```

### 4.8 Retrieval from Feast Serving

Ensure that Feast Serving returns results for the feature value for the specific driver

```text
grpc_cli call localhost:6566 GetOnlineFeatures '
features {
  name: "city"
  version: 1
  max_age {
    seconds: 3600
  }
}
entity_rows {
  fields {
    key: "driver_id"
    value {
      int64_val: 1234
    }
  }
}
'
```

```text
field_values {
  fields {
    key: "driver_id"
    value {
      int64_val: 1234
    }
  }
  fields {
    key: "city"
    value {
      string_val: "JAKARTA"
    }
  }
}
```

### 4.9 Summary

If you have made it to this point successfully you should have a functioning Feast deployment, at the very least using the Apache Beam DirectRunner for ingestion jobs and Redis for online serving.

It is important to note that most of the functionality demonstrated above is already available in a more abstracted form in the Python SDK \(Feast management, data ingestion, feature retrieval\) and the Java/Go SDKs \(feature retrieval\). However, it is useful to understand these internals from a development standpoint.

## 5 Appendix

### 5.1 Java / JDK Versions

Feast requires a Java 11 or greater JDK for building the project. This is checked by Maven so you'll be informed if you try to use an older version.

Leaf application modules of the build such as the Core and Serving APIs compile with [the `javac --release` flag](https://stackoverflow.com/questions/43102787/what-is-the-release-flag-in-the-java-9-compiler) set for 11, and Ingestion and shared library modules that it uses target release 8. Here's why.

While we want to take advantage of advancements in the \(long-term supported\) language and platform, and for contributors to enjoy those as well, Apache Beam forms a major part of Feast's Ingestion component and fully validating Beam on Java 11 [is an open issue](https://issues.apache.org/jira/browse/BEAM-2530). Moreover, Beam runners other than the DirectRunner may lag behind further still—Spark does not _build or run_ on Java 11 until its version 3.0 which is still in preview. Presumably Beam's SparkRunner will have to wait for Spark, and for its implementation to update to Spark 3.

To have confidence in Beam stability and our users' ability to deploy Feast on a range of Beam runners, we will continue to target Java 8 bytecode and Platform version for Feast Ingestion, until the ecosystem moves forward.

You do _not_ need a Java 8 SDK installed for development. Newer JDKs can build for the older platform, and Feast's Maven build does this automatically.

See [Feast issue \#517](https://github.com/feast-dev/feast/issues/517) for discussion.

### 5.2 IntelliJ Tips and Troubleshooting

For IntelliJ users, this section collects notes and setup recommendations for working comfortably on the Feast project, especially to coexist as peacefully as possible with the Maven build.

#### Language Level

IntelliJ uses a notion of "Language Level" to drive assistance features according to the target Java version of a project. It often infers this appropriately from a Maven import, but it's wise to check, especially for a multi-module project with differing target Java releases across modules, like ours. Language level [can be set per module](https://www.jetbrains.com/help/idea/sources-tab.html#module_language_level)—if IntelliJ is suggesting things that turn out to fail when building with `mvn`, make sure the language level of the module in question corresponds to the `<release>` set for `maven-compiler-plugin` in the module's `pom.xml` \(11 is project default if the module doesn't set one\).

Ensure that the Project _SDK_ \(not language level\) is set to a Java 11 JDK, and that all modules use the Project SDK \(see [the Dependencies tab](https://www.jetbrains.com/help/idea/dependencies.html) in the Modules view\).

