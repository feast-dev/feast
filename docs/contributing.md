# Contributing

## Getting Started

The following guide will help you quickly run Feast in your local machine.

The main components of Feast are:

* **Feast Core** handles FeatureSpec registration, starts and monitors Ingestion 

  jobs and ensures that Feast internal metadata is consistent.

* **Feast Ingestion** subscribes to streams of FeatureRow and writes the feature

  values to registered Stores. 

* **Feast Serving** handles requests for features values retrieval from the end users.

**Pre-requisites**

* Java SDK version 8
* Python version 3.6 \(or above\) and pip
* Access to Postgres database \(version 11 and above\)
* Access to [Redis](https://redis.io/topics/quickstart) instance \(tested on version 5.x\)
* Access to [Kafka](https://kafka.apache.org/) brokers \(tested on version 2.x\)
* [Maven ](https://maven.apache.org/install.html) version 3.6.x
* [grpc\_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) is useful for debugging and quick testing
* An overview of Feast specifications and protos

> **Assumptions:**
>
> 1. Postgres is running in "localhost:5432" and has a database called "postgres" which 
>
>    can be accessed with credentials user "postgres" and password "password". 
>
>    To use different database name and credentials, please update 
>
>    "$FEAST\_HOME/core/src/main/resources/application.yml" 
>
>    or set these environment variables: DB\_HOST, DB\_USERNAME, DB\_PASSWORD.
>
> 2. Redis is running locally and accessible from "localhost:6379"
> 3. Feast has admin access to BigQuery.

```text
# $FEAST_HOME will refer to be the root directory of this Feast Git repository

git clone https://github.com/gojek/feast
cd feast
```

#### Starting Feast Core

```text
# Please check the default configuration for Feast Core in 
# "$FEAST_HOME/core/src/main/resources/application.yml" and update it accordingly.
# 
# Start Feast Core GRPC server on localhost:6565
mvn --projects core spring-boot:run

# If Feast Core starts successfully, verify the correct Stores are registered
# correctly, for example by using grpc_cli.
grpc_cli call localhost:6565 GetStores ''

# Should return something similar to the following.
# Note that you should change BigQuery projectId and datasetId accordingly
# in "$FEAST_HOME/core/src/main/resources/application.yml"

store {
  name: "SERVING"
  type: REDIS
  subscriptions {
    project: "*"
    name: "*"
    version: "*"
  }
  redis_config {
    host: "localhost"
    port: 6379
  }
}
store {
  name: "WAREHOUSE"
  type: BIGQUERY
  subscriptions {
    project: "*"
    name: "*"
    version: "*"
  }
  bigquery_config {
    project_id: "my-google-project-id"
    dataset_id: "my-bigquery-dataset-id"
  }
}
```

#### Starting Feast Serving

Feast Serving requires administrators to provide an **existing** store name in Feast. An instance of Feast Serving can only retrieve features from a **single** store.

> In order to retrieve features from multiple stores you must start **multiple** instances of Feast serving. If you start multiple Feast serving on a single host, make sure that they are listening on different ports.

```text
# Start Feast Serving GRPC server on localhost:6566 with store name "SERVING"
mvn --projects serving spring-boot:run -Dspring-boot.run.arguments='--feast.store-name=SERVING'

# To verify Feast Serving starts successfully
grpc_cli call localhost:6566 GetFeastServingType ''

# Should return something similar to the following.
type: FEAST_SERVING_TYPE_ONLINE
```

#### Registering a FeatureSet

Create a new FeatureSet on Feast by sending a request to Feast Core. When a feature set is successfully registered, Feast Core will start an **ingestion** job that listens for new features in the FeatureSet. Note that Feast currently only supports source of type "KAFKA", so you must have access to a running Kafka broker to register a FeatureSet successfully.

```text
# Example of registering a new driver feature set 
# Note the source value, it assumes that you have access to a Kafka broker
# running on localhost:9092

grpc_cli call localhost:6565 ApplyFeatureSet '
feature_set {
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
    }
  }
}
'

# To check that the FeatureSet has been registered correctly.
# You should also see logs from Feast Core of the ingestion job being started
grpc_cli call localhost:6565 GetFeatureSets ''
```

#### Ingestion and Population of Feature Values

```text
# Produce FeatureRow messages to Kafka so it will be ingested by Feast
# and written to the registered stores.
# Make sure the value here is the topic assigned to the feature set
# ... producer.send("feast-driver-features" ...)
# 
# Install Python SDK to help writing FeatureRow messages to Kafka
cd $FEAST_HOME/sdk/python
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

# The format is [FEATURE_NAME]:[VERSION]
row.feature_set = "driver:1"

producer.send("feast-driver-features", row.SerializeToString())
producer.flush()
logger.info(row)
EOF

# Check that the ingested feature rows can be retrieved from Feast serving
grpc_cli call localhost:6566 GetOnlineFeatures '
feature_sets {
  name: "driver"
  version: 1
}
entity_dataset {
  entity_names: "driver_id"
  entity_dataset_rows {
    entity_ids {
      int64_val: 1234
    }
  }
}
'
```

## Development

Notes:

* Use of Lombok is being phased out, prefer to use [Google Auto](https://github.com/google/auto) in new code.

### Running Unit Tests

```text
$ mvn test
```

### Running Integration Tests

_Note: integration suite isn't yet separated from unit._

```text
$ mvn verify
```

### Running Components Locally

The `core` and `serving` modules are Spring Boot applications. These may be run as usual for [the Spring Boot Maven plugin](https://docs.spring.io/spring-boot/docs/current/maven-plugin/index.html):

```text
$ mvn --projects core spring-boot:run

# Or for short:
$ mvn -pl core spring-boot:run
```

Note that you should execute `mvn` from the Feast repository root directory, as there are intermodule dependencies that Maven will not resolve if you `cd` to subdirectories to run.

#### Running From IntelliJ

Compiling and running tests in IntelliJ should work as usual.

Running the Spring Boot apps may work out of the box in IDEA Ultimate, which has built-in support for Spring Boot projects, but the Community Edition needs a bit of help:

The Spring Boot Maven plugin automatically puts dependencies with `provided` scope on the runtime classpath when using `spring-boot:run`, such as its embedded Tomcat server. The "Play" buttons in the gutter or right-click menu of a `main()` method [do not do this](https://stackoverflow.com/questions/30237768/run-spring-boots-main-using-ide).

A solution to this is:

1. Open `View > Tool Windows > Maven`
2. Drill down to e.g. `Feast Core > Plugins > spring-boot:run`, right-click and `Create 'feast-core [spring-boot'…`
3. In the dialog that pops up, check the `Resolve Workspace artifacts` box
4. Click `OK`. You should now be able to select this run configuration for the Play button in the main toolbar, keyboard shortcuts, etc.

#### Tips for Running Postgres, Redis and Kafka with Docker

This guide assumes you are running Docker service on a bridge network \(which is usually the case if you're running Linux\). Otherwise, you may need to use different network options than shown below.

> `--net host` usually only works as expected when you're running Docker service in bridge networking mode.

```text
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

## Code reviews

Code submission to Feast \(including submission from project maintainers\) requires review and approval. Please submit a **pull request** to initiate the code review process. We use [prow](https://github.com/kubernetes/test-infra/tree/master/prow) to manage the testing and reviewing of pull requests. Please refer to [config.yaml](../.prow/config.yaml) for details on the test jobs.

## Code conventions

### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit:

```text
$ mvn spotless:apply
```

Formatting will be checked automatically during the `verify` phase. This can be skipped temporarily:

```text
$ mvn spotless:check  # Check is automatic upon `mvn verify`
$ mvn verify -Dspotless.check.skip
```

If you're using IntelliJ, you can import [these code style settings](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml) if you'd like to use the IDE's reformat function as you work.

### Go

Make sure you apply `go fmt`.

## Release process

Feast uses [semantic versioning](https://semver.org/).

* Major and minor releases are cut from the `master` branch.
* Whenever a major or minor release is cut, a branch is created for that release. This is called a "release branch". For example if `0.3` is released from `master`, a branch named `v0.3-branch` is created.
* You can create a release branch via the GitHub UI.
* From this branch a git tag is created for the specific release, for example `v0.3.0`.
* Tagging a release will automatically build and push the relevant artifacts to their repositories or package managers \(docker images, Python wheels, etc\).
* A release branch should be substantially _feature complete_ with respect to the intended release. Code that is committed to `master` may be merged or cherry-picked on to a release branch, but code that is directly committed to the release branch should be solely applicable to that release \(and should not be committed back to master\).
* In general, unless you're committing code that only applies to the release stream \(for example, temporary hotfixes, backported security fixes, or image hashes\), you should commit to `master` and then merge or cherry-pick to the release branch.
* It is also important to update the [CHANGELOG.md](https://github.com/gojek/feast/blob/master/CHANGELOG.md) when submitting a new release. This can be in the same PR or a separate PR.
* Finally it is also important to create a [GitHub release](https://github.com/gojek/feast/releases) which includes a summary of important changes as well as any artifacts associated with that release.

