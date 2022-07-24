## Getting Started Guide for Feast Serving Developers

### Overview
This guide is targeted at developers looking to contribute to Feast Serving:
- [Building and running Feast Serving locally](#building-and-running-feast-serving-locally)

### Pre-requisites:

- [Maven](https://maven.apache.org/install.html) build tool version 3.6.x
- A Feast feature repo (e.g. https://github.com/feast-dev/feast-demo)
- A running Store instance e.g. local Redis instance with `redis-server`

### Building and running Feast Serving locally: 
From the Feast GitHub root, run:

1. `mvn -f java/pom.xml install -Dmaven.test.skip=true`
2. Package an executable jar for serving: `mvn -f java/serving/pom.xml package -Dmaven.test.skip=true`
3. Make a file called `application-override.yaml` that specifies your Feast repo project and registry path:
   1. Note if you have a remote registry, you can specify that too (e.g. `gs://...`)
   ```yaml
    feast:
      project: feast_demo
      registry: /Users/[your username]/GitHub/feast-demo/feature_repo/data/registry.db
    ```
   2. An example of if you're using Redis with a remote registry:
      ```yaml
      feast:
        project: feast_java_demo
        registry: gs://[YOUR BUCKET]/demo-repo/registry.db
        activeStore: online
        stores:
        - name: online
          type: REDIS
          config:
            host: localhost
            port: 6379
            password: [YOUR PASSWORD]
      ```
4. Run the jar with dependencies that was built from Maven (note the version might vary):
   ```
   java \
     -Xms1g \
     -Xmx4g \
     -jar java/serving/target/feast-serving-0.17.1-SNAPSHOT-jar-with-dependencies.jar \
     classpath:/application.yml,file:./application-override.yaml
   ```
5. Now you have a Feast Serving gRPC service running on port 6566 locally!

### Running test queries
If you have [grpc_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) installed, you can check that Feast Serving is running
```
grpc_cli ls localhost:6566
```

An example of fetching features
```bash
grpc_cli call localhost:6566 GetOnlineFeatures '
features {
  val: "driver_hourly_stats:conv_rate"
  val: "driver_hourly_stats:acc_rate"
}
entities {
  key: "driver_id"
  value {
    val {
      int64_val: 1001
    }
    val {
      int64_val: 1002
    }
  }
}
'
```
Example output:
```
connecting to localhost:6566
metadata {
  feature_names {
    val: "driver_hourly_stats:conv_rate"
    val: "driver_hourly_stats:acc_rate"
  }
}
results {
  values {
    float_val: 0.812357187
  }
  values {
    float_val: 0.379484832
  }
  statuses: PRESENT
  statuses: PRESENT
  event_timestamps {
    seconds: 1631725200
  }
  event_timestamps {
    seconds: 1631725200
  }
}
results {
  values {
    float_val: 0.840873241
  }
  values {
    float_val: 0.151376978
  }
  statuses: PRESENT
  statuses: PRESENT
  event_timestamps {
    seconds: 1631725200
  }
  event_timestamps {
    seconds: 1631725200
  }
}
Rpc succeeded with OK status
```

### Debugging Feast Serving
You can debug this like any other Java executable. Swap the java command above with:
```
   java \
     -Xdebug \
     -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=y \
     -Xms1g \
     -Xmx4g \
     -jar java/serving/target/feast-serving-0.17.1-SNAPSHOT-jar-with-dependencies.jar \
     classpath:/application.yml,file:./application-override.yaml
   ```
Now you can attach e.g. a Remote debugger in IntelliJ to port 5005 to debug / make breakpoints.

### Unit / Integration Tests
Unit &amp; Integration Tests can be used to verify functionality:
```sh
# run unit tests
mvn test -pl serving --also-make
# run integration tests
mvn verify -pl serving --also-make
```