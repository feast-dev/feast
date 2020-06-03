### Getting Started Guide for Feast Serving Developers

Pre-requisites:

- [Maven](https://maven.apache.org/install.html) build tool version 3.6.x
- A running Feast Core instance
- A running Store instance e.g. local Redis Store instance

From the Feast project root directory, run the following Maven command to start Feast Serving gRPC service running on port 6566 locally:

```bash
# Assumptions: 
# - Local Feast Core is running on localhost:6565
mvn -pl serving spring-boot:run -Dspring-boot.run.arguments=\
--feast.store.config-path=./sample_redis_config.yml,\
--feast.core-host=localhost,\
--feast.core-port=6565
```

If you have [grpc_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) installed, you can check that Feast Serving is running
```
grpc_cli ls localhost:6566
grpc_cli call localhost:6566 GetFeastServingVersion ''
grpc_cli call localhost:6566 GetFeastServingType ''
```

```bash
grpc_cli call localhost:6565 ApplyFeatureSet '
feature_set {
  name: "driver"
  entities {
    name: "driver_id"
    value_type: STRING
  }
  features {
    name: "city"
    value_type: STRING
  }
  features {
    name: "booking_completed_count"
    value_type: INT64
  }
  source {
    type: KAFKA
    kafka_source_config {
      bootstrap_servers: "localhost:9092"
    }
  }
}
'

grpc_cli call localhost:6565 GetFeatureSets '
filter {
  feature_set_name: "driver"
}
'

grpc_cli call localhost:6566 GetBatchFeatures '
feature_sets {
  name: "driver"
  feature_names: "booking_completed_count"
  max_age {
    seconds: 86400
  }
}
entity_dataset {
  entity_names: "driver_id"
  entity_dataset_rows {
    entity_timestamp {
      seconds: 1569873954
    }
  }
}
'
```

```
python3 <<EOF
import pandas as pd
import fastavro

with open("/tmp/000000000000.avro", "rb") as f:
    reader = fastavro.reader(f)
    records = [r for r in reader]
    df = pd.DataFrame.from_records(records)
    print(df.columns)
    print(df.shape)
    print(df.head(5))
EOF
```
