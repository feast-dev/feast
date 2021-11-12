### Getting Started Guide for Feast Core Developers

Pre-requisites:
- [Maven](https://maven.apache.org/install.html) build tool version 3.6.x
- A running Postgres instance. For easier to get started, please configure the database like so
  ```
  database: postgres
  user: postgres 
  password: password
  ``` 
- A running Redis instance
  ```
  host: localhost
  port: 6379
  ```
- Access to Google Cloud BigQuery (optional)
- Access to Kafka brokers (to test starting ingestion jobs from Feast Core)

Run the following maven command to start Feast Core GRPC service running on port 6565 locally
```bash
# Using configuration from src/main/resources/application.yml
mvn spring-boot:run
# Using configuration from custom location e.g. /tmp/config.application.yml
mvn spring-boot:run -Dspring.config.location=/tmp/config.application.yml
```

If you have [grpc_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) installed, you can check that Feast Core is running
```
grpc_cli ls localhost:6565
grpc_cli call localhost:6565 GetFeastCoreVersion ""
grpc_cli call localhost:6565 ListStores ""
```