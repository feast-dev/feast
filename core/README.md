### Getting Started for Feast Core Developers

Pre-requisites:
- A running Postgres instance. For easier to get started, please configure the database like so:
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

Run the following maven command to start Feast core GRPC service running on port 6565 locally:
```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--feast.jobs.workspace=/tmp/feast-jobs-workspace
```

If you have [grpc_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) installed, you can test that Feast Core is started properly:
```
grpc_cli ls localhost:6565
grpc_cli call localhost:6565 GetFeastCoreVersion ""
grpc_cli call localhost:6565 GetStores ""
```