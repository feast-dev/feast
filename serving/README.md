### Getting Started Guide for Feast Serving Developers

Pre-requisites:
- [Maven](https://maven.apache.org/install.html) build tool version 3.6.x
- A running Feast Core instance
- A running Store instance e.g. local Redis Store instance

Run the following maven command to start Feast Serving GRPC service running on port 6566 locally
```bash
# Assumptions: 
# - Local Feast Core is running on localhost:6565
# - A store named "SERVING" has been registered in Feast
mvn spring-boot:run -Dspring-boot.run.arguments=\
--feast.store-name=SERVING,\
--feast.core-host=localhost,\
--feast.core-port=6565
```

If you have [grpc_cli](https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md) installed, you can check that Feast Serving is running
```
grpc_cli ls localhost:6566
grpc_cli call localhost:6566 GetFeastServingVersion ''
grpc_cli call localhost:6566 GetFeastServingType ''
```