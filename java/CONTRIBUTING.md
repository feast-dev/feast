# Development Guide: feast-java
> The higher level [Development Guide](https://docs.feast.dev/v/master/project/development-guide)
> gives contributing to Feast codebase as a whole.

## Overview
This guide is targeted at developers looking to contribute to Feast components in
the feast-java Repository:
- [Feast Serving](#feast-serving)
- [Feast Serving Client](#feast-serving-client)

> Don't see the Feast component that you want to contribute to here?  
> Check out the [Development Guide](https://docs.feast.dev/v/master/project/development-guide)
> to learn how Feast components are distributed over multiple repositories.

### Repository structure
There are four key top level packages:
- `serving`: Feast Serving (a gRPC service to serve features)
- `serving-client`: Feast Serving Client (a thin Java client to communicate with Feast serving via gRPC )
- `datatypes`: A symlink to the overall project protos. These include the core serving gRPC protos, proto representations of all objects in the Feast registry.
- `coverage`: Generates JaCoCo coverage reports

#### Feast Serving
> **Note:** there are references to metrics collection in the code. These are unused and exist for legacy reasons (from when this used Spring Boot), but remain in the code until published to StatsD / Prometheus Pushgateway.

The primary entrypoint into the Feast Serving server is `ServingGuiceApplication`, which connects to the rest of the packages:
- `connectors`: Contains online store connectors (e.g. Redis)
- `exception`: Contains user-facing exceptions thrown by Feast Serving
- `registry`: Logic to parse a Feast file-based registry (in GCS, S3, or local) into the `Registry` proto object, and automatically re-sync the registry. 
- `service`: Core logic that exposes and backs the serving APIs. This includes communication with a feature transformation server to execute on demand transformations
  - The root code in this package creates the main entrypoint (`ServingServiceV2`) which is injected into `OnlineServingGrpcServiceV2` in `grpc/` implement the gRPC service.
  - `config`: Guice modules to power the server and config
    - Includes server config / guice modules in `ServerModule` 
    - Maps overall Feast Serving user configuration from Java to YAML in `ApplicationPropertiesModule` and `ApplicationProperties`
  - `controller`: server controllers (right now, only a gRPC health check)
  - `grpc`: Implementation of the gRPC serving service
  - `interceptors`: gRPC interceptors (currently used to produce metrics around each gRPC request)

### Common Setup
Common Environment Setup for all feast-java Feast components:

Ensure following development tools are installed:
- Java SE Development Kit 11 (you may need to do `export JAVA_HOME=$(/usr/libexec/java_home -v 11)`)
- Maven 3.6
- `make`

### Code Style
Feast's Java codebase conforms to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

Automatically format the code to conform the style guide by:

```sh
# formats all code in the feast-java repository
mvn spotless:apply
```

> If you're using IntelliJ, you can import these [code style settings](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml)
> if you'd like to use the IDE's reformat function.

### Project Makefile
The Project Makefile provides useful shorthands for common development tasks:

> Note: These commands rely on a local version of `feast` (Python) to be installed

Run all Unit tests:
```
make test-java
```

Run all Integration tests (note: this also runs GCS + S3 based tests which should fail):
```
make test-java-integration
```

Building Docker images for Feast Serving:
```
make build-docker REGISTRY=gcr.io/kf-feast VERSION=develop
```


### IDE Setup
If you're using IntelliJ, some additional steps may be needed to make sure IntelliJ autocomplete works as expected.
Specifically, proto-generated code is not indexed by IntelliJ. To fix this, navigate to the following window in IntelliJ:
`Project Structure > Modules > datatypes-java`, and mark the following folders as `Source` directorys:
- target/generated-sources/protobuf/grpc-java
- target/generated-sources/protobuf/java
- target/generated-sources/annotations

## Feast Serving
See instructions [here](serving/README.md) for developing.

## Feast Serving Client
### Environment Setup
Setting up your development environment:
1. Complete the feast-java [Common Setup](#common-setup)

> Feast Serving Client is a Serving Client for retrieving Features from a running Feast Serving instance.  
> See the [Feast Serving Section](#feast-serving) section for how to get a Feast Serving instance running.

### Building
1. Build / Compile Feast Java Client with Maven:

```sh
mvn package -pl sdk/java --also-make -Dmaven.test.skip=true
```

### Unit Tests
Unit Tests can be used to verify functionality:

```sh
mvn package -pl sdk/java test --also-make
```
