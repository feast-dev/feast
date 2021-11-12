# Development Guide: feast-java
> The higher level [Development Guide](https://docs.feast.dev/contributing/development-guide)
> gives contributing to Feast codebase as a whole.

### Overview
This guide is targeted at developers looking to contribute to Feast components in
the feast-java Repository:
- [Feast Core](#feast-core)
- [Feast Serving](#feast-serving)
- [Feast Java Client](#feast-java-client)

> Don't see the Feast component that you want to contribute to here?  
> Check out the [Development Guide](https://docs.feast.dev/contributing/development-guide)
> to learn how Feast components are distributed over multiple repositories.

#### Common Setup
Common Environment Setup for all feast-java Feast components:
1. . Ensure following development tools are installed:
- Java SE Development Kit 11, Maven 3.6, `make`

#### Code Style
feast-java's codebase conforms to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

Automatically format the code to conform the style guide by:

```sh
# formats all code in the feast-java repository
mvn spotless:apply
```

> If you're using IntelliJ, you can import these [code style settings](https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml)
> if you'd like to use the IDE's reformat function.

#### Project Makefile
The Project Makefile provides useful shorthands for common development tasks:


Run all Unit tests:
```
make test-java
```

Run all Integration tests:
```
make test-java-integration
```

Building Docker images for Feast Core &amp; Feast Serving:
```
make build-docker REGISTRY=gcr.io/kf-feast VERSION=develop
```


#### IDE Setup
If you're using IntelliJ, some additional steps may be needed to make sure IntelliJ autocomplete works as expected.
Specifically, proto-generated code is not indexed by IntelliJ. To fix this, navigate to the following window in IntelliJ:
`Project Structure > Modules > datatypes-java`, and mark the following folders as `Source` directorys:
- target/generated-sources/protobuf/grpc-java
- target/generated-sources/protobuf/java
- target/generated-sources/annotations


## Feast Core
### Environment Setup
Setting up your development environment for Feast Core:
1. Complete the feast-java [Common Setup](#common-setup)
2. Boot up a PostgreSQL instance (version 11 and above). Example of doing so via Docker:
```sh
# spawn a PostgreSQL instance as a Docker container running in the background
docker run \
    --rm -it -d \
    --name postgres \
    -e POSTGRES_DB=postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=password \
    -p 5432:5432 postgres:12-alpine
```

### Configuration
Feast Core is configured using it's [application.yml](https://docs.feast.dev/reference/configuration-reference#1-feast-core-and-feast-online-serving).

### Building and Running
1. Build / Compile Feast Core with Maven to produce an executable Feast Core JAR
```sh
mvn package -pl core --also-make -Dmaven.test.skip=true 
```

2. Run Feast Core using the built JAR:
```sh
# where X.X.X is the version of the Feast Core JAR built
java -jar core/target/feast-core-X.X.X-exec.jar
```

### Unit / Integration Tests
Unit &amp; Integration Tests can be used to verify functionality:
```sh
# run unit tests
mvn test -pl core --also-make
# run integration tests
mvn verify -pl core --also-make
```

## Feast Serving
### Environment Setup
Setting up your development environment for Feast Serving:
1. Complete the feast-java [Common Setup](#common-setup)
2. Boot up a Redis instance (version 5.x). Example of doing so via Docker:
```sh
docker run --name redis --rm -it -d -p 6379:6379 redis:5-alpine
```

> Feast Serving requires a running Feast Core instance to retrieve Feature metadata
> in order to serve features. See the [Feast Core section](#feast-core) for
> how to get a Feast Core instance running.  
 
### Configuration
Feast Serving is configured using it's [application.yml](https://docs.feast.dev/reference/configuration-reference#1-feast-core-and-feast-online-serving).

### Building and Running
1. Build / Compile Feast Serving with Maven to produce an executable Feast Serving JAR
```sh
mvn package -pl serving --also-make -Dmaven.test.skip=true 

2. Run Feast Serving using the built JAR:
```sh
# where X.X.X is the version of the Feast serving JAR built
java -jar serving/target/feast-serving-X.X.X-exec.jar
```

### Unit / Integration Tests
Unit &amp; Integration Tests can be used to verify functionality:
```sh
# run unit tests
mvn test -pl serving --also-make
# run integration tests
mvn verify -pl serving --also-make
```

## Feast Java Client
### Environment Setup
Setting up your development environment for Feast Java SDK:
1. Complete the feast-java [Common Setup](#common-setup)

> Feast Java Client is a Java Client for retrieving Features from a running Feast Serving instance.  
> See the [Feast Serving Section](#feast-serving) section for how to get a Feast Serving instance running.

### Configuration
Feast Java Client is [configured as code](https://docs.feast.dev/v/master/reference/configuration-reference#4-feast-java-and-go-sdk)

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
