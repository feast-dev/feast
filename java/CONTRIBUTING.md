# Development Guide: feast-java
> The higher level [Development Guide](https://docs.feast.dev/contributing/development-guide)
> gives contributing to Feast codebase as a whole.

### Overview
This guide is targeted at developers looking to contribute to Feast components in
the feast-java Repository:
- [Feast Serving](#feast-serving)
- [Feast Java Client](#feast-java-client)

> Don't see the Feast component that you want to contribute to here?  
> Check out the [Development Guide](https://docs.feast.dev/contributing/development-guide)
> to learn how Feast components are distributed over multiple repositories.

#### Common Setup
Common Environment Setup for all feast-java Feast components:

Ensure following development tools are installed:
- Java SE Development Kit 11
- Maven 3.6
- `make`

#### Code Style
Feast's Java codebase conforms to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html).

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

## Feast Serving
See instructions [here](serving/README.md) for developing.

## Feast Java Client
### Environment Setup
Setting up your development environment for Feast Java SDK:
1. Complete the feast-java [Common Setup](#common-setup)

> Feast Java Client is a Java Client for retrieving Features from a running Feast Serving instance.  
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
