Feast Data Types for Java
=========================

This module produces Java class files for Feast's data type and gRPC service
definitions, from Protobuf IDL. These are used across Feast components for wire
interchange, contracts, etc.

End users of Feast will be best served by our Java SDK which adds higher-level
conveniences, but the data types are published independently for custom needs,
without any additional dependencies the SDK may add.

Dependency Coordinates
----------------------

```xml
<dependency>
  <groupId>dev.feast</groupId>
  <artifactId>datatypes-java</artifactId>
  <version>0.3.6-SNAPSHOT</version>
</dependency>
```

Using the `.proto` Definitions
------------------------------

The `.proto` definitions are packaged as resources within the Maven artifact,
which may be useful to `include` them in dependent Protobuf definitions in a
downstream project, or for other JVM languages to consume from their builds to
generate more idiomatic bindings.

Google's Gradle plugin, for instance, [can use protos in dependencies][Gradle]
either for `include` or to compile with a different `protoc` plugin than Java.

[sbt-protoc] offers similar functionality for sbt/Scala.

[Gradle]: https://github.com/google/protobuf-gradle-plugin#protos-in-dependencies
[sbt-protoc]: https://github.com/thesamet/sbt-protoc

Publishing
----------

TODO: this module should be published to Maven Central upon Feast releases—this
needs to be set up in POM configuration and release automation.
