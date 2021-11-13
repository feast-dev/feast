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
  <version>0.26.2</version>
</dependency>
```

Use the version corresponding to the Feast release you have deployed in your
environment—see the [Feast release notes] for details.

[Feast release notes]: ../../CHANGELOG.md

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

Releases
--------

The module is published to Maven Central upon each release of Feast (since
v0.3.7).

For developers, the publishing process is automated along with the Java SDK by
[the `publish-java-sdk` build task in Prow][prow task], where you can see how
it works. Artifacts are staged to Sonatype where a maintainer needs to take a
release action for them to go live on Maven Central.

[prow task]: https://github.com/feast-dev/feast/blob/17e7dca8238aae4dcbf0ff9f0db5d80ef8e035cf/.prow/config.yaml#L166-L192
