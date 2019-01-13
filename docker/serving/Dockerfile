FROM openjdk:8-slim
ARG RELEASE

ADD serving/target/feast-serving-$RELEASE.jar /usr/share/feast/feast-serving.jar

ENTRYPOINT ["/usr/bin/java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]
