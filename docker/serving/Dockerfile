FROM openjdk:8-slim

ADD serving/target/feast-serving-0.3.0.jar /usr/share/feast/feast-serving.jar

ENTRYPOINT ["/usr/bin/java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]
