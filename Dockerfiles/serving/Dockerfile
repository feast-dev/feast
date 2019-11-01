FROM openjdk:11-jdk
ARG REVISION=dev
COPY serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar
ENTRYPOINT ["java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]
