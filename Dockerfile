FROM openjdk:8-jre-alpine
ARG REVISION=0.3.0-dev
COPY ./serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar
ENTRYPOINT ["java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]