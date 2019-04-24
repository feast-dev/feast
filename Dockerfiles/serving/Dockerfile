FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY . /build 
WORKDIR /build
ENV MAVEN_OPTS="-Dmaven.repo.local=.m2 -DdependencyLocationsEnabled=false"
RUN mvn --projects serving -Drevision=$REVISION -DskipTests package

FROM openjdk:8-jre-alpine as production
ARG REVISION=dev
COPY --from=builder /build/serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar
ENTRYPOINT ["/usr/bin/java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]
