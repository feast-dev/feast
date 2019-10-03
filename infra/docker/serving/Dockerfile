FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY . /build 
WORKDIR /build
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --projects serving -Drevision=$REVISION -DskipTests=true --batch-mode package

FROM openjdk:8-jre-alpine as production
ARG REVISION=dev
COPY --from=builder /build/serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar
ENTRYPOINT ["java", \
      "-XX:+UseG1GC", \
      "-XX:+UseStringDeduplication", \
      "-XX:+UnlockExperimentalVMOptions", \
      "-XX:+UseCGroupMemoryLimitForHeap", \
      "-jar", "/usr/share/feast/feast-serving.jar"]
