FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY . /build
WORKDIR /build
ENV MAVEN_OPTS="-Dmaven.repo.local=.m2 -DdependencyLocationsEnabled=false"
RUN mvn --projects core,ingestion -Drevision=$REVISION -DskipTests package

FROM openjdk:8-jre as production
ARG REVISION=dev
COPY --from=builder /build/core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
COPY --from=builder /build/ingestion/target/feast-ingestion-$REVISION.jar /usr/share/feast/feast-ingestion.jar
ENV JOB_EXECUTABLE=/usr/share/feast/feast-ingestion.jar
ENTRYPOINT ["/usr/bin/java", \
"-XX:+UnlockExperimentalVMOptions", \
"-XX:+UseCGroupMemoryLimitForHeap", \
"-jar", "/usr/share/feast/feast-core.jar"]
