#FROM maven:3.6-jdk-8-slim as builder
#ARG REVISION=dev
#COPY . /build
#WORKDIR /build
#ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
#RUN mvn --projects serving -Drevision=$REVISION -DskipTests=true --batch-mode package

FROM openjdk:8-jre-alpine as production
ARG REVISION=dev

RUN GRPC_HEALTH_PROBE_VERSION=v0.1.0-alpha.1 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

COPY ./serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar
CMD ["java", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "/usr/share/feast/feast-serving.jar"]
