# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11-slim as builder
ARG REVISION=dev
COPY  . /build
WORKDIR /build
#
# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
#
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --also-make --projects core,ingestion -Drevision=$REVISION \
  -DskipTests=true --batch-mode clean package
#
# Unpack the jar and copy the files into production Docker image
# for faster startup time when starting Dataflow jobs from Feast Core.
# This is because we need to stage the classes and dependencies when using Dataflow.
# The final size of the production image will be bigger but it seems
# a good tradeoff between speed and size.
#
# https://github.com/gojek/feast/pull/291
RUN apt-get -qq update && apt-get -y install unar && \
  unar /build/core/target/feast-core-$REVISION.jar -o /build/core/target/

#
# Download grpc_health_probe to run health check for Feast Serving
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
#
RUN wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 \
         -O /usr/bin/grpc-health-probe && \
    chmod +x /usr/bin/grpc-health-probe

# ============================================================
# Build stage 2: Production
# ============================================================

FROM openjdk:11-jre as production
ARG REVISION=dev

COPY --from=builder /build/core/target/feast-core-$REVISION.jar /opt/feast/feast-core.jar
# Required for staging jar dependencies when submitting Dataflow jobs.
COPY --from=builder /build/core/target/feast-core-$REVISION /opt/feast/feast-core
COPY --from=builder /usr/bin/grpc-health-probe /usr/bin/grpc-health-probe

CMD ["java",\
     "-Xms2048m",\
     "-Xmx2048m",\
     "-jar",\
     "/opt/feast/feast-core.jar"]
