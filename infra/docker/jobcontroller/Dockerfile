# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11 as builder

WORKDIR /build

COPY pom.xml .
COPY datatypes/java/pom.xml datatypes/java/pom.xml
COPY common/pom.xml common/pom.xml
COPY auth/pom.xml auth/pom.xml
COPY ingestion/pom.xml ingestion/pom.xml
COPY core/pom.xml core/pom.xml
COPY serving/pom.xml serving/pom.xml
COPY storage/api/pom.xml storage/api/pom.xml
COPY storage/connectors/pom.xml storage/connectors/pom.xml
COPY storage/connectors/redis/pom.xml storage/connectors/redis/pom.xml
COPY storage/connectors/bigquery/pom.xml storage/connectors/bigquery/pom.xml
COPY sdk/java/pom.xml sdk/java/pom.xml
COPY job-controller/pom.xml job-controller/pom.xml
COPY docs/coverage/java/pom.xml docs/coverage/java/pom.xml
COPY protos/ protos/

# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
COPY pom.xml .m2/* .m2/
RUN mvn dependency:go-offline -DexcludeGroupIds:dev.feast 2>/dev/null || true

COPY . .

ARG REVISION=dev
RUN mvn --also-make --projects job-controller,ingestion -Drevision=$REVISION \
  -DskipUTs=true --batch-mode clean package
#
# Unpack the jar and copy the files into production Docker image
# for faster startup time when starting Dataflow jobs from Feast Job Controller.
# This is because we need to stage the classes and dependencies when using Dataflow.
# The final size of the production image will be bigger but it seems
# a good tradeoff between speed and size.
#
# https://github.com/feast-dev/feast/pull/291
RUN apt-get -qq update && apt-get -y install unar && \
  unar /build/job-controller/target/feast-job-controller-$REVISION-exec.jar -o /build/job-controller/target/

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

COPY --from=builder /build/job-controller/target/feast-job-controller-$REVISION-exec.jar /opt/feast/feast-job-controller.jar
# Required for staging jar dependencies when submitting Dataflow jobs.
COPY --from=builder /build/job-controller/target/feast-job-controller-$REVISION-exec /opt/feast/feast-job-controller
COPY --from=builder /usr/bin/grpc-health-probe /usr/bin/grpc-health-probe

CMD ["java",\
     "-Xms2048m",\
     "-Xmx2048m",\
     "-jar",\
     "/opt/feast/feast-job-controller.jar"]
