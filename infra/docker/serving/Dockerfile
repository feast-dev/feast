# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11 as builder
ARG REVISION=dev
COPY  . /build
WORKDIR /build
#
# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
#
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --also-make --projects serving -Drevision=$REVISION \
  -DskipTests=true --batch-mode clean package
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

FROM openjdk:11-jre-slim as production
ARG REVISION=dev
COPY --from=builder /build/serving/target/feast-serving-$REVISION.jar /opt/feast/feast-serving.jar
COPY --from=builder /usr/bin/grpc-health-probe /usr/bin/grpc-health-probe
CMD ["java",\
     "-Xms1024m",\
     "-Xmx1024m",\
     "-jar",\
     "/opt/feast/feast-serving.jar"]
