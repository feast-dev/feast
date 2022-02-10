# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11 as builder

WORKDIR /build

COPY java/pom.xml .
COPY java/datatypes/pom.xml datatypes/pom.xml
COPY java/common/pom.xml common/pom.xml
COPY java/serving/pom.xml serving/pom.xml
COPY java/storage/api/pom.xml storage/api/pom.xml
COPY java/storage/connectors/pom.xml storage/connectors/pom.xml
COPY java/storage/connectors/redis/pom.xml storage/connectors/redis/pom.xml
COPY java/sdk/pom.xml sdk/pom.xml
COPY java/docs/coverage/pom.xml docs/coverage/pom.xml

# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3"
COPY java/pom.xml .m2/* .m2/
RUN mvn dependency:go-offline -DexcludeGroupIds:dev.feast 2>/dev/null || true

COPY java/ .
COPY protos/feast datatypes/src/main/proto/feast

ARG VERSION=dev
RUN mvn --also-make --projects serving -Drevision=$VERSION \
  -DskipUTs=true --batch-mode clean package
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

FROM amazoncorretto:11 as production
ARG VERSION=dev
COPY --from=builder /build/serving/target/feast-serving-$VERSION-jar-with-dependencies.jar /opt/feast/feast-serving.jar
COPY --from=builder /usr/bin/grpc-health-probe /usr/bin/grpc-health-probe
CMD ["java",\
     "-Xms1g",\
     "-Xmx4g",\
     "-jar",\
     "/opt/feast/feast-serving.jar"]
