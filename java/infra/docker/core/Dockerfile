# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11 as builder

WORKDIR /build

COPY pom.xml .
COPY datatypes/java/pom.xml datatypes/java/pom.xml
COPY common/pom.xml common/pom.xml
COPY core/pom.xml core/pom.xml
COPY serving/pom.xml serving/pom.xml
COPY storage/api/pom.xml storage/api/pom.xml
COPY storage/connectors/pom.xml storage/connectors/pom.xml
COPY storage/connectors/redis/pom.xml storage/connectors/redis/pom.xml
COPY sdk/java/pom.xml sdk/java/pom.xml
COPY docs/coverage/java/pom.xml docs/coverage/java/pom.xml
COPY deps/feast/protos/ deps/feast/protos/

# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=25 -Dmaven.wagon.http.retryHandler.count=3"
COPY pom.xml .m2/* .m2/
RUN mvn dependency:go-offline -DexcludeGroupIds:dev.feast 2>/dev/null || true

COPY . .

ARG VERSION=dev
RUN mvn --also-make --projects core -Drevision=$VERSION \
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

FROM openjdk:11-jre as production
ARG VERSION=dev

COPY --from=builder /build/core/target/feast-core-$VERSION-exec.jar /opt/feast/feast-core.jar
COPY --from=builder /usr/bin/grpc-health-probe /usr/bin/grpc-health-probe

CMD ["java",\
     "-Xms2048m",\
     "-Xmx2048m",\
     "-jar",\
     "/opt/feast/feast-core.jar"]
