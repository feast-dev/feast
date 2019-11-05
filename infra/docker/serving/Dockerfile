FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY  . /build
WORKDIR /build
# Setting Maven repository .m2 directory relative to /build folder gives the 
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --also-make --projects serving -Drevision=$REVISION \
  -DskipTests=true --batch-mode package

FROM openjdk:8-jre-alpine as production
ARG REVISION=dev
# Download grpc_health_probe to run health check for Feast Serving 
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
RUN wget -q https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 \
         -O /usr/bin/grpc-health-probe && \
    chmod +x /usr/bin/grpc-health-probe
COPY --from=builder /build/serving/target/feast-serving-$REVISION.jar /opt/feast/feast-serving.jar
CMD ["java",\
     "-Xms1024m",\
     "-Xmx1024m",\
     "-jar",\
     "/opt/feast/feast-serving.jar"]
