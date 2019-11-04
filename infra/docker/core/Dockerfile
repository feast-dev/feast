FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY  . /build
WORKDIR /build
# Setting Maven repository .m2 directory relative to /build folder gives the 
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --also-make --projects core,ingestion -Drevision=$REVISION \
  -DskipTests=true --batch-mode package

FROM openjdk:8-jre as production
ARG REVISION=dev
COPY --from=builder /build/core/target/feast-core-$REVISION.jar /opt/feast/feast-core.jar
CMD ["java",\
     "-Xms1024m",\
     "-Xmx1024m",\
     "-jar",\
     "/opt/feast/feast-core.jar"]
