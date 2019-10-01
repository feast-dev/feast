FROM maven:3.6-jdk-8-slim as builder
ARG REVISION=dev
COPY . /build
WORKDIR /build
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --projects core,ingestion -Drevision=$REVISION -DskipTests=true --batch-mode package

FROM openjdk:8-jre as production
ARG REVISION=dev
COPY --from=builder /build/core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
COPY --from=builder /build/infra/Dockerfiles/core/bootstrap.sh /usr/share/feast/bootstrap.sh
ENTRYPOINT ["/usr/share/feast/boostrap.sh"]
