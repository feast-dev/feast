#FROM maven:3.6-jdk-8-slim as builder
#ARG REVISION=dev
#
#WORKDIR /build
#COPY pom.xml .
#COPY core/pom.xml core/pom.xml
#COPY ingestion/pom.xml ingestion/pom.xml
#COPY serving/pom.xml serving/pom.xml
#
#RUN mvn dependency:go-offline -B
#
#COPY . /build
#WORKDIR /build
#RUN mvn --projects core,ingestion -Drevision=$REVISION -DskipTests=true --batch-mode package

FROM openjdk:8-jre as production
ARG REVISION=dev
COPY ./core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
CMD ["java", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "/usr/share/feast/feast-core.jar"]