# ============================================================
# Build stage 1: Builder
# ============================================================

FROM maven:3.6-jdk-11 as builder

ARG SPARK_VERSION=2.4.5
ARG HADOOP_VERSION=2.7

# Install Spark runtime
WORKDIR /

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -zxf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

RUN cd spark/jars && for jar in \
        org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.5/spark-sql-kafka-0-10_2.11-2.4.5.jar \
        org/apache/kafka/kafka-clients/2.3.0/kafka-clients-2.3.0.jar \
        ; do \
      wget https://repo1.maven.org/maven2/$jar; \
    done

RUN mv spark/conf/log4j.properties.template spark/conf/log4j.properties

ARG REVISION=dev

WORKDIR /build

COPY datatypes datatypes
COPY storage storage
COPY sdk/java sdk/java
COPY core core
COPY ingestion ingestion
COPY protos protos
COPY serving serving
COPY spark spark
COPY pom.xml pom.xml

# Trick to copy .m2 directory only if it exists.
# The LICENSE file is any file that actually exists, to make sure the command doesn't fail.
COPY LICENSE .m[2] .m2/

#
# Setting Maven repository .m2 directory relative to /build folder gives the
# user to optionally use cached repository when building the image by copying
# the existing .m2 directory to $FEAST_REPO_ROOT/.m2
#
ENV MAVEN_OPTS="-Dmaven.repo.local=/build/.m2/repository -DdependencyLocationsEnabled=false"
RUN mvn --also-make --projects spark/databricks-emulator,spark/spark-ingestion-job -Drevision=$REVISION \
  --batch-mode clean package

# ============================================================
# Build stage 2: Production
# ============================================================

FROM openjdk:8u252-jre as production
ARG REVISION=dev

ENV SPARK_HOME /spark

COPY --from=builder /spark /spark
COPY --from=builder /build/spark/databricks-emulator/target/databricks-emulator-$REVISION.jar /opt/databricks-emulator.jar
COPY --from=builder /build/spark/spark-ingestion-job/target/spark-ingestion-job-$REVISION.jar /opt/spark-ingestion-job.jar
CMD ["java",\
     "-Xms2048m",\
     "-Xmx2048m",\
     "-jar",\
     "/opt/databricks-emulator.jar"]
