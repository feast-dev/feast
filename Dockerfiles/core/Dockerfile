FROM openjdk:11-jdk

ARG REVISION=dev
COPY core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
COPY ingestion/target/feast-ingestion-$REVISION.jar /usr/share/feast/feast-ingestion.jar

ENV JAVA_OPTS=-XX:+UnlockExperimentalVMOptions
ENV JOB_EXECUTABLE=/usr/share/feast/feast-ingestion.jar

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /usr/share/feast/feast-core.jar"]
