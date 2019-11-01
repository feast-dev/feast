FROM openjdk:11-jdk
ARG REVISION=dev
COPY core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
COPY ingestion/target/feast-ingestion-$REVISION.jar /usr/share/feast/feast-ingestion.jar
ENV JOB_EXECUTABLE=/usr/share/feast/feast-ingestion.jar
ENTRYPOINT ["java", \
"-XX:+UnlockExperimentalVMOptions", \
"-XX:+UseCGroupMemoryLimitForHeap", \
"-jar", "/usr/share/feast/feast-core.jar"]
