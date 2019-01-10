FROM openjdk:8-slim
ARG RELEASE

RUN mkdir -p /usr/share/feast/ingestion

ADD core/target/feast-core-$RELEASE.jar /usr/share/feast/feast-core.jar
ADD ingestion/target/feast-ingestion-$RELEASE.jar /usr/share/feast/ingestion/feast-ingestion.jar
ADD ingestion/dependency-reduced-pom.xml /usr/share/feast/ingestion/pom.xml

ENV JOB_EXECUTABLE=/usr/share/feast/ingestion/feast-ingestion.jar

ENTRYPOINT ["/usr/bin/java", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "/usr/share/feast/feast-core.jar"]
