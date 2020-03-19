FROM openjdk:11-jre

ARG REVISION=dev
COPY core/target/feast-core-$REVISION.jar /usr/share/feast/feast-core.jar
RUN unzip -d /usr/share/feast/feast-core /usr/share/feast/feast-core.jar

ENV JAVA_OPTS=-XX:+UnlockExperimentalVMOptions

ENTRYPOINT java $JAVA_OPTS -jar /usr/share/feast/feast-core.jar
