FROM openjdk:11-jdk

ARG REVISION=dev

COPY serving/target/feast-serving-$REVISION.jar /usr/share/feast/feast-serving.jar

ENV JAVA_OPTS=-XX:+UnlockExperimentalVMOptions

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /usr/share/feast/feast-serving.jar"]
